#![allow(dead_code)]
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;
use std::time::Duration;
use async_std::task;
use base64::Engine;
use clap::{Arg, ArgAction, Command, value_parser, ArgMatches};
use inquire::validator::{ErrorMessage, Validation};
use dmc_miner_contracts::{ContractClient, ContractClientConfig, SectorOccupyOptions, ContractFilter, ContractState};
use dmc_miner_sectors_client::{SectorClient, SectorClientConfig};
use log::{error, info, Level};
use dmc_miner_account_client::{AccountClient, AccountClientConfig, CreateSectorOptions, SectorFilter, SectorState, RemoveSectorOptions};
use dmc_spv::{SpvClient, SpvClientConfig};
use dmc_tools_common::{DmcBillOptions, DmcError, DmcErrorCode, DmcResult, dmc_err};
use dmc_miner_lite_server::ServerConfig;
use url::{Url};
use dmc_eos::key::EosPrivateKey;
use serde::Deserialize;

struct CommandEnv {
    sector_endpoint: Url,
    account_endpoint: Url,
    contract_endpoint: Url,
    spv_endpoint: Url,
}

async fn check_lite_server(account_endpoint: Url) -> DmcResult<()> {
    println!("checking lite server...");
    let account_client = AccountClient::new(AccountClientConfig { endpoint: account_endpoint }).unwrap();
    match account_client.name().await {
        Ok(addr) => {
            println!("lite server started, account name {}", addr);
            Ok(())
        },
        Err(e) => {
            println!("ERROR: lite user server err {}", e);
            Err(e)
        }
    }
}

fn save_config(config: &ServerConfig, path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, toml::to_string(config).unwrap()).unwrap();
}

async fn open_mine_active() -> bool {
    #[derive(Deserialize)]
    struct ActiveResult {
        err: u16,
        msg: String,
        result: bool
    }
    match surf::get("https://open.dmctech.io/api/mine/is_active").await {
        Ok(mut resp) => {
            if resp.status().is_success() {
                let ret  = resp.body_json::<ActiveResult>().await.unwrap();
                ret.result
            } else {
                false
            }
        }
        Err(e) => {
            println!("check open mine active err {}", e);
            false
        }
    }
}

#[tokio::main]
async fn main() -> DmcResult<()> {
    simple_logger::init_with_level(Level::Info).unwrap();
    let app = Command::new("dmc-miner-client")
        .arg(Arg::new("sector_service_endpoint").long("sector-endpoint")
            .env("DMC_MINER_SECTOR_ENDPOINT")
            .value_parser(value_parser!(Url))
            .default_value("http://127.0.0.1:4451"))
        .arg(Arg::new("account_service_endpoint").long("account-endpoint")
            .env("DMC_MINER_ACCOUNT_ENDPOINT")
            .value_parser(value_parser!(Url))
            .default_value("http://127.0.0.1:4448"))
        .arg(Arg::new("spv_host").long("spv-endpoint")
            .env("DMC_MINER_SPV_ENDPOINT")
            .value_parser(value_parser!(Url))
            .default_value("https://dmc.dmc.tbudr.top/spv"))
        .arg(Arg::new("contract_service_endpoint").long("contract-endpoint")
            .env("DMC_MINER_CONTRACT_ENDPOINT")
            .value_parser(value_parser!(Url))
            .default_value("http://127.0.0.1:4450"))
        .subcommand(Command::new("createbill")
            .arg(Arg::new("local_path").long("path").short('l')
                .required(true)
                .value_parser(value_parser!(PathBuf))
                .help("sector path"))
            .arg(Arg::new("chunk_size").long("chunk-size").short('s')
                .value_parser(value_parser!(usize))
                .default_value((10).to_string())
                .help("chunk size, MB"))
            .arg(Arg::new("capacity").long("capacity").short('c')
                .required(true)
                .value_parser(value_parser!(u64))
                .help("bill's capacity, PST. 1 PST=1 GB"))
            .arg(Arg::new("duration").long("duration").short('d')
                .default_value("100")
                .value_parser(value_parser!(u32))
                .help("bill duration, week."))
            .arg(Arg::new("price").long("price").short('p')
                .default_value("0.011")
                .value_parser(value_parser!(f32))
                .help("price pre PST, unit DMC, 4 digits precision"))
            .arg(Arg::new("node_id").long("node").short('n')
                .value_parser(value_parser!(u32))
                .action(ArgAction::Set)
                .help("node id. required when use node gateway"))
            .arg(Arg::new("pledge_rate").long("pledge-rate").short('r').value_parser(value_parser!(u32))
                .action(ArgAction::Set)
                .help("if set, spend DMC to mint PST first. otherwise spend existed PST directly. treat capacity as PST amount"))
        )
        .subcommand(Command::new("cancelbill")
            .arg(Arg::new("id").index(1)
                .required(true)
                .value_parser(value_parser!(u64))
                .help("bill-id or sector-id with options -s"))
            .arg(Arg::new("is_sector").long("sector").short('s')
                .action(ArgAction::SetTrue)
                .help("if set, will cancel bill by sector-id, otherwise, cancel it by bill-id"))
        )
        .subcommand(Command::new("report").arg(Arg::new("bill_id").index(1).required(true).value_parser(value_parser!(u64))))
        .subcommand(Command::new("setmethod")
            .arg(Arg::new("method").index(1).required(true))
        )
        .subcommand(Command::new("method"))
        .subcommand(Command::new("list")
            .subcommand(Command::new("sector")
                .arg(Arg::new("id").index(1)
                    .value_parser(value_parser!(u64))
                    .help("query by sector-id or bill-id, query all if not set."))
                .arg(Arg::new("is_bill").long("bill").short('b')
                    .action(ArgAction::SetTrue)
                    .help("if set, will query sector by bill-id, otherwise, cancel it by sector-id"))
            )
            .subcommand(Command::new("order")
                .arg(Arg::new("id").index(1)
                    .required(true)
                    .value_parser(value_parser!(u64))
                    .help("query by order-id, sector-id, bill-id or raw-sector-id, query by order-id if no options set."))
                .arg(Arg::new("is_bill").long("bill").short('b')
                    .action(ArgAction::SetTrue)
                    .help("if set, will query order by bill-id."))
                .arg(Arg::new("is_sector").long("sector").short('s')
                    .action(ArgAction::SetTrue)
                    .help("if set, will query order by sector-id."))
                .arg(Arg::new("is_raw_sector").long("raw").short('r')
                    .action(ArgAction::SetTrue)
                    .help("if set, will query order by raw-sector-id."))
            )
            .subcommand(Command::new("raw-sector")
                .arg(Arg::new("id").index(1)
                    .value_parser(value_parser!(u64))
                    .help("query by raw-sector-id, query all if not set."))
            )
        )
        .subcommand(Command::new("regnode")
            .arg(Arg::new("node_id").long("node").short('n')
                .required(true)
                .value_parser(value_parser!(u32))
                .help("node id"))
            .arg(Arg::new("node_host").long("host")
                .required(true)
                .value_parser(value_parser!(Url))
                .help("node host")))
        .subcommand(Command::new("listnode"))
        .subcommand(Command::new("init"));

    let cur_exe = std::env::current_exe().unwrap();
    let lite_server = cur_exe.parent().unwrap().join("dmc-miner-lite-server").with_extension(cur_exe.extension().unwrap_or("".as_ref()));
    let lite_mode = if lite_server.exists() {
        println!("lite server {} exists, run in lite mode", lite_server.display());
        true
    } else {
        false
    };

    let matches = app.get_matches();
    let sector_endpoint;
    let account_endpoint;
    let contract_endpoint;
    if lite_mode {
        sector_endpoint = "http://127.0.0.1:4451".parse().unwrap();
        account_endpoint = "http://127.0.0.1:4448".parse().unwrap();
        contract_endpoint = "http://127.0.0.1:4450".parse().unwrap();
    } else {
        sector_endpoint = matches.get_one::<Url>("sector_service_endpoint").unwrap().clone();
        account_endpoint = matches.get_one::<Url>("account_service_endpoint").unwrap().clone();
        contract_endpoint = matches.get_one::<Url>("contract_service_endpoint").unwrap().clone();
    }
    let spv_endpoint = matches.get_one::<Url>("spv_host").unwrap().clone();

    println!("use sector endpoint: {}", &sector_endpoint);
    println!("use account endpoint: {}", &account_endpoint);
    println!("use contract endpoint: {}", &contract_endpoint);
    println!("use spv endpoint: {}", &spv_endpoint);

    if lite_mode {
        // check config
        let home = dirs::home_dir();
        if home.is_none() {
            println!("not found home dir. program exit;");
            std::process::exit(1);
        }
        let server_config_path = home.unwrap().join(".dmcminer").join("config.toml");
        let mut config = if server_config_path.exists() {
            match toml::from_str::<ServerConfig>(&std::fs::read_to_string(&server_config_path).unwrap()) {
                Ok(config) => config,
                Err(e) => {
                    println!("parse server config err {}, reset.", e);
                    ServerConfig::default()
                }
            }
        } else {
            ServerConfig::default()
        };

        if config.sql_url.len() == 0 {
            println!("dmc miner need a pre created mysql database, please input database`s information below:");
            let host = inquire::Text::new("Please input database host:").with_default("localhost").prompt().unwrap();
            let port = inquire::Text::new("Please input database port:").with_default("3306").prompt().unwrap();
            let name = inquire::Text::new("Please input database username:").with_default("root").prompt().unwrap();
            let pass = inquire::Text::new("Please input database password:").prompt().unwrap();
            let database = inquire::Text::new("Please input database name:").prompt().unwrap();
            config.sql_url = format!("mysql://{}:{}@{}:{}/{}", name, pass, host, port, database);
            save_config(&config, server_config_path.as_path())
        }

        if config.account_private_key.len() == 0 || config.account_name.len() == 0 {
            let mut open_mine = false;
            if open_mine_active().await {
                open_mine = inquire::Confirm::new("DMC Open Mine is going on right now. Do you want to join?").with_default(true).prompt().unwrap();
            }

            if open_mine {
                if config.account_private_key.len() == 0 {
                    println!("generating miner private key...");
                    let pk = EosPrivateKey::gen_key();
                    config.account_private_key = pk.to_legacy_string().unwrap();
                    save_config(&config, server_config_path.as_path())
                }

                if config.account_name.len() == 0 {
                    println!("DMC account name not set.");
                    println!("Please paste DMC public key and token to https://open.dmctech.io/applyPledge");
                    let pk = EosPrivateKey::from_str(&config.account_private_key).unwrap();
                    let mut public_str = pk.get_public_key().to_legacy_string().unwrap();
                    println!("Public Key: {}", &public_str);
                    public_str.insert_str(public_str.len()-1, "dmc-open-mine");
                    let token = base64::engine::general_purpose::STANDARD_NO_PAD.encode(crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(public_str.as_bytes()).to_le_bytes());
                    println!("Token: {}", token);
                    println!("You can press Ctrl+C to temporary exit initialization\nre-run `./dmc-miner-client init` again to continue from here");
                    config.account_name = inquire::Text::new("Please input DMC account name from page:").prompt().unwrap();
                    save_config(&config, server_config_path.as_path());
                }

                if config.permission.len() == 0 {
                    config.permission = "prem".to_owned();
                    save_config(&config, server_config_path.as_path());
                }
            } else {
                config.account_name = inquire::Text::new("Please input DMC account name:").prompt().unwrap();
                config.account_private_key = inquire::Text::new("Please input DMC account private key:").prompt().unwrap();
                save_config(&config, server_config_path.as_path());

                if config.permission.len() == 0 {
                    config.permission = "active".to_owned();
                    save_config(&config, server_config_path.as_path());
                }
            }
        }

        if check_lite_server(account_endpoint.clone()).await.is_err() {
            println!("try start user lite server...");
            let mut command = std::process::Command::new(lite_server);
            command.stdout(Stdio::null()).stderr(Stdio::null()).stdin(Stdio::null());
            #[cfg(target_os = "windows")]
            {
                use std::os::windows::process::CommandExt;
                pub const DETACHED_PROCESS: u32 = 0x00000008;
                pub const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;
                pub const CREATE_NO_WINDOW: u32 = 0x08000000;

                let flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW;
                command.creation_flags(flags);
            }

            if let Err(e) = command.spawn() {
                println!("ERROR: spawn lite server err {}, client exit.", e);
                std::process::exit(1);
            }

            async_std::task::sleep(Duration::from_secs(5)).await;

            if check_lite_server(account_endpoint.clone()).await.is_err() {
                println!("ERROR: check lite server again failed, client exit.");
                std::process::exit(1);
            }
        }
        /*
        if config.bill_price == 0f64 {
            config.bill_price = 0.011f64;
            save_config(&config, server_config_path.as_path());
        }

        if first {
            config.auto_bill = inquire::Confirm::new(&format!("Can you want to enable auto bill?\nIf Set, all your available PSTs will be billed at {} DMC per PST.", config.bill_price))
                .with_default(true).prompt().unwrap();
            save_config(&config, server_config_path.as_path());
        }
         */

        println!("checking method...");
        let account_client = AccountClient::new(AccountClientConfig {
            endpoint: account_endpoint.clone()
        }).unwrap();
        let method = account_client.get_method().await.unwrap();
        if method.len() == 0 {
            println!("WARN： miner method not set");
            println!("method used for user to connect with this miner, it MUST be a URL connect to miner's 4450 port");

            let method = inquire::Text::new("Please input a valid URL:").with_validator(|str: &str| {
                match Url::parse(str) {
                    Ok(_) => {
                        Ok(Validation::Valid)
                    }
                    Err(e) => {
                        Ok(Validation::Invalid(ErrorMessage::Custom(format!("invalid url: {}", e))))
                    }
                }
            }).prompt().unwrap();

            account_client.set_method(method.clone()).await.unwrap();
            println!("miner {} set method to {}", &config.account_name, &method);
        } else {
            println!("miner {} has method {}", &config.account_name, &method);
        }
    }

    let account_client = AccountClient::new(AccountClientConfig {
        endpoint: account_endpoint.clone()
    }).unwrap();
    println!("checking available PSTs...");
    let mut pst_amount = 0f64;
    for asset in account_client.get_available_assets().await.unwrap() {
        if asset.name == "PST" {
            pst_amount = asset.amount;
        }
    }

    if pst_amount == 0f64 {
        println!("no any available PST.");
    } else {
        println!("you have {} available PSTs, you can use ./dmc-miner-client createbill to bill it.", pst_amount);
    }

    match matches.subcommand() {
        Some(("createbill", sub_matches)) => {
            let sector_admin = dmc_miner_sectors_node::SectorAdmin::new(dmc_miner_sectors_node::SectorAdminConfig {
                endpoint: sector_endpoint
            }).unwrap();

            let duration = *sub_matches.get_one::<u32>("duration").unwrap();
            if duration <= 24 {
                error!("duration MUST greater then 24 weeks");
                return Err(DmcError::from(DmcErrorCode::InvalidParam));
            }

            let local_path = sub_matches.get_one::<PathBuf>("local_path").unwrap();
            if !local_path.exists() {
                info!("add bill's path {} not exists, create it.", local_path.display());
                std::fs::create_dir_all(local_path).map_err(|e| {
                    error!("create bill's path {} err {}, exit.", local_path.display(), e);
                    e
                })?;
            }

            let chunk_size = *sub_matches.get_one::<usize>("chunk_size").unwrap();
            let capacity = *sub_matches.get_one::<u64>("capacity").unwrap();
            let price = (*sub_matches.get_one::<f32>("price").unwrap() * 10000f32).floor() as u64;

            let sector_meta = sector_admin.add(dmc_miner_sectors_node::AddSectorOptions {
                node_id: sub_matches.get_one::<u32>("node_id").map(|id|*id),
                local_path: local_path.to_string_lossy().to_string(),
                chunk_size: chunk_size * 1024 * 1024,
                capacity: capacity*1024*1024*1024,
            }).await.map_err(|e| {
                error!("add sector {} to sector node err {}", local_path.display(), e);
                e
            })?;
            info!("add sector {} to sector node success, sector {}", local_path.display(), sector_meta.sector_id);

            let account_client = AccountClient::new(AccountClientConfig {
                endpoint: account_endpoint
            }).unwrap();

            let _account_name = account_client.name().await.unwrap();

            let sector = account_client.create_sector(CreateSectorOptions {
                raw_sector_id: sector_meta.sector_id.clone().into(),
                bill: DmcBillOptions {
                    asset: capacity,
                    price,
                    pledge_rate: sub_matches.get_one::<u32>("pledge_rate").map(|p|*p),
                    min_asset: None,
                    duration,
                }
            }).await.map_err(|e| {
                error!("create bill for sector {} err {}", sector_meta.sector_id, e);
                e
            })?;
            info!("create bill for raw sector {} success, sector id {}, now wait bill on chain", sector_meta.sector_id, sector.sector_id);

            loop {
                task::sleep(Duration::from_secs(10)).await;
                let sector = account_client.sectors(SectorFilter::from_sector_id(sector.sector_id), 1).await.map_err(|e| {
                    error!("get sector info for sector id {} err {}", sector.sector_id, e);
                    e
                })?.next_page().await.map_err(|e| {
                    error!("get sector info for sector id {} err {}", sector.sector_id, e);
                    e
                })?[0].clone();
                match sector.info.state {
                    SectorState::PreBill(_tx) => {
                        info!("tx sent, continue wait tx result on chain");
                    }
                    SectorState::BillError(error) => {
                        error!("create bill error, tx block number {}, err {}", error.block_number, error.error);
                        break;
                    }
                    SectorState::PostBill(info) => {
                        info!("create bill success, bill id {}", info.bill_id);
                        if let Some(spv_host) = matches.get_one::<Url>("spv_host") {
                            let client = SpvClient::new(SpvClientConfig {
                                host: spv_host.clone(),
                            }).unwrap();
                            let bill = account_client.get_bill(info.bill_id).await?;
                            if let Some(bill) = bill {
                                client.add_bill(bill).await.unwrap();
                            }
                        }

                        break;
                    }
                    SectorState::Removed(_) => {
                        error!("bill removed!?");
                        break;
                    }
                    SectorState::Waiting => {
                        info!("sector waiting");
                    }
                }
            }
        },
        Some(("report", sub_matches)) => {
            let account_client = AccountClient::new(AccountClientConfig {
                endpoint: account_endpoint
            }).unwrap();
            let bill_id = *sub_matches.get_one::<u64>("bill_id").unwrap();
            info!("add bill {} to spv", bill_id);
            let client = SpvClient::new(SpvClientConfig {
                host: spv_endpoint,
            }).unwrap();
            let bill = account_client.get_bill(bill_id).await?;
            if let Some(bill) = bill {
                client.add_bill(bill).await.unwrap();
            }
        },
        Some(("setmethod", sub_matches)) => {
            let account_client = AccountClient::new(AccountClientConfig {
                endpoint: account_endpoint
            }).unwrap();
            let name = account_client.name().await.unwrap();
            let method = sub_matches.get_one::<String>("method").unwrap();
            account_client.set_method(method.clone()).await.unwrap();
            info!("account {} set method {} success", name, method);
        }
        Some(("method", _sub_matches)) => {
            let account_client = AccountClient::new(AccountClientConfig {
                endpoint: account_endpoint
            }).unwrap();
            let name = account_client.name().await.unwrap();
            let method = account_client.get_method().await.unwrap();
            info!("get account {} method {}", name, method);
        },
        Some(("list", sub_matches)) => {
            let env = CommandEnv { sector_endpoint, account_endpoint, spv_endpoint, contract_endpoint };

            match sub_matches.subcommand() {
                Some(("sector", sub_matches)) => {
                    let _ = list_sector(env, sub_matches).await;
                }
                Some(("order", sub_matches)) => {
                    let _ = list_order(env, sub_matches).await;
                }
                Some(("raw-sector", sub_matches)) => {
                    let _ = list_raw_sector(env, sub_matches).await;}
                _ => {
                    info!("unknown list type");
                }
            }
        }
        Some(("regnode", sub_matches)) => {
            let sector_admin = dmc_miner_sectors_gateway::GatewaySectorAdmin::new(dmc_miner_sectors_gateway::GatewaySectorAdminConfig {
                endpoint: sector_endpoint
            })?;

            let node_id = *sub_matches.get_one::<u32>("node_id").unwrap();
            let node_host = sub_matches.get_one::<Url>("node_host").unwrap();
            sector_admin.register_node(dmc_miner_sectors_gateway::SectorNodeInfo {
                node_id,
                endpoint: node_host.clone()
            }).await.map_err(|err| {
                error!("register node {} with host {} err {}", node_id, node_host, err);
                err
            })?;

            info!("register node {} with host {} ok.", node_id, node_host);
        },
        Some(("listnode", _)) => {
            let sector_admin = dmc_miner_sectors_gateway::GatewaySectorAdmin::new(dmc_miner_sectors_gateway::GatewaySectorAdminConfig {
                endpoint: sector_endpoint
            })?;
            let nodes = sector_admin.list_node().await.map_err(|e| {
                error!("list node err {}", e);
                e
            })?;

            for node in nodes {
                println!("\t node id {}, host {}", node.node_id, node.endpoint)
            }
        }
        Some(("cancelbill", sub_matches)) => {
            let id = *sub_matches.get_one::<u64>("id").unwrap();
            let is_sector = sub_matches.get_flag("is_sector");

            let remove_key_msg = if is_sector {
                format!("sector-id({})", id)
            } else {
                format!("bill-id({})", id)
            };
            info!("will cancel bill by {}.", remove_key_msg);

            let account_client = AccountClient::new(AccountClientConfig {
                endpoint: account_endpoint
            })?;

            let options = if is_sector {
                RemoveSectorOptions {
                    sector_id: Some(id),
                    bill_id: None,
                }
            } else {
                RemoveSectorOptions {
                    sector_id: None,
                    bill_id: Some(id),
                }
            };

            match account_client.remove_sector(options).await {
                Ok(_) => info!("cancel bill by {} successed.", remove_key_msg),
                Err(err) => error!("cancel bill by {} failed, {}", remove_key_msg, err)
            }
        }
        Some(("init", _sub_matches)) => {
            info!("init finish.");
        }

        _ => {}
    }

    Ok(())
}

async fn list_sector(env: CommandEnv, sub_matches: &ArgMatches) -> DmcResult<()> {
    let account_client = AccountClient::new(AccountClientConfig {
        endpoint: env.account_endpoint.clone()
    }).unwrap();

    let contract_client = ContractClient::new(ContractClientConfig {
        endpoint: env.contract_endpoint.clone()
    }).unwrap();

    let id = sub_matches.get_one::<u64>("id");
    let is_bill = sub_matches.get_flag("is_bill");

    let filter = match id {
        Some(id) => {
            if is_bill {
                SectorFilter::from_bill(*id)
            } else {
                SectorFilter::from_sector_id(*id)
            }
        },
        None => SectorFilter::default()
    };

    let mut iter = account_client.sectors(filter, 99).await.unwrap();
    let mut row_seq = 0;

    println!("ROW\tsector-id\traw-sector-id\tbill-id\tcapcity\toccupy(G)\tstatus");

    loop {
        let bills = iter.next_page().await?;
        if bills.len() == 0 {
            break;
        }
        for bill in bills {
            row_seq += 1;
            
            let (bill_id, state) = match &bill.info.state {
                SectorState::PreBill(_) => {
                    (None, "waiting bill on chain".to_string())
                }
                SectorState::BillError(err) => {
                    (None, format!("create bill err {} on block {}", err.error, err.block_number))
                }
                SectorState::PostBill(info) => {
                    let dmc_bill = account_client.get_bill(info.bill_id).await;
                    match dmc_bill {
                        Ok(bill) => {
                            match bill {
                                Some(bill) => (Some(bill.bill_id), format!("bill created: {}", bill)),
                                None => (None, "bill created, not found".to_string()),
                            }
                        },
                        _ => (None, "bill created, but load failed".to_string())
                    }
                }
                SectorState::Removed(_) => {
                    (None, "status: Removed".to_string())
                }
                SectorState::Waiting => {
                    (None, "status: waiting".to_string())
                }
            };

            let (bill_id, occupy) = match bill_id {
                Some(bill_id) => {
                    let occupy = contract_client.occupy(SectorOccupyOptions::from_bill(bill_id)).await
                        .map_or("unknown".to_string(), |occupy| (occupy.occupy >> 30).to_string());
                    (bill_id.to_string(), occupy)
                },
                None => ("".to_string(), "0".to_string())
            };

            println!("{}\t{}\t{}\t{}\t{}\t{}\t{}", row_seq, bill.sector_id, bill.info.raw_sector_id, bill_id, bill.info.bill_options.asset, occupy, state);
        }
    }

    Ok(())
}

async fn list_order(env: CommandEnv, sub_matches: &ArgMatches) -> DmcResult<()> {
    let account_client = AccountClient::new(AccountClientConfig {
        endpoint: env.account_endpoint.clone()
    }).unwrap();

    let contract_client = ContractClient::new(ContractClientConfig {
        endpoint: env.contract_endpoint.clone()
    }).unwrap();

    let id = *sub_matches.get_one::<u64>("id").unwrap();
    let is_bill = sub_matches.get_flag("is_bill");
    let is_sector = sub_matches.get_flag("is_sector");
    let is_raw_sector = sub_matches.get_flag("is_raw_sector");

    let (sector_capcity, mut orders, mut order_iter) = if is_bill {
        if is_sector || is_raw_sector {
            error!("you should specify the query conditions clearly with -b or -s or -r");
            return Ok(());
        }

        let sector_capcity = account_client.sectors(SectorFilter::from_bill(id), 1).await?
            .next_page().await?
            .pop()
            .map_or("unknown".to_string(), |sector| sector.info.bill_options.asset.to_string());

        let order_iter = contract_client.get(ContractFilter::from_bill(id), 99).await?;

        (sector_capcity, vec![], Some(order_iter))
    } else if is_sector {
        if is_bill || is_raw_sector {
            error!("you should specify the query conditions clearly with -b or -s or -r");
            return Ok(());
        }

        let sector = account_client.sectors(SectorFilter::from_sector_id(id), 1).await?
            .next_page().await?
            .pop()
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "no found sector by sector-id({})", id))?;

        let sector_capcity = sector.info.bill_options.asset.to_string();

        let order_iter = match &sector.info.state {
            SectorState::PostBill(bill) => {
                let order_iter = contract_client.get(ContractFilter::from_bill(bill.bill_id), 99).await?;
                Some(order_iter)
            },
            SectorState::Removed(bill) => {
                match bill {
                    Some(bill) => {
                        let order_iter = contract_client.get(ContractFilter::from_bill(bill.bill_id), 99).await?;
                        Some(order_iter)
                    },
                    None => None
                }
            },
            SectorState::Waiting => None,
            SectorState::PreBill(_) => None,
            SectorState::BillError(_) => None,
        };

        (sector_capcity, vec![], order_iter)
    } else if is_raw_sector {
        if is_bill || is_sector {
            error!("you should specify the query conditions clearly with -b or -s or -r");
            return Ok(());
        }

        let sector_capcity = account_client.sectors(SectorFilter::from_raw_sector(id), 1).await?
            .next_page().await?
            .pop()
            .map_or("unknown".to_string(), |sector| sector.info.bill_options.asset.to_string());

        let order_iter = contract_client.get(ContractFilter::from_raw_sector(id), 99).await?;

        (sector_capcity, vec![], Some(order_iter))
    } else {
        let mut order_iter = contract_client.get(ContractFilter::from_order_id(id), 1).await?;
        let contract = order_iter.next_page().await?
            .pop()
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "not found contract by order-id({})", id))?;

        let filter = match contract.bill_id.as_ref() {
            Some(bill_id) => SectorFilter::from_bill(*bill_id),
            None => SectorFilter::from_raw_sector(id),
        };
        let sector_capcity = account_client.sectors(filter, 1).await?
            .next_page().await?
            .pop()
            .map_or("unknown".to_string(), |sector| sector.info.bill_options.asset.to_string());
        
        (sector_capcity, vec![contract], None)
    };

    println!("total size: {}", sector_capcity);

    println!("order list:");

    println!("ROW\torder-id\tstate");

    let mut row_seq = 0;

    loop {
        for order in orders {
            row_seq += 1;

            let state = match &order.state {
                ContractState::Unknown => "Unknown".to_string(),
                ContractState::Applying => "Applying".to_string(),
                ContractState::Refused => "Refused".to_string(),
                ContractState::Writing { sector, writen } => {
                    let total_size = sector.total_size();
                    format!("Writing to raw-sector({}): {:.2}%, writen size: {}, total size: {}", sector.sector_id, *writen as f64 * 100.0f64 / total_size as f64, writen, total_size)
                },
                ContractState::Calculating { sector, writen, calculated } => {
                    let total_size = sector.total_size();
                    format!("Calculating to raw-sector({}): {:.2}%, writen size: {}, total size: {}, calculated: {}", sector.sector_id, *writen as f64 * 100.0f64 / total_size as f64, writen, total_size, calculated)
                },
                ContractState::Calculated { sector, writen, .. } => {
                    let total_size = sector.total_size();
                    format!("Calculated to raw-sector({}): {:.2}%, writen size: {}, total size: {}", sector.sector_id, *writen as f64 * 100.0f64 / total_size as f64, writen, total_size)
                },
                ContractState::PrepareError(msg) => {
                    format!("PrepareError: {}", msg)
                },
                ContractState::Storing { sector, writen, .. } => {
                    let total_size = sector.total_size();
                    format!("Storing to raw-sector({}): {:.2}%, writen size: {}, total size: {}", sector.sector_id, *writen as f64 * 100.0f64 / total_size as f64, writen, total_size)
                },
                ContractState::Canceled => format!("Canceled.")
            };

            println!("{}\t{}\t{}", row_seq, order.order_id, state);
        }

        match order_iter.as_mut() {
            Some(order_iter) => {
                orders = order_iter.next_page().await?;
                if orders.len() == 0 {
                    break;
                }
            },
            None => break,
        }
    }

    Ok(())
}

async fn list_raw_sector(env: CommandEnv, sub_matches: &ArgMatches) -> DmcResult<()> {
    let sector_client = SectorClient::new(SectorClientConfig {
        endpoint: env.sector_endpoint.clone()
    }).unwrap();

    let _account_client = AccountClient::new(AccountClientConfig {
        endpoint: env.account_endpoint.clone()
    }).unwrap();

    let contract_client = ContractClient::new(ContractClientConfig {
        endpoint: env.contract_endpoint.clone()
    }).unwrap();

    let id = sub_matches.get_one::<u64>("id");
    let filter = match id {
        Some(id) => dmc_miner_sectors_client::SectorFilter::from_sector_id(*id),
        None => dmc_miner_sectors_client::SectorFilter::default()
    };

    let mut iter = sector_client.get(filter, 99).await.unwrap();
    let mut row_seq = 0;

    println!("ROW\traw-sector-id\ttotal\tpath\toccupy(G)");

    loop {
        let sectors = iter.next_page().await?;
        if sectors.len() == 0 {
            break;
        }
        for sector in sectors {
            row_seq += 1;
            
            let occupy = contract_client.occupy(SectorOccupyOptions::from_raw_sector(sector.sector_id)).await
                .map_or("unknown".to_string(), |occupy| (occupy.occupy >> 30).to_string());

            println!("{}\t{}\t{}\t{}", row_seq, sector.sector_id, sector.capacity, occupy);
        }
    }

    Ok(())
}