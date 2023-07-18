use std::time::Duration;
use flexi_logger::{Cleanup, Criterion, Duplicate, Naming};
#[cfg(feature = "eos")]
use dmc_eos::*;
use dmc_user_journal::{JournalClientConfig, JournalServer, JournalServerConfig};
use log::*;
use dmc_spv::SpvClientConfig;
use dmc_user_account_server::{AccountServer, AccountServerConfig};
use dmc_user_contracts::{ContractClientConfig, ContractsServer, ContractsServerConfig};
use dmc_user_sources::{SourceClientConfig, SourceServer, SourceServerConfig};

use dmc_tools_common::log_utils::{ModuleLogFilter, with_line};

mod config;
use crate::config::{ACCOUNT_ADDR, CONTRACT_ADDR, JOURNAL_ADDR, LiteServerConfig, NODE_ADDR};

#[tokio::main]
async fn main() {
    let home = dirs::home_dir();
    if home.is_none() {
        println!("not found home dir. program exit;");
        std::process::exit(1);
    }
    let data_root = home.unwrap().join(".dmcuser");
    if let Err(e) = std::fs::create_dir_all(&data_root) {
        println!("create data dir {} err {}", data_root.display(), e);
        std::process::exit(1);
    }

    let log_dir = data_root.as_path().join("server_log");
    std::fs::create_dir_all(log_dir.as_path()).unwrap();

    if let Err(e) = std::env::set_current_dir(&data_root) {
        println!("set current dir to {} err {}", data_root.display(), e);
        std::process::exit(1);
    }
    
    let file_spec = flexi_logger::FileSpec::default()
        .directory(log_dir.as_path())
        .discriminant(std::process::id().to_string());
    flexi_logger::Logger::try_with_env_or_str("info").unwrap()
        .log_to_file(file_spec)
        .format_for_files(with_line)
        .duplicate_to_stdout(Duplicate::All)
        .format_for_stdout(with_line)
        .rotate(Criterion::Size(100*1024*1024), Naming::Numbers, Cleanup::Never)
        .filter(Box::new(ModuleLogFilter { disable_modules: vec!["tide::log", "sqlx::query"] }))
        .start().unwrap();

    let sql_url;
    #[cfg(feature="sqlite")] 
    {
        let db_path = data_root.join("server.db");
       
        #[cfg(target_os = "windows")]
        {
            let parts: Vec<String> = db_path.components().enumerate().filter_map(|(i, p)| if i == 1 { None } else { Some(p.as_os_str().to_str().unwrap().to_owned()) }).collect();
            let db_path = parts.join("/");
            sql_url = format!("sqlite:///{}", db_path);
        }
        #[cfg(not(target_os = "windows"))]
        {   
            sql_url = format!("sqlite:///{}", db_path.display());
        }
    }

    let config = toml::from_str::<LiteServerConfig>(&std::fs::read_to_string("config.toml").unwrap()).unwrap();

    info!("starting user journal server...");
    let journal_server = JournalServer::new(JournalServerConfig { 
        host: JOURNAL_ADDR.to_owned(),
        sql_url: sql_url.clone(), 
    }).await.unwrap();
    journal_server.init().await.unwrap();
    async_std::task::spawn(journal_server.listen());

    info!("starting user source server...");
    let source_server = SourceServer::new(SourceServerConfig { 
        host: NODE_ADDR.to_owned(),
        sql_url: sql_url.clone(),
        journal_client: JournalClientConfig { host: JOURNAL_ADDR.to_owned() },
    }).await.unwrap();
    source_server.init().await.unwrap();
    async_std::task::spawn(source_server.listen());

    let eos_config = EosClientConfig {
        host: "explorer.dmctech.io".to_string(),
        chain_id: "4d1fb981dd562d2827447dafa89645622bdcd4e29185d60eeb45539f25d2d85d".to_string(),
        account: Some((config.account_name.clone(), config.account_private_key.clone())),
        retry_rpc_interval: Duration::from_secs(1),
        trans_expire: Duration::from_secs(600),
        piece_size: 1024,
        permission: "active".to_owned()
    };

    info!("starting user contract server...");
    let contract_server = ContractsServer::new(
        EosClient::new(eos_config.clone()).unwrap(),
        ContractsServerConfig {
            host: CONTRACT_ADDR.to_owned(),
            sql_url: sql_url.clone(),
            source_client: SourceClientConfig { host: NODE_ADDR.to_owned() },
            journal_client: JournalClientConfig { host: JOURNAL_ADDR.to_owned() },
            apply_atomic_interval: Duration::from_secs(10),
            apply_retry_interval: Duration::from_secs(2),
            challenge_atomic_interval: config.challenge_atomic_interval,
            challenge_interval: config.challenge_interval,
        }
    ).await.unwrap();
    contract_server.init().await.unwrap();
    async_std::task::spawn(contract_server.listen());

    info!("starting user address server...");
    let account_server = AccountServer::new(
        EosClient::new(eos_config.clone()).unwrap(),
        AccountServerConfig {
            host: ACCOUNT_ADDR.to_owned(),
            sql_url: sql_url.clone(), 
            retry_event_interval: Duration::from_secs(2),
            journal_client: JournalClientConfig { host: JOURNAL_ADDR.to_owned() },
            journal_listener_interval: Duration::from_secs(10),
            journal_listener_page_size: 20,
            order_atomic_interval: Duration::from_secs(10),
            order_retry_interval: Duration::from_secs(2),
            source_client: SourceClientConfig { host: NODE_ADDR.to_owned() },
            contract_client: ContractClientConfig { host: CONTRACT_ADDR.to_owned() },
            spv_client: Some(SpvClientConfig { host: url::Url::parse(&config.spv_url).unwrap() }),
            auto_retry_order: None,
            auto_onchain_challenge: config.auto_onchain_challenge,
        }).await.unwrap();
    account_server.init().await.unwrap();
    account_server.listen().await.unwrap();
}
