#![allow(dead_code)]
use std::{
    time::Duration, 
    path::{Path, PathBuf}
};
use std::fs::{File};
use std::io::{Read, Seek, Write};
use std::process::Stdio;
use async_std::io::{ReadExt, WriteExt};
use clap::*;
use indicatif::{ProgressBar, ProgressStyle};
use once_cell::sync::OnceCell;
use tide::http::Url;
use walkdir::WalkDir;
use zip::write::FileOptions;
use zip::ZipWriter;
use dmc_tools_common::{DmcError, DmcErrorCode, DmcOrderOptions, DmcResult, MerkleStubProc, MerkleStubSha256};
use dmc_user_journal::*;
use dmc_user_sources::*;
use dmc_user_account_client::*;
use dmc_user_contracts::*;
use dmc_miner_contracts as miner;
use dmc_user_lite_server::config::{ACCOUNT_ADDR, CONTRACT_ADDR, JOURNAL_ADDR, LiteServerConfig, NODE_ADDR};

#[derive(Subcommand)]
enum MainCommands {
    /// Register source path to backup
    Register {
        /// Local file path 
        #[arg(short, long)]
        file: PathBuf,

        /// Merkle leaf size of leaves for on-chain challenge  
        #[arg(long, default_value_t = 1024)]
        merkle_piece_size: u16,

        /// Stub counts to save for off-chain challenge  
        #[arg(long, default_value_t = 100)]
        stub_count: u32,
    }, 
    
    /// Backup source to DMC network
    Backup {
        /// Source id returned from register command
        #[arg(long)]
        source_id: u64,

        /// Backup duration in weeks
        #[arg(long, short)]
        duration: u32,

        /// order bill by id
        #[arg(long, short)]
        bill: Option<u64>,

        /// Order this miner's bill 
        #[arg(long)]
        miner: Option<String>, 

        /// Order bill at least asset space
        #[arg(long)]
        min_asset: Option<u64>,

        /// Order bill has max price
        #[arg(long)]
        max_price: Option<u64>,

        /// Order bill has min pledge rate
        #[arg(long)]
        min_pledge_rate: Option<u64>, 

        /// Continue watch backup progress
        #[arg(long, short, default_value_t = true)]
        watch: bool
    },

    /// Watch backup progress
    Watch {
        /// sector id returned from backup command
        #[arg(long)]
        sector_id: u64,
    },

    /// Restore from DMC network
    Restore {
        /// sector id returned from backup command
        #[arg(long)]
        sector_id: u64, 

        /// restore file to path
        #[arg(short, long)]
        path: PathBuf,
    }, 

    /// List all backup progress
    List {
        /// Page index 
        #[arg(long, default_value_t=0)]
        page_index: usize,

        /// Page size
        #[arg(long, default_value_t=10)]
        page_size: usize,
    },

    /// re-calcute source's merkle root, used for test.
    Calculate {
        #[arg(long, short)]
        source_id: u64,
    },

    /// show order's journal log
    Journal {
        #[arg(long)]
        source_id: Option<u64>,
        #[arg(long)]
        sector_id: Option<u64>,
    },

    /// init DMC account
    Init {

    }
}

#[derive(Parser)]
struct MainOptions {
    /// Config file path 
    #[arg(short, long)]
    config: Option<PathBuf>, 
 
    /// Commands 
    #[command(subcommand)]
    command: MainCommands
}

struct Config {
    journal: JournalClientConfig, 
    source: SourceClientConfig,
    account: AccountClientConfig, 
    contract: ContractClientConfig, 
}

const TRANSFER_BAR_STYLE: OnceCell<ProgressStyle> = OnceCell::new();
const SPIN_BAR_STYLE: OnceCell<ProgressStyle> = OnceCell::new();
const PROCESS_BAR_STYLE: OnceCell<ProgressStyle> = OnceCell::new();

fn transfer_bar_style() -> ProgressStyle {
    TRANSFER_BAR_STYLE.get_or_init(|| {
        ProgressStyle::with_template("ETA:{eta_precise} {bar:60} {bytes}/{total_bytes} {msg}").unwrap()
    }).clone()
}

fn spin_bar_style() -> ProgressStyle {
    SPIN_BAR_STYLE.get_or_init(|| {
        ProgressStyle::with_template("{elapsed_precise} {spinner} {msg}").unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ ")
    }).clone()
}

fn process_bar_style() -> ProgressStyle {
    PROCESS_BAR_STYLE.get_or_init(|| {
        ProgressStyle::with_template("ETA:{eta_precise} {bar:60} {pos}/{len} {msg}").unwrap()
    }).clone()
}

fn local_file_url(path: &Path) -> Url {
    #[cfg(target_os = "windows")]
    {
        let parts: Vec<String> = path.components().enumerate().filter_map(|(i, p)| if i == 1 { None } else { Some(p.as_os_str().to_str().unwrap().to_owned()) }).collect();
        let file_path = parts.join("/");
        tide::http::Url::parse(&format!("file://{}", file_path)).unwrap()
    }
    #[cfg(not(target_os = "windows"))]
    {
        tide::http::Url::parse(&format!("file://{}", path.as_os_str().to_str().unwrap())).unwrap()
    }
}

fn path_to_string(path: &std::path::Path) -> String {
    let mut path_str = String::new();
    for component in path.components() {
        if let std::path::Component::Normal(os_str) = component {
            if !path_str.is_empty() {
                path_str.push('/');
            }
            path_str.push_str(&os_str.to_string_lossy());
        }
    }
    path_str
}

fn zip_file<T, S>(zip: &mut ZipWriter<T>, source_path: &Path, name: S, options: &FileOptions, process: &ProgressBar) -> zip::result::ZipResult<()>
    where
        T: Write + Seek,
        S: Into<String>,
{
    let mut source = File::open(&source_path).map_err(|e| {
        println!("ERR: open source file {} err {}", source_path.display(), e);
        e
    })?;
    let total_len = source.metadata().map_err(|e| {
        println!("ERR: get source file {} meta err {}", source_path.display(), e);
        e
    })?.len();
    let mut buf = vec![0u8;64*1024*1024];   // 64MB Buffer

    process.inc_length(total_len);

    let final_options;
    #[cfg(windows)] {
        final_options = options.clone();
    }
    #[cfg(not(windows))]
    {
        let mut options = options.clone();
        use std::os::unix::fs::PermissionsExt;
        let metadata = source.metadata()?;
        //info!("mode {}", metadata.permissions().mode());
        final_options = options.unix_permissions(metadata.permissions().mode());
    }

    zip.start_file(name, final_options).map_err(|e| {
        println!("ERR: start zip err {}", e);
        e
    })?;

    loop {
        let size = source.read(&mut buf).map_err(|e| {
            println!("ERR: read source err {}", &e);
            e
        })?;
        if size == 0 {
            break;
        }
        zip.write(&buf[0..size]).map_err(|e| {
            println!("ERR: write to zip err {}", e);
            e
        })?;
        process.inc(size as u64);
    }

    Ok(())
}

fn zip_dir<T>(
    it: &mut dyn Iterator<Item = walkdir::DirEntry>,
    prefix: &Path,
    zip: &mut ZipWriter<T>,
    options: &FileOptions,
    progess: &ProgressBar,
) -> zip::result::ZipResult<()>
    where
        T: Write + Seek,
{
    for entry in it {
        let path = entry.path();
        let name = path.strip_prefix(prefix).unwrap();

        // Write file or directory explicitly
        // Some unzip tools unzip files with directory paths correctly, some do not!
        if path.is_file() {
            zip_file(zip, path, path_to_string(name), options, progess)?;
        }
    }
    Ok(())
}

async fn wait_source_ready(source_client: &SourceClient, source_id: u64) -> Result<(), String> {
    println!("waiting source {} ready.", source_id);
    let bar = ProgressBar::new(0).with_style(process_bar_style());
    let mut state = 0;
    loop {
        let detail = source_client.get_source_detail(source_id).await.map_err(|e| {
            format!("get source {} detail err {}", source_id, e.to_string())
        })?;

        match detail.state {
            SourceState::PreparingMerkle => {
                if state != 1 {
                    state = 1;
                    bar.reset();
                    bar.set_message("preparing merkle tree");
                }
                bar.set_length(detail.count);
                bar.set_position(detail.index);
            }
            SourceState::PreparingStub => {
                if state != 2 {
                    state = 2;
                    bar.reset();
                    bar.set_message("preparing stub")
                }
                bar.set_length(detail.count);
                bar.set_position(detail.index);
            }
            SourceState::Ready => {
                bar.finish_and_clear();
                println!("source id {} ready", detail.source_id);
                break;
            }
        }
        async_std::task::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn register_source(config: &Config, source_path: &Path, merkle_piece_size: u16, stub_count: u32) -> Result<(), String> {
    let mut source_path = if source_path.is_relative() {
        std::fs::canonicalize(source_path)
            .map_err(|err| format!("open source file {} failed for {}", source_path.display(), err.to_string()).to_owned())?
    } else {
        source_path.to_owned()
    };
   
    println!("register file to source service {}", source_path.display());

    println!("zipping, DO NOT exit this program.");
    let crc_hex = hex::encode(crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(source_path.to_string_lossy().as_bytes()).to_le_bytes());
    let zip_path = std::env::temp_dir().join(crc_hex).with_extension("zip");
    let mut zip = ZipWriter::new(File::create(&zip_path).unwrap());
    let options = FileOptions::default().compression_method(zip::CompressionMethod::Stored).large_file(true);
    let bar = ProgressBar::new(0).with_style(transfer_bar_style()).with_message("zipping...");
    if source_path.is_dir() {
        let walkdir = WalkDir::new(&source_path);
        let it = walkdir.into_iter();

        zip_dir(&mut it.filter_map(|e| e.ok()), &source_path, &mut zip, &options, &bar).map_err(|e| {
            format!("zip dir {} to {} err {}", source_path.display(), zip_path.display(), e)
        })?;
    } else {
        let name = source_path.file_name().unwrap().to_string_lossy();
        zip_file(&mut zip, &source_path, name, &options, &bar).map_err(|e| {
            format!("zip file {} to {} err {}", source_path.display(), zip_path.display(), e)
        })?;
    }

    zip.finish().map_err(|e| {
        format!("finish zip file {} err {}", zip_path.display(), e)
    })?;

    bar.finish_and_clear();

    source_path = zip_path;

    let source_length = {
        let source_file = File::open(source_path.as_path())
            .map_err(|err| format!("open source file {} failed for {}", source_path.display(), err.to_string()).to_owned())?;

        source_file.metadata()
            .map_err(|err| format!("open source file {} failed for {}", source_path.display(), err.to_string()).to_owned())?
            .len()
    };

    let source_url = local_file_url(source_path.as_path());

    let source_client = SourceClient::new(config.source.clone()).unwrap();
    // if let Some(exists) = source_client.sources(SourceFilter::from_source_url(source_url.to_string()), 1)
    //     .await.map_err(|err| format!("source serivce error {}", err.msg()).to_owned())?
    //     .next_page().await.map_err(|err| format!("source serivce error {}", err.msg()).to_owned())?
    //     .get(0) {
    //     return Err(format!("source already exists in source serivce, source_id={}", exists.source_id).to_owned());
    // }

    let source = source_client.create(CreateSourceOptions {
        source_url: source_url.to_string(), 
        length: source_length, 
        merkle: SourceMerkleOptions::Prepare { piece_size: merkle_piece_size }, 
        stub: SourceStubOptions {
            count: stub_count,
        }
    }).await.map_err(|err| format!("source serivce error {}", err.msg()).to_owned())?;

    println!("source registered, source_id is {}", source.source_id);

    wait_source_ready(&source_client, source.source_id).await
}


async fn create_sector(config: &Config, source_id: u64, duration: u32, bill: Option<u64>, filter: FindBillFilter) -> Result<u64, String> {
    println!("backup {} to DMC network", source_id);
    let source_client = SourceClient::new(config.source.clone()).unwrap();
    wait_source_ready(&source_client, source_id).await?;

    let account_client = AccountClient::new(config.account.clone()).unwrap();

    let order = if let Some(bill) = bill {
        SectorOrderOptions::Bill(DmcOrderOptions {
            bill_id: bill,
            asset: 0,
            duration,
        })
    } else {
        SectorOrderOptions::Filter(SectorOrderFilter {
            bill_filter: filter,
            duration
        })
    };

    let sector = account_client.create_sector(CreateSectorOptions {
        source_id, 
        order
    }).await.map_err(|err| format!("account service error {}", err.msg()).to_owned())?;

    println!("backup started, sector_id={}", sector.sector_id);

    Ok(sector.sector_id)
}

async fn get_state_from_miner(contract: &Contract) -> Result<miner::ContractState, String> {
    let miner_contract_client = miner::ContractClient::new(dmc_miner_contracts::ContractClientConfig {
        endpoint: Url::parse(&contract.miner_endpoint).map_err(|e| format!("parse miner endpoint {} err {}", &contract.miner_endpoint, e))?,
    }).unwrap();

    let contract: miner::Contract = miner_contract_client.get(miner::ContractFilter::from_order_id(contract.order_id), 1)
        .await.map_err(|err| format!("connect to miner contract serivce error {}, url {}", err.msg(), &contract.miner_endpoint).to_owned())?
        .next_page().await.map_err(|err| format!("get info from miner contract serivce error {}, url {}", err.msg(), &contract.miner_endpoint).to_owned())?
        .get(0).cloned().ok_or_else(||format!("cannot find order {} from miner service", contract.order_id))?;

    Ok(contract.state)
}

async fn watch_sector(config: &Config, sector_id: u64) -> Result<(), String> {
    println!("watching sector_id={} backup progress", sector_id);
    let account_client = AccountClient::new(config.account.clone()).unwrap();
    let bar = ProgressBar::new(0);
    let mut cur_state = 0;
    println!("waiting sector {} order on DMC network...", sector_id);
    let (order_id, source_id) = loop {
        let sector: SectorWithId = account_client.sectors(SectorFilter::from_sector_id(sector_id), SectorNavigator::default())
            .await.map_err(|err| format!("account serivce error {}", err.msg()).to_owned())?
            .next_page().await.map_err(|err| format!("account serivce error {}", err.msg()).to_owned())?
            .get(0).cloned().ok_or_else(|| format!("account serivce error {}", "sector not exists").to_owned())?;
        match sector.info.state {
            SectorState::Waiting => {
                if cur_state != 1 {
                    cur_state = 1;
                    bar.reset();
                    bar.set_style(spin_bar_style());
                    bar.set_message("finding bill.");
                }
                bar.tick();
            }
            SectorState::PreOrder(_) => {
                if cur_state != 2 {
                    cur_state = 2;
                    bar.reset();
                    bar.set_style(spin_bar_style());
                    bar.set_message("waiting order on DMC chain...");
                }
                bar.tick();
            }
            SectorState::OrderError(e) => {
                bar.finish_and_clear();
                println!("order err: {}", e.error);
                std::process::exit(1);
            }
            SectorState::PostOrder(order) => {
                bar.finish_and_clear();
                println!("ordered on DMC network order_id={}", order.order_id);
                break (order.order_id, sector.info.source_id);
            }
            SectorState::Removed => {
                println!("sector {} already removed", sector_id);
                std::process::exit(1);
            }
        }

        async_std::task::sleep(Duration::from_secs(1)).await;
    };

    let source_client = SourceClient::new(config.source.clone()).unwrap();
    let source: DataSource = source_client.sources(SourceFilter::from_source_id(source_id),1)
        .await.map_err(|err| format!("connect source serivce error {}", err.msg()).to_owned())?
        .next_page().await.map_err(|err| format!(" get source error {}", err.msg()).to_owned())?
        .get(0).cloned().ok_or_else(|| format!("source not exists").to_owned())?;
    
    let contract_client = ContractClient::new(config.contract.clone()).unwrap();
    let proc = MerkleStubProc::new(source.length, source.merkle_stub.as_ref().unwrap().piece_size, true);
    loop {
        let contract = contract_client.get(ContractFilter::from_order_id(order_id), 1)
            .await.map_err(|err| format!("contract serivce error {}", err.msg()).to_owned())?
            .next_page().await.map_err(|err| format!("contract serivce error {}", err.msg()).to_owned())?
            .get(0).cloned();

        if let Some(contract) = contract {
            match &contract.state {
                ContractState::Storing => {
                    bar.finish_and_clear();
                    println!("source {} has backup to miner {}, order id {}", contract.source_id, contract.miner_endpoint, contract.order_id);
                    break;
                },
                ContractState::Applying => {
                    if cur_state != 7 {
                        cur_state = 7;
                        bar.reset();
                        bar.set_style(spin_bar_style());
                        bar.set_message(format!("waiting miner {} recv order...", contract.miner_endpoint));
                    }
                    bar.tick();
                }
                ContractState::Writing => {
                    match get_state_from_miner(&contract).await {
                        Ok(state) => {
                            match state {
                                miner::ContractState::Writing {writen, ..} => {
                                    if cur_state != 3 {
                                        cur_state = 3;
                                        bar.reset();
                                        bar.set_style(transfer_bar_style());
                                        bar.set_message("sending...");
                                        bar.set_length(source.length);
                                    }
                                    bar.set_position(writen);
                                },
                                miner::ContractState::Calculating {calculated, ..} => {
                                    if cur_state != 4 {
                                        cur_state = 4;
                                        bar.reset();
                                        bar.set_style(process_bar_style());
                                        bar.set_message("miner calculating merkle tree...");
                                        bar.set_length(proc.stub_count() as u64);
                                    }
                                    bar.set_position(calculated as u64);
                                },
                                miner::ContractState::Calculated{..} => {
                                    if cur_state != 5 {
                                        cur_state = 5;
                                        bar.reset();
                                        bar.set_style(spin_bar_style());
                                        bar.set_message("miner waiting order update...");
                                    }
                                    bar.tick();
                                },
                                miner::ContractState::Storing {..} => {
                                    if cur_state != 6 {
                                        cur_state = 6;
                                        bar.reset();
                                        bar.set_style(spin_bar_style());
                                        bar.set_message("miner storing data...");
                                    }
                                    bar.tick();
                                }
                                miner::ContractState::Refused => {
                                    bar.finish_and_clear();
                                    println!("ERROR: miner {} refused order id {}", contract.miner_endpoint, contract.order_id);
                                }
                                miner::ContractState::PrepareError(err) => {
                                    bar.finish_and_clear();
                                    println!("ERROR: miner {} prepare order {} err {}", contract.miner_endpoint, contract.order_id, err)
                                }
                                _ => {}
                            }
                        }
                        Err(err) => {
                            bar.suspend(||{println!("{}, waiting next query.", err)});
                        }
                    }
                }, 
                ContractState::Error(error) => {
                    println!("ERROR: contract error {}", error);
                    break;
                },
                ContractState::Unknown => {
                    bar.suspend(||{println!("contract state unknown! order id {}", contract.order_id);});
                }
                ContractState::Refused => {
                    println!("order {} refused by miner {}", contract.order_id, contract.miner_endpoint);
                    break;
                }
                ContractState::Preparing => {
                    if cur_state != 8 {
                        cur_state = 8;
                        bar.reset();
                        bar.set_style(spin_bar_style());
                        bar.set_message("waiting contract preparing...");
                    }
                    bar.tick();
                }
                ContractState::Canceled => {
                    println!("order {} canceled", contract.order_id);
                    break;
                }
            }
        }
        async_std::task::sleep(Duration::from_secs(1)).await;
    };
    Ok(())
}

async fn list_sector(config: &Config, page_index: usize, page_size: usize) -> Result<(), String> {
    println!("list sectors page={}", page_index);
    let account_client = AccountClient::new(config.account.clone()).unwrap();
    let sectors = account_client.sectors(SectorFilter::default(), SectorNavigator { page_size, page_index })
        .await.map_err(|err| format!("account serivce error {}", err.msg()).to_owned())?
        .next_page().await.map_err(|err| format!("account serivce error {}", err.msg()).to_owned())?;

    for sector in sectors {
        println!("sector_id:{}, state {}", sector.sector_id, sector.info.state);
    }

    Ok(())
} 

async fn restore_sector(config: &Config, sector_id: u64, local: &Path) -> Result<(), String> {
    println!("restore sector_id={} to {}", sector_id, local.display());
    let account_client = AccountClient::new(config.account.clone()).unwrap();
    let sector = account_client.sectors(SectorFilter::from_sector_id(sector_id), SectorNavigator::default())
        .await.map_err(|err| format!("account serivce error {}", err.msg()).to_owned())?
        .next_page().await.map_err(|err| format!("account serivce error {}", err.msg()).to_owned())?
        .get(0).cloned().ok_or_else(|| format!("account serivce error {}", "sector not exists").to_owned())?;
    
    let source_client = SourceClient::new(config.source.clone()).unwrap();
    let datasource = source_client.sources(SourceFilter::from_source_id(sector.info.source_id), 1)
        .await.map_err(|err| format!("source serivce error {}", err.msg()).to_owned())?
        .next_page().await.map_err(|err| format!("source serivce error {}", err.msg()).to_owned())?
        .get(0).cloned().ok_or_else(|| format!("source serivce error {}", "source not registered").to_owned())?;
    
    if let SectorState::PostOrder(order) = sector.info.state {
        let contract_client = ContractClient::new(config.contract.clone()).unwrap();

        if local.is_file() {
            return Err(format!("path {} is an already exists file!, please input a dir path.", local.display()));
        }

        if !local.exists() {
            let _ = std::fs::create_dir_all(&local);
        }

        let crc_hex = hex::encode(crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(local.to_string_lossy().as_bytes()).to_le_bytes());
        let zip_path = local.join(format!("~{}", crc_hex)).with_extension("zip");

        let mut restore_file = async_std::fs::OpenOptions::new().append(true).read(true).create(true).open(&zip_path)
            .await.map_err(|err| format!("open path {} failed for {}", zip_path.display(), err.to_string()).to_owned())?;
        
        let writen = restore_file.metadata().await
            .map_err(|err| format!("open source file {} failed for {}", zip_path.display(), err.to_string()).to_owned())?
            .len();

        if writen > datasource.length {
            return Err(format!("path {} content mistach", local.display(),).to_owned());
        }

        let bar = ProgressBar::new(datasource.length).with_position(writen)
            .with_style(transfer_bar_style())
            .with_message("restoring...");

        if writen != datasource.length {
            let mut restore_reader = contract_client.restore(RestoreContractOptions {
                navigator: ContractDataNavigator {
                    order_id: order.order_id,
                    offset: writen,
                    len: datasource.length - writen
                }
            }).await.map_err(|err| format!("contract serivce error {}", err.msg()).to_owned())?;
            let mut read_buf = vec![0u8; 64*1024];
            loop {
                let readed = restore_reader.read(&mut read_buf).await.map_err(|e| {
                    format!("read from miner error {}", e.to_string()).to_owned()
                })?;

                if readed == 0 {
                    break;
                }

                restore_file.write_all(&read_buf[0..readed]).await.map_err(|e| {
                    format!("write to {} err {}", zip_path.display(), e)
                })?;

                bar.inc(readed as u64);
            }
            restore_file.sync_all().await.map_err(|e| {
                format!("flush {} err {}", zip_path.display(), e)
            })?;
        }

        {
            let mut archive = zip::ZipArchive::new(File::open(&zip_path).map_err(|e| {
                format!("open file {} err {}", zip_path.display(), e)
            })?).map_err(|e| {
                format!("open zip file {} err {}", zip_path.display(), e)
            })?;
            bar.reset();
            bar.set_style(spin_bar_style());
            bar.set_message("extracting...");
            bar.enable_steady_tick(Duration::from_micros(200));
            archive.extract(local).map_err(|e| {
                format!("extract {} to {} err {}", zip_path.display(), local.display(), e)
            })?;
            bar.finish_and_clear();
        }

        let _ = std::fs::remove_file(&zip_path);

        println!("restored to local");
        Ok(())
    } else {
        Err(format!("sector has not storing, please try later").to_owned())
    }

    
}

async fn check_server(config: &Config) -> DmcResult<()> {
    println!("checking user server...");
    let account_client = AccountClient::new(config.account.clone()).unwrap();
    match account_client.account().await {
        Ok(addr) => {
            println!("user server started, account name {}", addr);
            Ok(())
        },
        Err(e) => {
            println!("ERROR: check user server err {}", e);
            Err(e)
        }
    }
}

fn save_config(config: &LiteServerConfig, path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, toml::to_string(config).unwrap()).unwrap();
}

#[async_std::main]
async fn main() -> DmcResult<()> {
    let cli = MainOptions::parse();

    let home = dirs::home_dir();
    if home.is_none() {
        println!("not found home dir. program exit;");
        std::process::exit(1);
    }
    let config_path = home.unwrap().join(".dmcuser").join("config.toml");
    let mut server_config = if config_path.exists() {
        match toml::from_str::<LiteServerConfig>(&std::fs::read_to_string(&config_path).unwrap()) {
            Ok(config) => config,
            Err(e) => {
                println!("parse server config err {}, reset.", e);
                LiteServerConfig::default()
            }
        }
    } else {
        LiteServerConfig::default()
    };

    if server_config.account_private_key.len() == 0 || server_config.account_name.len() == 0 {
        server_config.account_name = inquire::Text::new("Please input DMC account name:").prompt().unwrap();
        server_config.account_private_key = inquire::Text::new("Please input DMC account private key:").prompt().unwrap();
        save_config(&server_config, config_path.as_path());

        if server_config.spv_url.len() == 0 {
            server_config.spv_url = "https://dmc.dmc.tbudr.top/spv".to_owned();
            save_config(&server_config, config_path.as_path());
        }
    }

    save_config(&server_config, config_path.as_path());
    
    let config = Config {
        journal: JournalClientConfig { host: JOURNAL_ADDR.to_owned() },
        source: SourceClientConfig { host: NODE_ADDR.to_owned() },
        account: AccountClientConfig { host: ACCOUNT_ADDR.to_owned() },
        contract: ContractClientConfig { host: CONTRACT_ADDR.to_owned() },
    };

    if check_server(&config).await.is_err() {
        println!("try start user lite server...");
        let user_server_path = std::env::current_exe().unwrap().parent().unwrap().join("dmc-user-lite-server");
        let mut command = std::process::Command::new(user_server_path);
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

        if check_server(&config).await.is_err() {
            println!("ERROR: check lite server again failed, client exit.");
            std::process::exit(1);
        }
    }


    
    let _ = match cli.command {
        MainCommands::Register {
            file, 
            merkle_piece_size, 
            stub_count,
        } => {
            let _ = register_source(
                &config, 
                file.as_path(), 
                merkle_piece_size, 
                stub_count,
            ).await.map_err(|err| println!("{}", err));
        }, 
        MainCommands::Backup { 
            watch, 
            source_id,
            duration,
            bill,
            miner, 
            min_asset, 
            max_price, 
            min_pledge_rate 
        } => {
            if duration < 24 {
                println!("ERROR: backup duration MUST not less than 24");
                std::process::exit(1);
            }
            let mut filter = FindBillFilter::default();
            filter.miner = miner;
            filter.min_asset = min_asset;
            filter.max_price = max_price;
            filter.min_pledge_rate = min_pledge_rate;
            if let Ok(sector_id) = create_sector(&config, source_id, duration, bill, filter).await
                .map_err(|err| println!("{}", err)) {
                if watch {
                    let _ = watch_sector(&config, sector_id).await
                        .map_err(|err| println!("{}", err));
                }
            }
        },
        MainCommands::Watch { sector_id } => {
            let _ = watch_sector(&config, sector_id).await
                .map_err(|err| println!("{}", err));
        },
        MainCommands::Restore { sector_id, path } => {
            let _ = restore_sector(&config, sector_id, path.as_path()).await
                .map_err(|err| println!("{}", err));
        },
        MainCommands::List { page_index, page_size } => {
            let _ = list_sector(&config, page_index, page_size).await
                .map_err(|err| println!("{}", err));
        },
        MainCommands::Journal { source_id, sector_id } => {
            let order_id = if let Some(sector_id) = sector_id {
                let account_client = AccountClient::new(config.account.clone()).unwrap();
                let sector = account_client.sectors(SectorFilter {
                    sector_id: Some(sector_id),
                    order_id: None,
                }, SectorNavigator { page_size: 1, page_index: 0 }).await.map_err(|e| {
                    println!("find sector err {}", e);
                    e
                })?.next_page().await.map_err(|e| {
                    println!("find sector err {}", e);
                    e
                })?.get(0).ok_or(DmcError::from(DmcErrorCode::NotFound)).map_err(|e| {
                    println!("cannot found sector {}", sector_id);
                    e
                })?.clone();

                if let SectorState::PostOrder(order)= &sector.info.state {
                    Some(order.order_id)
                } else {
                    println!("sector {} have not a valid order!", sector_id);
                    None
                }
            } else {
                None
            };
            let journal_client = JournalClient::new(config.journal.clone()).unwrap();

            let mut iter = journal_client.get(JournalFilter {
                event_type: None,
                source_id,
                order_id,
            }, None, 10).await.map_err(|e| {
                println!("get journal err {}", e);
                std::process::exit(1);
            }).unwrap();

            while let Ok(logs) = iter.next_page().await {
                if logs.len() == 0 {
                    break;
                }
                for log in logs {
                    println!("{}", log);
                }
            }
        }
        MainCommands::Calculate { source_id } => {
            let source_client = SourceClient::new(config.source.clone()).unwrap();
            wait_source_ready(&source_client, source_id).await.unwrap();

            let source = source_client.sources(SourceFilter::from_source_id(source_id),1)
                .await.map_err(|err| format!("connect source serivce error {}", err.msg()).to_owned()).unwrap()
                .next_page().await.map_err(|err| format!(" get source error {}", err.msg()).to_owned()).unwrap()
                .get(0).cloned().ok_or_else(|| format!("source not exists").to_owned()).unwrap();

            let local_url = Url::parse(&source.source_url).unwrap();
            #[cfg(target_os = "windows")]
            let local_path = PathBuf::from(&local_url.path()[1..]);
            #[cfg(not(target_os = "windows"))]
            let local_path = PathBuf::from(&local_url.path());
            println!("re-calculating merkle root, please wait...");
            let file = async_std::fs::File::open(local_path).await.unwrap();
            let proc1 = MerkleStubProc::new(file.metadata().await.unwrap().len(), 1024, true);

            let mut reader = proc1.wrap_reader(file);
            let mut stubs = vec![];
            for i in 0..proc1.stub_count() {
                stubs.push(proc1.calc_path_stub::<_, MerkleStubSha256>(i, &mut reader).await.unwrap());
            }
            let root1 = proc1.calc_root_from_path_stub::<MerkleStubSha256>(stubs.clone()).unwrap();

            println!("stored merkle root {}", source.merkle_stub.unwrap().root.to_hex_string());
            println!("re-calc merkle root {}", root1.to_hex_string());
        },
        MainCommands::Init{} => {
            println!("init finish.");
        }
    };

    Ok(())
}