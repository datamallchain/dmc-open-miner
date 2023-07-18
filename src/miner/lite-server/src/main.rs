use std::time::Duration;
use flexi_logger::{Cleanup, Criterion, Duplicate, Naming};
use log::info;
use dmc_eos::{EosClient, EosClientConfig};
use dmc_miner_account_server::{AccountServer, AccountServerConfig};
use dmc_miner_contracts::{ContractClientConfig, ContractsServer, ContractsServerConfig};
use dmc_miner_journal::{JournalClientConfig, JournalServer, JournalServerConfig};
use dmc_miner_sectors_client::SectorClientConfig;
use dmc_miner_sectors_node::{SectorsNode, SectorsNodeConfig};
use dmc_tools_common::log_utils::{ModuleLogFilter, with_line};
use crate::config::ServerConfig;

mod config;

#[tokio::main]
async fn main() {
    let home = dirs::home_dir();
    if home.is_none() {
        println!("not found home dir. program exit;");
        std::process::exit(1);
    }

    let data_root = home.unwrap().join(".dmcminer");
    if let Err(e) = std::fs::create_dir_all(&data_root) {
        println!("create data dir {} err {}", data_root.display(), e);
        std::process::exit(1);
    }

    if let Err(e) = std::env::set_current_dir(&data_root) {
        println!("set current dir to {} err {}", data_root.display(), e);
        std::process::exit(1);
    }

    let log_dir = data_root.join("log");
    std::fs::create_dir_all(log_dir.as_path()).unwrap();

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

    let server_config = toml::from_str::<ServerConfig>(&std::fs::read_to_string("config.toml").unwrap()).unwrap();

    const JOURNAL_ADDR: &str = "127.0.0.1:4452";
    const NODE_ADDR: &str = "127.0.0.1:4451";
    const CONTRACT_PORT: u16 = 4450;
    const ACCOUNT_ADDR: &str = "127.0.0.1:4448";

    let journal_client = JournalClientConfig { endpoint: format!("http://{}", JOURNAL_ADDR).parse().unwrap() };
    let sector_client = SectorClientConfig { endpoint: format!("http://{}", NODE_ADDR).parse().unwrap() };

    info!("starting journal server...");
    let journal_config = JournalServerConfig {
        listen_address: JOURNAL_ADDR.to_string(),
        sql_url: server_config.sql_url.clone(),
    };

    let journal = JournalServer::new(journal_config).await.unwrap();
    journal.init().await.unwrap();
    async_std::task::spawn(journal.listen());

    info!("starting node server...");
    let node_config = SectorsNodeConfig {
        listen_address: NODE_ADDR.to_string(),
        node_id: 0,
        sql_url: server_config.sql_url.clone(),
        journal_client: journal_client.clone(),
    };

    let node = SectorsNode::new(node_config).await.unwrap();
    node.init().await.unwrap();
    async_std::task::spawn(node.listen());

    info!("starting contract server...");
    let contract_config = ContractsServerConfig {
        prepare_retry_interval: Duration::from_secs(10),
        prepare_atomic_interval: Duration::from_secs(2),
        journal_client: journal_client.clone(),
        sector_client: sector_client.clone(),
        prefix_table_name: false,
    };

    let eos_config = EosClientConfig {
        host: "explorer.dmctech.io".to_string(),
        chain_id: "4d1fb981dd562d2827447dafa89645622bdcd4e29185d60eeb45539f25d2d85d".to_string(),
        account: Some((server_config.account_name.clone(), server_config.account_private_key.clone())),
        retry_rpc_interval: Duration::from_secs(1),
        trans_expire: Duration::from_secs(600),
        piece_size: 1024,
        permission: server_config.permission.clone()
    };
    let chain_client = EosClient::new(eos_config).unwrap();

    let contract = ContractsServer::with_sql_url(chain_client.clone(), server_config.sql_url.clone(), contract_config).await.unwrap();
    contract.init().await.unwrap();
    async_std::task::spawn(contract.listen(format!("0.0.0.0:{}", CONTRACT_PORT)));

    info!("starting account server...");
    let account_config = AccountServerConfig {
        listen_address: ACCOUNT_ADDR.to_string(),
        sql_url: server_config.sql_url.clone(),
        journal_client: journal_client.clone(),
        sector_client: sector_client.clone(),
        contract_client: ContractClientConfig { endpoint: format!("http://127.0.0.1:{}", CONTRACT_PORT).parse().unwrap() },
        retry_event_interval: Duration::from_secs(10),
        bill_atomic_interval: Duration::from_secs(10),
        bill_retry_interval: Duration::from_secs(2),
    };
    let account = AccountServer::new(chain_client, account_config).await.unwrap();
    account.init().await.unwrap();
    account.listen().await.unwrap();
}
