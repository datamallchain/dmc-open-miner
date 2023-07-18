#![allow(dead_code)]
mod backend;
use backend::*;
#[cfg(feature = "eos")]
use dmc_eos::*;
use dmc_tools_common::log_utils::{ModuleLogFilter, with_line};

#[tokio::main]
async fn main() {
    flexi_logger::Logger::try_with_env_or_str("info").unwrap()
        .format_for_stderr(with_line)
        .format_for_stdout(with_line)
        .filter(Box::new(ModuleLogFilter { disable_modules: vec!["tide::log", "sqlx::query"] }))
        .start().unwrap();

    let config = toml::from_str::<AccountServerConfig>(&std::fs::read_to_string("account-config.toml").unwrap()).unwrap();
    let chain_client;
    #[cfg(feature = "eos")]
    {
        let eos_config = toml::from_str::<EosClientConfig>(&std::fs::read_to_string("eos-config.toml").unwrap()).unwrap();
        chain_client = EosClient::new(eos_config).unwrap();
    }
    let server = AccountServer::new(chain_client, config).await.unwrap();
    server.init().await.unwrap();
    server.listen().await.unwrap();
}
