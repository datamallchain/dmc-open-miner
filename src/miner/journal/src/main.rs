use dmc_miner_journal::{JournalServer, JournalServerConfig};
use dmc_tools_common::log_utils::{ModuleLogFilter, with_line};

#[async_std::main]
async fn main() {
    flexi_logger::Logger::try_with_env_or_str("info").unwrap()
        .format_for_stdout(with_line)
        .format_for_stderr(with_line)
        .filter(Box::new(ModuleLogFilter { disable_modules: vec!["tide::log", "sqlx::query"] }))
        .start().unwrap();
    let config = toml::from_str::<JournalServerConfig>(&std::fs::read_to_string("journal-config.toml").unwrap()).unwrap();

    let server = JournalServer::new(config).await.unwrap();
    server.init().await.unwrap();
    server.listen().await.unwrap();
}
