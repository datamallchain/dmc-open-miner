#![allow(dead_code)]
mod backend;
mod types;

use backend::*;
use dmc_tools_common::log_utils::{ModuleLogFilter, with_line};

#[async_std::main]
async fn main() {
    flexi_logger::Logger::try_with_env_or_str("info").unwrap()
        .format_for_stdout(with_line)
        .format_for_stderr(with_line)
        .filter(Box::new(ModuleLogFilter { disable_modules: vec!["tide::log", "sqlx::query"] }))
        .start().unwrap();
    let config = toml::from_str::<SectorsGatewayConfig>(&std::fs::read_to_string("gateway_config.toml").unwrap()).unwrap();
    let gateway = SectorsGateway::new(config).await.unwrap();
    gateway.init().await.unwrap();
    gateway.listen().await.unwrap();
}
