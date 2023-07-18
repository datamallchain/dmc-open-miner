#![allow(dead_code)]
mod types;
mod backend;

use backend::*;
use dmc_tools_common::log_utils::{ModuleLogFilter, with_line};

#[async_std::main]
async fn main() {
    flexi_logger::Logger::try_with_env_or_str("info").unwrap()
        .format_for_stdout(with_line)
        .format_for_stderr(with_line)
        .filter(Box::new(ModuleLogFilter { disable_modules: vec!["tide::log", "sqlx::query"] }))
        .start().unwrap();
    let config = toml::from_str::<SectorsNodeConfig>(&std::fs::read_to_string("node_config.toml").unwrap()).unwrap();
    let node = SectorsNode::new(config).await.unwrap();
    node.init().await.unwrap();
    node.listen().await.unwrap();
}