use std::path::PathBuf;
use dmc_miner_account_server::AccountServerConfig;
use dmc_miner_contracts::ContractsServerConfig;
use dmc_miner_journal::JournalServerConfig;
use dmc_miner_sectors_gateway::SectorsGatewayConfig;
use dmc_miner_sectors_node::SectorsNodeConfig;

fn main() {
    let config_folder = std::env::args().nth(1).unwrap();

    println!("checking account config.");
    let path = PathBuf::from(&config_folder).join("account-config.toml");
    toml::from_str::<AccountServerConfig>(&std::fs::read_to_string(&path).unwrap()).unwrap();
    println!("checking account config success");

    println!("checking contract config.");
    let path = PathBuf::from(&config_folder).join("contract-config.toml");
    toml::from_str::<ContractsServerConfig>(&std::fs::read_to_string(&path).unwrap()).unwrap();
    println!("checking contract config success");

    println!("checking node config.");
    let path = PathBuf::from(&config_folder).join("node_config.toml");
    toml::from_str::<SectorsNodeConfig>(&std::fs::read_to_string(&path).unwrap()).unwrap();
    println!("checking node config success");

    println!("checking gateway config.");
    let path = PathBuf::from(&config_folder).join("gateway_config.toml");
    toml::from_str::<SectorsGatewayConfig>(&std::fs::read_to_string(&path).unwrap()).unwrap();
    println!("checking gateway config success");

    println!("checking journal config.");
    let path = PathBuf::from(&config_folder).join("journal-config.toml");
    toml::from_str::<JournalServerConfig>(&std::fs::read_to_string(&path).unwrap()).unwrap();
    println!("checking journal config success");
}