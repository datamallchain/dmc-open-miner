#![allow(dead_code)]
mod backend;
mod types;

use log::Level;
#[cfg(feature = "eos")]
use dmc_eos::*;
use backend::*;

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(Level::Info).unwrap();
    let client;
    #[cfg(feature = "eos")]
    {
        let eos_config = toml::from_str::<EosClientConfig>(&std::fs::read_to_string("eos-config.toml").unwrap()).unwrap();
        client = EosClient::new(eos_config).unwrap();
    }
    let config = toml::from_str::<SpvServerConfig>(&std::fs::read_to_string("spv-config.toml").unwrap()).unwrap();
    let server = SpvServer::new(client, config).await.unwrap();
    server.listen().await.unwrap();
}
