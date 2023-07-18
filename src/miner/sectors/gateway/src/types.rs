use serde::{Deserialize, Serialize};
use tide::http::Url;
use dmc_miner_sectors_client::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SectorNodeInfo {
    pub node_id: SectorNodeId,
    pub endpoint: Url
}