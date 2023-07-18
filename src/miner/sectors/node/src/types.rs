use serde::{Serialize, Deserialize};
use dmc_miner_sectors_client::*;


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SectorMeta {
    pub sector_id: SectorFullId, 
    pub capacity: u64,
    pub chunk_size: usize, 
    pub local_path: String
}

impl Into<Sector> for SectorMeta {
    fn into(self) -> Sector {
        Sector {
            sector_id: self.sector_id.into(), 
            capacity: self.capacity, 
            chunk_size: self.chunk_size,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AddSectorOptions {
    pub node_id: Option<SectorNodeId>, 
    pub local_path: String, 
    pub chunk_size: usize, 
    pub capacity: u64,
}