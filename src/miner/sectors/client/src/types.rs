use serde::{Serialize, Deserialize};

pub type SectorNodeId = u32;
pub type SectorLocalId = u32;


#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug)]
#[serde(try_from = "u64", into = "u64")]
pub struct SectorFullId(u64);

impl From<u64> for SectorFullId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl Into<u64> for SectorFullId {
    fn into(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SectorFullId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(node id: {}, local id: {})", self.node_id(), self.local_id())
    }
}

impl From<(SectorNodeId, SectorLocalId)> for SectorFullId {
    fn from(parts: (SectorNodeId, SectorLocalId)) -> Self {
        let (node_id, local_id) = parts;
        let high = node_id.to_be_bytes();
        let low = local_id.to_be_bytes();
        Self(u64::from_be_bytes([high[0], high[1], high[2], high[3], low[0], low[1], low[2], low[3]]))
    }
}

impl SectorFullId {
    pub fn node_id(&self) -> SectorNodeId {
        let bytes = self.0.to_be_bytes();
        SectorNodeId::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    pub fn local_id(&self) -> SectorLocalId {
        let bytes = self.0.to_be_bytes();
        SectorNodeId::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Sector {
    pub sector_id: u64,
    pub capacity: u64, 
    pub chunk_size: usize,
}

#[derive(Deserialize)]
pub struct QuerySectorId {
    pub id: u64
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SectorFilter {
    pub sector_id: Option<u64>
}

impl Default for SectorFilter {
    fn default() -> Self {
        Self {
            sector_id: None
        }
    }
}

impl SectorFilter {
    pub fn from_sector_id(sector_id: u64) -> Self {
        Self { sector_id: Some(sector_id) }
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct SectorNavigator {
    pub page_size: usize, 
    pub page_index: usize
}

impl Default for SectorNavigator {
    fn default() -> Self {
        Self {
            page_size: 1,
            page_index: 0
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct SectorFilterAndNavigator {
    #[serde(flatten)]
    pub filter: SectorFilter,
    #[serde(flatten)] 
    pub navigator: SectorNavigator
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteChunkNavigator {
    pub sector_id: u64, 
    pub offset: u64, 
    pub len: usize, 
    // pub md: HashValue
}

impl Default for WriteChunkNavigator {
    fn default() -> Self {
        Self {
            sector_id: 0, 
            offset: 0, 
            len: 0, 
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ReadChunkNavigator {
    pub sector_id: u64,
    pub offset: u64, 
}

pub type ChunkMeta = WriteChunkNavigator;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ClearChunkNavigator {
    pub sector_id: u64,
    pub offset: u64, 
}
