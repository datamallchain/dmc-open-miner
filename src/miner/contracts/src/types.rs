use serde::{Serialize, Deserialize};
use dmc_tools_common::*;
use dmc_miner_sectors_client::Sector;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AttachContractOptions {
    pub order: DmcOrder, 
    pub sector: Sector,
    pub block_number: u64, 
    pub tx_index: u32
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UpdateContractOptions {
    pub order: DmcOrder, 
    pub challenge: Option<DmcChallenge>, 
    pub sector: Option<Sector>,
    pub block_number: u64, 
    pub tx_index: u32
}

#[derive(Debug)]
pub struct TxPosition {
    pub block_number: u64,
    pub tx_index: u32
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ContractChainOrderCanceledOptions {
    pub order_id: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, sqlx::FromRow)]
pub struct ContractSector {
    pub sector_id: u64, 
    pub sector_offset_start: u64, 
    pub sector_offset_end: u64,  
    #[sqlx(try_from = "u32")]
    pub chunk_size: usize, 
}

impl Default for ContractSector {
    fn default() -> Self {
        Self {
            sector_id: 0, 
            sector_offset_start: 0, 
            sector_offset_end: 0, 
            chunk_size: 0, 
        }
    }
}

impl ContractSector {
    pub fn total_size(&self) -> u64 {
        self.sector_offset_end - self.sector_offset_start
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ContractPreparingState {
    pub raw_tx: Vec<u8>, 
    pub merkel_stub: DmcMerkleStub
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ContractState {
    Unknown, 
    Applying,  
    Refused, 
    Writing {
        sector: ContractSector, 
        writen: u64
    }, 
    Calculating {
        sector: ContractSector,
        writen: u64, 
        calculated: u32
    }, 
    Calculated {
        merkle: DmcMerkleStub, 
        sector: ContractSector,
        writen: u64, 
    }, 
    PrepareError(String), 
    Storing {
        merkle: DmcMerkleStub, 
        sector: ContractSector, 
        writen: u64, 
    },
    Canceled, 
}


#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Contract {
    pub order_id: u64, 
    pub bill_id: Option<u64>,
    pub update_at: u64, 
    pub state: ContractState,
}

impl Contract {
    pub fn sector(&self) -> Option<&ContractSector> {
        match &self.state {
            ContractState::Writing {
                sector, 
                ..
            } => Some(sector), 
            ContractState::Calculating {
                sector,
                ..
            } => Some(sector), 
            ContractState::Calculated { 
                sector,
                .. 
            } => Some(sector), 
            ContractState::Storing { 
                sector,
                ..
            } => Some(sector),
            _ => None
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ContractFilter {
    pub order_id: Option<u64>,
    pub raw_sector_id: Option<u64>,
    pub bill_id: Option<u64>,
}

impl Default for ContractFilter {
    fn default() -> Self {
        Self {
            order_id: None,
            raw_sector_id: None,
            bill_id: None
        }
    }
}

impl ContractFilter {
    pub fn from_order_id(order_id: u64) -> Self {
        Self { order_id: Some(order_id), raw_sector_id: None, bill_id: None }
    }

    pub fn from_raw_sector(raw_sector_id: u64) -> Self {
        Self { order_id: None, raw_sector_id: Some(raw_sector_id), bill_id: None }
    }

    pub fn from_bill(bill_id: u64) -> Self {
        Self { order_id: None, raw_sector_id: None, bill_id: Some(bill_id) }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContractNavigator {
    pub page_size: usize, 
    pub page_index: usize
}

impl Default for ContractNavigator {
    fn default() -> Self {
        Self {
            page_size: 1, 
            page_index: 0
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct ContractFilterAndNavigator {
    #[serde(flatten)]
    pub filter: ContractFilter,
    #[serde(flatten)] 
    pub navigator: ContractNavigator
}



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OffChainChallenge {
    pub order_id: u64,  
    pub offset: u64,
    pub length: u16
}

#[derive(Serialize, Deserialize)]
pub struct OffChainProof {
    pub order_id: u64,  
    pub offset: u64,
    pub length: u16, 
    pub content: DmcData
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum OnChainChallengeState {
    Waiting, 
    Calculating, 
    Ready(DmcTypedProof),
    Prooved, 
    Expired, 
    Error(String)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OnChainChallenge {
    pub challenge_id: u64, 
    pub order_id: u64, 
    pub start_at: u64, 
    pub params: DmcChallengeParams,
    pub state: OnChainChallengeState,
    pub update_at: u64
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ContractDataNavigator {
    pub order_id: u64, 
    pub offset: u64,  
    pub len: u64
}

impl Default for ContractDataNavigator {
    fn default() -> Self {
        Self {
            order_id: 0, 
            offset: 0, 
            len: 0, 
        }
    }
}

impl ContractDataNavigator {
    pub fn next_chunk(nav: &ContractDataNavigator, chunk_size: usize, offset: u64) -> Self {
        let total = (nav.offset + nav.len as u64) - offset;
        let page = std::cmp::min(total, chunk_size as u64) as usize;
        Self {
            order_id: nav.order_id, 
            offset,  
            len: page as u64,  
        }
    }

    pub fn end(&self) -> u64 {
        self.offset + self.len as u64
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct WriteContractOptions {
    #[serde(flatten)]
    pub navigator: ContractDataNavigator
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RestoreContractOptions {
    #[serde(flatten)]
    pub navigator: ContractDataNavigator
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SectorOccupyOptions {
    pub raw_sector_id: Option<u64>,
    pub bill_id: Option<u64>,
}

impl Default for SectorOccupyOptions {
    fn default() -> Self {
        Self { raw_sector_id: None, bill_id: None }
    }
}

impl SectorOccupyOptions {
    pub fn from_raw_sector(raw_sector_id: u64) -> Self {
        Self { raw_sector_id: Some(raw_sector_id), bill_id: None }
    }

    pub fn from_bill(bill_id: u64) -> Self {
        Self { raw_sector_id: None, bill_id: Some(bill_id) }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SectorOccupy {
    pub occupy: u64,
}