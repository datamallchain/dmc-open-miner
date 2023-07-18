use serde::{Serialize, Deserialize};
use dmc_tools_common::*;
use dmc_spv::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SectorOrderError {
    pub block_number: u64, 
    pub tx_index: u32, 
    pub error: String
}

impl std::fmt::Display for SectorOrderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "order failed at {}:{}, {}", self.block_number, self.tx_index, self.error)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SectorState {
    Waiting, 
    PreOrder(DmcData/*signed raw transation*/), 
    OrderError(SectorOrderError), 
    PostOrder(SectorOrderInfo), 
    Removed, 
}

impl std::fmt::Display for SectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SectorState::Waiting => {write!(f, "waiting")}
            SectorState::PreOrder(_) => {write!(f, "preorder, wait tx on chain")}
            SectorState::OrderError(error) => {write!(f, "error {}", error)}
            SectorState::PostOrder(info) => {write!(f, "{}", info)}
            SectorState::Removed => {write!(f, "removed")}
        }

    }
}

#[derive(Deserialize)]
pub struct QueryAccount {
    pub account: String
}


#[derive(Deserialize)]
pub struct QuerySectorId {
    pub id: u64
}

#[derive(Deserialize)]
pub struct QueryBlackList {
    pub account: Option<String>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SectorOrderInfo {
    pub bill_id: u64,
    pub order_id: u64, 
    pub block_number: u64,
    pub tx_index: u32
}

impl std::fmt::Display for SectorOrderInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "order {} in bill {}, at block {}:{}", self.order_id, self.bill_id, self.block_number, self.tx_index)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SectorOrderFilter{
    pub bill_filter: FindBillFilter,
    pub duration: u32
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SectorOrderOptions {
    Order(SectorOrderInfo), 
    Bill(DmcOrderOptions), 
    Filter(SectorOrderFilter)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateSectorOptions {
    pub source_id: u64,  
    pub order: SectorOrderOptions
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ApplyContractState {
    Waiting, 
    Applying, 
    Applied
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Sector {
    pub source_id: u64, 
    pub asset: u64, 
    pub order: SectorOrderOptions, 
    pub state: SectorState, 
    pub apply: ApplyContractState, 
    pub update_at: u64
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SectorWithId {
    pub sector_id: u64,
    #[serde(flatten)]
    pub info: Sector
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SectorFilter {
    pub sector_id: Option<u64>, 
    pub order_id: Option<u64>
}

impl Default for SectorFilter {
    fn default() -> Self {
        Self {
            sector_id: None, 
            order_id: None
        }
    }
}


impl SectorFilter {
    pub fn from_sector_id(sector_id: u64) -> Self {
        Self { sector_id: Some(sector_id), order_id: None }
    }

    pub fn from_order(order_id: u64) -> Self {
        Self { sector_id: None, order_id: Some(order_id) }
    }
}

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub struct BlackListFilter {
    account: Option<String>
}

impl BlackListFilter {
    pub fn from_account(account: &String) -> Self {
        Self { account: Some(account.clone()) }
    }
}

#[derive(Serialize, Deserialize)]
pub struct BlackListNavigator {
    pub page_size: usize, 
    pub page_index: usize
}

#[derive(Serialize, Deserialize)]
pub struct BlackListFilterAndNavigator {
    #[serde(flatten)]
    pub filter: BlackListFilter,
    #[serde(flatten)] 
    pub navigator: BlackListNavigator
}
