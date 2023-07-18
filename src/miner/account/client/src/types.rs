use serde::{Serialize, Deserialize};
use dmc_tools_common::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SectorBillError {
    pub block_number: u64, 
    pub tx_index: u32, 
    pub error: String
}

impl std::fmt::Display for SectorBillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bill failed at {}:{}, {}", self.block_number, self.tx_index, self.error)
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SectorBillInfo {
    pub bill_id: u64, 
    pub block_number: u64,
    pub tx_index: u32
}

impl std::fmt::Display for SectorBillInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bill {}, at block {}:{}", self.bill_id, self.block_number, self.tx_index)
    }
}



#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SectorState {
    Waiting, 
    PreBill(DmcData/*signed raw transation*/), 
    BillError(SectorBillError), 
    PostBill(SectorBillInfo), 
    Removed(Option<SectorBillInfo>), 
}

impl std::fmt::Display for SectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SectorState::Waiting => {write!(f, "waiting")}
            SectorState::PreBill(_) => {write!(f, "preorder, wait tx on chain")}
            SectorState::BillError(error) => {write!(f, "error {}", error)}
            SectorState::PostBill(info) => {write!(f, "{}", info)}
            SectorState::Removed(_) => {write!(f, "removed")}
        }
    }
}

#[derive(Deserialize)]
pub struct QuerySectorId {
    pub id: u64
}

#[derive(Deserialize)]
pub struct QueryAccount {
    pub account: String
}

#[derive(Deserialize)]
pub struct QueryBlackList {
    pub account: Option<String>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateSectorOptions {
    pub raw_sector_id: u64, 
    pub bill: DmcBillOptions
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RemoveSectorOptions {
    pub sector_id: Option<u64>,
    pub bill_id: Option<u64>,
}

impl Default for RemoveSectorOptions {
    fn default() -> Self {
        Self { sector_id: None, bill_id: None }
    }
}

impl RemoveSectorOptions {
    pub fn from_sector(sector_id: u64) -> Self {
        Self { sector_id: Some(sector_id), bill_id: None }
    }

    pub fn from_bill(bill_id: u64) -> Self {
        Self { sector_id: None, bill_id: Some(bill_id) }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Sector {
    pub raw_sector_id: u64,
    pub bill_options: DmcBillOptions, 
    pub state: SectorState, 
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
    pub raw_sector_id: Option<u64>, 
    pub bill_id: Option<u64>
}

impl Default for SectorFilter {
    fn default() -> Self {
        Self {
            sector_id: None, 
            bill_id: None,
            raw_sector_id: None,
        }
    }
}


impl SectorFilter {
    pub fn from_sector_id(sector_id: u64) -> Self {
        Self { sector_id: Some(sector_id), bill_id: None, raw_sector_id: None }
    }

    pub fn from_bill(bill_id: u64) -> Self {
        Self { sector_id: None, bill_id: Some(bill_id), raw_sector_id: None }
    }

    pub fn from_raw_sector(raw_sector_id: u64) -> Self {
        Self { sector_id: None, raw_sector_id: Some(raw_sector_id), bill_id: None }
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
