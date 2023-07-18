use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct FindBillFilter {
    pub miner: Option<String>,
    pub min_asset: Option<u64>,
    pub max_asset: Option<u64>,
    pub min_price: Option<u64>,
    pub max_price: Option<u64>,
    pub min_pledge_rate: Option<u64>,
    pub max_pledge_rate: Option<u64>,
    pub min_min_asset: Option<u64>,
    pub max_min_asset: Option<u64>,
    pub expire_after: Option<u64>   
}

#[derive(Serialize, Deserialize)]
pub struct FindBillNavigator {
    pub page_index: usize, 
    pub page_size: usize
}

impl Default for FindBillNavigator {
    fn default() -> Self {
        Self {
            page_index: 0,
            page_size: 1
        }
    }
}


#[derive(Serialize, Deserialize)]
pub struct FindBillFilterAndNavigator {
    #[serde(flatten)]
    pub filter: FindBillFilter,
    #[serde(flatten)] 
    pub navigator: FindBillNavigator
}