use serde::{Serialize, Deserialize};
use dmc_tools_common::*;
use dmc_user_sources::*;


#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ContractState {
    Unknown, 
    Refused,
    Applying,  
    Preparing, 
    Writing, 
    Storing, 
    Canceled, 
    Error(String)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SourceChallengeStub {
    pub piece_index: u64, 
    pub nonce: String, 
    pub hash: HashValue,  
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SourceChallengeState {
    Waiting, 
    Created(SourceChallengeStub),
    Commited,
    Prooved, 
    Expired,
    Failed, 
    Error(String)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum MerkleChallengeState {

}


#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum OnChainChallengeState {
    SourceStub(SourceChallengeState), 
    MerklePath(MerkleChallengeState)
}


#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OnChainChallenge {
    pub order_id: u64,
    pub index: u32, 
    pub state: OnChainChallengeState, 
    pub update_at: u64
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Contract {
    pub order_id: u64, 
    pub process_id: Option<u32>, 
    pub update_at: u64, 
    pub miner_endpoint: String,
    pub source_id: u64, 
    pub state: ContractState, 
    pub challenge: Option<OnChainChallenge>
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ContractFilter {
    pub order_id: Option<u64>, 
}

impl Default for ContractFilter {
    fn default() -> Self {
        Self {
            order_id: None
        }
    }
}

impl ContractFilter {
    pub fn from_order_id(order_id: u64) -> Self {
        Self { order_id: Some(order_id) }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ContractNavigator {
    pub page_size: usize, 
    pub page_index: usize
}

impl Default for ContractNavigator {
    fn default() -> Self {
        Self {
            page_size: 0,
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

#[derive(Serialize, Deserialize, Debug)]
pub struct ApplyContractOptions {
    pub order_id: u64, 
    pub source_id: u64, 
    pub block_number: u64,
    pub tx_index: u32
}


#[derive(Serialize, Deserialize, Debug)]
pub struct QueryContract {
    pub order_id: u64
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UpdateContractOptions {
    pub order: DmcOrder, 
    pub challenge: Option<DmcChallenge>, 
    pub block_number: u64, 
    pub tx_index: u32
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum OffChainChallengeState {
    Creating,
    Requesting(SourceStub), 
    Prooved,
    Error
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct OffChainChallenge {
    pub order_id: u64,
    pub index: u32,
    pub state: OffChainChallengeState,
    pub update_at: u64
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct OnChainChallengeOptions {
    pub order_id: u64, 
    pub challenge: Option<DmcChallengeParams>
}
