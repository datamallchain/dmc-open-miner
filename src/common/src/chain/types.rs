use std::fmt::{Display, Formatter};
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use serde::{Serialize, Deserialize};
use crate::{
    utils::*, 
    error::*
};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DmcBillOptions {
    pub asset: u64, 
    pub price: u64,     // solidity的Token单位必须是整数，外边展示时可以展示成n位小数
    pub pledge_rate: Option<u32>, 
    pub min_asset: Option<u64>, // TODO： 这里是不是要换个名字？这个值表示购买时的asset必须是min_asset的倍数
    pub duration: u32
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DmcAsset {
    pub amount: f64,
    pub name: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DmcBill {
    pub bill_id: u64, 
    pub miner: String, 
    pub asset: u64, 
    pub price: u64,
    pub pledge_rate: u32, 
    pub min_asset: u64, 
    pub expire_at: u64, 
}

impl Display for DmcBill {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "bill_id {}, miner {}, asset {}, price {}, pledge_rate {}", self.bill_id, self.miner, self.asset, self.price, self.pledge_rate)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct DmcMerkleStub {
    pub piece_size: u16, 
    pub leaves: u64, 
    pub root: HashValue
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum DmcOrderState {
    Preparing {
        miner: Option<DmcMerkleStub>, 
        user: Option<DmcMerkleStub>, 
    }, 
    Storing(DmcMerkleStub),  
    Canceled
}

// pub enum DmcOrderChallengeState {
    
// }

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DmcOrderOptions {
    pub bill_id: u64,  
    pub asset: u64, 
    pub duration: u32,  
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DmcPrepareOrderOptions {
    pub order_id: u64, 
    pub merkle_stub: DmcMerkleStub
}


#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DmcOrder {
    pub order_id: u64, 
    pub bill_id: u64, 
    pub user: String, 
    pub miner: String, 
    pub asset: u64, 
    pub duration: u32, 
    pub price: u64,
    pub pledge_rate: u32, 
    pub state: DmcOrderState, 
    // pub challenge_state: DmcOrderChallengeState, 
    pub start_at: u128
}

impl DmcOrder {
    pub fn capacity(&self) -> u64 {
        self.asset * 1024 * 1024 * 1024
    }
}


#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DmcEvent {
    pub block_number: u64,
    pub tx_index: u32, 
    pub event: DmcTypedEvent
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum DmcTypedEvent {
    BillChanged(DmcBill),
    BillCanceled(u64),
    OrderChanged(DmcOrder),
    OrderCanceled(u64),
    ChallengeChanged(DmcOrder, DmcChallenge),
    EmptyEvent(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum DmcChallengeTypeCode {
    SourceStub = 0, 
    MerklePath = 1
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DmcChallengeTypedOptions {
    SourceStub {
        piece_index: u64, 
        nonce: String, 
        hash: HashValue
    }, 
    MerklePath {
        piece_index: u64, 
        pathes: Vec<HashValue>
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DmcChallengeOptions {
    pub order_id: u64, 
    pub options: DmcChallengeTypedOptions
}



#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum DmcChallengeState {
    Waiting, 
    Prooved, 
    Expired, 
    Failed,
    Invalid
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DmcChallengeParams {
    SourceStub {
        piece_index: u64, 
        nonce: String, 
        hash: HashValue
    }, 
    MerklePath {
        piece_index: u64
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DmcChallenge {
    pub challenge_id: u64, 
    pub order_id: u64, 
    pub start_at: u128,
    pub state: DmcChallengeState, 
    pub params: DmcChallengeParams
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DmcTypedProof {
    SourceStub {
       hash: HashValue
    }, 
    MerklePath {
        data: DmcData, 
        pathes: Vec<HashValue>
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DmcProofOptions {
    pub order_id: u64, 
    pub challenge_id: u64, 
    pub proof: DmcTypedProof 
}



#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(into="String", from="String")]
pub struct TimePointSec {
    inner: String
}

impl From<String> for TimePointSec {
    fn from(inner: String) -> Self {
        Self {
            inner
        }
    }
}

impl Into<String> for TimePointSec {
    fn into(self) -> String {
        self.inner
    }
}


impl TimePointSec {
    pub fn from_now() -> Self {
        Self::from_secs(chrono::Utc::now().timestamp())
    }

    pub fn as_str(&self) -> &str {
        &self.inner
    }

    pub fn as_secs(&self) -> DmcResult<i64> {
        let time = DateTime::parse_from_rfc3339(format!("{}Z", self.as_str()).as_str()).map_err(|e| {
            DmcError::new(DmcErrorCode::InvalidParam, format!("Invalid time format {} err {}", self.as_str(), e))
        })?;
        
        Ok(time.timestamp())
    }

    pub fn from_millis(us: i64) -> Self {
        Self {
            inner: Utc.timestamp_millis_opt(us).unwrap().to_rfc3339_opts(SecondsFormat::Millis, true).trim_end_matches("Z").to_string()
        }
        
    }

    pub fn from_secs(sec: i64) -> Self {
        Self {
            inner: Utc.timestamp_opt(sec, 0).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true).trim_end_matches("Z").to_string()
        }
    }

    pub fn offset(&self, secs: i64) -> Self {
        Self::from_secs(self.as_secs().unwrap() + secs)
    }
}


#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(into="Vec<u8>", try_from="Vec<u8>")]
pub struct DmcData(Vec<u8>);

impl DmcData {
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Into<Vec<u8>> for DmcData {
    fn into(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for DmcData {
    fn from(d: Vec<u8>) -> Self {
        Self(d)
    }
}

impl From<&[u8]> for DmcData {
    fn from(d: &[u8]) -> Self {
        Self(Vec::from(d))
    }
}


impl std::fmt::Debug for DmcData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "data {} bytes", self.0.len())
    }
}



