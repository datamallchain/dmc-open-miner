use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use dmc_tools_common::*;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(try_from="u16", into="u16")]
pub enum JournalEventType {
    SectorCreated = 10,  

    BillCreated = 20, 
    BillFailed = 21,
    BillCanceled = 22, 
    
    OrderApplied = 30, 
    OrderFailed = 31, 
    OrderWriten = 32,
    OrderStored = 33, 
    OrderCanceled = 34, 
    OrderRefused = 39, 

    // OffchainChallengeOk = 50, 
    // OffchainChallengeFailed = 51,
    OnchainChallengeCommited = 52, 
    OnchainChallengeProoved = 53, 
    // OnchainChallengeFailed = 54,
    // OnchainChallengeExpired = 55
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JournalEvent {
    pub sector_id: u64, 
    pub order_id: Option<u64>,  
    pub event_type: JournalEventType, 
    pub event_params: Option<String>, 
}

impl Into<u16> for JournalEventType {
    fn into(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for JournalEventType {
    type Error = DmcError;
    fn try_from(i: u16) -> DmcResult<Self> {
        match i {
            10 => Ok(Self::SectorCreated), 

            20 => Ok(Self::BillCreated), 
            21 => Ok(Self::BillFailed),
            22 => Ok(Self::BillCanceled), 

            30 => Ok(Self::OrderApplied), 
            31 => Ok(Self::OrderFailed), 
            32 => Ok(Self::OrderWriten),
            33 => Ok(Self::OrderStored), 
            34 => Ok(Self::OrderCanceled), 
            35 => Ok(Self::OrderRefused),  

            // 50 => Ok(Self::OffchainChallengeOk),
            // 51 => Ok(Self::OffchainChallengeFailed),
            52 => Ok(Self::OnchainChallengeCommited),
            53 => Ok(Self::OnchainChallengeProoved),
            // 54 => Ok(Self::OnchainChallengeFailed),
            // 55 => Ok(Self::OnchainChallengeExpired),
            _ => Err(DmcError::new(DmcErrorCode::InvalidParam, "invalid event type"))
        }
    }
}

impl JournalEvent {
    pub fn set_params(&mut self, value: &impl Serialize) -> DmcResult<()> {
        self.event_params = Some(serde_json::to_string(value)?);
        Ok(())
    }

    pub fn get_params<'a, T: serde::Deserialize<'a>>(&'a self) -> DmcResult<T> {
        let value = serde_json::from_str(self.event_params.as_ref().ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no params"))?)?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JournalLog {
    pub log_id: u64, 
    #[serde(flatten)] 
    pub event: JournalEvent, 
    pub timestamp: DateTime<Utc>
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JournalFilter {
    pub event_type: Option<Vec<JournalEventType>>
}

impl Default for JournalFilter {
    fn default() -> Self {
        Self { event_type: None }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JournalNavigator {
    pub from_id: Option<u64>, 
    pub page_size: usize
}

impl Default for JournalNavigator {
    fn default() -> Self {
        Self {
            from_id: None,
            page_size: 1
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct JournalFilterAndNavigator {
    #[serde(flatten)]
    pub filter: JournalFilter, 
    #[serde(flatten)]
    pub navigator: JournalNavigator
}





