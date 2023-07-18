use serde::{Serialize, Deserialize};
use dmc_tools_common::*;


#[derive(Clone, Debug, Copy, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
pub enum SourceState {  
    PreparingMerkle = 0,
    PreparingStub = 1, 
    Ready = 2, 
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SourceDetailState {
    pub source_id: u64,
    pub state: SourceState,
    pub index: u64,
    pub count: u64,
}

impl Into<u8> for SourceState {
    fn into(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for SourceState {
    type Error = DmcError;
    fn try_from(code: u8) -> DmcResult<Self> {
        match code {
            0 => Ok(Self::PreparingMerkle), 
            1 => Ok(Self::PreparingStub), 
            2 => Ok(Self::Ready), 
            _ => Err(DmcError::new(DmcErrorCode::InvalidData, format!("invalid sector state {}", code)))
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SourceMerkleOptions {
    Ready(DmcMerkleStub),
    Prepare {
        piece_size: u16
    } 
}

impl SourceMerkleOptions {
    pub fn piece_size(&self) -> u16 {
        match self {
            SourceMerkleOptions::Ready(stub) => stub.piece_size,
            SourceMerkleOptions::Prepare { piece_size } => *piece_size
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SourceStubOptions { 
    pub count: u32, 
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateSourceOptions {
    pub source_url: String, 
    pub length: u64, 
    pub merkle: SourceMerkleOptions, 
    pub stub: SourceStubOptions
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SourceStub {
    pub source_id: u64, 
    pub index: u32,
    pub offset: u64, 
    pub length: u16, 
    pub content: DmcData
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MerklePathStub {
    pub source_id: u64, 
    pub index: u32, 
    pub content: Vec<u8>
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DataSource {
    pub source_id: u64,  
    pub length: u64, 
    pub update_at: u64, 
    pub source_url: String, 
    pub merkle_stub: Option<DmcMerkleStub>, 
    pub state: SourceState
}


#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SourceFilter {
    pub source_id: Option<u64>, 
    pub source_url: Option<String>
}

impl Default for SourceFilter {
    fn default() -> Self {
        Self {
            source_id: None, 
            source_url: None
        }
    }
}

impl SourceFilter {
    pub fn from_source_id(source_id: u64) -> Self {
        Self { source_id: Some(source_id), source_url: None }
    }

    pub fn from_source_url(source_url: String) -> Self {
        Self { source_url: Some(source_url), source_id: None }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SourceNavigator {
    pub page_size: usize, 
    pub page_index: usize
}

impl Default for SourceNavigator {
    fn default() -> Self {
        Self {
            page_size: 0,
            page_index: 0
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct SourceFilterAndNavigator {
    #[serde(flatten)]
    pub filter: SourceFilter,
    #[serde(flatten)] 
    pub navigator: SourceNavigator
}

#[derive(Serialize, Deserialize, Default)]
pub struct QuerySource {
    pub source_id: u64
}
