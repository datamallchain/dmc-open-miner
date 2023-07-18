use async_trait::*;
use serde::{Serialize, Deserialize};
use crate::error::*;
use super::types::*;


#[derive(Debug, Serialize, Deserialize)]
pub struct DmcPendingResult<T: std::fmt::Debug> {
    pub block_number: u64, 
    pub tx_index: u32, 
    pub result: Result<T, String>
}

#[async_trait]
pub trait DmcPendingBill: Send + Sync + AsRef<[u8]> {
    fn tx_id(&self) -> &[u8];
    async fn wait(&self) -> DmcResult<DmcPendingResult<DmcBill>>;
}

#[async_trait]
pub trait DmcPendingOrder: Send + Sync + AsRef<[u8]> {
    fn tx_id(&self) -> &[u8];
    async fn wait(&self) -> DmcResult<DmcPendingResult<DmcOrder>>;
}

#[async_trait]
pub trait DmcEventListener: Send + Sync {
    async fn next(&self) -> DmcResult<DmcEvent>;
}


#[async_trait]
pub trait DmcChainClient: 'static + Clone + Send + Sync {
    type EventListener: DmcEventListener;
    async fn get_bill_by_id(&self, bill_id: u64) -> DmcResult<Option<DmcBill>>;
    async fn get_order_by_id(&self, order_id: u64) -> DmcResult<Option<DmcOrder>>;

    async fn get_apply_method(&self, account: &str) -> DmcResult<Option<String>>;
    async fn event_listener(&self, start_block: Option<u64>) -> Self::EventListener;

    async fn verify(&self, from: &str, data: &[u8], sign: &str) -> DmcResult<bool>;
}

#[async_trait]
pub trait DmcChainAccountClient: DmcChainClient {
    fn account(&self) -> &str;
    async fn get_available_assets(&self) -> DmcResult<Vec<DmcAsset>>;

    async fn sign(&self, data: &[u8]) -> DmcResult<String>;
    async fn set_apply_method(&self, method: String) -> DmcResult<DmcResult<()>>;

    type PendingBill: DmcPendingBill;
    async fn create_bill(&self, options: DmcBillOptions) -> DmcResult<Self::PendingBill>;
    async fn load_bill(&self, pending: &[u8]) -> DmcResult<Self::PendingBill>;
    async fn finish_bill(&self, bill_id: u64) -> DmcResult<DmcResult<()>>;

    fn support_modify_bill() -> bool {
        false
    }
    async fn modify_bill_asset(&self, _bill_id: u64, _inc: i64) -> DmcResult<DmcResult<()>> {
        unimplemented!()
    }

    type PendingOrder: DmcPendingOrder;
    async fn create_order(&self, options: DmcOrderOptions) -> DmcResult<Self::PendingOrder>;
    async fn load_order(&self, pending: &[u8]) -> DmcResult<Self::PendingOrder>;
    async fn prepare_order(&self, options: DmcPrepareOrderOptions) -> DmcResult<DmcResult<()>>;
    async fn finish_order(&self, order_id: u64) -> DmcResult<DmcResult<f64>>;

    fn supported_challenge_type() -> DmcChallengeTypeCode;
    async fn challenge(&self, options: DmcChallengeOptions) -> DmcResult<DmcResult<()>>;
    async fn proof(&self, options: DmcProofOptions) -> DmcResult<DmcResult<()>>;

    async fn miner_listener(&self, start_block: Option<u64>) -> Self::EventListener;
    async fn user_listener(&self, start_block: Option<u64>) -> Self::EventListener;
}

#[async_trait]
pub trait DmcChainClientFactory: 'static + Send + Sync {
    type ChainClient: DmcChainAccountClient;
    fn new_client(&self, account: &str) -> DmcResult<Self::ChainClient>;
}