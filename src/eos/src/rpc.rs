use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tide::{Body, http::{Request, self, Url}};
use dmc_tools_common::*;

use crate::{
    types::*, 
    key::*
};

struct RpcImpl {
    host: String
}

#[derive(Clone)]
pub struct EosRpc(Arc<RpcImpl>);


impl std::fmt::Display for EosRpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EosRpc{{host={}}}", self.host())
    }
}

#[derive(Serialize, Deserialize)]
pub struct GetInfoResult {
    pub server_version: String,
    pub chain_id: String,
    pub head_block_num: i64,
    pub last_irreversible_block_num: i64,
    pub last_irreversible_block_id: String,
    pub last_irreversible_block_time: Option<TimePointSec>,
    pub block_cpu_limit: i64,
    pub block_net_limit: i64,
    pub server_version_string: Option<String>,
    pub fork_db_head_block_num: Option<i64>,
    pub fork_db_head_block_id: Option<String>,
    pub server_full_version_string: Option<String>,
    pub first_block_num: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetBlockInfoResult {
    pub timestamp: TimePointSec,
    pub producer: String,
    pub confirmed: i64,
    pub previous: String,
    pub transaction_mroot: String,
    pub action_mroot: String,
    pub schedule_version: i64,
    pub producer_signature: String,
    pub id: String,
    pub block_num: i64,
    pub ref_block_num: i64,
    pub ref_block_prefix: i64,
}


#[derive(Serialize, Deserialize)]
pub struct GetBlockResult {
    pub timestamp: TimePointSec,
    pub producer: String,
    pub confirmed: i64,
    pub previous: String,
    pub transaction_mroot: String,
    pub action_mroot: String,
    pub schedule_version: i64,
    pub producer_signature: String,
    pub id: String,
    pub block_num: i64,
    pub ref_block_prefix: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProducerKey {
    pub producer_name: String,
    pub block_signing_key: String,
}


#[derive(Serialize, Deserialize, Debug)]
pub struct ProducerScheduleType {
    pub version: i64,
    pub producers: Vec<ProducerKey>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockHeader {
    pub timestamp: TimePointSec,
    pub producer: String,
    pub confirmed: i64,
    pub previous: String,
    pub transaction_mroot: String,
    pub action_mroot: String,
    pub schedule_version: i64,
    pub new_producers: Option<ProducerScheduleType>,
    pub header_extensions: Vec<(i64, String)>,
}


#[derive(Serialize, Deserialize)]
pub struct GetRawAbiResult {
    pub account_name: String,
    pub code_hash: String,
    pub abi_hash: String,
    pub abi: String,
}


pub struct BinaryAbi {
    pub account_name: String,
    pub abi: Vec<u8>,
}


#[derive(Serialize, Deserialize)]
pub struct TransactionReceiptHeader {
    pub status: String,
    pub cpu_usage_us: u64,
    pub net_usage_words: u64,
}

#[derive(Serialize, Deserialize)]
pub struct ActionReceipt {
    pub receiver: String,
    pub act_digest: String,
    pub global_sequence: i64,
    pub recv_sequence: i64,
    pub auth_sequence: Vec<(String, i64)>,
    pub code_sequence: i64,
    pub abi_sequence: i64,
}

#[derive(Serialize, Deserialize)]
pub struct AccountDelta {
    pub account: String,
    pub delta: i64,
}

#[derive(Serialize, Deserialize)]
pub struct ProcessedAction {
    pub account: String,
    pub name: String,
    pub authorization: Vec<PermissionLevel>,
    // pub data: Option<D>,
    pub hex_data: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ActionTrace {
    pub action_ordinal: i64,
    pub creator_action_ordinal: i64,
    pub closest_unnotified_ancestor_action_ordinal: i64,
    pub receipt: ActionReceipt,
    pub receiver: String,
    pub act: ProcessedAction,
    pub context_free: bool,
    pub elapsed: i64,
    pub console: String,
    pub trx_id: String,
    pub block_num: i64,
    pub block_time: String,
    pub producer_block_id: Option<String>,
    //pub account_ram_deltas: Vec<AccountDelta>,
    //pub account_disk_deltas: Vec<AccountDelta>,
    // pub except: E,
    pub error_code: Option<i64>,
    // pub return_value: Option<R>,
    pub return_value_hex_data: Option<String>,
    // pub return_value_data: Option<RD>,
    pub inline_traces: Option<Vec<ActionTrace>>
}

#[derive(Serialize, Deserialize)]
pub struct TransactionTrace {
    pub id: String,
    pub block_num: i64,
    pub block_time: String,
    pub producer_block_id: Option<String>,
    pub receipt: Option<TransactionReceiptHeader>,
    pub elapsed: i64,
    pub net_usage: i64,
    pub scheduled: bool,
    pub action_traces: Vec<ActionTrace>,
    pub account_ram_delta: Option<AccountDelta>,
    pub except: Option<String>,
    pub error_code: Option<String>,
    pub bill_to_accounts: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
pub struct TransactResult {
    pub transaction_id: String,
    pub processed: TransactionTrace,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactErrorDetail {
    pub message: String,
    pub file: String,
    pub line_number: u32,
    pub method: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactInternalError {
    pub code: u32, 
    pub name: String, 
    pub what: String, 
    pub details: Vec<TransactErrorDetail>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactError {
    pub code: u16, 
    pub message: String, 
    pub error: TransactInternalError
}

#[derive(Serialize)]
pub struct GetTableRowsReq<'a, T: Serialize> {
    pub json: bool,
    pub code: &'a str,
    pub table: &'a str,
    pub scope: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_position: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encode_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub show_payer: Option<bool>
}

#[derive(Deserialize)]
pub struct GetTableRowsResult<T> {
    pub rows: Vec<T>,
    pub more: bool,
    pub next_key: String,
}


#[derive(Deserialize)]
pub struct Permission {
    pub perm_name: String,
    pub parent: String,
    pub required_auth: Authority,
}

#[derive(Deserialize)]
pub struct AccountResourceInfo {
    pub used: i64,
    pub available: serde_json::Value,
    pub max: serde_json::Value,
    pub last_usage_update_time: Option<String>,
    pub current_used: Option<i64>,
}

#[derive(Deserialize)]
pub struct ResourceOverview {
    pub owner: String,
    pub ram_bytes: i64,
    pub net_weight: String,
    pub cpu_weight: String,
}

#[derive(Deserialize)]
pub struct ResourceDelegation {
    pub from: String,
    pub to: String,
    pub net_weight: String,
    pub cpu_weight: String,
}

#[derive(Deserialize)]
pub struct RefundRequest {
    pub owner: String,
    pub request_time: String,
    pub net_amount: String,
    pub cpu_amount: String,
}


#[derive(Deserialize)]
pub struct GetAccountResult {
    pub account_name: String,
    pub head_block_num: i64,
    pub head_block_time: String,
    pub privileged: bool,
    pub last_code_update: String,
    pub created: String,
    pub core_liquid_balance: Option<String>,
    pub ram_quota: i64,
    pub net_weight: i64,
    pub cpu_weight: i64,
    pub net_limit: AccountResourceInfo,
    pub cpu_limit: AccountResourceInfo,
    pub ram_usage: i64,
    pub permissions: Vec<Permission>,
    pub total_resources: Option<ResourceOverview>,
    pub self_delegated_bandwidth: Option<ResourceDelegation>,
    pub refund_request: Option<RefundRequest>,
    pub voter_info: serde_json::Value,
    pub rex_info: serde_json::Value,
}


impl EosRpc {
    pub fn new(host: String) -> Self {
        Self(Arc::new(RpcImpl {
            host
        }))
    }

    fn host(&self) -> &str {
        &self.0.host
    }
}

impl EosRpc {
    pub async fn get_info(&self) -> DmcResult<GetInfoResult> {
        let url = Url::parse(&format!("https://{}/v1/chain/get_info", self.host()))?;
        let req = Request::new(http::Method::Post, url);
        let mut resp = surf::client().send(req).await?;
        let result = resp.body_json().await?;
        Ok(result)
    }

    pub async fn get_block_info(&self, block_num: i64) -> DmcResult<GetBlockInfoResult> {
        let url = Url::parse(&format!("https://{}/v1/chain/get_block_info", self.host()))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&serde_json::json!({
            "block_num": block_num
        }))?);
        let mut resp = surf::client().send(req).await?;
        let result = resp.body_json().await?;
        Ok(result)
    }

    pub async fn get_block(&self, block_num_or_id: String) -> DmcResult<GetBlockResult> {
        let url = Url::parse(&format!("https://{}/v1/chain/get_block", self.host()))?;
       
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&serde_json::json!({
            "block_num_or_id": block_num_or_id
        }))?);
        let mut resp = surf::client().send(req).await?;
        let result = resp.body_json().await?;
        Ok(result)
    }

    pub async fn push_transaction(&self, serialized: &[u8], signatures: &[EosSignature]) -> DmcResult<Result<TransactResult, TransactError>> {
        let url = Url::parse(&format!("https://{}/v1/chain/push_transaction", self.host()))?;
        let signatures: Vec<String> = signatures.iter().map(|s| s.to_string()).collect();
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&serde_json::json!(serde_json::json! ({
            "signatures": signatures,
            "compression": 0,
            "packed_context_free_data": "".to_owned(),
            "packed_trx": hex::encode(serialized).to_uppercase()
        })))?);
        let mut resp = surf::client().send(req).await?;
        if resp.status().is_success() {
            let result = resp.body_json().await?;
            Ok(Ok(result))
        } else {
            let error = resp.body_json().await?;
            Ok(Err(error))
        }
    }

    pub async fn get_table_rows<'a, S: Serialize, T: for <'de> Deserialize<'de>>(&self, rows_req: &GetTableRowsReq<'a, S>) -> DmcResult<GetTableRowsResult<T>> {
        let url = Url::parse(&format!("https://{}/v1/chain/get_table_rows", self.host()))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&rows_req)?);
        let mut resp = surf::client().send(req).await?;
        let result = resp.body_json().await?;
        Ok(result)
    }

    pub async fn get_account(&self, account_name: &str) -> DmcResult<GetAccountResult> {
        let url = Url::parse(&format!("https://{}/v1/chain/get_account", self.host()))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&serde_json::json!({
            "account_name": account_name
        }))?);
        let mut resp = surf::client().send(req).await?;
        let result = resp.body_json().await?;
        Ok(result)
    }
}