use log::*;
use std::{
    time::Duration, 
    sync::{Arc, Mutex}, 
    collections::LinkedList 
};
use futures::future::{AbortHandle, AbortRegistration};
use serde::{Serialize, Deserialize};
use sha2::{digest::FixedOutput, Digest, Sha256};
use dmc_tools_common::*;
use super::{
    buffer::*, 
    key::*, 
    types::*, 
    rpc::*, 
    listener::*
};

#[derive(Serialize, Deserialize, Clone)]
pub struct EosClientConfig {
    pub host: String, 
    pub chain_id: String, 
    pub account: Option<(String, String)>, 
    pub retry_rpc_interval: Duration, 
    pub trans_expire: Duration, 
    pub piece_size: u16,
    pub permission: String
}

struct ClientImpl {
    config: EosClientConfig, 
    rpc: EosRpc, 
    // cached_abis: RwLock<HashMap<String, Arc<CachedAbi>>>,
    chain_id: String, 
    account: Option<Name>, 
    key: Option<EosPrivateKey>, 
    pending_bills: EosTxSerilizer<DmcBill, EosPendingBill>, 
    pending_orders: EosTxSerilizer<DmcOrder, EosPendingOrder>
}

#[derive(Clone)]
pub struct EosClient(Arc<ClientImpl>);

struct EosPendingTx {
    client: EosClient, 
    tx_id: HashValue, 
    serialized: Vec<u8>, 
}

impl std::fmt::Display for EosPendingTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EosPendingTx:{{tx_id={:?}}}", self.tx_id)
    }
}

impl AsRef<[u8]> for EosPendingTx {
    fn as_ref(&self) -> &[u8] {
        self.serialized.as_slice()
    }
}


impl EosPendingTx {
    async fn execute(client: EosClient, builder: TransactionBuilder) -> DmcResult<DmcResult<()>> {
        let tx = builder.build(client.rpc(), client.config().trans_expire.as_secs() as i64).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} build transaction, failed, err={}", client, err))?;
        let pending = EosPendingTx::from_tx(client.clone(), tx)
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create pending, failed, err={}", client, err))?;
        let result = pending.wait().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} wait, failed, err={}", pending, err))?;
        Ok(result.result.map_err(|err| {
            DmcError::new(DmcErrorCode::Failed, err)
        }).map(|_| {
            ()
        }))
    }

    fn from_tx(client: EosClient, tx: Transaction) -> DmcResult<Self> {
        let mut buffer = EosSerialWrite::new();
        tx.eos_serialize(&mut buffer)?;
        Ok(Self::from_serialized(client, buffer.into()))
    }

    fn from_serialized(client: EosClient, serialized: Vec<u8>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        let tx_id = HashValue::from(hasher.finalize_fixed());

        Self {
            client, 
            tx_id, 
            serialized
        }
    }

    fn tx_id(&self) -> &[u8] {
        self.tx_id.as_slice()
    }

    async fn wait(&self) -> DmcResult<DmcPendingResult<()>> {
        info!("{} sending", self);
        let signatures = self.client.sign_transaction(self.as_ref()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} send, failed, err={}", self, err))?;
        loop {
            match self.client.rpc().push_transaction(self.as_ref(), &signatures).await {
                Ok(result) => {
                    match result {
                        Ok(result) => {
                            let result = if let Some(receipt) = result.processed.receipt {
                                match receipt.status.as_str() {
                                    "executed" => {
                                        Ok(DmcPendingResult {
                                            block_number: result.processed.block_num as u64, 
                                            tx_index: 0, 
                                            result: Ok(())
                                        })
                                    },
                                    "soft_fail" | "hard_fail" => {
                                        Ok(DmcPendingResult {
                                            block_number: result.processed.block_num as u64, 
                                            tx_index: 0, 
                                            result: Err(result.processed.error_code.clone().unwrap_or("unknown".to_owned()))
                                        })
                                    }, 
                                    "expired" => {
                                        Err(dmc_err!(DmcErrorCode::Expired, "{} send, failed, err={}", self, "expired"))
                                    },
                                    _ => unreachable!("{} send, failed, unexpected, err={} {}", self, "invalid receipt", receipt.status.as_str())
                                }
                            } else {
                                unreachable!("{} send, failed, unexpected, err={}", self, "result without receipt")
                            };
        
                            info!("{} send, success, result={:?}", self, result);
                            break result;
                        }, 
                        Err(err) => {
                            error!("{} send, returned error, err={:?}", self, err);
                            // duplicate transaction, trait as executed
                            if err.error.code == 3040008 {
                                break Ok(DmcPendingResult {
                                    block_number: 0, 
                                    tx_index: 0, 
                                    result: Ok(())
                                });
                            } else {
                                let result = if err.error.code >= 3040000 && err.error.code < 3050000 {
                                    Err(dmc_err!(DmcErrorCode::Failed, "{}", err.error.details[0].message.clone()))
                                } else {
                                    Ok(DmcPendingResult {
                                        block_number: 0, 
                                        tx_index: 0, 
                                        result: Err(err.error.details[0].message.clone())
                                    })
                                };
    
                                error!("{} send, sucess, result={:?}", self, result);
                                break result;
                            }
                        }
                    }
                },
                Err(err) => {
                    error!("{} send, failed, wait retry, err={}", self, err);
                    let _ = async_std::task::sleep(self.client.config().retry_rpc_interval).await;
                }
            }
        }
    }
}

#[async_trait::async_trait]
trait EosSerializedTx: Clone {
    type Result: std::fmt::Debug;
    async fn execute(&self) -> DmcResult<DmcPendingResult<Self::Result>>;
}



struct EosSerializedTxResult<T: std::fmt::Debug> {
    inner: Arc<Mutex<Option<DmcResult<DmcPendingResult<T>>>>> 
}

impl<T: std::fmt::Debug> Clone for EosSerializedTxResult<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl<T: std::fmt::Debug> EosSerializedTxResult<T> {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None))
        }
    }

    fn set(&self, result: DmcResult<DmcPendingResult<T>>) {
        *self.inner.lock().unwrap() = Some(result)
    }

    fn take(self) -> DmcResult<DmcPendingResult<T>> {
        let mut result = None;
        std::mem::swap(&mut result, &mut *self.inner.lock().unwrap());
        result.unwrap()
    }
}


struct EosSerializedTxWaiter<T: std::fmt::Debug> {
    waiter: AbortRegistration, 
    result: EosSerializedTxResult<T>
}

impl<T: std::fmt::Debug> EosSerializedTxWaiter<T> {
    async fn wait(self) -> DmcResult<DmcPendingResult<T>> {
        StateWaiter::wait(self.waiter, || self.result.take()).await 
    }
}


struct EosSerializedTxWaker<T: std::fmt::Debug> {
    waker: AbortHandle, 
    result: EosSerializedTxResult<T>
}

impl<T: std::fmt::Debug> Clone for EosSerializedTxWaker<T> {
    fn clone(&self) -> Self {
        Self {
            waker: self.waker.clone(),
            result: self.result.clone()
        }
    }
}

impl<T: std::fmt::Debug> EosSerializedTxWaker<T> {
    fn new_pair() -> (Self, EosSerializedTxWaiter<T>)  {
        let (waker, waiter) = AbortHandle::new_pair();
        let result = EosSerializedTxResult::new();
        (Self { waker, result: result.clone() }, EosSerializedTxWaiter { waiter, result })
    }

    fn wake(self, result: DmcResult<DmcPendingResult<T>>) {
        self.result.set(result);
        self.waker.abort();
    }
}


struct EosTxSerilizer<T: std::fmt::Debug, TX: EosSerializedTx<Result=T>>(Mutex<LinkedList<(TX, EosSerializedTxWaker<T>)>>);


impl<T: std::fmt::Debug, TX: EosSerializedTx<Result=T>> EosTxSerilizer<T, TX> {
    fn new() -> Self {
        Self(Mutex::new(LinkedList::new()))
    }

    fn add(&self, pending: TX) -> (EosSerializedTxWaiter<T>, bool) {
        let (waker, waiter) = EosSerializedTxWaker::new_pair();
        let exec = {
            let mut queue = self.0.lock().unwrap();
            queue.push_back((pending, waker));
            if queue.len() == 1 {
                true
            } else {
                false
            }
        };
        (waiter, exec)
    }

    async fn exec(&self) {
        let mut exec: Option<(TX, EosSerializedTxWaker<T>)> = {
            self.0.lock().unwrap().front().cloned()
        };
        loop {
            let (pending, waker) = exec.unwrap();
            let result = pending.execute().await;
            waker.wake(result);

            exec = {
                let mut queue = self.0.lock().unwrap();
                queue.pop_front();
                queue.front().cloned()
            };

            if exec.is_none() {
                break;
            }
        }
    }
}

#[derive(Clone)]
pub struct EosPendingBill {
    client: EosClient, 
    tx: Arc<EosPendingTx>,
}

impl AsRef<[u8]> for EosPendingBill {
    fn as_ref(&self) -> &[u8] {
        self.tx.as_ref().as_ref()
    }
}

#[async_trait::async_trait]
impl EosSerializedTx for EosPendingBill {
    type Result = DmcBill;

    async fn execute(&self) -> DmcResult<DmcPendingResult<DmcBill>> {
        let pending = self.tx.wait().await?;
        if pending.result.is_err() {
            let err = pending.result.unwrap_err();
            return Ok(DmcPendingResult {
                block_number: pending.block_number, 
                tx_index: pending.tx_index, 
                result: Err(err)
            })
        }
        
        loop {
            match self.client.rpc().get_table_rows::<_, EosBillTableRow>(&GetTableRowsReq {
                json: true,
                code: "dmc.token",
                table: "billrec",
                scope: "dmc.token",
                index_position: Some(3),
                key_type: Some("name"),
                encode_type: None,
                lower_bound: Some(self.client.account()),
                upper_bound: Some(self.client.account()),
                limit: Some(1),
                reverse: Some(true),
                show_payer: None
            }).await.and_then(|result| result.rows.get(0).cloned().ok_or_else(|| DmcError::new(DmcErrorCode::Failed, "bill created not found"))) {

                Ok(row) => {
                    let bill = DmcBill {
                        bill_id: row.bill_id, 
                        miner: row.owner.clone(), 
                        asset: row.unmatched.quantity.amount as u64, 
                        price: row.price,
                        pledge_rate: row.deposit_ratio as u32, 
                        min_asset: 1, 
                        expire_at: row.expire_on.as_secs().unwrap() as u64 
                    };
                    
                    let result = Ok(DmcPendingResult { 
                        block_number: pending.block_number, 
                        tx_index: pending.tx_index, 
                        result: Ok(bill)
                    });

                    info!("{} send, success, result={:?}", self.tx, result);

                    break result
                },
                Err(err) => {
                    error!("{} get bill table, wait retry, err={}", self.tx, err);
                    let _ = async_std::task::sleep(self.client.config().retry_rpc_interval).await;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl DmcPendingBill for EosPendingBill {
    fn tx_id(&self) -> &[u8] {
        self.tx.tx_id()
    }

    async fn wait(&self) -> DmcResult<DmcPendingResult<DmcBill>> {
        let (waiter, exec) = self.client.0.pending_bills.add(self.clone());
        if exec {
            let client = self.client.clone();
            async_std::task::spawn(async move {
                let _ = client.0.pending_bills.exec().await;
            });
        }

        waiter.wait().await
    }
}

#[derive(Clone)]
pub struct EosPendingOrder {
    client: EosClient, 
    tx: Arc<EosPendingTx>
}

impl AsRef<[u8]> for EosPendingOrder {
    fn as_ref(&self) -> &[u8] {
        self.tx.as_ref().as_ref()
    }
}

#[async_trait::async_trait]
impl EosSerializedTx for EosPendingOrder {
    type Result = DmcOrder;

    async fn execute(&self) -> DmcResult<DmcPendingResult<DmcOrder>> {
        let pending = self.tx.wait().await?;
        if pending.result.is_err() {
            let err = pending.result.unwrap_err();
            return Ok(DmcPendingResult {
                block_number: pending.block_number, 
                tx_index: pending.tx_index, 
                result: Err(err)
            })
        }

        loop {
            match self.client.rpc().get_table_rows::<_, EosOrderTableRow>(&GetTableRowsReq {
                json: true,
                code: "dmc.token",
                table: "dmcorder",
                scope: "dmc.token",
                index_position: Some(2),
                key_type: Some("name"),
                encode_type: None,
                lower_bound: Some(self.client.account()),
                upper_bound: Some(self.client.account()),
                limit: Some(1),
                reverse: Some(true),
                show_payer: None
            }).await.and_then(|result| result.rows.get(0).cloned().ok_or_else(|| DmcError::new(DmcErrorCode::Failed, "order created not found"))) {
                Ok(row) => {
                    let order = DmcOrder {
                        order_id: row.order_id, 
                        bill_id: row.bill_id, 
                        user: row.user, 
                        miner: row.miner, 
                        asset: row.miner_lock_pst.quantity.amount as u64, 
                        duration: row.epoch as u32, 
                        price: (row.price.quantity.amount / EosClient::atomic_dmc_asset()) as u64,
                        pledge_rate: (row.miner_lock_dmc.quantity.amount / row.miner_lock_pst.quantity.amount) as u32, 
                        state: DmcOrderState::Preparing { miner: None, user: None }, 
                        start_at: Duration::from_secs(row.deliver_start_date.as_secs().unwrap() as u64).as_micros()
                    };
                    
                    let result = Ok(DmcPendingResult { 
                        block_number: pending.block_number, 
                        tx_index: pending.tx_index, 
                        result: Ok(order)
                    });

                    info!("{} send, success, result={:?}", self.tx, result);

                    break result
                },
                Err(err) => {
                    error!("{} get bill table, wait retry, err={}", self.tx, err);
                    let _ = async_std::task::sleep(self.client.config().retry_rpc_interval).await;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl DmcPendingOrder for EosPendingOrder {
    fn tx_id(&self) -> &[u8] {
        self.tx.tx_id()
    }

    async fn wait(&self) -> DmcResult<DmcPendingResult<DmcOrder>> {
        let (waiter, exec) = self.client.0.pending_orders.add(self.clone());
        if exec {
            let client = self.client.clone();
            async_std::task::spawn(async move {
                let _ = client.0.pending_orders.exec().await;
            });
        }

        waiter.wait().await
    }
}



struct TransactionBuilder {
    actions: Vec<Action>,
}

impl TransactionBuilder {
    fn new() -> Self {
        Self {
            actions: vec![],
        }
    }

    fn add_action<T: EosSerialize>(
        mut self, 
        contract_name: &str, 
        action_name: &str, 
        authorization: Vec<Authorization>, 
        data: T
    ) -> DmcResult<Self> {
        let mut serial_buf = EosSerialWrite::new();
        data.eos_serialize(&mut serial_buf)?;
        self.actions.push(Action {
            account: contract_name.to_string(),
            name: action_name.to_string(),
            authorization,
            data: serial_buf.into(),
            hex_data: None
        });
        Ok(self)
    }

    async fn build(self, rpc: &EosRpc, expire_seconds: i64) -> DmcResult<Transaction> {
        let header = Self::generate_tapos(rpc, expire_seconds).await?;
        Ok(Transaction {
            header, 
            context_free_actions: vec![],
            actions: self.actions,
            transaction_extensions: vec![],
        })
    }

    async fn generate_tapos(rpc: &EosRpc, expire_seconds: i64) -> DmcResult<TransHeader> {
        let block_info = rpc.get_info().await
            .map_err(|err| {
                error!("{} get_info failed, err={}", rpc, err);
                err
            })?;

        #[derive(Debug)]
        struct BlockTaposInfo {
            pub block_num: i64,
            pub id: String,
            pub timestamp: Option<TimePointSec>,
            pub header: Option<BlockHeader>,
        }
       
        let ref_block = if block_info.last_irreversible_block_time.is_some() {
            BlockTaposInfo {
                block_num: block_info.last_irreversible_block_num,
                id: block_info.last_irreversible_block_id.clone(),
                timestamp: block_info.last_irreversible_block_time.clone(),
                header: None
            }
        } else {
            let block_info = match rpc.get_block_info(block_info.last_irreversible_block_num).await {
                Ok(info) => {
                    info
                },
                Err(err) => {
                    error!("{} get_block_info failed, err={}", rpc, err);
                    let ret = rpc.get_block(block_info.last_irreversible_block_num.to_string()).await
                        .map_err(|err| {
                            error!("{} get_block failed, err={}", rpc, err);
                            err
                        })?;
                    GetBlockInfoResult {
                        timestamp: ret.timestamp,
                        producer: ret.producer,
                        confirmed: ret.confirmed,
                        previous: ret.previous,
                        transaction_mroot: ret.transaction_mroot,
                        action_mroot: ret.action_mroot,
                        schedule_version: ret.schedule_version,
                        producer_signature: ret.producer_signature,
                        id: ret.id,
                        block_num: ret.block_num,
                        ref_block_num: 0,
                        ref_block_prefix: ret.ref_block_prefix
                    }
                }
            };
            BlockTaposInfo {
                block_num: block_info.block_num,
                id: block_info.id,
                timestamp: Some(block_info.timestamp),
                header: None
            }
        };

        let timestamp: TimePointSec = if ref_block.header.is_some() {
            ref_block.header.as_ref().unwrap().timestamp.clone()
        } else {
            ref_block.timestamp.clone().unwrap()
        };

        fn reverse_hex(h: &str) -> String {
            format!("{}{}{}{}", &h[6..8], &h[4..6], &h[2..4], &h[0..2])
        }

        let prefix = u32::from_str_radix(reverse_hex(&ref_block.id[16..24]).as_str(), 16).unwrap();
        
        Ok(TransHeader {
            ref_block_prefix: prefix,
            expiration: timestamp.offset(expire_seconds),
            ref_block_num: ref_block.block_num as u16 & 0xffff, 
            max_net_usage_words: 0,
            max_cpu_usage_ms: 0,
            delay_sec: 0,
        })
    }
}

impl std::fmt::Display for EosClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EosClient")
    }
}

impl EosClient {
    pub fn new(config: EosClientConfig) -> DmcResult<Self> {
        let mut client = ClientImpl {
            rpc: EosRpc::new(config.host.clone()),
            chain_id: config.chain_id.clone(), 
            account: None, 
            key: None,  
            config: config.clone(),  
            pending_bills: EosTxSerilizer::new(), 
            pending_orders: EosTxSerilizer::new(), 
        };
        if let Some((name, secret)) = &config.account {
            use std::str::FromStr;
            client.key = Some(EosPrivateKey::from_str(secret)?);
            client.account = Some(name.clone());
        }
        let client = Self(Arc::new(client));
        Ok(client)
    }

    fn atomic_dmc_asset() -> f64 {
        0.0001
    }

    fn default_initial_price() -> u64 {
        10
    }

    fn key(&self) -> &EosPrivateKey {
        self.0.key.as_ref().unwrap()
    }

    async fn get_chain_config(&self, key: &str, default: u64) -> DmcResult<u64> {
        self.rpc().get_table_rows::<_, EosChainConfigRow>(&GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "dmcconfig",
            scope: "dmc.token",
            index_position: Some(1),
            key_type: Some("name"),
            encode_type: None,
            lower_bound: Some(key), 
            upper_bound: Some(key),
            limit: Some(1), 
            reverse: None,
            show_payer: None
        }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get_chain_config, failed, err={}", self, err))
        .map(|result| result.rows.get(0).map(|row| row.value).unwrap_or(default))
    } 

    async fn get_benchmark_price(&self) -> DmcResult<u64> {
        let benchmark_price = self.rpc().get_table_rows::<(), EosBenchmarkPriceRow>(&GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "bcprice",
            scope: "dmc.token",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: None,
            upper_bound: None,
            limit: Some(1), 
            reverse: None,
            show_payer: None
        }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get_benchmark_price, failed, err={}", self, err))
        .map(|result| result.rows.get(0).map(|row| row.benchmark_price.clone()))?;

        if let Some(price) = benchmark_price {
            let price = price.parse::<f64>().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get_benchmark_price, failed, err={}", self, err))?;
            Ok((price / Self::atomic_dmc_asset()) as u64)
        } else {
            self.get_chain_config("initalprice", Self::default_initial_price()).await
        }
    }

    pub(crate) async fn get_challenge_by_id(&self, order_id: u64) -> DmcResult<Option<EosChallengeTableRow>> {
        self.rpc().get_table_rows::<_, EosChallengeTableRow>(&GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "dmchallenge",
            scope: "dmc.token",
            index_position: Some(1),
            key_type: None,
            encode_type: None,
            lower_bound: Some(order_id),
            upper_bound: Some(order_id),
            limit: Some(1),
            reverse: None,
            show_payer: None
        }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get challenge by id, order_id={}, failed, err={}", self, order_id, err))
        .map(|result| result.rows.get(0).cloned())
    }

    pub(crate) fn dmc_challenge_from_row(&self, challenge: &EosChallengeTableRow) -> DmcChallenge {
        let state = match challenge.state {
            EosChallengeState::Request => DmcChallengeState::Waiting,
            EosChallengeState::Answer => DmcChallengeState::Prooved,
            EosChallengeState::ArbitrationMinerPay => DmcChallengeState::Prooved,
            EosChallengeState::ArbitrationUserPay => DmcChallengeState::Failed,
            EosChallengeState::Timeout => DmcChallengeState::Expired,
            // Prepare, Consistent状态不是有效的challenge
            // Cancel状态实际在合约逻辑里不存在
            _ => DmcChallengeState::Invalid
        };

        DmcChallenge {
            // 我这里用高4byte的order_id加低四byte的challenge_times做id
            challenge_id: make_dmc_challenge_id(challenge.order_id as u32, challenge.challenge_times as u32),
            order_id: challenge.order_id,
            start_at: Duration::from_secs(challenge.challenge_date.as_secs().unwrap() as u64).as_micros(),
            state,
            params: DmcChallengeParams::SourceStub { piece_index: challenge.data_id, nonce: challenge.nonce.clone(), hash: challenge.hash_data },
        }
    }

    pub(crate) fn dmc_order_from_row(&self, order: &EosOrderTableRow, challenge: &EosChallengeTableRow) -> DmcResult<DmcOrder> {
        let order_state = {
            match order.state {
                EosOrderState::Waiting | EosOrderState::Deliver => {
                    match challenge.state {
                        EosChallengeState::Prepare => {
                            if challenge.merkle_submitter.eq("dmc.token") {
                                DmcOrderState::Preparing { miner: None, user: None }
                            } else if challenge.merkle_submitter.eq(&order.miner) {
                                DmcOrderState::Preparing { miner: Some(DmcMerkleStub { 
                                    piece_size: self.config().piece_size, 
                                    leaves: challenge.pre_data_block_count, 
                                    root: challenge.pre_merkle_root.clone() 
                                }), user: None }
                            } else if challenge.merkle_submitter.eq(&order.user) {
                                DmcOrderState::Preparing { user: Some(DmcMerkleStub { 
                                    piece_size: self.config().piece_size, 
                                    leaves: challenge.pre_data_block_count,     
                                    root: challenge.pre_merkle_root.clone() 
                                }), miner: None }
                            } else {
                                unreachable!()
                            }
                        }, 
                        EosChallengeState::Consistent => {
                            DmcOrderState::Storing(DmcMerkleStub {
                                piece_size: self.config().piece_size,  
                                leaves: challenge.data_block_count, 
                                root: challenge.merkle_root.clone() 
                            })
                        },
                        _ => {
                            DmcOrderState::Storing(DmcMerkleStub {
                                piece_size: self.config().piece_size,
                                leaves: challenge.data_block_count,
                                root: challenge.merkle_root.clone()
                            })
                        }
                    }
                }, 
                EosOrderState::PreEnd => {
                    DmcOrderState::Storing(DmcMerkleStub {
                        piece_size: self.config().piece_size,  
                        leaves: challenge.data_block_count, 
                        root: challenge.merkle_root.clone() 
                    })
                }, 
                EosOrderState::PreCont => {
                    DmcOrderState::Storing(DmcMerkleStub { 
                        piece_size: self.config().piece_size, 
                        leaves: challenge.data_block_count, 
                        root: challenge.merkle_root.clone() 
                    })
                }, 
                EosOrderState::End => {
                    DmcOrderState::Canceled
                }, 
                EosOrderState::Cancel => {
                    DmcOrderState::Canceled
                },
                EosOrderState::PreCancel => {
                    DmcOrderState::Storing(DmcMerkleStub { 
                        piece_size: self.config().piece_size, 
                        leaves: challenge.data_block_count, 
                        root: challenge.merkle_root.clone() 
                    })
                }
            }
        };
       
        Ok(DmcOrder {
            order_id: order.order_id, 
            bill_id: order.bill_id, 
            user: order.user.clone(), 
            miner: order.miner.clone(), 
            asset: order.miner_lock_pst.quantity.amount as u64, 
            duration: order.epoch as u32, 
            price: (order.price.quantity.amount / EosClient::atomic_dmc_asset()) as u64,
            pledge_rate: (order.miner_lock_dmc.quantity.amount / order.miner_lock_pst.quantity.amount) as u32, 
            state: order_state, 
            start_at: Duration::from_secs(order.deliver_start_date.as_secs().unwrap() as u64).as_micros()
        })
    }

    fn config(&self) -> &EosClientConfig {
        &self.0.config
    }

    pub(crate) fn rpc(&self) -> &EosRpc {
        &self.0.rpc
    }

    fn chain_id(&self) -> &str {
        &self.0.chain_id
    }

    // async fn get_cached_abi(&self, account_name: &str) -> DmcResult<Arc<CachedAbi>> {
    //     {
    //         let cached = self.0.cached_abis.read().unwrap().get(account_name).cloned();
    //         if cached.is_some() {
    //             return Ok(cached.unwrap());
    //         }
    //     }

    //     let mut raw_abi = self.rpc().get_bin_abi(account_name).await?;
    //     let abi = AbiDef::parse(&mut raw_abi.abi)?;
    //     let cached = Arc::new(CachedAbi {
    //         raw_abi: raw_abi.abi,
    //         abi
    //     });

    //     let mut cached_abis = self.0.cached_abis.write().unwrap();
    //     cached_abis.insert(account_name.to_owned(), cached.clone());

    //     Ok(cached)
    // }

    // async fn get_transaction_abis(&self, transaction: &Transaction) -> DmcResult<Vec<BinaryAbi>> {
    //     let account_list: Vec<Name> = transaction.actions.iter().map(|a| a.account.clone()).collect();

    //     let ret_list = futures::future::try_join_all(account_list.iter().map(|account| self.get_cached_abi(account)).collect()).await?;
        
    //     Ok(account_list.into_iter().zip(ret_list.into_iter()).map(|(account_name, cached_abi)| BinaryAbi { account_name, abi: cached_abi.raw_abi.clone() }).collect())
    // }

    async fn sign_transaction(&self, serialized: &[u8]) -> DmcResult<Vec<EosSignature>> {
        let chain_id = hex::decode(self.chain_id()).unwrap();
        
        let sign_buf = vec![chain_id, Vec::from(serialized), vec![0u8;32]].concat();

        let mut sha256 = sha2::Sha256::new();
        sha256.update(sign_buf.as_slice());
        let sign_hash = sha256.finalize().to_vec();

        let mut signatures = Vec::new();
       
        let signature = self.key().sign(sign_hash.as_slice(), false)?;
        signatures.push(signature);
       
        Ok(signatures)
    }

    
    async fn update_auth(&self, permission: &str, auth: Authority) -> DmcResult<()> {
        info!("{} update_auth, permission={}", self, permission);
        struct UpdateAuth {
            pub account: Name,
            pub permission: Name,
            pub parent: Name,
            pub auth: Authority
        }

        impl EosSerialize for UpdateAuth {
            fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
                buf.push_name(self.account.as_str())?;
                buf.push_name(self.permission.as_str())?;
                buf.push_name(self.parent.as_str())?;
                self.auth.eos_serialize(buf)?;
                Ok(())
            }
        }

        let params = UpdateAuth {
            account: self.account().to_owned(),
            permission: permission.to_owned(),
            parent: "active".to_owned(),
            auth
        };

        let builder = TransactionBuilder::new().add_action(
            "dmc",
            "updateauth", 
            vec![Authorization { actor: self.account().to_owned(), permission: "active".to_owned() }], params)?;
        
        let result = EosPendingTx::execute(self.clone(), builder).await.map_err(|err| {
            error!("{} update_auth, permission={}, failed, err={}", self, permission, err);
            err
        })?;
        result.map(|_| {
            info!("{} update_auth, permission={}, success", self, permission);
            ()
        }).map_err(|err| {
            error!("{} update_auth, permission={}, failed, err={}", self, permission, err);
            err
        })
    }

    async fn link_auth(&self, code: &str, ty: &str, permission: &str) -> DmcResult<()> {
        info!("{} link_auth, permission={}", self, permission);
        struct LinkAuth {
            account: Name,
            code: Name,
            ty: Name,
            requirement: Name,
        }        

        impl EosSerialize for LinkAuth {
            fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
                buf.push_name(self.account.as_str())?;
                buf.push_name(self.code.as_str())?;
                buf.push_name(self.ty.as_str())?;
                buf.push_name(self.requirement.as_str())?;
                Ok(())
            }
        }

        let params = LinkAuth {
            account: self.account().to_owned(),
            code: code.to_owned(), 
            ty: ty.to_owned(),
            requirement: permission.to_owned()
        };

        let builder = TransactionBuilder::new().add_action(
            "dmc",
            "linkauth", 
            vec![Authorization { actor: self.account().to_owned(), permission: "active".to_owned() }], 
            params
        )?;
        
        let result = EosPendingTx::execute(self.clone(), builder).await.map_err(|err| {
            error!("{} link_auth, permission={}, failed, err={}", self, permission, err);
            err
        })?;
        result.map(|_| {
            info!("{} link_auth, permission={}, success", self, permission);
            ()
        }).map_err(|err| {
            error!("{} link_auth, permission={}, failed, err={}", self, permission, err);
            err
        })
    }

    async fn init_light_permission(&self, key: String) -> DmcResult<()> {
        let account_info = self.rpc().get_account(self.account()).await
        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_prem_permission, failed, err={}", self, err))?;

        if account_info.permissions.iter().find(|permission| permission.perm_name.eq(self.config().permission.as_str())).is_none() {
            let _ = self.update_auth(self.config().permission.as_str(), Authority {
                threshold: 1,
                accounts: vec![PermissionLevelWeight {
                    permission: PermissionLevel {
                        actor: self.account().to_owned(), 
                        permission: "active".to_owned() 
                    }, 
                    weight: 1,
                }],
                keys: vec![KeyWeight {
                    key, 
                    weight: 1
                }],
                waits: vec![],
            }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_light_permission, failed, err={}", self, err))?;
        }


        let _ = self.link_auth("cyfsaddrinfo", "bind", self.config().permission.as_str()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_light_permission, failed, err={}", self, err))?;
        let _ = self.link_auth("dmc.token", "bill", self.config().permission.as_str()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_light_permission, failed, err={}", self, err))?;
        let _ = self.link_auth("dmc.token", "unbill", self.config().permission.as_str()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_light_permission, failed, err={}", self, err))?;
        let _ = self.link_auth("dmc.token", "order", self.config().permission.as_str()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_light_permission, failed, err={}", self, err))?;
        let _ = self.link_auth("dmc.token", "addmerkle", self.config().permission.as_str()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_light_permission, failed, err={}", self, err))?;
        let _ = self.link_auth("dmc.token", "reqchallenge", self.config().permission.as_str()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_light_permission, failed, err={}", self, err))?;
        let _ = self.link_auth("dmc.token", "anschallenge", self.config().permission.as_str()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_light_permission, failed, err={}", self, err))?;
        let _ = self.link_auth("dmc.token", "arbitration", self.config().permission.as_str()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init_light_permission, failed, err={}", self, err))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl DmcChainClient for EosClient {
    type EventListener = EosListener;

    async fn get_bill_by_id(&self, bill_id: u64) -> DmcResult<Option<DmcBill>> {
        let result = self.rpc().get_table_rows::<_, EosBillTableRow>(&GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "billrec",
            scope: "dmc.token",
            index_position: Some(1),
            key_type: None,
            encode_type: None,
            lower_bound: Some(bill_id),
            upper_bound: Some(bill_id),
            limit: Some(1),
            reverse: None,
            show_payer: None
        }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get bill by id, bill_id={}, failed, err={}", self, bill_id, err))?;

        if result.rows.len() == 0 {
            return Ok(None);
        }

        let row = &result.rows[0];

        Ok(Some(DmcBill {
            bill_id: row.bill_id,
            miner: row.owner.clone(),
            asset: row.unmatched.quantity.amount as u64,
            price: row.price,
            pledge_rate: row.deposit_ratio as u32,
            min_asset: 1,
            expire_at: row.expire_on.as_secs().unwrap() as u64
        }))
    }

    async fn get_order_by_id(&self, order_id: u64) -> DmcResult<Option<DmcOrder>> {
        let result = self.rpc().get_table_rows::<_, EosOrderTableRow>(&GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "dmcorder",
            scope: "dmc.token",
            index_position: Some(1),
            key_type: None,
            encode_type: None,
            lower_bound: Some(order_id),
            upper_bound: Some(order_id),
            limit: Some(1),
            reverse: None,
            show_payer: None
        }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get order by id, order_id={}, failed, err={}", self, order_id, err))?;
        
        if result.rows.len() == 0 {
            return Ok(None);
        }

        let challenge = self.get_challenge_by_id(order_id).await?.unwrap();

        let order = self.dmc_order_from_row(&result.rows[0], &challenge)?;
        Ok(Some(order))
    }

    async fn get_apply_method(&self, account: &str) -> DmcResult<Option<String>> {
        #[derive(Deserialize, Clone)]
        pub struct CyfsAccount {
            pub account: String,
            pub address: String,
        }

        let req = GetTableRowsReq {
            json: true,
            code: "cyfsaddrinfo",
            table: "accountmap",
            scope: "cyfsaddrinfo",
            index_position: Some(1),
            key_type: Some("name"),
            encode_type: None,
            lower_bound: Some(account),
            upper_bound: Some(account),
            limit: Some(1),
            reverse: None,
            show_payer: None
        };

        let resp= self.rpc().get_table_rows::<_, CyfsAccount>(&req).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get_apply_method, failed, err={}", self, err))?;
        if resp.rows.len() == 0 {
            Ok(None)
        } else {
           Ok(Some(resp.rows[0].address.clone()))
        }
    }

    async fn event_listener(&self, _: Option<u64>) -> Self::EventListener {
        EosListener::new(self.clone(), EosEventListenerFilter::Miner)
    }

    async fn verify(&self, from: &str, data: &[u8], perm_and_sign: &str) -> DmcResult<bool> {
        let account_info = self.rpc().get_account(from).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} verify, failed, err={}", self, err))?;

        let parts: Vec<_> = perm_and_sign.split("-").collect();
        let (perm, sign) = if parts.len() == 1 {
            ("active".to_owned(), parts[0].to_owned())
        } else if parts.len() == 2 {
            (parts[0].to_owned(), parts[1].to_owned())
        } else {
            return Err(dmc_err!(DmcErrorCode::Failed, "{} verify, failed, err={}", self, "invalid sign and permission"));
        };
        let permission = account_info.permissions.iter()
            .find(|permission| permission.perm_name == perm)
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} verify, failed, err={}", self, format!("{} no active permission", from)))?;
        let key = permission.required_auth.keys.get(0)
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} verify, failed, err={}", self, format!("{} no active permission key", from)))?;
        use std::str::FromStr;
        let public_key = EosPublicKey::from_str(key.key.as_str()).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} verify, failed, err={}", self, err))?;
        let dig_sign = EosSignature::from_str(&sign).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} verify, failed, err={}", self, err))?;
        let ret = dig_sign.verify(data, &public_key, true).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} verify, failed, err={}", self, err))?;
        Ok(ret)
    }
}

#[async_trait::async_trait]
impl DmcChainAccountClient for EosClient {
    fn account(&self) -> &str {
        self.0.account.as_ref().unwrap().as_str()
    }

    async fn get_available_assets(&self) -> DmcResult<Vec<DmcAsset>> {
        let result = self.rpc().get_table_rows::<_, EosAssetRow>(&GetTableRowsReq::<String> {
            json: true,
            code: "dmc.token",
            table: "accounts",
            scope: self.account(),
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: None,
            upper_bound: None,
            limit: None,
            reverse: None,
            show_payer: None
        }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get available assets, name={}, failed, err={}", self, self.account(), err))?;

        let mut ret = vec![];

        for row in result.rows {
            ret.push(DmcAsset {
                amount: row.balance.quantity.amount,
                name: row.balance.quantity.symbol,
            })
        }

        Ok(ret)
    }
    
    async fn sign(&self, data: &[u8]) -> DmcResult<String> {
        let sign = self.key().sign(data, true)?.to_string();
        if self.config().permission.eq("active") {
            Ok(sign)
        } else {
            Ok(format!("{}-{}", self.config().permission, sign))
        }
       
    }
    
    async fn set_apply_method(&self, method: String) -> DmcResult<DmcResult<()>> {
        info!("{} set apply method, method={}", self, method);
        if let Some(exists) = self.get_apply_method(self.account()).await? {
            if exists == method {
                info!("{} set apply method, method={}, ignored", self, method);
                return Ok(Ok(()));
            }
        }

        struct SetApplyMethod {
            owner: Name,
            address: String,
        }
        
        impl EosSerialize for SetApplyMethod {
            fn eos_serialize(&self, buf: &mut EosSerialWrite) -> DmcResult<()> {
                self.owner.eos_serialize(buf)?;
                buf.push_string(self.address.as_str());
                Ok(())
            }
        }
        
        let params = SetApplyMethod {
            owner: self.account().to_owned(),
            address: method.clone()
        };

        let builder = TransactionBuilder::new().add_action(
            "cyfsaddrinfo",
            "bind", 
            vec![Authorization { actor: self.account().to_owned(), permission: self.config().permission.to_owned() }],
            params
        ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} set apply method, method={:?}, failed, err={}", self, method, err))?;

        let result = EosPendingTx::execute(self.clone(), builder).await.map_err(|err| {
            error!("{} set apply method, method={:?}, failed, err={}", self, method, err);
            err
        })?;
        Ok(result.map(|_| {
            info!("{} set apply method, method={:?}, success", self, method);
            ()
        }).map_err(|err| {
            error!("{} set apply method, method={:?}, failed, err={}", self, method, err);
            err
        }))
    }
    
    type PendingBill = EosPendingBill;

    async fn create_bill(&self, options: DmcBillOptions) -> DmcResult<Self::PendingBill> {
        info!("{} create bill, options={:?}", self, options);

        let builder = if let Some(pledge_rate) = options.pledge_rate {
            let builder = TransactionBuilder::new();

            let increase = EosIncreaseOptions {
                owner: self.account().to_owned(),
                asset: ExtendedAsset::new("datamall", Asset::with_precision(options.asset as f64 * pledge_rate as f64, "DMC", 4)),
                miner: self.account().to_owned(),
            };
    
            let mint = EosMintOptions {
                owner: self.account().to_owned(),
                asset: ExtendedAsset::new("datamall", Asset::int(options.asset, "PST"))
            }; 

            builder.add_action(
                "dmc.token",
                "increase",
                vec![Authorization { actor: self.account().to_owned(), permission: "active".to_owned()}],
                increase
            ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create bill, options={:?}, failed, err={}", self, options, err))?
            .add_action(
                "dmc.token",
                "mint",
                vec![Authorization { actor: self.account().to_owned(), permission: "active".to_owned()}],
                mint
            ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create bill, options={:?}, failed, err={}", self, options, err))?
        } else {
            TransactionBuilder::new()
        };
       

        let bill = EosBillOptions {
            owner: self.account().to_owned(),
            asset: ExtendedAsset::new("datamall", Asset::int(options.asset, "PST")),
            price: options.price as f64 * EosClient::atomic_dmc_asset(),
            expire_on: TimePointSec::from_now().offset(options.duration as i64 * 60 * 60 * 24 * 7), 
            deposit_ratio: 0, 
            memo: "".to_owned()
        };

        let tx = builder.add_action(
            "dmc.token",
            "bill",
            vec![Authorization { actor: self.account().to_owned(), permission: self.config().permission.to_owned()}],
            bill
        ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create bill, options={:?}, failed, err={}", self, options, err))?
        .build(self.rpc(), self.config().trans_expire.as_secs() as i64).await
        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create bill, options={:?}, failed, err={}", self, options, err))?;

        let pending = EosPendingBill {
            client: self.clone(), 
            tx: Arc::new(EosPendingTx::from_tx(self.clone(), tx)?)
        };

        info!("{} create bill, options={:?}, success, pending={}", self, options, pending.tx);
        Ok(pending)
    }

    async fn load_bill(&self, pending: &[u8]) -> DmcResult<Self::PendingBill> {
        Ok(EosPendingBill {
            client: self.clone(), 
            tx: Arc::new(EosPendingTx::from_serialized(self.clone(), Vec::from(pending)))
        })
    }

    async fn finish_bill(&self, bill_id: u64) -> DmcResult<DmcResult<()>> {
        info!("{} finish bill, bill={:?}", self, bill_id);

        let params = EosUnbillOptions {
            owner: self.account().to_owned(),
            bill_id, 
            memo: format!("{}", dmc_time_now())
        };

        let builder = TransactionBuilder::new().add_action(
            "dmc.token",
            "unbill",
            vec![Authorization { actor: self.account().to_owned(), permission: self.config().permission.to_owned()}],
            params
        ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} finish bill, bill={:?}, failed, err={}", self, bill_id, err))?;

        let result = EosPendingTx::execute(self.clone(), builder).await.map_err(|err| {
            error!("{} finish bill, bill={:?}, failed, err={}", self, bill_id, err);
            err
        })?;

        Ok(result.map(|_| {
            info!("{} finish bill, bill={:?}, success", self, bill_id);
            ()
        }).map_err(|err| {
            error!("{} finish bill, bill={:?}, failed, err={}", self, bill_id, err);
            err
        }))
    }


    type PendingOrder = EosPendingOrder;
    async fn create_order(&self, options: DmcOrderOptions) -> DmcResult<Self::PendingOrder> {
        info!("{} create order, options={:?}", self, options);

        let bill_price = self.rpc().get_table_rows::<_, EosBillTableRow>(&GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "billrec",
            scope: "dmc.token",
            index_position: Some(1),
            key_type: None,
            encode_type: None,
            lower_bound: Some(options.bill_id), 
            upper_bound: Some(options.bill_id),
            limit: Some(1),
            reverse: None,
            show_payer: None
        }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create order, options={:?}, failed, err={}", self, options, err))
        .map(|result| result.rows.get(0).cloned())?
        .map(|row| row.price as f64 * Self::atomic_dmc_asset()).unwrap_or_default();  
        
        let benchmark_price = self.get_benchmark_price().await?;

        let order = EosOrderOptions {
            owner: self.account().to_owned(), 
            bill_id: options.bill_id,
            benchmark_price, 
            price_range: EosOrderPriceRange::NoLimit, 
            epoch: options.duration as u64, 
            asset: ExtendedAsset::new("datamall", Asset::int(options.asset, "PST")), 
            reserve: ExtendedAsset::new("datamall", Asset::with_precision(bill_price * options.duration as f64, "DMC", 4)), 
            // make tx different if params all same 
            memo: format!("{}", dmc_time_now())
        };

        let tx = TransactionBuilder::new().add_action(
            "dmc.token",
            "order",
            vec![Authorization { actor: self.account().to_owned(), permission: self.config().permission.to_owned()}],
            order
        ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create order, options={:?}, failed, err={}", self, options, err))?
        .build(self.rpc(), self.config().trans_expire.as_secs() as i64).await
        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create bill, options={:?}, failed, err={}", self, options, err))?;

        let pending = EosPendingOrder {
            client: self.clone(), 
            tx: Arc::new(EosPendingTx::from_tx(self.clone(), tx)?)
        };

        info!("{} create order, options={:?}, success, pending={}", self, options, pending.tx);
        Ok(pending)
    }
    
    async fn load_order(&self, pending: &[u8]) -> DmcResult<Self::PendingOrder> {
        Ok(EosPendingOrder {
            client: self.clone(), 
            tx: Arc::new(EosPendingTx::from_serialized(self.clone(), Vec::from(pending)))
        })
    }

    async fn prepare_order(&self, options: DmcPrepareOrderOptions) -> DmcResult<DmcResult<()>> {
        info!("{} prepare order, options={:?}", self, options);
        let params = EosPrepareOrderOptions {
            owner: self.account().to_owned(), 
            order_id: options.order_id, 
            merkle_root: options.merkle_stub.root.clone(), 
            data_block_count: options.merkle_stub.leaves
        };

        let builder = TransactionBuilder::new().add_action(
            "dmc.token",
            "addmerkle",
            vec![Authorization { actor: self.account().to_owned(), permission: self.config().permission.to_owned()}],
            params
        ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare order, options={:?}, failed, err={}", self, options, err))?;

        let result = EosPendingTx::execute(self.clone(), builder).await.map_err(|err| {
            error!("{} prepare order, options={:?},, failed, err={}", self, options, err);
            err
        })?;

        Ok(result.map(|_| {
            info!("{} prepare order, options={:?}, success", self, options);
            ()
        }).map_err(|err| {
            error!("{} prepare order, options={:?}, failed, err={}", self, options, err);
            err
        }))
        
    }

    async fn finish_order(&self, _: u64) -> DmcResult<DmcResult<f64>> {
        unimplemented!()
    }

    
    fn supported_challenge_type() -> DmcChallengeTypeCode {
        DmcChallengeTypeCode::SourceStub
    }

    async fn challenge(&self, options: DmcChallengeOptions) -> DmcResult<DmcResult<()>> {
        info!("{} challenge, options={:?}", self, options);
        if let DmcChallengeTypedOptions::SourceStub { piece_index, nonce, hash } = &options.options {
            let params = EosChallengeOptions {
                owner: self.account().to_owned(), 
                order_id: options.order_id, 
                data_id: *piece_index, 
                hash_data: hash.clone(), 
                nonce: nonce.clone()
            };
    
            let builder = TransactionBuilder::new().add_action(
                "dmc.token",
                "reqchallenge",
                vec![Authorization { actor: self.account().to_owned(), permission: self.config().permission.to_owned()}],
                params
            ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} challenge, options={:?}, failed, err={}", self, options, err))?;
    
            let result = EosPendingTx::execute(self.clone(), builder).await.map_err(|err| {
                error!("{} challenge, options={:?},, failed, err={}", self, options, err);
                err
            })?;
    
            Ok(result.map(|_| {
                info!("{} challenge, options={:?}, success", self, options);
                ()
            }).map_err(|err| {
                error!("{} challenge, options={:?}, failed, err={}", self, options, err);
                err
            }))
        } else {
            Ok(Err(dmc_err!(DmcErrorCode::Failed, "{} prepare order, options={:?}, failed, err={}", self, options, "invalid challenge")))
        }
    }

    async fn proof(&self, options: DmcProofOptions) -> DmcResult<DmcResult<()>> {
        info!("{} proof, options={:?}", self, options);
        match &options.proof {
            DmcTypedProof::SourceStub { hash } => {
                let params = EosAnsChallengeOptions {
                    owner: self.account().to_owned(), 
                    order_id: options.order_id, 
                    reply_hash: hash.clone(), 
                };
        
                let builder = TransactionBuilder::new().add_action(
                    "dmc.token",
                    "anschallenge",
                    vec![Authorization { actor: self.account().to_owned(), permission: self.config().permission.to_owned()}],
                    params
                ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} challenge, options={:?}, failed, err={}", self, options, err))?;
        
                let result = EosPendingTx::execute(self.clone(), builder).await.map_err(|err| {
                    error!("{} proof, options={:?},, failed, err={}", self, options, err);
                    err
                })?;
        
                Ok(result.map(|_| {
                    info!("{} proof, options={:?}, success", self, options);
                    ()
                }).map_err(|err| {
                    error!("{} proof, options={:?}, failed, err={}", self, options, err);
                    err
                }))
            },
            DmcTypedProof::MerklePath { data, pathes } => {
                let params = EosArbitrationOptions {
                    owner: self.account().to_owned(), 
                    order_id: options.order_id, 
                    data: data.clone(), 
                    cur_merkle: pathes.clone()
                };
        
                let builder = TransactionBuilder::new().add_action(
                    "dmc.token",
                    "arbitration",
                    vec![Authorization { actor: self.account().to_owned(), permission: self.config().permission.to_owned()}],
                    params
                ).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} challenge, options={:?}, failed, err={}", self, options, err))?;
        
                let result = EosPendingTx::execute(self.clone(), builder).await.map_err(|err| {
                    error!("{} proof, options={:?},, failed, err={}", self, options, err);
                    err
                })?;
        
                Ok(result.map(|_| {
                    info!("{} proof, options={:?}, success", self, options);
                    ()
                }).map_err(|err| {
                    error!("{} proof, options={:?}, failed, err={}", self, options, err);
                    err
                }))
            }
        }
    }

    async fn miner_listener(&self, _: Option<u64>) -> Self::EventListener {
        let listener = EosListener::new(self.clone(), EosEventListenerFilter::Miner);
        listener.start();
        listener
    }

    async fn user_listener(&self, _: Option<u64>) -> Self::EventListener {
        let listener = EosListener::new(self.clone(), EosEventListenerFilter::User);
        listener.start();
        listener
    }
}