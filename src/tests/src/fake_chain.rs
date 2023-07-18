use log::*;
use std::{
    sync::{Arc, RwLock}, 
    collections::{HashMap, BTreeMap, LinkedList}, 
    time::Duration
};
use futures::future::{AbortRegistration};
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use sha2::{Digest};
use dmc_tools_common::*;

#[derive(Clone)]
enum ChainEvent {
    NewBill(DmcBill),
    NewOrder(DmcOrder),
    OrderChanged(DmcOrder),
    ChallengeChanged(DmcChallenge)
}

#[derive(Clone)]
struct ChainLog {
    block_number: u64, 
    tx_index: u32, 
    event: ChainEvent
}


#[derive(Debug, Clone, Serialize, Deserialize)]
enum ChainOption {
    SetApplyMethod(String), 
    Bill(DmcBillOptions), 
    Order(DmcOrderOptions),
    PrepareOrder(DmcPrepareOrderOptions), 
    Challenge(DmcChallengeOptions), 
    Proof(DmcProofOptions),
    CancelBill(u64),
    ModifyBillAsset((u64, i64)) // <bill-id, new-asset>
}   

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Transaction {
    from: String, 
    nonce: u32, 
    option: ChainOption, 
}

#[derive(Clone)]
struct Receipt {
    block_number: u64,
    tx_index: u32, 
    result: Result<Vec<u8>, String>
}

struct PendingQueue {
    queue: Vec<HashValue>
}

enum TransactionState {
    Executed(Receipt), 
    Pending {
        waiters: StateWaiter, 
        tx: Transaction, 
    }
}

struct AccountStub {
    nonce: u32, 
    apply_method: Option<String>
}

struct ChainImpl {
    accounts: HashMap<String, AccountStub>, 
    next_id: u64, 
    bills: BTreeMap<u64, DmcBill>, 
    orders: BTreeMap<u64, DmcOrder>, 
    challenges: BTreeMap<u64, DmcChallenge>, 
    block_number: u64, 

    pending: LinkedList<HashValue>, 
    transactions: BTreeMap<HashValue, TransactionState>, 
    logs: Vec<Vec<ChainLog>>, 
    listeners: Vec<FakeListener>
}

#[derive(Clone)]
pub struct FakeChain(Arc<RwLock<ChainImpl>>);

impl FakeChain {
    pub fn new() -> Self {
        let chain = Self(Arc::new(RwLock::new(ChainImpl {
            accounts: Default::default(), 
            next_id: 0, 
            bills: Default::default(), 
            orders: Default::default(), 
            challenges: Default::default(), 
            block_number: 0, 
            pending: Default::default(), 
            transactions: Default::default(), 
            logs: Default::default(),
            listeners: vec![]
        })));

        {
            let chain = chain.clone();
            async_std::task::spawn(async move {
                loop {
                    async_std::task::sleep(Duration::from_secs(1)).await;
                    chain.mine();
                }
            });
        }

        chain
    }

    pub fn mine(&self) {
        let logs = {
            let mut chain = self.0.write().unwrap();

            let block_number = chain.block_number + 1;
            info!("fake chain mine, begin, block_number={}", block_number);
            let mut pending = LinkedList::new();
            let mut tx_index = 0;
            let mut logs = vec![];
            while let Some(tx_id) = chain.pending.pop_front() {
                let tx = chain.transactions.get(&tx_id)
                    .and_then(|state| if let TransactionState::Pending { tx, .. } = state { Some(tx.clone()) } else { None }).unwrap(); 

                let exec = if let Some(stub) = chain.accounts.get_mut(&tx.from) {
                    if tx.nonce == stub.nonce {
                        stub.nonce += 1;
                        Some(tx.clone())
                    } else if tx.nonce > stub.nonce {
                        info!("fake chain mine, delay tx, block_number={}, tx_id={}", block_number, tx_id);
                        pending.push_back(tx_id.clone());
                        None
                    } else {
                        unreachable!()
                    }
                } else {
                    if tx.nonce == 0 {
                        chain.accounts.insert(tx.from.clone(), AccountStub { nonce: 1, apply_method: None });
                        Some(tx.clone())
                    } else {
                        info!("fake chain mine, delay tx, block_number={}, tx_id={}", block_number, tx_id);
                        pending.push_back(tx_id.clone());
                        None
                    }
                };
                
            
                if let Some(tx) = exec {
                    info!("fake chain mine, execute tx, block_number={}, tx_id={}", block_number, tx_id);
                    let result = match tx.option {
                        ChainOption::SetApplyMethod(method) => {
                            info!("fake chain mine, execute set apply method, block_number={}, tx_id={}, options={:?}", block_number, tx_id, method);

                            chain.accounts.get_mut(&tx.from).unwrap().apply_method = Some(method);

                            Ok((vec![], vec![]))
                        }, 
                        ChainOption::Bill(options) => {
                            info!("fake chain mine, execute bill, block_number={}, tx_id={}, options={:?}", block_number, tx_id, options);
                            let next_id = chain.next_id;
                            chain.next_id += 1;
                            let bill = DmcBill {
                                bill_id: next_id, 
                                miner: tx.from.clone(), 
                                asset: options.asset, 
                                price: options.price, 
                                pledge_rate: options.pledge_rate.unwrap_or(1), 
                                min_asset: options.min_asset.unwrap_or(1), 
                                expire_at: TimePointSec::from_now().offset(options.duration as i64 * 60 * 60 * 24 * 7).as_secs().unwrap() as u64, 
                            };
                            let raw = serde_json::to_vec(&bill).unwrap();
                            chain.bills.insert(next_id, bill.clone());

                            Ok((raw, vec![ChainEvent::NewBill(bill)]))
                        },
                        ChainOption::Order(options) => {
                            fn deal_order(chain: &mut ChainImpl, from: String, options: DmcOrderOptions) -> DmcResult<(Vec<u8>, Vec<ChainEvent>)> {
                                let bill = {
                                    let bill = chain.bills.get_mut(&options.bill_id).ok_or_else(|| DmcError::new(DmcErrorCode::Failed, "no bill"))?;
                                    if options.asset > bill.asset || options.asset % bill.min_asset > 0 {
                                        return Err(DmcError::new(DmcErrorCode::Failed, "invalid asset"));
                                    }
                                    bill.asset -= options.asset;
                                    bill.clone()
                                };

                                let next_id = chain.next_id;
                                chain.next_id += 1;

                                let order = DmcOrder {
                                    order_id: next_id, 
                                    bill_id: options.bill_id, 
                                    user: from, 
                                    miner: bill.miner, 
                                    asset: options.asset, 
                                    duration: options.duration, 
                                    price: bill.price, 
                                    pledge_rate: bill.pledge_rate, 
                                    state: DmcOrderState::Preparing { miner: None, user: None }, 
                                    start_at: dmc_time_now() as u128
                                };
                                let raw = serde_json::to_vec(&order).unwrap();
                                chain.orders.insert(next_id, order.clone());
                                Ok((raw, vec![ChainEvent::NewOrder(order)]))
                            }

                            deal_order(&mut *chain, tx.from, options)
                        }, 
                        ChainOption::PrepareOrder(options) => {
                            fn prepare_order(chain: &mut ChainImpl, from: String, options: DmcPrepareOrderOptions) -> DmcResult<(Vec<u8>, Vec<ChainEvent>)> {
                                let order = chain.orders.get_mut(&options.order_id).ok_or_else(|| DmcError::new(DmcErrorCode::Failed, "no order"))?;
                                let miner_side = if from.eq(&order.miner) {
                                    true
                                } else if from.eq(&order.user) {
                                    false
                                } else {
                                    return Err(DmcError::new(DmcErrorCode::Failed, "invalid sender"));
                                };

                                if let DmcOrderState::Preparing { miner, user } = &mut order.state {
                                    if miner_side && miner.is_none() {
                                        *miner = Some(options.merkle_stub);
                                    } else if !miner_side && user.is_none() {
                                        *user = Some(options.merkle_stub);
                                    } else {
                                        return Err(DmcError::new(DmcErrorCode::Failed, "already commited"));
                                    }
                                } else {
                                    return Err(DmcError::new(DmcErrorCode::Failed, "not preparing"));
                                }

                                if let DmcOrderState::Preparing { miner, user } = &order.state {
                                    if miner.is_some() && user.is_some() {
                                        let mut new_state = order.clone();
                                        let left = miner.as_ref().unwrap();
                                        let right = user.as_ref().unwrap();
                                        if left == right {
                                            new_state.state = DmcOrderState::Storing(left.clone());
                                        } else {
                                            error!("merkle not matched: miner: {:?}, user: {:?}", left, right);
                                            new_state.state = DmcOrderState::Canceled;
                                        }
                                        *order = new_state.clone();
                                        Ok((vec![], vec![ChainEvent::OrderChanged(new_state)]))
                                    } else {
                                        Ok((vec![], vec![]))
                                    }
                                } else {
                                    unreachable!()
                                }
                            }

                            prepare_order(&mut *chain, tx.from, options)
                        },
                        ChainOption::Challenge(options) => {
                            fn challenge(chain: &mut ChainImpl, from: String, options: DmcChallengeOptions) -> DmcResult<(Vec<u8>, Vec<ChainEvent>)> {
                                let order = chain.orders.get_mut(&options.order_id).ok_or_else(|| DmcError::new(DmcErrorCode::Failed, "no order"))?;
                                if !from.eq(&order.user) {
                                    return Err(DmcError::new(DmcErrorCode::Failed, "invalid sender"));
                                };

                                if let DmcOrderState::Storing {..} = &mut order.state {
                                    if chain.challenges.get(&order.order_id).is_some() {
                                        return Err(DmcError::new(DmcErrorCode::Failed, "exists challenge"));
                                    }

                                    let challenge_id = chain.next_id;
                                    chain.next_id += 1;
                                    let challenge = DmcChallenge { 
                                        challenge_id, 
                                        order_id: order.order_id, 
                                        start_at: dmc_time_now() as u128, 
                                        state: DmcChallengeState::Waiting, 
                                        params: match options.options {
                                            DmcChallengeTypedOptions::SourceStub { piece_index, nonce, hash } => {
                                                DmcChallengeParams::SourceStub { piece_index, nonce, hash }
                                            },
                                            DmcChallengeTypedOptions::MerklePath { .. } => {todo!()}, 
                                        } 
                                    };
                                    chain.challenges.insert(challenge_id, challenge.clone());
                                    Ok((vec![], vec![ChainEvent::ChallengeChanged(challenge)]))
                                } else {
                                    return Err(DmcError::new(DmcErrorCode::Failed, "not storing"));
                                }
                            }

                            challenge(&mut *chain, tx.from, options)
                        },
                        ChainOption::Proof(options) => {
                            fn proof(chain: &mut ChainImpl, from: String, options: DmcProofOptions) -> DmcResult<(Vec<u8>, Vec<ChainEvent>)> {
                                let stub = chain.challenges.get_mut(&options.challenge_id).ok_or_else(|| DmcError::new(DmcErrorCode::Failed, "no challenge"))?;
                                let order = chain.orders.get_mut(&options.order_id).ok_or_else(|| DmcError::new(DmcErrorCode::Failed, "no order"))?;
                                if !from.eq(&order.miner) {
                                    return Err(DmcError::new(DmcErrorCode::Failed, "invalid sender"));
                                };
                                if let DmcOrderState::Storing(merkle_stub) = &order.state {
                                    if let DmcChallengeParams::SourceStub { hash: challenge, piece_index, .. } = &stub.params {
                                        match options.proof {
                                            DmcTypedProof::SourceStub { hash: proof } => {
                                                let proof: [u8; 32] = sha2::Sha256::digest(proof.as_slice()).into();
                                                let proof = HashValue::from(&proof);    
                                                if proof.eq(challenge) {
                                                    stub.state = DmcChallengeState::Prooved;
                                                    Ok((vec![], vec![ChainEvent::ChallengeChanged(stub.clone())]))
                                                } else {
                                                    Err(DmcError::new(DmcErrorCode::Failed, "unmatched proof"))
                                                }
                                            },
                                            DmcTypedProof::MerklePath { data, pathes } => {
                                                let verifier = MerkleStubProc::verifier(merkle_stub.leaves, merkle_stub.piece_size);
                                                let proof: MerkleStubProof<MerkleStubSha256> = MerkleStubProof {
                                                    piece_index: *piece_index, 
                                                    piece_content: data.into(),
                                                    pathes
                                                };
                                                if verifier.verify_root(merkle_stub.root, *piece_index, proof).unwrap() {
                                                    error!("proof verify ok");
                                            
                                                    stub.state = DmcChallengeState::Failed;
                                                    order.state = DmcOrderState::Canceled;
                                                    Ok((vec![], vec![ChainEvent::ChallengeChanged(stub.clone()), ChainEvent::OrderChanged(order.clone())]))
                                                } else {
                                                    Err(DmcError::new(DmcErrorCode::Failed, "unmatched proof"))
                                                }
                                            }
                                        }
                                    } else {
                                        unimplemented!()
                                    }
                                } else {
                                    Err(DmcError::new(DmcErrorCode::Failed, "order not storing"))
                                }
                               
                            }
                            info!("fake chain mine, execute proof, block_number={}, tx_id={}, options={:?}", block_number, tx_id, options);
                            proof(&mut *chain, tx.from, options)
                                .map(|r| {
                                    info!("fake chain mine, execute proof, prooved");
                                    r
                                })
                                .map_err(|err| {
                                    info!("fake chain mine, execute proof, failed, err={}", err);
                                    err
                                })
                        }
                        ChainOption::CancelBill(bill_id) => {
                            match chain.bills.remove(&bill_id) {
                                Some(_) => Ok((vec![], vec![])),
                                None => Err(dmc_err!(DmcErrorCode::NotFound, "fake chain mine, remove bill({}) failed for not found", bill_id))
                            }
                        },
                        ChainOption::ModifyBillAsset((bill_id, asset)) => {
                            match chain.bills.get_mut(&bill_id) {
                                Some(bill) => {
                                    if asset > 0 {
                                        bill.asset += asset as u64;
                                    } else {
                                        bill.asset -= (-asset) as u64;
                                    }
                                    
                                    Ok((vec![], vec![]))
                                },
                                None => Err(dmc_err!(DmcErrorCode::NotFound, "fake chain mine, remove bill({}) failed for not found", bill_id))
                            }
                        },
                    };

                    
                    let receipt = match result {
                        Ok((raw,events)) => {
                            logs.append(&mut events.into_iter().map(|event| ChainLog {
                                block_number, 
                                tx_index, 
                                event
                            }).collect());

                            Receipt {
                                block_number, 
                                tx_index, 
                                result: Ok(raw)
                            }
                        }, 
                        Err(err) => {
                            Receipt {
                                block_number, 
                                tx_index, 
                                result: Err(err.msg().to_owned())
                            }
                        }
                    };
                    if let Some(TransactionState::Pending { waiters, ..}) = chain.transactions.insert(tx_id, TransactionState::Executed(receipt)) {
                        waiters.wake();
                    } else {
                        unreachable!()
                    }

                    tx_index += 1;
                } 
            }
            chain.logs.push(logs.clone());
            chain.block_number = block_number;
            chain.pending = pending;
            logs
        };
        
        let listeners = self.0.read().unwrap().listeners.clone();
        for listener in listeners {
            for log in &logs {
                listener.push_log(self, log);
            }
        }
    } 
}

impl FakeChain {
    fn get_receipt(&self, id: &HashValue) -> Option<Receipt> {
        self.0.read().unwrap().transactions.get(id).and_then(|tx| 
            if let TransactionState::Executed(receipt) = tx {
                Some(receipt.clone())
            } else{
                None
            })
    }

    fn apply_method_of(&self, account: &str) -> Option<String> {
        self.0.read().unwrap().accounts.get(account).and_then(|stub| stub.apply_method.clone())
    }

    fn nonce_of(&self, account: &str) -> u32 {
        self.0.read().unwrap().accounts.get(account).map(|stub| stub.nonce).unwrap_or_default()
    }

    fn get_order_by_id(&self, order_id: &u64) -> Option<DmcOrder> {
        self.0.read().unwrap().orders.get(order_id).cloned()
    }

    fn get_bill_by_id(&self, bill_id: &u64) -> Option<DmcBill> {
        self.0.read().unwrap().bills.get(bill_id).cloned()
    }

    fn create_pending(&self, tx: Transaction) -> FakePending {
        let raw = serde_json::to_vec(&tx).unwrap();
        let hash: [u8; 32] = sha2::Sha256::digest(raw.as_slice()).into();
        let hash = HashValue::from(&hash);
        
        let state = {
            let mut chain = self.0.write().unwrap();
            if let Some(state) = chain.transactions.get_mut(&hash) {
                match state {
                    TransactionState::Executed(receipt) => PendingState::Executed(receipt.clone()), 
                    TransactionState::Pending { waiters, ..} => PendingState::Pending(Some(waiters.new_waiter())), 
                }
            } else {
                let nonce = chain.accounts.get(&tx.from).map(|stub| stub.nonce).unwrap_or_default();
                if tx.nonce >= nonce {
                    info!("fake chain, add to pending, tx_id={}", hash);
                    let mut waiters = StateWaiter::new(); 
                   
                    let ret = PendingState::Pending(Some(waiters.new_waiter()));
                    let state = TransactionState::Pending {
                        waiters, 
                        tx: tx.clone()
                    };
                    chain.transactions.insert(hash.clone(), state);
                    chain.pending.push_back(hash.clone());
                    ret
                } else {
                    info!("fake chain, drop tx for low nonce, tx_id={}", hash);
                    PendingState::Err(DmcError::new(DmcErrorCode::InvalidInput, "error nonce"))
                }
            }
        };
        
        FakePending {
            raw, 
            hash, 
            chain: self.clone(),
            state: RwLock::new(state)
        }
    }


    fn add_listener(&self, listener: FakeListener) {
        let mut chain = self.0.write().unwrap();
        for logs in &chain.logs {
            for log in logs {
                listener.push_log(self, log);
            }
        }
        chain.listeners.push(listener);
    }
}


enum PendingState {
    Err(DmcError), 
    Pending(Option<AbortRegistration>),
    Executed(Receipt)
}

pub struct FakePending {
    hash: HashValue, 
    raw: Vec<u8>, 
    chain: FakeChain, 
    state: RwLock<PendingState>
}

impl FakePending {
    async fn wait(&self) -> DmcResult<Receipt> {
        enum Next {
            Return(DmcResult<Receipt>),
            Wait(AbortRegistration)
        }
        let next = {
            match &mut *self.state.write().unwrap() {
                PendingState::Err(err) => Next::Return(Err(err.clone())), 
                PendingState::Executed(receipt) => Next::Return(Ok(receipt.clone())), 
                PendingState::Pending(waiter) => {
                    let mut ret = None;
                    std::mem::swap(&mut ret, waiter);
                    Next::Wait(ret.unwrap())
                }
            }
        };

        match next {
            Next::Return(result) => result, 
            Next::Wait(waiter) => {
                let _ = StateWaiter::wait(waiter, || ()).await;
                self.chain.get_receipt(&self.hash).ok_or_else(|| DmcError::new(DmcErrorCode::InvalidInput, "nonce error"))
            }
        }
        
    }
}

impl AsRef<[u8]> for FakePending {
    fn as_ref(&self) -> &[u8] {
        self.raw.as_slice()
    }
}

#[async_trait]
impl DmcPendingBill for FakePending {
    fn tx_id(&self) -> &[u8] {
        self.hash.as_slice()
    }
    async fn wait(&self) -> DmcResult<DmcPendingResult<DmcBill>> {
        let receipt = self.wait().await?;
        Ok(DmcPendingResult {
            block_number: receipt.block_number,
            tx_index: receipt.tx_index,
            result: receipt.result.map(|raw| serde_json::from_slice(raw.as_slice()).unwrap())
        })    
    }
}


#[async_trait]
impl DmcPendingOrder for FakePending {
    fn tx_id(&self) -> &[u8] {
        self.hash.as_slice()
    }
    async fn wait(&self) -> DmcResult<DmcPendingResult<DmcOrder>> {
        let receipt = self.wait().await?;
        Ok(DmcPendingResult {
            block_number: receipt.block_number,
            tx_index: receipt.tx_index,
            result: receipt.result.map(|raw| serde_json::from_slice(raw.as_slice()).unwrap())
        })    
    }
}


trait FakeEventFilter: Send + Sync {
    fn push_log(&self, chain: &FakeChain, log: &ChainLog) -> Option<DmcTypedEvent>;
}

struct ListenerImpl {
    send: async_std::channel::Sender<DmcEvent>, 
    recv: async_std::channel::Receiver<DmcEvent>,  
    filter: Box<dyn 'static + FakeEventFilter>
}

#[derive(Clone)]
pub struct FakeListener(Arc<ListenerImpl>);

impl FakeListener {
    fn new<T: 'static + FakeEventFilter>(filter: T) -> Self {
        let (send, recv) = async_std::channel::unbounded();
        Self(Arc::new(ListenerImpl {
            send, 
            recv, 
            filter: Box::new(filter)
        }))
    }

    fn filter(&self) ->&dyn FakeEventFilter {
        self.0.filter.as_ref()
    }

    fn push_log(&self, chain: &FakeChain, log: &ChainLog) {
        if let Some(event) = self.filter().push_log(chain, log) {
            let _ = self.0.send.try_send(DmcEvent {
                block_number: log.block_number,
                tx_index: log.tx_index, 
                event
            });
        }
    }
}

#[async_trait]
impl DmcEventListener for FakeListener {
    async fn next(&self) -> DmcResult<DmcEvent> {
        Ok(self.0.recv.recv().await.unwrap())
    }
}



struct ClientImpl {
    account: String,
    chain: FakeChain
}

#[derive(Clone)]
pub struct FakeChainClient(Arc<ClientImpl>); 

impl FakeChainClient {
    pub fn none_account(chain: FakeChain) -> Self {
        Self(Arc::new(ClientImpl {
            account: "".to_owned(), 
            chain, 
        }))
    }

    pub fn with_account(account: String, chain: FakeChain) -> Self {
        Self(Arc::new(ClientImpl {
            account, 
            chain, 
        }))
    }

    fn chain(&self) -> &FakeChain {
        &self.0.chain
    }
}

#[async_trait]
impl DmcChainClient for FakeChainClient {
    async fn get_apply_method(&self, account: &str) -> DmcResult<Option<String>> {
        Ok(self.chain().apply_method_of(account))
    }

    async fn get_bill_by_id(&self, bill_id: u64) -> DmcResult<Option<DmcBill>> {
        Ok(self.chain().get_bill_by_id(&bill_id))
    }

    async fn get_order_by_id(&self, order_id: u64) -> DmcResult<Option<DmcOrder>> {
        Ok(self.chain().get_order_by_id(&order_id))
    }

    type EventListener = FakeListener;
    async fn event_listener(&self, _: Option<u64>) -> Self::EventListener {
        struct Filter {
        }

        impl FakeEventFilter for Filter {
            fn push_log(&self, _: &FakeChain, log: &ChainLog) -> Option<DmcTypedEvent> {
                match &log.event {
                    ChainEvent::NewBill(bill) => Some(DmcTypedEvent::BillChanged(bill.clone())), 
                    _ => None
                }
            }
        }

        let listener = FakeListener::new(Filter {});
        {
            let chain = self.chain().clone();
            let listener = listener.clone();
            async_std::task::spawn(async move {
                chain.add_listener(listener);
            });
        }
        listener
    }

    async fn verify(&self, _: &str, _: &[u8], _: &str) -> DmcResult<bool> {
        Ok(true)
    }
}

#[async_trait]
impl DmcChainAccountClient for FakeChainClient {
    fn account(&self) -> &str {
        &self.0.account
    }

    async fn get_available_assets(&self) -> DmcResult<Vec<DmcAsset>> {
        Ok(vec![])
    }

    async fn sign(&self, _: &[u8]) -> DmcResult<String> {
        Ok("no-signature".to_owned())
    }

    async fn set_apply_method(&self, method: String) -> DmcResult<DmcResult<()>> {
        let receipt = self.chain().create_pending(Transaction {
            from: self.account().to_owned(),
            nonce: self.chain().nonce_of(&self.account()),
            option: ChainOption::SetApplyMethod(method)
        }).wait().await?;
        Ok(receipt.result.map(|_| ()).map_err(|err| DmcError::new(DmcErrorCode::Failed, err)))
    }

    type PendingBill = FakePending;
    async fn create_bill(&self, options: DmcBillOptions) -> DmcResult<Self::PendingBill> {
        info!("fake chain create bill, options={:?}", options);
        Ok(self.chain().create_pending(Transaction {
            from: self.account().to_owned(),
            nonce: self.chain().nonce_of(&self.account()),
            option: ChainOption::Bill(options)
        }))
    }

    async fn load_bill(&self, pending: &[u8]) -> DmcResult<Self::PendingBill> {
        let tx = serde_json::from_slice(pending)?;
        Ok(self.chain().create_pending(tx))
    }

    async fn finish_bill(&self, bill_id: u64) -> DmcResult<DmcResult<()>> {
        info!("fake chain cancel bill, bill_id={:?}", bill_id);
        
        let pending = self.chain().create_pending(Transaction {
            from: self.account().to_owned(),
            nonce: self.chain().nonce_of(&self.account()),
            option: ChainOption::CancelBill(bill_id)
        });

        let receipt = pending.wait().await?;
        Ok(receipt.result.map(|_| ()).map_err(|err| DmcError::new(DmcErrorCode::Failed, err)))
    }

    fn support_modify_bill() -> bool {
        true
    }

    async fn modify_bill_asset(&self, bill_id: u64, inc: i64) -> DmcResult<DmcResult<()>> {
        info!("fake chain set bill capcity, bill_id={:?}, inc={}", bill_id, inc);

        let pending = self.chain().create_pending(Transaction {
            from: self.account().to_owned(),
            nonce: self.chain().nonce_of(&self.account()),
            option: ChainOption::ModifyBillAsset((bill_id, inc))
        });

        let receipt = pending.wait().await?;

        Ok(receipt.result.map(|_| ()).map_err(|err| DmcError::new(DmcErrorCode::Failed, err)))
    }
    
    type PendingOrder = FakePending;

    async fn create_order(&self, options: DmcOrderOptions) -> DmcResult<Self::PendingOrder>  {
        Ok(self.chain().create_pending(Transaction {
            from: self.account().to_owned(),
            nonce: self.chain().nonce_of(&self.account()),
            option: ChainOption::Order(options)
        }))
    }

    async fn load_order(&self, pending: &[u8]) -> DmcResult<Self::PendingOrder> {
        let tx = serde_json::from_slice(pending)?;
        Ok(self.chain().create_pending(tx))
    }

    async fn prepare_order(&self, options: DmcPrepareOrderOptions) -> DmcResult<DmcResult<()>> {
        let receipt = self.chain().create_pending(Transaction {
            from: self.account().to_owned(),
            nonce: self.chain().nonce_of(&self.account()),
            option: ChainOption::PrepareOrder(options)
        }).wait().await?;
        Ok(receipt.result.map(|_| ()).map_err(|err| DmcError::new(DmcErrorCode::Failed, err)))
    }

    async fn finish_order(&self, _: u64) -> DmcResult<DmcResult<f64>> {
        unimplemented!()
    }

    fn supported_challenge_type() -> DmcChallengeTypeCode {
        DmcChallengeTypeCode::SourceStub
    }

    async fn challenge(&self, options: DmcChallengeOptions) -> DmcResult<DmcResult<()>> {
        let receipt = self.chain().create_pending(Transaction {
            from: self.account().to_owned(),
            nonce: self.chain().nonce_of(&self.account()),
            option: ChainOption::Challenge(options)
        }).wait().await?;
        Ok(receipt.result.map(|_| ()).map_err(|err| DmcError::new(DmcErrorCode::Failed, err)))
    }

    async fn proof(&self, options: DmcProofOptions) -> DmcResult<DmcResult<()>> {
        let receipt = self.chain().create_pending(Transaction {
            from: self.account().to_owned(),
            nonce: self.chain().nonce_of(&self.account()),
            option: ChainOption::Proof(options)
        }).wait().await?;
        Ok(receipt.result.map(|_| ()).map_err(|err| DmcError::new(DmcErrorCode::Failed, err)))
    }

    async fn miner_listener(&self, _: Option<u64>) -> Self::EventListener {
        struct Filter {
            account: String
        }

        impl FakeEventFilter for Filter {
            fn push_log(&self, chain: &FakeChain, log: &ChainLog) -> Option<DmcTypedEvent> {
                match &log.event {
                    ChainEvent::NewOrder(order) => {
                        if order.miner == self.account {
                            Some(DmcTypedEvent::OrderChanged(order.clone()))
                        } else {
                            None
                        }
                    }, 
                    ChainEvent::OrderChanged(order) => {
                        if order.miner == self.account {
                            Some(DmcTypedEvent::OrderChanged(order.clone()))
                        } else {
                            None
                        }
                    }, 
                    ChainEvent::ChallengeChanged(challenge) => {
                        let order = chain.get_order_by_id(&challenge.order_id).unwrap();
                        if order.miner == self.account {
                            Some(DmcTypedEvent::ChallengeChanged(order, challenge.clone()))
                        } else {
                            None
                        }
                    }, 
                    _ => None
                }
            }
        }

        let listener = FakeListener::new(Filter { account: self.account().to_owned() });

        {
            let chain = self.chain().clone();
            let listener = listener.clone();
            async_std::task::spawn(async move {
                chain.add_listener(listener);
            });
        }
        
        listener
    }

    async fn user_listener(&self, _: Option<u64>) -> Self::EventListener {
        struct Filter {
            account: String
        }

        impl FakeEventFilter for Filter {
            fn push_log(&self, chain: &FakeChain, log: &ChainLog) -> Option<DmcTypedEvent> {
                match &log.event {
                    ChainEvent::OrderChanged(order) => {
                        if order.user == self.account {
                            Some(DmcTypedEvent::OrderChanged(order.clone()))
                        } else {
                            None
                        }
                    }, 
                    ChainEvent::ChallengeChanged(challenge) => {
                        let order = chain.get_order_by_id(&challenge.order_id).unwrap();
                        if order.user == self.account {
                            Some(DmcTypedEvent::ChallengeChanged(order, challenge.clone()))
                        } else {
                            None
                        }
                    }, 
                    _ => None
                }
            }
        }

        let listener = FakeListener::new(Filter { account: self.account().to_owned() });
        {
            let chain = self.chain().clone();
            let listener = listener.clone();
            async_std::task::spawn(async move {
                chain.add_listener(listener);
            });
        }

        listener
    }
}

