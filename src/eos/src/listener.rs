use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::io::{Write};
use std::ops::Deref;
use std::path::{PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::{error, info};
use dmc_tools_common::*;
use crate::EosClient;
use crate::rpc::GetTableRowsReq;
use crate::types::EosOrderTableRow;

const MIN_ORDER_ID: u64 = 174;

#[derive(Clone)]
pub struct EosListener(Arc<EosListenerImpl>);

pub struct EosListenerImpl {
    client: EosClient,
    filter: EosEventListenerFilter,

    send: async_std::channel::Sender<DmcEvent>,
    recv: async_std::channel::Receiver<DmcEvent>,
    cur_order: Mutex<HashMap<u64, DmcOrder>>,
    cur_challenge: Mutex<HashMap<u64, DmcChallenge>>,
}

#[derive(PartialEq)]
pub(crate) enum EosEventListenerFilter {
    Miner,
    User
}

impl Display for EosEventListenerFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EosEventListenerFilter::Miner => write!(f, "miner"),
            EosEventListenerFilter::User => write!(f, "user")
        }
    }
}

impl EosListener {
    pub(crate) fn new(client: EosClient, filter: EosEventListenerFilter) -> Self {
        let (send, recv) = async_std::channel::bounded::<DmcEvent>(100);
        let this = Self(Arc::new(EosListenerImpl {
            client,
            filter,
            send,
            recv,
            cur_order: Mutex::new(HashMap::new()),
            cur_challenge: Mutex::new(HashMap::new()),
        }));

        if let Err(_) = this.load_order() {
            let order_storage = this.order_storage_path();
            info!("try delete error storage {}", order_storage.display());
            let _ = std::fs::remove_file(order_storage);
        }
        this
    }

    fn order_storage_path(&self) -> PathBuf {
        PathBuf::from(format!("eos_order_storage_{}_{}.bin", self.0.client.account(), &self.0.filter))
    }

    fn save_order(&self) -> DmcResult<()> {
        let order_storage = self.order_storage_path();

        let mut file = std::fs::File::create(&order_storage).map_err(|e| {
            error!("write order id to file {} err {}", order_storage.display(), e);
            e
        })?;

        if let Err(e) = serde_json::to_writer(&file, self.0.cur_order.lock().unwrap().deref()).map_err(|e| {
            error!("write storaged orders to {} err {}", order_storage.display(), e);
            e
        }) {
            let _ = std::fs::remove_file(&order_storage);
            return Err(DmcError::from(e));
        }

        let _ = file.flush();

        Ok(())
    }

    fn load_order(&self) -> DmcResult<()> {
        let order_storage = self.order_storage_path();
        if order_storage.exists() {
            let file = std::fs::File::open(&order_storage).map_err(|e| {
                error!("open order storage file {} err {}", order_storage.display(), e);
                e
            })?;

            let cur_orders = serde_json::from_reader(file).map_err(|e| {
                error!("load order storage {} err {}", order_storage.display(), e);
                e
            })?;

            *self.0.cur_order.lock().unwrap() = cur_orders;
        }

        Ok(())
    }

    pub(crate) fn start(&self) {
        let listener = self.clone();
        let position = match self.0.filter {
            EosEventListenerFilter::Miner => 3,
            EosEventListenerFilter::User => 2
        };
        async_std::task::spawn(async move {
            let mut cur_block = 0;
            loop {
                match listener.0.client.rpc().get_info().await {
                    Ok(info) => {
                        if cur_block < info.head_block_num {
                            info!("start scan order table");

                            let _ = listener.update_orders(position, info.head_block_num).await;

                            cur_block = info.head_block_num;
                        }
                    }
                    Err(e) => {
                        error!("get latest block err {}", e);
                    }
                }
                async_std::task::sleep(Duration::from_secs(10)).await;
            }

        });
    }

    // also update challenge when checking order, because challenge is binded to order
    async fn update_orders(&self, position: usize, block_number: i64) -> DmcResult<()> {
        let mut cur_order = HashMap::new();
        let mut cur_challenge = HashMap::new();
        info!("get orders by name {}, index {}", self.0.client.account(), position);
        let row_ret = self.0.client.rpc().get_table_rows::<_, EosOrderTableRow>(&GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "dmcorder",
            scope: "dmc.token",
            index_position: Some(position),
            key_type: Some("name"),
            encode_type: None,
            lower_bound: Some(self.0.client.account()),
            upper_bound: Some(self.0.client.account()),
            limit: Some(1000),
            reverse: None,
            show_payer: None,
        }).await.map_err(|e| {
            error!("get order table rows err {}", e);
            e
        })?;

        info!("get {} orders", row_ret.rows.len());

        for row in row_ret.rows {
            if row.order_id < MIN_ORDER_ID {
                continue;
            }
            if let Ok((order, challenge)) = self.get_dmc_order(row).await {
                if challenge.state != DmcChallengeState::Invalid {
                    cur_challenge.insert(order.order_id, challenge);
                }

                cur_order.insert(order.order_id, order);
            }
        }

        let mut events = Vec::new();
        {
            let mut store_orders = self.0.cur_order.lock().unwrap();
            for (id, order) in cur_order.iter() {
                if !store_orders.contains_key(id) {
                    info!("order id {} create", *id);
                    events.push(DmcEvent {
                        block_number: block_number as u64,
                        tx_index: 0,
                        event: DmcTypedEvent::OrderChanged(order.clone()),
                    });
                } else {
                    let store_order = store_orders.get(id).unwrap();
                    if store_order.state != order.state {
                        info!("order id {} changed", *id);
                        events.push(DmcEvent {
                            block_number: block_number as u64,
                            tx_index: 0,
                            event: DmcTypedEvent::OrderChanged(order.clone()),
                        });
                    }
                }
            }

            let stored_id: HashSet<u64> = store_orders.keys().map(|k|*k).collect();
            for id in stored_id.difference(&cur_order.keys().map(|k|*k).collect()) {
                events.push(DmcEvent {
                    block_number: block_number as u64,
                    tx_index: 0,
                    event: DmcTypedEvent::OrderCanceled(*id)
                });
            }

            *store_orders = cur_order.clone();
        }

        {
            let mut store_challenges = self.0.cur_challenge.lock().unwrap();
            for (id, challenge) in cur_challenge.iter() {
                let cur_order = cur_order.get(id).unwrap().clone();

                if let Some(store_challenge) = store_challenges.get(id) {
                    if store_challenge.challenge_id != challenge.challenge_id {
                        // 换了一个challenge，我不知道上一个是什么结果.....上一个当prooved算
                        info!("order {} changed challenge id from {} to {} ", cur_order.order_id, store_challenge.challenge_id, challenge.challenge_id);
                        let mut finished_challenge = store_challenge.clone();
                        finished_challenge.state = DmcChallengeState::Prooved;
                        events.push(DmcEvent {
                            block_number: block_number as u64,
                            tx_index: 0,
                            event: DmcTypedEvent::ChallengeChanged(cur_order.clone(), finished_challenge),
                        });
                        events.push(DmcEvent {
                            block_number: block_number as u64,
                            tx_index: 0,
                            event: DmcTypedEvent::ChallengeChanged(cur_order, challenge.clone()),
                        });
                    } else if store_challenge.state != challenge.state {
                        info!("challenge id {} changed", *id);
                        events.push(DmcEvent {
                            block_number: block_number as u64,
                            tx_index: 0,
                            event: DmcTypedEvent::ChallengeChanged(cur_order, challenge.clone()),
                        });
                    }
                } else {
                    info!("challenge id {} create", *id);
                    events.push(DmcEvent {
                        block_number: block_number as u64,
                        tx_index: 0,
                        event: DmcTypedEvent::ChallengeChanged(cur_order, challenge.clone()),
                    });
                }
            }

            *store_challenges = cur_challenge.clone();
        }

        let _ = self.save_order();


        for event in events {
            let _ = self.0.send.send(event).await;
        }

        Ok(())
    }

    async fn get_dmc_order(&self, row: EosOrderTableRow) -> DmcResult<(DmcOrder, DmcChallenge)> {
        let challenge_row = loop {
            match self.0.client.get_challenge_by_id(row.order_id).await {
                Ok(row) => {
                    break row;
                }
                Err(e) => {
                    error!("get challenge by order {} err {}, retry after 1 sec", row.order_id, e);
                }
            }

            async_std::task::sleep(Duration::from_secs(1)).await;
        };
        let challenge = challenge_row.ok_or(DmcError::from(DmcErrorCode::NotFound)).map_err(|e| {
            error!("get challenge by order id {} err {}", row.order_id, e);
            e
        })?;

        let dmc_challenge = self.0.client.dmc_challenge_from_row(&challenge);

        Ok((self.0.client.dmc_order_from_row(&row, &challenge)?, dmc_challenge))
    }
}

#[async_trait::async_trait]
impl DmcEventListener for EosListener {
    async fn next(&self) -> DmcResult<DmcEvent> {
        Ok(self.0.recv.recv().await.map_err(|e| {
            DmcError::new(DmcErrorCode::MpscRecvError, e.to_string())
        })?)
    }
}
