use log::*;
use std::{
    sync::Arc,
    time::Duration
};
use std::str::FromStr;
use sqlx::{ConnectOptions, Pool};
use serde::{Serialize, Deserialize};
use tide::{Request, Response, Body};
use dmc_tools_common::*;
use dmc_miner_journal::*;
use dmc_miner_sectors_client as raw_sectors;
use dmc_miner_account_client::*;
use dmc_miner_contracts::*;

#[derive(Serialize, Deserialize)]
pub struct AccountServerConfig {
    pub listen_address: String,
    pub sql_url: String, 
    pub journal_client: JournalClientConfig, 
    pub sector_client: raw_sectors::SectorClientConfig, 
    pub contract_client: ContractClientConfig, 
    pub retry_event_interval: Duration,  
    pub bill_atomic_interval: Duration,
    pub bill_retry_interval: Duration, 
}

#[derive(sqlx::FromRow)]
struct ActiveSectorRow {
    sector_id: u64, 
    raw_sector_id: u64,  
    bill_options: Vec<u8>, 
    state_code: u8, 
    state_value: Vec<u8>, 
    bill_id: Option<u64>, 
    block_number: Option<u64>, 
    tx_index: Option<u32>, 
    process_id: Option<u32>, 
    update_at: u64
}

impl TryInto<SectorWithId> for ActiveSectorRow {
    type Error = DmcError;
    fn try_into(self) -> DmcResult<SectorWithId> {
        let state = if self.state_code == Self::state_code_waiting() {
            SectorState::Waiting
        } else if self.state_code == Self::state_code_pre_bill() {
            SectorState::PreBill(DmcData::from(self.state_value))
        } else if self.state_code == Self::state_code_bill_error() {
            let error = String::from_utf8(self.state_value).map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid bill error {}", err)))?;
            let block_number = self.block_number.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no block number"))?;
            let tx_index = self.tx_index.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no tx index"))?;
            SectorState::BillError(SectorBillError { block_number, tx_index, error })
        } else if self.state_code == Self::state_code_post_bill() {
            let block_number = self.block_number.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no block number"))?;
            let tx_index = self.tx_index.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no tx index"))?;
            let bill_id = self.bill_id.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no bill id"))?;
            SectorState::PostBill(SectorBillInfo { bill_id, block_number, tx_index })
        } else if self.state_code == Self::state_code_remove_bill() {
            match self.bill_id {
                Some(bill_id) => {
                    let block_number = self.block_number.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no block number"))?;
                    let tx_index = self.tx_index.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no tx index"))?;
                    SectorState::Removed(Some(SectorBillInfo { bill_id, block_number, tx_index }))
                },
                None => SectorState::Removed(None)
            }
        } else {
            unreachable!()
        };

        let bill_options = serde_json::from_slice(&self.bill_options)
            .map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid bill options {}", err)))?;

        Ok(SectorWithId {
            sector_id: self.sector_id,
            info: Sector {
                raw_sector_id: self.raw_sector_id, 
                bill_options, 
                update_at: self.update_at, 
                state
            }
        })
    }
}


impl ActiveSectorRow {
    fn state_code_waiting() -> u8 {
        0
    }

    fn state_code_pre_bill() -> u8 {
        1
    }

    fn state_code_bill_error() -> u8 {
        2
    }

    fn state_code_post_bill() -> u8 {
        3
    }
    
    fn state_code_remove_bill() -> u8 {
        4
    }
}

struct ServerImpl<T: DmcChainAccountClient> {
    config: AccountServerConfig, 
    journal_client: JournalClient, 
    raw_sector_client: raw_sectors::SectorClient, 
    contract_client: ContractClient, 
    chain_client: T, 
    sql_pool: Pool<sqlx::MySql>, 
}


#[derive(Clone)]
pub struct AccountServer<T: DmcChainAccountClient>(Arc<ServerImpl<T>>);

impl<T: DmcChainAccountClient> std::fmt::Display for AccountServer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MinerAccountServer{{account={}}}", self.chain_client().account())
    }
}
 

impl<T: DmcChainAccountClient> AccountServer<T> {
    fn raw_sector_client(&self) -> &raw_sectors::SectorClient {
        &self.0.raw_sector_client
    }

    fn sql_pool(&self) -> &sqlx::Pool<sqlx::MySql> {
        &self.0.sql_pool
    }

    fn chain_client(&self) -> &T {
        &self.0.chain_client
    }

    fn account(&self) -> &str {
        self.chain_client().account()
    }

    fn config(&self) -> &AccountServerConfig {
        &self.0.config
    }

    fn journal_client(&self) -> &JournalClient {
        &self.0.journal_client
    }

    fn contract_client(&self) -> &ContractClient {
        &self.0.contract_client
    }

    fn sql_create_tbl_active_sector() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `active_sector` (
            `sector_id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
            `raw_sector_id` BIGINT UNSIGNED NOT NULL,
            `bill_options` BLOB, 
            `bill_id` BIGINT UNSIGNED, 
            `block_number` BIGINT UNSIGNED, 
            `tx_index` INT UNSIGNED, 
            `state_code` TINYINT UNSIGNED NOT NULL, 
            `state_value` BLOB,      
            `process_id` INT UNSIGNED, 
            `update_at` BIGINT UNSIGNED NOT NULL, 
            `removed_sector_id` BIGINT UNSIGNED, -- set to sector_id when removed
            INDEX (`bill_id`),
            UNIQUE INDEX `unique_active_raw` (`raw_sector_id`, `removed_sector_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
    }

    fn sql_update_tbl_active_sector() -> Vec<&'static str> {
        vec![
            r#"CREATE INDEX `update_at` ON `active_sector`(`update_at`);"#,
            r#"ALTER TABLE `active_sector` ADD COLUMN `removed_sector_id` BIGINT UNSIGNED"#,
            r#"ALTER TABLE `active_sector` DROP UNIQUE `raw_sector_id`"#,
            r#"ALTER TABLE `active_sector` ADD CONSTRAINT `unique_active_raw` UNIQUE (`raw_sector_id`, `removed_sector_id`)"#
        ]
    }

    async fn sync_bill_result(&self, sector: SectorWithId, pending: T::PendingBill, result: DmcPendingResult<DmcBill>) -> DmcResult<()> {
        info!("{} sync bill result, sector={:?}, result={:?}", self, sector, result);
        let update_at = dmc_time_now();
        let mut sector = sector;
        sector.info.update_at = update_at;
        let (qeury, journal) = match &result.result {
            Ok(bill) => {
                sector.info.state = SectorState::PostBill(SectorBillInfo { bill_id: bill.bill_id, block_number: result.block_number, tx_index: result.tx_index });
                let query = sqlx::query("UPDATE active_sector SET process_id=NULL, state_code=?, state_value=?, bill_id=?, block_number=?, tx_index=?, update_at=? WHERE sector_id=? AND state_code=? AND state_value=?")
                    .bind(ActiveSectorRow::state_code_post_bill()).bind(vec![]).bind(bill.bill_id).bind(result.block_number).bind(result.tx_index)
                    .bind(update_at).bind(sector.sector_id).bind(ActiveSectorRow::state_code_pre_bill()).bind(pending.as_ref());
                let journal = self.journal_client().append(JournalEvent { sector_id: sector.info.raw_sector_id, order_id: None, event_type: JournalEventType::BillCreated, event_params: Some(serde_json::to_string(bill).unwrap()) });
                (query, journal)
            }, 
            Err(err) => {
                let query = sqlx::query("UPDATE active_sector SET process_id=NULL, state_code=?, state_value=?, block_number=?, tx_index=?, update_at=? WHERE sector_id=? AND state_code=? AND state_value=?")
                    .bind(ActiveSectorRow::state_code_bill_error()).bind(err.as_bytes().to_vec()).bind(result.block_number)
                    .bind(result.tx_index).bind(update_at).bind(sector.sector_id).bind(ActiveSectorRow::state_code_pre_bill()).bind(pending.as_ref());
                let journal = self.journal_client().append(JournalEvent { sector_id: sector.info.raw_sector_id, order_id: None, event_type: JournalEventType::BillFailed, event_params: Some(err.to_owned()) });
                (query, journal)
            }
        };

        if qeury.execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} sync bill result, sector_id={}, result={:?}, err={}", self, sector.sector_id, result, err))?
            .rows_affected() > 0 {
            let _ = journal.await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} sync bill result, sector_id={}, result={:?}, err={}", self, sector.sector_id, result, err))?;
            info!("{} sync bill result, sector={}, result={:?}, saved result", self, sector.sector_id, result);
        } else {
            info!("{} sync bill result, sector={}, result={:?}, ignored", self, sector.sector_id, result);
        }

        Ok(())
    }

    async fn on_order_changed(&self, order: DmcOrder, block_number: u64, tx_index: u32) -> DmcResult<()> {
        let sector = self.sectors(&SectorFilter::from_bill(order.bill_id), &SectorNavigator::default()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} on order changed, order={:?}, err={}", self, order.order_id, err))?
            .get(0).cloned();
        let sector = if let Some(sector) = sector {
            if let SectorState::Removed(_) = sector.info.state {
                None
            } else {
                self.raw_sector_client().get(raw_sectors::SectorFilter::from_sector_id(sector.info.raw_sector_id), 1).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} on order changed, order={:?}, err={}", self,  order.order_id, err))?.next_page().await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} on order changed, order={:?}, err={}", self,  order.order_id, err))?.get(0).cloned()
            }
        } else {
            None
        };
        
        self.contract_client().update(UpdateContractOptions { 
            order: order.clone(), 
            challenge: None, 
            sector, 
            block_number,
            tx_index
        }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} on order changed, order={:?}, err={}", self,  order.order_id, err))
    }

    async fn listen_chain(&self) {
        info!("{} listen chain", self);
        // todo: set correct event start block number;
        let listener = self.chain_client().miner_listener(None).await;
        loop {
            match listener.next().await {
                Err(err) => {
                    error!("{} listen chain, err={}", self, err);
                }, 
                Ok(event) => {
                    info!("{} on event, event={:?}", self, event);
                    match event.event {
                        DmcTypedEvent::BillChanged(_) => {}, 
                        DmcTypedEvent::BillCanceled(_) => {}, 
                        DmcTypedEvent::EmptyEvent(()) => {},
                        DmcTypedEvent::OrderCanceled(order_id) => {
                            loop {
                                match self.contract_client().update(UpdateContractOptions { 
                                    order: DmcOrder {
                                        order_id, 
                                        bill_id: 0, 
                                        user: "".to_owned(), 
                                        miner: "".to_owned(), 
                                        asset: 0, 
                                        duration: 0, 
                                        price: 0,
                                        pledge_rate: 0, 
                                        state: DmcOrderState::Canceled, 
                                        start_at: 0
                                    }, 
                                    challenge: None, 
                                    sector: None, 
                                    block_number: event.block_number,
                                    tx_index: event.tx_index
                                }).await {
                                    Ok(_) => {
                                        break;
                                    }, 
                                    Err(err) => {
                                        info!("{} on event, order={:?}, got err retry, err={}", self, order_id, err);
                                        async_std::task::sleep(self.config().retry_event_interval).await;
                                        continue;
                                    }
                                }
                            }
                        }
                        DmcTypedEvent::OrderChanged(order) => {
                            loop {
                                match self.on_order_changed(order.clone(), event.block_number, event.tx_index).await {
                                    Ok(_) => {
                                        break;
                                    }, 
                                    Err(_) => {
                                        info!("{} on event order changed, got err retry, order={:?}", self, order);
                                        async_std::task::sleep(self.config().retry_event_interval).await;
                                        continue;
                                    }
                                }
                            }
                        },
                        DmcTypedEvent::ChallengeChanged(order, challenge) => {
                            loop {
                                match self.contract_client().update(UpdateContractOptions { 
                                    order: order.clone(), 
                                    challenge: Some(challenge.clone()), 
                                    sector: None, 
                                    block_number: event.block_number,
                                    tx_index: event.tx_index
                                }).await {
                                    Ok(_) => {
                                        break;
                                    }, 
                                    Err(_) => {
                                        info!("{} on event, got err retry, order={:?}", self, order);
                                        async_std::task::sleep(self.config().retry_event_interval).await;
                                        continue;
                                    }
                                }
                            }
                        },
                    }
                }
            }
        }
    }


    #[async_recursion::async_recursion]
    async fn create_bill_inner(&self, sector: SectorWithId) -> DmcResult<()> {
        info!("{} create pending bill, sector={:?}, update_at={}", self, sector, sector.info.update_at);
        let pending = self.chain_client().create_bill(sector.info.bill_options.clone()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create pending bill, sector={}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?;
        let raw_tx = pending.as_ref();

        let update_at = dmc_time_now();
        if sqlx::query("UPDATE active_sector SET process_id=NULL, state_code=?, state_value=?, update_at=? WHERE sector_id=? AND state_code=? AND update_at=?")
                .bind(ActiveSectorRow::state_code_pre_bill()).bind(raw_tx).bind(update_at)
                .bind(sector.sector_id).bind(ActiveSectorRow::state_code_waiting()).bind(sector.info.update_at)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create pending bill, sector={}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?
                .rows_affected() > 0 {
            info!("{} create pending bill, sector={:?}, update_at={}, pre ordered", self, sector.sector_id, sector.info.update_at);
            let mut sector = sector;
            sector.info.state = SectorState::PreBill(DmcData::from(raw_tx));
            sector.info.update_at = update_at;
            let server = self.clone();
            async_std::task::spawn(async move {
                let _ = server.create_bill(sector).await;
            });
        } else {
            info!("{} create pending bill, sector={:?}, update_at={}, ingnored", self, sector.sector_id, sector.info.update_at);
        }
        Ok(())
    }


    async fn send_bill_inner(&self, sector: SectorWithId) -> DmcResult<()> {
        info!("{} send pending bill, sector={:?}, update_at={}", self, sector, sector.info.update_at);
        
        match &sector.info.state {
            SectorState::PreBill(raw_tx) => {
                let pending = self.chain_client().load_bill(raw_tx.as_slice()).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} send pending bill, sector={}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?;
                match pending.wait().await {
                    Err(err) => {
                        error!("{} send pending bill, sector={}, update_at={}, sending tx, tx_id={}, err={}", self, sector.sector_id, sector.info.update_at, hex::encode(pending.tx_id()), err);
                        let update_at = dmc_time_now();
                        if sqlx::query("UPDATE active_sector SET process_id=NULL, state_code=?, update_at=? WHERE sector_id=? AND state_code=? AND state_value=?")
                            .bind(ActiveSectorRow::state_code_waiting()).bind(update_at).bind(sector.sector_id)
                            .bind(ActiveSectorRow::state_code_pre_bill()).bind(raw_tx.as_slice())
                            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} send pending bill, sector={}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?
                            .rows_affected() > 0 {
                            info!("{} send pending bill, sector={:?}, update_at={}, reset bill", self, sector, sector.info.update_at);
                        } 
                        Ok(())
                    },
                    Ok(result) => {
                        info!("{} send pending bill, sector={}, update_at={}, sent tx, tx_id={}", self, sector.sector_id, sector.info.update_at, hex::encode(pending.tx_id()));
                        self.sync_bill_result(sector, pending, result).await
                    }
                }
            },
            _ => unreachable!()
        }
    }

    async fn create_bill(&self, sector: SectorWithId) -> DmcResult<()> {
        info!("{} create bill, sector={:?}, update_at={}", self, sector, sector.info.update_at);
        let sector_id = sector.sector_id;

        let state_code =   match &sector.info.state {
            SectorState::Waiting => ActiveSectorRow::state_code_waiting(),
            SectorState::PreBill(_) => ActiveSectorRow::state_code_pre_bill(), 
            _ => {
                info!("{} create bill, sector={:?}, update_at={}, ignored", self, sector.sector_id, sector.info.update_at); 
                return Ok(());
            } 
        };
        let update_at = dmc_time_now();
        if sqlx::query("UPDATE active_sector SET process_id=?, update_at=? WHERE sector_id=? AND state_code=? AND update_at=? AND (process_id IS NULL OR process_id!=?)")
            .bind(std::process::id()).bind(update_at).bind(sector.sector_id)
            .bind(state_code).bind(sector.info.update_at).bind(std::process::id())
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create bill, sector={}, update_at={}, err={}", self, sector_id, sector.info.update_at, err))?
            .rows_affected() > 0 {
            let mut sector = sector;
            sector.info.update_at = update_at;
            if match &sector.info.state {
                SectorState::Waiting => {
                    info!("{} create bill, sector={:?}, update_at={}, create order", self, sector, sector.info.update_at);
                    self.create_bill_inner(sector.clone()).await
                }, 
                SectorState::PreBill(_) => {
                    info!("{} create bill, sector={:?}, update_at={}, send order", self, sector, sector.info.update_at);
                    self.send_bill_inner(sector.clone()).await
                },
                _ => Ok(())
            }.is_err() {
                let _ = sqlx::query("UPDATE active_sector SET process_id=NULL, update_at=? WHERE sector_id=? AND state_code=? AND update_at=?")
                    .bind(dmc_time_now()).bind(sector.sector_id)
                    .bind(state_code).bind(sector.info.update_at)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create bill, sector={}, update_at={}, err={}", self, sector_id, sector.info.update_at, err))?;
            }
        } else {
            info!("{} create bill, sector={:?}, update_at={}, ignored", self, sector.sector_id, sector.info.update_at);
        }

        Ok(())
    }

    // async fn modify_bill_asset(&self) {
    //     if T::support_modify_bill() && self.config().auto_repost_bill {
    //         let sector = self.sector_client().get(sectors::SectorFilter { sector_id: Some(contract.sector.sector_id) }, 1).await?
    //         .next_page().await?
    //         .pop()
    //         .ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} update contract for canceled order({}) failed for target sector({}) not found", self, order_id, contract.sector.sector_id))?;
        
        
    //         let bill = self.account_client().sectors(account::SectorFilter::from_raw_sector(sector.sector_id), 1).await?
    //             .next_page().await?
    //             .pop()
    //             .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} update contract for canceled order({}) failed for target bill from raw-sector-id({}) not found", self, order_id, sector.sector_id))?;

    //         if let SectorState::PostBill(bill_info) = &bill.info.state {
    //             let asset = (contract.sector.total_size() / (1 * 1024 * 1024 * 1024)) as i64;
    //             match self.chain_client().modify_bill_asset(bill_info.bill_id, asset).await {
    //                 Ok(_) => info!("{} update contract for canceled order({}), update capcity from {} to {} for bill({}) success.", self, order_id, bill.info.bill_options.asset, asset, bill_info.bill_id),
    //                 Err(err) => {
    //                     error!("{} update contract for canceled order({}), update to capcity from {} to {} for bill({}) failed, err={}", self, order_id, bill.info.bill_options.asset, asset, bill_info.bill_id, err);
    //                     return Err(err);
    //                 }
    //             }
    //         }
    //     }
    // }

    async fn on_time_escape(&self) {
        let now = dmc_time_now();
        debug!("{} on time escape, when={}", self, now);

        let apply_expired = now - self.config().bill_retry_interval.as_micros() as u64;
        {
            let mut stream = sqlx::query_as::<_, ActiveSectorRow>("SELECT * FROM active_sector WHERE state_code<? AND update_at<? AND (process_id IS NULL OR process_id!=?) ORDER BY update_at")
                .bind(ActiveSectorRow::state_code_post_bill()).bind(apply_expired).bind(std::process::id()).fetch(self.sql_pool());
            loop {
                use async_std::stream::StreamExt;
                let result = stream.next().await;
                if result.is_none() {
                    debug!("{} on time escape, when={}, finish retry apply", self, now);
                    break;
                }
                let result = result.unwrap();
                match result {
                    Ok(row) => {
                        let sector: DmcResult<SectorWithId> = row.try_into();
                        match sector {
                            Ok(sector) => {
                                let server = self.clone();
                                let sector = sector.clone();
                                async_std::task::spawn(async move {
                                    let _ = server.create_bill(sector).await;
                                });
                            },
                            Err(err) => {
                                error!("{} on time escape, when={}, err={}", self, now, err);
                                continue;
                            }
                        }
                    }, 
                    Err(err) => {
                        error!("{} on apply time escape, err, when={}, err={}", self, now, err);
                        break;
                    }
                }
            }
        }
    }
}

impl<T: DmcChainAccountClient> AccountServer<T> {
    pub async fn new(chain_client:T, config: AccountServerConfig) -> DmcResult<Self> {
        let mut options = sqlx::mysql::MySqlConnectOptions::from_str(&config.sql_url)?;
        options.log_statements(LevelFilter::Off);
        let sql_pool = sqlx::mysql::MySqlPoolOptions::new().connect_with(options).await?;
        let journal_client = JournalClient::new(config.journal_client.clone())?;
        Ok(Self(Arc::new(ServerImpl {
            chain_client, 
            sql_pool,
            journal_client, 
            raw_sector_client: raw_sectors::SectorClient::new(config.sector_client.clone())?, 
            contract_client: ContractClient::new(config.contract_client.clone())?, 
            config
        })))
    }

    pub async fn listen(self) -> DmcResult<()> {
        {
            let server = self.clone();
            async_std::task::spawn(async move {
                let _ = server.listen_chain().await;
            });
        }

        {
            let server = self.clone();
            async_std::task::spawn(async move {
                loop {
                    server.on_time_escape().await;
                    async_std::task::sleep(server.config().bill_atomic_interval).await;
                }
            });
        }

        let listen_address = self.config().listen_address.clone();
        let mut http_server = tide::with_state(self);
    
        http_server.at("/account/sector").post(|mut req: Request<Self>| async move {
            let options = req.body_json().await?;
            let mut resp = Response::new(200);
            resp.set_body(Body::from_json(&req.state().create_sector(options).await?)?);
            Ok(resp)
        });

      
        http_server.at("/account/sector").get(|req: Request<Self>| async move {
            debug!("{} http get sector, url={}", req.state(), req.url());
            let mut query = SectorFilterAndNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "sector_id" => {
                        query.filter.sector_id = Some(u64::from_str_radix(&*value, 10)?);
                    },
                    "bill_id" => {
                        query.filter.bill_id = Some(u64::from_str_radix(&*value, 10)?);
                    },
                    "page_size" => {
                        query.navigator.page_size = usize::from_str_radix(&*value, 10)?;
                    },
                    "page_index" => {
                        query.navigator.page_index = usize::from_str_radix(&*value, 10)?;
                    }, 
                    _ => {}
                }
            }

            // let query = req.query::<SectorFilterAndNavigator>()?;
            let sectors = req.state().sectors(&query.filter, &query.navigator).await?;
            let mut resp = Response::new(200);

            debug!("{} http get sector, returns, url={}, sectors={:?}", req.state(), req.url(), sectors);
            resp.set_body(Body::from_json(&sectors).map_err(|e| {
                error!("encode sectors to json err {}", e);
                e
            })?);
            Ok(resp)
        });

        http_server.at("/account/sector").delete(|mut req: Request<Self>| async move {
            let options = req.body_json().await?;
            let mut resp = Response::new(200);
            resp.set_body(Body::from_json(&req.state().remove_sector(options).await?)?);
            Ok(resp)
        });

        http_server.at("/account/method").post(|mut req: Request<Self>| async move {
            let method = req.body_string().await?;
            req.state().set_method(method).await?;
            Ok(Response::new(200))
        });

        http_server.at("/account/method").get(|req: Request<Self>| async move {
            let method = req.state().get_method().await?;
            Ok(Response::from(method))
        });

        http_server.at("/account/name").get(|req: Request<Self>| async move {
            let name = req.state().account();
            let resp = Response::from(name);
            Ok(resp)
        });

        http_server.at("/account/bill").get(|req: Request<Self>| async move {
            #[derive(Deserialize)]
            struct BillFilter {
                bill_id: u64
            }
            let bill_filter = req.query::<BillFilter>()?;
            let bill = req.state().get_bill(bill_filter.bill_id).await?;

            let mut resp = Response::new(200);
            resp.set_body(Body::from_json(&bill.map_or(vec![], |b| vec![b]))?);
            Ok(resp)
        });

        http_server.at("/account/assets").get(|req: Request<Self>| async move {
            let assets = req.state().chain_client().get_available_assets().await?;

            let mut resp = Response::new(200);
            resp.set_body(Body::from_json(&assets)?);
            Ok(resp)
        });

        let _ = http_server.listen(listen_address.as_str()).await?;

        Ok(())
    }

    pub async fn init(&self) -> DmcResult<()> {
        let _ = sqlx::query(Self::sql_create_tbl_active_sector()).execute(self.sql_pool()).await?;
        for update_sql in Self::sql_update_tbl_active_sector() {
            let _ = sqlx::query(update_sql).execute(self.sql_pool()).await;
        }
        Ok(())
    }

    pub async fn reset(self) -> DmcResult<Self> {
        self.init().await?;
        let _ = sqlx::query("DELETE FROM active_sector WHERE raw_sector_id > 0").execute(self.sql_pool()).await?;
        Ok(self)
    }
}

impl<T: DmcChainAccountClient> AccountServer<T> {
    async fn create_sector(&self, mut options: CreateSectorOptions) -> DmcResult<SectorWithId> {
        let update_at = dmc_time_now();
        info!("{} create sector, options={:?}, update_at={}", self, options, update_at);
        let sector = self.raw_sector_client().get(raw_sectors::SectorFilter::from_sector_id(options.raw_sector_id), 1).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.raw_sector_id, update_at, err))?
            .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.raw_sector_id, update_at, err))?
            .get(0).cloned().ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} create sector, options={:?}, update_at={}, err=sector not found", self, options.raw_sector_id, update_at))?;
        
        let occupy = self.contract_client().occupy(SectorOccupyOptions::from_raw_sector(options.raw_sector_id)).await?;
        let max_asset = (sector.capacity - occupy.occupy) >> 30;
        if options.bill.asset == 0 {
            options.bill.asset = max_asset;
        } else {
            if options.bill.asset > max_asset {
                return Err(dmc_err!(DmcErrorCode::OutOfLimit, "{} create sector failed for asset overflow, expected={}, capacity={}, occupy={}, options={:?}", self, options.bill.asset, sector.capacity, occupy.occupy, options));
            }
        }

        let query_result = sqlx::query("INSERT INTO active_sector (raw_sector_id, bill_options, state_code, state_value, update_at) VALUES(?, ?, ?, ?, ?)")
            .bind(options.raw_sector_id).bind(serde_json::to_vec(&options.bill).unwrap()).bind(ActiveSectorRow::state_code_waiting()).bind(vec![]).bind(update_at)
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.raw_sector_id, update_at, err))?;
        
        if query_result.rows_affected() > 0 {
            let sector = SectorWithId {
                sector_id: query_result.last_insert_id(), 
                info: Sector {
                    raw_sector_id: options.raw_sector_id,
                    bill_options: options.bill.clone(), 
                    state: SectorState::Waiting, 
                    update_at
                }
            };
              
            {
                let server = self.clone();
                let sector = sector.clone();
                async_std::task::spawn(async move {
                    let _ = server.create_bill(sector).await;
                });
            }
            info!("{} create sector, success, options={:?}, update_at={}, sector={:?}", self, options.raw_sector_id, update_at, sector);
            Ok(sector)
        } else {
            Err(dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err=already exists", self, options.raw_sector_id, update_at))
        }
    }

    async fn remove_sector(&self, options: RemoveSectorOptions) -> DmcResult<SectorWithId> {
        // 1. check bill is exists.
        // 2. cancel bill
        // 3. set state to Removed

        let update_at = dmc_time_now();

        info!("{} remove sector, options={:?}, update_at={}", self, options, update_at);

        // query from active-sector
        let mut sector_vec = if let Some(sector_id) = options.sector_id {
            self.sectors(&SectorFilter::from_sector_id(sector_id), &SectorNavigator{page_size: 1, page_index: 0}).await?
        } else if let Some(bill_id) = options.bill_id {
            self.sectors(&SectorFilter::from_bill(bill_id), &SectorNavigator{page_size: 1, page_index: 0}).await?
        } else {
            return Err(dmc_err!(DmcErrorCode::InvalidParam, "{} remove sector without any parameters, options={:?}", self, options));
        };

        let sector = sector_vec.get(0)
            .ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} target sector not found, options={:?}", self, options))?;

        let bill = if let SectorState::PostBill(bill) = &sector.info.state {
            info!("{} sector is effective, will remove it, bill_id={}", self, bill.bill_id);
            bill
        } else if let SectorState::Removed(_) = sector.info.state {
            return Err(dmc_err!(DmcErrorCode::ErrorState, "{} already removed, options={:?}.", self, options));
        } else {
            return Err(dmc_err!(DmcErrorCode::ErrorState, "{} the state is not stable, options={:?}.", self, options));
        };

        self.chain_client().get_bill_by_id(bill.bill_id).await.map_err(|err| {
            error!("{} remove sector failed when get bill({}), options={:?}, err={}.", self, bill.bill_id, options, err);
            err
        })?.ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} remove sector failed for the bill({}) is not exist, maybe it's sold or removed. options={:?}.", self, bill.bill_id, options))?;

        let _ = self.chain_client().finish_bill(bill.bill_id).await.map_err(|err| {
            error!("{} remove sector failed when send tx for cancel bill({}), options={:?}, err={}.", self, bill.bill_id, options, err);
            err
        })?.map_err(|err| {
            error!("{} remove sector failed when cancel bill({}), options={:?}, err={}.", self, bill.bill_id, options, err);
            err
        })?;

        if sqlx::query("UPDATE active_sector SET process_id=NULL, state_code=?, removed_sector_id=?, update_at=? WHERE sector_id=? AND state_code=?")
            .bind(ActiveSectorRow::state_code_remove_bill()).bind(sector.sector_id).bind(update_at)
            .bind(sector.sector_id).bind(ActiveSectorRow::state_code_post_bill())
            .execute(self.sql_pool()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} remove sector failed when update state, sector={}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?
            .rows_affected() == 0 {

            Err(dmc_err!(DmcErrorCode::Failed, "{} remove sector failed when update state, maybe it's removed, sector={:?}, update_at={}", self, sector.sector_id, sector.info.update_at))
        } else {
            Ok(sector_vec.pop().unwrap())
        }
    }

    async fn sectors(&self, filter: &SectorFilter, navigator: &SectorNavigator) -> DmcResult<Vec<SectorWithId>> {
        if navigator.page_size == 0 {
            return Ok(vec![]);
        }
        if let Some(sector_id) = filter.sector_id {
            let result: Option<ActiveSectorRow> = sqlx::query_as("SELECT * FROM active_sector WHERE sector_id=?").bind(sector_id)
                .fetch_optional(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
            if let Some(row) = result {
                let sector = row.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
                Ok(vec![sector])
            } else {
                Ok(vec![])
            }
        } else if let Some(bill_id) = filter.bill_id {
            let result: Option<ActiveSectorRow> = sqlx::query_as("SELECT * FROM active_sector WHERE bill_id=?").bind(bill_id)
                .fetch_optional(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
            if let Some(row) = result {
                let sector = row.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
                Ok(vec![sector])
            } else {
                Ok(vec![])
            }
        } else if let Some(raw_sector_id) = filter.raw_sector_id {
            let result: Option<ActiveSectorRow> = sqlx::query_as("SELECT * FROM active_sector WHERE raw_sector_id=? AND removed_sector_id IS NULL").bind(raw_sector_id)
                .fetch_optional(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
            if let Some(row) = result {
                let sector = row.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
                Ok(vec![sector])
            } else {
                Ok(vec![])
            }
        } else {
            let result: Vec<ActiveSectorRow> = sqlx::query_as("SELECT * FROM active_sector")
                .fetch_all(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
            let mut bills = vec![];
            for row in result {
                if let Ok(bill) = row.try_into() {
                    bills.push(bill);
                }
            }
            Ok(bills)
        }
    }

    async fn get_bill(&self, bill_id: u64) -> DmcResult<Option<DmcBill>> {
        match self.0.chain_client.get_bill_by_id(bill_id).await {
            Ok(bill) => Ok(bill),
            Err(err) => {
                if err.code() == DmcErrorCode::NotFound {
                    Ok(None)
                } else {
                    error!("{} get bill failed, bill_id={}, err: {}", self, bill_id, err);
                    Err(err)
                }
            }
        }
    }

    async fn set_method(&self, method: String) -> DmcResult<()> {
        self.0.chain_client.set_apply_method(method).await?
    }

    async fn get_method(&self) -> DmcResult<String> {
        Ok(self.0.chain_client.get_apply_method(self.0.chain_client.account()).await?.unwrap_or("".to_owned()))
    }
}


