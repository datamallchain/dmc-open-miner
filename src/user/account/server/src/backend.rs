use log::*;
use std::{
    sync::Arc,
    time::Duration
};
use serde::{Serialize, Deserialize};
use tide::{Request, Response, Body};
use dmc_tools_common::*;
#[cfg(feature = "sqlite")]
use sqlx_sqlite as sqlx_types;
#[cfg(feature = "mysql")]
use sqlx_mysql as sqlx_types;
use dmc_spv::*;
use dmc_user_journal::*;
use dmc_user_sources::*;
use dmc_user_account_client::*;
use dmc_user_contracts::*;

#[derive(Serialize, Deserialize)]
pub struct AccountServerConfig {
    pub host: String, 
    pub sql_url: String, 
    pub retry_event_interval: Duration, 
    pub journal_client: JournalClientConfig, 
    pub journal_listener_interval: Duration, 
    pub journal_listener_page_size: usize,
    pub order_atomic_interval: Duration,  
    pub order_retry_interval: Duration,  
    pub source_client: SourceClientConfig, 
    pub contract_client: ContractClientConfig, 
    pub spv_client: Option<SpvClientConfig>, 
    pub auto_retry_order: Option<Duration>,  
    pub auto_onchain_challenge: bool
}

#[derive(sqlx::FromRow)]
struct ActiveSectorRow {
    sector_id: sqlx_types::U64, 
    source_id: sqlx_types::U64, 
    asset: sqlx_types::U64, 
    order_code: sqlx_types::U8, 
    order_value: Vec<u8>,  
    state_code: sqlx_types::U8, 
    state_value: Vec<u8>, 
    order_id: Option<sqlx_types::U64>, 
    bill_id: Option<sqlx_types::U64>, 
    block_number: Option<sqlx_types::U64>, 
    tx_index: Option<sqlx_types::U32>,  
    update_at: sqlx_types::U64
}

impl TryInto<SectorWithId> for ActiveSectorRow {
    type Error = DmcError;
    fn try_into(self) -> DmcResult<SectorWithId> {
        let (state, apply) = if self.state_code == Self::state_code_waiting() {
            (SectorState::Waiting, ApplyContractState::Waiting) 
        } else if self.state_code == Self::state_code_pre_order() {
            (SectorState::PreOrder(DmcData::from(self.state_value)), ApplyContractState::Waiting)
        } else if self.state_code == Self::state_code_order_error() {
            let error = String::from_utf8(self.state_value).map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid bill error {}", err)))?;
            let block_number = self.block_number.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no block number"))?;
            let tx_index = self.tx_index.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no tx index"))?;
            (SectorState::OrderError(SectorOrderError { block_number: block_number as u64, tx_index: tx_index as u32, error} ), ApplyContractState::Waiting)
        } else if self.state_code == Self::state_code_post_order()
            || self.state_code == Self::state_code_applied() {
            let block_number = self.block_number.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no block number"))?;
            let tx_index = self.tx_index.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no tx index"))?;
            let bill_id = self.bill_id.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no bill id"))?;
            let order_id = self.order_id.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no order id"))?;
            let state = SectorState::PostOrder(SectorOrderInfo { block_number: block_number as u64, tx_index: tx_index as u32, order_id: order_id as u64, bill_id: bill_id as u64});
            let apply = if self.state_code == Self::state_code_post_order() {
                ApplyContractState::Applying
            } else if self.state_code == Self::state_code_applied() {
                ApplyContractState::Applied
            } else {
                unreachable!()
            };
            (state, apply)
        } else {
            unreachable!()
        };

        let order = if self.order_code == Self::order_code_order() {
            let order_info = serde_json::from_slice(self.order_value.as_slice()).map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid order error {}", err)))?;
            SectorOrderOptions::Order(order_info)
        } else if self.order_code == Self::order_code_bill() {
            let bill_options = serde_json::from_slice(self.order_value.as_slice()).map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid order error {}", err)))?;
            SectorOrderOptions::Bill(bill_options)
        } else if self.order_code == Self::order_code_filter() {
            let filter = serde_json::from_slice(self.order_value.as_slice()).map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid order error {}", err)))?;
            SectorOrderOptions::Filter(filter)
        } else {
            unreachable!()
        };

        Ok(SectorWithId {
            sector_id: self.sector_id as u64,
            info: Sector {
                source_id: self.source_id as u64, 
                asset: self.asset as u64, 
                order, 
                state, 
                apply, 
                update_at: self.update_at as u64
            }
        })
    }
}


impl ActiveSectorRow {
    fn state_code_waiting() -> sqlx_types::U8 {
        0
    }

    fn state_code_pre_order() -> sqlx_types::U8 {
        1
    }

    fn state_code_order_error() -> sqlx_types::U8 {
        2
    }

    fn state_code_post_order() -> sqlx_types::U8 {
        3
    }

    fn state_code_applied() -> sqlx_types::U8 {
        4
    }

    fn order_code_order() -> sqlx_types::U8 {
        0
    }

    fn order_code_bill() -> sqlx_types::U8 {
        1
    }

    fn order_code_filter() -> sqlx_types::U8 {
        2
    }
}

struct ServerImpl<T: DmcChainAccountClient> {
    config: AccountServerConfig, 
    spv_client: Option<SpvClient>, 
    journal_client: JournalClient, 
    contract_client: ContractClient, 
    source_client: SourceClient, 
    chain_client: T, 
    sql_pool: sqlx_types::SqlPool
}


#[derive(Clone)]
pub struct AccountServer<T: DmcChainAccountClient>(Arc<ServerImpl<T>>);

impl<T: DmcChainAccountClient> std::fmt::Display for AccountServer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserAccountServer{{account={}}}", self.chain_client().account())
    }
}
 

impl<T: DmcChainAccountClient> AccountServer<T> {
    fn sql_pool(&self) -> &sqlx_types::SqlPool {
        &self.0.sql_pool
    }

    fn config(&self) -> &AccountServerConfig {
        &self.0.config
    }

    fn chain_client(&self) -> &T {
        &self.0.chain_client
    }

    fn spv_client(&self) -> Option<&SpvClient> {
        self.0.spv_client.as_ref()
    }

    fn account(&self) -> &str {
        self.chain_client().account()
    }

    fn journal_client(&self) -> &JournalClient {
        &self.0.journal_client
    }

    fn contract_client(&self) -> &ContractClient {
        &self.0.contract_client
    }

    fn source_client(&self) -> &SourceClient {
        &self.0.source_client
    }

    #[cfg(feature = "mysql")]
    fn sql_create_tbl_active_sector() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `active_sector` (
            `sector_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, 
            `source_id` BIGINT UNSIGNED NOT NULL, 
            `asset` BIGINT UNSIGNED NOT NULL,
            `bill_id` BIGINT UNSIGNED, 
            `order_id` BIGINT UNSIGNED, 
            `block_number` BIGINT UNSIGNED, 
            `tx_index` INT UNSIGNED, 
            `state_code` TINYINT UNSIGNED NOT NULL, 
            `state_value` BLOB,      
            `order_code` TINYINT UNSIGNED NOT NULL, 
            `order_value` BLOB,
            `process_id` INT UNSIGNED, 
            `update_at` BIGINT UNSIGNED NOT NULL,   
            INDEX (`bill_id`),  
            INDEX (`order_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
    }

    #[cfg(feature = "mysql")]
    fn sql_update_tbl_active_sector() -> &'static str {
        "CREATE INDEX `update_at` ON `active_sector`(`update_at`);"
    }

    #[cfg(feature = "sqlite")]
    fn sql_create_tbl_active_sector() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `active_sector` (
            `sector_id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            `source_id` INTEGER NOT NULL,
            `asset` INTEGER NOT NULL,
            `bill_id` INTEGER,
            `order_id` INTEGER,
            `block_number` INTEGER,
            `tx_index` INTEGER,
            `state_code` INTEGER NOT NULL,
            `state_value` BLOB,
            `order_code` INTEGER NOT NULL,
            `order_value` BLOB,
            `process_id` INTEGER,
            `update_at` INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS bill_id ON active_sector (
            bill_id
        );
        CREATE INDEX IF NOT EXISTS order_id ON active_sector (
            order_id
        );
        CREATE INDEX IF NOT EXISTS update_at ON active_sector (
            update_at
        );"#
    }


    #[cfg(feature = "sqlite")]
    fn sql_update_tbl_active_sector() -> &'static str {
        ""
    }

    #[cfg(feature = "mysql")]
    fn sql_create_tbl_event_index() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `active_sector` (
            `index_id` TINYINT UNSIGNED PRIMARY KEY, 
            `block_number` BIGINT UNSIGNED NOT NULL, 
            `journal_id` BIGINT UNSIGNED NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
    }

    #[cfg(feature = "sqlite")]
    fn sql_create_tbl_event_index() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `active_sector` (
            `index_id` INTEGER PRIMARY KEY,
            `block_number` INTEGER NOT NULL,
            `journal_id` INTEGER NOT NULL
        );"#
    }

    async fn order_with_filter(&self, sector: SectorWithId, filter: SectorOrderFilter) -> DmcResult<()> {
        info!("{} order with fitler, sector={:?}, filter={:?}, update_at={}", self, sector, filter, sector.info.update_at);
        let sector_id = sector.sector_id;

        let state_code =   match &sector.info.state {
            SectorState::Waiting => ActiveSectorRow::state_code_waiting(),
            SectorState::PreOrder(_) => ActiveSectorRow::state_code_pre_order(), 
            _ => {
                info!("{} order with fitler, sector={:?}, update_at={}, ignored", self, sector.sector_id, sector.info.update_at);
                return Ok(());
            }
        };
        let update_at = dmc_time_now();
        if sqlx::query("UPDATE active_sector SET process_id=?, update_at=? WHERE sector_id=? AND state_code=? AND update_at=? AND (process_id IS NULL OR process_id!=?)")
            .bind(std::process::id() as sqlx_types::U32).bind(update_at as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64)
            .bind(state_code as sqlx_types::U8).bind(sector.info.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} order on bill, sector={}, update_at={}, err={}", self, sector_id, sector.info.update_at, err))?
            .rows_affected() > 0 {
            let mut sector = sector;
            sector.info.update_at = update_at;
            if match &sector.info.state {
                SectorState::Waiting => {
                    info!("{} order with fitler, sector={:?}, update_at={}, find bill", self, sector, sector.info.update_at);
                    self.find_bill_inner(sector.clone(), filter).await
                }, 
                SectorState::PreOrder(_) => {
                    info!("{} oorder with fitler, sector={:?}, update_at={}, send order", self, sector, sector.info.update_at);
                    self.send_order_inner(sector.clone()).await
                },
                _ => Ok(())
            }.is_err() {
                let _ = sqlx::query("UPDATE active_sector SET process_id=NULL, update_at=? WHERE sector_id=? AND state_code=? AND update_at=?")
                    .bind(dmc_time_now() as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64)
                    .bind(state_code as sqlx_types::U8).bind(sector.info.update_at as sqlx_types::U64)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} order on bill, sector={}, update_at={}, err={}", self, sector_id, sector.info.update_at, err))?;
            }
        } else {
            info!("{} order with fitler, sector={:?}, update_at={}, ignored", self, sector.sector_id, sector.info.update_at);
        }

        Ok(())
    }

    async fn retry_post_order(&self, _: SectorWithId, _: T::PendingOrder) -> DmcResult<()> {
        unimplemented!()
    }

    async fn sync_order_result(&self, sector: SectorWithId, pending: T::PendingOrder, result: DmcPendingResult<DmcOrder>) -> DmcResult<()> {
        info!("{} sync order result, sector={:?}, result={:?}", self, sector, result);
        let update_at = dmc_time_now();
        let mut sector = sector;
        sector.info.update_at = update_at;
        let source = self.source_client().sources(SourceFilter::from_source_id(sector.info.source_id), 1).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} sync order result, sector_id={}, result={:?}, err={}", self, sector.sector_id, result, err))?
            .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} sync order result, sector_id={}, result={:?}, err={}", self, sector.sector_id, result, err))?
            .get(0).cloned().ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} sync order result, sector_id={}, result={:?}, err={}", self, sector.sector_id, result, "source not found"))?;

        let (qeury, journal) = match &result.result {
            Ok(order) => {
                sector.info.state = SectorState::PostOrder(SectorOrderInfo { bill_id: order.bill_id, order_id: order.order_id, block_number: result.block_number, tx_index: result.tx_index });
                let query = sqlx::query("UPDATE active_sector SET process_id=NULL, state_code=?, state_value=?, bill_id=?, order_id=?, block_number=?, tx_index=?, update_at=? WHERE sector_id=? AND state_code=? AND state_value=?")
                    .bind(ActiveSectorRow::state_code_post_order()).bind(vec![]).bind(order.bill_id as sqlx_types::U64)
                    .bind(order.order_id as sqlx_types::U64).bind(result.block_number as sqlx_types::U64).bind(result.tx_index as sqlx_types::U32)
                    .bind(update_at as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64).bind(ActiveSectorRow::state_code_pre_order()).bind(pending.as_ref());
                let journal = self.journal_client().append(JournalEvent { source_id: source.source_id, order_id: Some(order.order_id), event_type: JournalEventType::OrderCreated, event_params: Some(serde_json::to_string(order).unwrap()) });
                (query, journal)
            }, 
            Err(err) => {
                let query = sqlx::query("UPDATE active_sector SET process_id=NULL, state_code=?, state_value=?, block_number=?, tx_index=?, update_at=? WHERE sector_id=? AND state_code=? AND state_value=?")
                    .bind(ActiveSectorRow::state_code_order_error()).bind(err.as_bytes().to_vec()).bind(result.block_number as sqlx_types::U64)
                    .bind(result.tx_index as sqlx_types::U32).bind(update_at as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64)
                    .bind(ActiveSectorRow::state_code_pre_order()).bind(pending.as_ref());
                let journal = self.journal_client().append(JournalEvent { source_id: source.source_id, order_id: None, event_type: JournalEventType::OrderFailed, event_params: Some(err.to_owned()) });
                (query, journal)
            }
        };

        if qeury.execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} sync order result, sector_id={}, result={:?}, err={}", self, sector.sector_id, result, err))?
            .rows_affected() > 0 {
            let _ = journal.await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} sync order result, sector_id={}, result={:?}, err={}", self, sector.sector_id, result, err))?;
            info!("{} sync order result, sector={}, result={:?}, saved result", self, sector.sector_id, result);
            let server = self.clone();
            if result.result.is_ok() {
                async_std::task::spawn(async move {
                    let _ = server.apply_contract(sector).await;
                });
            }
        } else {
            info!("{} sync order result, sector={}, result={:?}, ignored", self, sector.sector_id, result);
        }

        Ok(())
    }

    async fn find_bill_inner(&self, sector: SectorWithId, filter: SectorOrderFilter) -> DmcResult<()> {
        info!("{} find bill, sector={:?}, filter={:?}, update_at={}", self, sector, filter, sector.info.update_at);

        if self.spv_client().is_none() {
            return Err(dmc_err!(DmcErrorCode::Failed, "{} find bill, sector={:?}, filter={:?}, update_at={}, err={}", self, sector, filter, sector.info.update_at, "no spv client"));
        }

        let bill = self.spv_client().unwrap().bills(filter.bill_filter.clone(), 1).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} find bill, sector={:?}, filter={:?}, update_at={}, err={}", self, sector, filter, sector.info.update_at, err))?
            .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} find bill, sector={:?}, filter={:?}, update_at={}, err={}", self, sector, filter, sector.info.update_at, err))?
            .iter().find(|bill| bill.miner.as_str() != self.account())
            .cloned().ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} find bill, sector={:?}, filter={:?}, update_at={}, err={}", self, sector, filter, sector.info.update_at, "no bill match"))?;

        self.create_order_inner(sector.clone(), DmcOrderOptions { 
            bill_id: bill.bill_id, asset: sector.info.asset, duration: filter.duration
         }).await
        
    }

    #[async_recursion::async_recursion]
    async fn create_order_inner(&self, sector: SectorWithId, options: DmcOrderOptions) -> DmcResult<()> {
        info!("{} create pending order, sector={:?}, options={:?}, update_at={}", self, sector, options, sector.info.update_at);
        let pending = self.chain_client().create_order(options.clone()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create pending order, sector={}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?;
        let raw_tx = pending.as_ref();

        let update_at = dmc_time_now();
        if sqlx::query("UPDATE active_sector SET process_id=NULL, state_code=?, state_value=?, update_at=? WHERE sector_id=? AND state_code=? AND update_at=?")
                .bind(ActiveSectorRow::state_code_pre_order()).bind(raw_tx).bind(update_at as sqlx_types::U64)
                .bind(sector.sector_id as sqlx_types::U64).bind(ActiveSectorRow::state_code_waiting()).bind(sector.info.update_at as sqlx_types::U64)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create pending order, sector={}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?
                .rows_affected() > 0 {
            info!("{} create pending order, sector={:?}, options={:?}, update_at={}, pre ordered", self, sector.sector_id, options, sector.info.update_at);
            let mut sector = sector;
            sector.info.state = SectorState::PreOrder(DmcData::from(raw_tx));
            sector.info.update_at = update_at;
            let server = self.clone();
            async_std::task::spawn(async move {
                let _ = server.order_on_bill(sector, options).await;
            });
        } else {
            info!("{} create pending order, sector={:?}, options={:?}, update_at={}, ingnored", self, sector.sector_id, options, sector.info.update_at);
        }
        Ok(())
    }


    async fn send_order_inner(&self, sector: SectorWithId) -> DmcResult<()> {
        info!("{} send pending order, sector={:?}, update_at={}", self, sector, sector.info.update_at);
        
        match &sector.info.state {
            SectorState::PreOrder(raw_tx) => {
                let pending = self.chain_client().load_order(raw_tx.as_slice()).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} send pending order, sector={}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?;
                match pending.wait().await {
                    Err(err) => {
                        error!("{} send pending order, sector={}, update_at={}, sending tx, tx_id={}, err={}", self, sector.sector_id, sector.info.update_at, hex::encode(pending.tx_id()), err);
                        let update_at = dmc_time_now();
                        if sqlx::query("UPDATE active_sector SET process_id=NULL, state_code=?, update_at=? WHERE sector_id=? AND state_code=? AND state_value=?")
                            .bind(ActiveSectorRow::state_code_waiting()).bind(update_at as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64)
                            .bind(ActiveSectorRow::state_code_pre_order()).bind(raw_tx.as_slice())
                            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} send pending order, sector={}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?
                            .rows_affected() > 0 {
                            info!("{} send pending order, sector={:?}, update_at={}, reset order", self, sector, sector.info.update_at);
                        } 
                        Ok(())
                    },
                    Ok(result) => {
                        info!("{} send pending order, sector={}, update_at={}, sent tx, tx_id={}", self, sector.sector_id, sector.info.update_at, hex::encode(pending.tx_id()));
                        self.sync_order_result(sector, pending, result).await
                    }
                }
            },
            _ => unreachable!()
        }
    }

    async fn order_on_bill(&self, sector: SectorWithId, options: DmcOrderOptions) -> DmcResult<()> {
        info!("{} order on bill, sector={:?}, options={:?}, update_at={}", self, sector, options, sector.info.update_at);
        let sector_id = sector.sector_id;

        let state_code =   match &sector.info.state {
            SectorState::Waiting => ActiveSectorRow::state_code_waiting(),
            SectorState::PreOrder(_) => ActiveSectorRow::state_code_pre_order(), 
            _ => {
                info!("{} order on bill, sector={:?}, options={:?}, update_at={}, ignored", self, sector.sector_id, options, sector.info.update_at);
                return Ok(());
            }
        };
        let update_at = dmc_time_now();
        if sqlx::query("UPDATE active_sector SET process_id=?, update_at=? WHERE sector_id=? AND state_code=? AND update_at=? AND (process_id IS NULL OR process_id!=?)")
            .bind(std::process::id() as sqlx_types::U32).bind(update_at as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64)
            .bind(state_code).bind(sector.info.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} order on bill, sector={}, update_at={}, err={}", self, sector_id, sector.info.update_at, err))?
            .rows_affected() > 0 {
            let mut sector = sector;
            sector.info.update_at = update_at;
            if match &sector.info.state {
                SectorState::Waiting => {
                    info!("{} order on bill, sector={:?}, options={:?}, update_at={}, create order", self, sector, options, sector.info.update_at);
                    self.create_order_inner(sector.clone(), options).await
                }, 
                SectorState::PreOrder(_) => {
                    info!("{} order on bill, sector={:?}, options={:?}, update_at={}, send order", self, sector, options, sector.info.update_at);
                    self.send_order_inner(sector.clone()).await
                },
                _ => Ok(())
            }.is_err() {
                let _ = sqlx::query("UPDATE active_sector SET process_id=NULL, update_at=? WHERE sector_id=? AND state_code=? AND update_at=?")
                    .bind(dmc_time_now() as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64)
                    .bind(state_code).bind(sector.info.update_at as sqlx_types::U64)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} order on bill, sector={}, update_at={}, err={}", self, sector_id, sector.info.update_at, err))?;
            }
        } else {
            info!("{} order on bill, sector={:?}, options={:?}, update_at={}, ignored", self, sector.sector_id, options, sector.info.update_at);
        }

        Ok(())
    }

    async fn apply_contract(&self, sector: SectorWithId) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} apply contract, sector={:?}, update_at={}", self, sector, update_at);
        if sqlx::query("UPDATE active_sector SET process_id=?, update_at=? WHERE sector_id=? AND state_code=? AND update_at=? AND (process_id IS NULL OR process_id != ?)")
            .bind(std::process::id() as sqlx_types::U32).bind(update_at as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64)
            .bind(ActiveSectorRow::state_code_post_order()).bind(sector.info.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, sector={:?}, update_at={}, err={}", self, sector.sector_id, update_at, err))?
            .rows_affected() > 0 {
            let mut sector = sector;
            sector.info.update_at = update_at;
            if self.apply_contract_inner(sector.clone()).await.is_err() {
                let _ = sqlx::query("UPDATE active_sector SET process_id=NULL, update_at=? WHERE sector_id=? AND state_code=? AND update_at=?")
                    .bind(dmc_time_now() as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64)
                    .bind(ActiveSectorRow::state_code_post_order()).bind(sector.info.update_at as sqlx_types::U64)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, sector={:?}, update_at={}, err={}", self, sector.sector_id, update_at, err))?;
            }
        } else {
            info!("{} apply contract, sector={:?}, update_at={}, ignored", self, sector.sector_id, sector.info.update_at);
        }
        Ok(())
    }

    async fn apply_contract_inner(&self, sector: SectorWithId) -> DmcResult<()> {
        info!("{} apply contract, sector={:?}, update_at={}", self, sector, sector.info.update_at);
        if let SectorState::PostOrder(order_info) = sector.info.state {
            let _ = self.contract_client().apply(ApplyContractOptions { 
                order_id: order_info.order_id, 
                source_id: sector.info.source_id, 
                block_number: order_info.block_number, 
                tx_index: order_info.tx_index 
            }).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, sector={:?}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?;
            if sqlx::query("UPDATE active_sector SET state_code=?, process_id=NULL, update_at=? WHERE sector_id=? AND state_code=? AND update_at=?")
                .bind(ActiveSectorRow::state_code_applied()).bind(dmc_time_now() as sqlx_types::U64).bind(sector.sector_id as sqlx_types::U64)
                .bind(ActiveSectorRow::state_code_post_order()).bind(sector.info.update_at as sqlx_types::U64)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, sector={:?}, update_at={}, err={}", self, sector.sector_id, sector.info.update_at, err))?
                .rows_affected() > 0 {
                info!("{} apply contract, sector={:?}, update_at={}, applied", self, sector.sector_id, sector.info.update_at);
            } else {
                info!("{} apply contract, sector={:?}, update_at={}, ignored", self, sector.sector_id, sector.info.update_at);
            }
            Ok(())
        } else {
            unreachable!()
        }
    }

    async fn listen_chain(&self) {
        info!("{} listen chain", self);
        // todo: set correct block
        let listener = self.chain_client().user_listener(None).await;
        loop {
            match listener.next().await {
                Err(err) => {
                    error!("{} listen chain, err={}", self, err);
                }, 
                Ok(event) => {
                    info!("{} on chain event, event={:?}", self, event);
                    match event.event {
                        DmcTypedEvent::BillChanged(_) => {}, 
                        DmcTypedEvent::BillCanceled(_) => {}, 
                        DmcTypedEvent::EmptyEvent(()) => {},
                        DmcTypedEvent::OrderChanged(order) => {
                            loop {
                                match self.contract_client().update(UpdateContractOptions { 
                                    order: order.clone(), 
                                    challenge: None, 
                                    block_number: event.block_number,
                                    tx_index: event.tx_index
                                }).await {
                                    Ok(_) => {
                                        break;
                                    }, 
                                    Err(err) => {
                                        info!("{} on event, order={:?}, got err retry, err={}", self, order, err);
                                        async_std::task::sleep(self.config().retry_event_interval).await;
                                        continue;
                                    }
                                }
                            }
                        },
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
                        DmcTypedEvent::ChallengeChanged(order, challenge) => {
                            loop {
                                match self.contract_client().update(UpdateContractOptions { 
                                    order: order.clone(), 
                                    challenge: Some(challenge.clone()), 
                                    block_number: event.block_number,
                                    tx_index: event.tx_index
                                }).await {
                                    Ok(_) => {
                                        break;
                                    }, 
                                    Err(_) => {
                                        info!("{} on event, got err retry, order={:?}", self, order);
                                        async_std::task::sleep(Duration::from_secs(1)).await;
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

    async fn listen_journal(&self) {
        info!("{} listen journal", self);
        let filter = JournalFilter {
            event_type: Some(vec![JournalEventType::OffchainChallengeFailed]),
            source_id: None,
            order_id: None,
        };
        let mut listener = self.journal_client().listener(filter, None, self.config().journal_listener_interval, self.config().journal_listener_page_size);
        loop {
            match listener.next().await {
                Err(err) => {
                    error!("{} listen journal, err={}", self, err);
                }, 
                Ok(log) => {
                    info!("{} on journal log, log={:?}", self, log);
                    match &log.event.event_type {
                        JournalEventType::OffchainChallengeFailed => {
                            if self.config().auto_onchain_challenge {
                                let order_id = log.event.order_id.unwrap();
                                loop {
                                    match self.contract_client().challenge(OnChainChallengeOptions { order_id, challenge: None }).await {
                                        Ok(_) => {
                                            break;
                                        }, 
                                        Err(err) => {
                                            info!("{} on journal, log={:?}, got err retry, err={}", self, log, err);
                                            async_std::task::sleep(self.config().retry_event_interval).await;
                                            continue;
                                        }
                                    }
                                }
                            }
                        },
                        _ => unreachable!()
                    }
                }
            }
        }
    }

    async fn on_time_escape(&self) {
        let now = dmc_time_now();
        info!("{} on time escape, when={}", self, now);

        let apply_expired = now - self.config().order_retry_interval.as_micros() as u64;
        {
            let mut stream = sqlx::query_as::<_, ActiveSectorRow>("SELECT * FROM active_sector WHERE state_code<? AND update_at<? AND (process_id IS NULL OR process_id!=?) ORDER BY update_at")
                .bind(ActiveSectorRow::state_code_applied()).bind(apply_expired as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32).fetch(self.sql_pool());
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
                                match &sector.info.order {
                                    SectorOrderOptions::Order(_) => {
                                        let server = self.clone();
                                        let sector = sector.clone();
                                        async_std::task::spawn(async move {
                                            let _ = server.apply_contract(sector).await;
                                        });
                                    },
                                    SectorOrderOptions::Bill(bill_options) => {
                                        let server = self.clone();
                                        let sector = sector.clone();
                                        let bill_options = bill_options.clone();
                                        async_std::task::spawn(async move {
                                            let _ = server.order_on_bill(sector, bill_options).await;
                                        });
                                    },
                                    SectorOrderOptions::Filter(filter) => {
                                        let server = self.clone();
                                        let sector = sector.clone();
                                        let filter = filter.clone();
                                        async_std::task::spawn(async move {
                                            let _ = server.order_with_filter(sector, filter).await;
                                        });
                                    }
                                }   
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
    pub async fn listen(self) -> DmcResult<()> {
        {
            let server = self.clone();
            async_std::task::spawn(async move {
                server.listen_chain().await;
            });
            
        }

        {
            let server = self.clone();
            async_std::task::spawn(async move {
                server.listen_journal().await;
            });
            
        }

        {
            let server = self.clone();
            async_std::task::spawn(async move {
                loop {
                    server.on_time_escape().await;
                    async_std::task::sleep(server.config().order_atomic_interval).await;
                }
            });
        }

        let host = self.config().host.clone();
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
                    "order_id" => {
                        query.filter.order_id = Some(u64::from_str_radix(&*value, 10)?);
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
            resp.set_body(Body::from_json(&sectors)?);
            Ok(resp)
        });

        http_server.at("/account/address").get(|req: Request<Self>| async move {
           Ok(Response::from(req.state().chain_client().account()))
        });

        let _ = http_server.listen(host.as_str()).await?;

        Ok(())
    }

    pub async fn new(chain_client:T, config: AccountServerConfig) -> DmcResult<Self> {
        let sql_pool; 
        #[cfg(feature = "mysql")]
        {
            sql_pool = sqlx::mysql::MySqlPoolOptions::new().connect(&config.sql_url).await?;
        }
        #[cfg(feature = "sqlite")]
        {
            sql_pool = sqlx::sqlite::SqlitePoolOptions::new().connect(&config.sql_url).await?;
        }
        let server = Self(Arc::new(ServerImpl {
            chain_client, 
            sql_pool,
            spv_client: config.spv_client.clone().map(|config| SpvClient::new(config.clone()).map(|client| Some(client))).unwrap_or(Ok(None))?, 
            journal_client: JournalClient::new(config.journal_client.clone())?, 
            source_client: SourceClient::new(config.source_client.clone())?, 
            contract_client: ContractClient::new(config.contract_client.clone())?, 
            config
        }));
        Ok(server)
    }

    pub async fn init(&self) -> DmcResult<()> {
        let _ = sqlx::query(&Self::sql_create_tbl_active_sector()).execute(self.sql_pool()).await?;
        let _ = sqlx::query(&Self::sql_update_tbl_active_sector()).execute(self.sql_pool()).await;
        Ok(())
    }

    pub async fn reset(self) -> DmcResult<Self> {
        self.init().await?;
        let _ = sqlx::query("DELETE FROM active_sector WHERE sector_id > 0").execute(self.sql_pool()).await?;
        Ok(self)
    }
}

impl<T: DmcChainAccountClient> AccountServer<T> {
    pub async fn create_sector(&self, options: CreateSectorOptions) -> DmcResult<SectorWithId> {
        let update_at = dmc_time_now();
        info!("{} create sector, options={:?}, update_at={}", self, options, update_at);

        let source = self.source_client().sources(SourceFilter::from_source_id(options.source_id), 1).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, err))?
            .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, err))?
            .get(0).cloned().ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, "source not found"))?;
        
        info!("{} create sector, options={:?}, update_at={}, got source = {:?}", self, options, update_at, source);
        
        if source.merkle_stub.is_none() {
            return Err(dmc_err!(DmcErrorCode::InvalidData, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, "source not ready"));
        }

        let asset = (source.length as f64 / (1 * 1024 * 1024 * 1024) as f64).ceil() as u64;

        let mut conn = self.sql_pool().acquire().await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, err))?;
        let query = match options.order {
            SectorOrderOptions::Order(order_info) => {
                let state_value = serde_json::to_vec(&order_info).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, err))?;
                sqlx::query("INSERT INTO active_sector (source_id, asset, state_code, state_value, bill_id, order_id, order_code, order_value, update_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)")
                    .bind(options.source_id as sqlx_types::U64).bind(asset as sqlx_types::U64).bind(ActiveSectorRow::state_code_post_order())
                    .bind(vec![]).bind(order_info.bill_id as sqlx_types::U64).bind(order_info.order_id as sqlx_types::U64).bind(ActiveSectorRow::order_code_order())
                    .bind(state_value).bind(update_at as sqlx_types::U64)
            },
            SectorOrderOptions::Bill(bill_options) => {
                let mut bill_options = bill_options;
                bill_options.asset = asset;
                let state_value = serde_json::to_vec(&bill_options).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, err))?;
                sqlx::query("INSERT INTO active_sector (source_id, asset, state_code, state_value, bill_id, order_code, order_value, update_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)")
                    .bind(options.source_id as sqlx_types::U64).bind(asset as sqlx_types::U64).bind(ActiveSectorRow::state_code_waiting())
                    .bind(vec![]).bind(bill_options.bill_id as sqlx_types::U64).bind(ActiveSectorRow::order_code_bill())
                    .bind(state_value).bind(update_at as sqlx_types::U64)
            },
            SectorOrderOptions::Filter(filter) => {
                let mut filter = filter;
                filter.bill_filter.min_asset = Some(asset);
                let expire_after = TimePointSec::from_now().offset(filter.duration as i64 * 60 * 60 * 24 * 7).as_secs().unwrap() as u64;
                filter.bill_filter.expire_after = Some(expire_after);
                
                let state_value = serde_json::to_vec(&filter).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, err))?;
                sqlx::query("INSERT INTO active_sector (source_id, asset, state_code, state_value, order_code, order_value, update_at) VALUES(?, ?, ?, ?, ?, ?, ?)")
                    .bind(options.source_id as sqlx_types::U64).bind(asset as sqlx_types::U64).bind(ActiveSectorRow::state_code_waiting())
                    .bind(vec![]).bind(ActiveSectorRow::order_code_filter()).bind(state_value).bind(update_at as sqlx_types::U64)
            }
        };
        let query_result = query.execute(&mut conn).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, err))?;
        if query_result.rows_affected() > 0 {
            let rowid = sqlx_types::last_inersert(&query_result);
            let row: ActiveSectorRow = sqlx::query_as(&format!("SELECT * FROM active_sector WHERE {}=?", sqlx_types::rowid_name("sector_id"))).bind(rowid)
                .fetch_one(&mut conn).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, err))?;
            let sector: SectorWithId = row.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err={}", self, options.source_id, update_at, err))?;
            
            match &sector.info.order {
                SectorOrderOptions::Order(_) => {
                    let server = self.clone();
                    let sector = sector.clone();
                    async_std::task::spawn(async move {
                        let _ = server.apply_contract(sector).await;
                    });
                },
                SectorOrderOptions::Bill(bill_options) => {
                    let server = self.clone();
                    let sector = sector.clone();
                    let bill_options = bill_options.clone();
                    async_std::task::spawn(async move {
                        let _ = server.order_on_bill(sector, bill_options).await;
                    });
                },
                SectorOrderOptions::Filter(filter) => {
                    let server = self.clone();
                    let sector = sector.clone();
                    let filter = filter.clone();
                    async_std::task::spawn(async move {
                        let _ = server.order_with_filter(sector, filter.clone()).await;
                    });
                }
            }

            info!("{} create sector,return, options={:?}, update_at={}, sector={:?}", self, source, update_at, sector);
            Ok(sector)
        } else {
            Err(dmc_err!(DmcErrorCode::Failed, "{} create sector, options={:?}, update_at={}, err=already exists", self, source, update_at))
        }
    }

    pub async fn remove_sector(&self, _: u64) -> DmcResult<Sector> {
        unimplemented!()
    }

    pub async fn sectors(&self, filter: &SectorFilter, navigator: &SectorNavigator) -> DmcResult<Vec<SectorWithId>> {
        if navigator.page_size == 0 {
            return Ok(vec![]);
        }
        if let Some(sector_id) = filter.sector_id {
            let result: Option<ActiveSectorRow> = sqlx::query_as("SELECT * FROM active_sector WHERE sector_id=?").bind(sector_id as sqlx_types::U64)
                .fetch_optional(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
            if let Some(row) = result {
                let sector = row.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
                Ok(vec![sector])
            } else {
                Ok(vec![])
            }
        } else {
            let results: Vec<ActiveSectorRow> = sqlx::query_as("SELECT * FROM active_sector ORDER BY sector_id LIMIT ?, ?")
                .bind(navigator.page_index as sqlx_types::U64 * navigator.page_size as sqlx_types::U64).bind(navigator.page_size as sqlx_types::U64)
                .fetch_all(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, err={}", self, err))?;
            Ok(results.into_iter().map(|row| row.try_into().unwrap()).collect())
        }
       
    }
}


