use log::*;
use std::{
    sync::Arc,
    time::Duration
};
use async_std::{task, fs, stream::StreamExt};
use serde::{Serialize, Deserialize};
use tide::{Request, Response, Body, http::Url};
use sha2::{digest::FixedOutput, Digest, Sha256};

use dmc_tools_common::*;
#[cfg(feature = "sqlite")]
use sqlx_sqlite as sqlx_types;
#[cfg(feature = "mysql")]
use sqlx_mysql as sqlx_types;

use dmc_user_journal::*;
use dmc_user_sources::*;
use dmc_miner_contracts as miner;
use crate::{
    types::*
};

#[derive(sqlx::FromRow)]
pub struct ContractStateRow {
    order_id: sqlx_types::U64, 
    process_id: Option<sqlx_types::U32>, 
    update_at: sqlx_types::U64, 
    miner_host: String, 
    source_id: sqlx_types::U64, 
    state_code: sqlx_types::U8,
    state_value: Option<Vec<u8>>, 
    onchain_challenge: Option<sqlx_types::U32>
}

impl ContractStateRow {
    fn state_code_of(state: &ContractState) -> sqlx_types::U8 {
        use ContractState::*;
        match state {
            Unknown => Self::state_code_unknown(), 
            Refused => Self::state_code_refused(), 
            Applying => Self::state_code_applying(), 
            Preparing => Self::state_code_preparing(),  
            Writing => Self::state_code_writing(), 
            Storing => Self::state_code_storing(),  
            Canceled => Self::state_code_canceled(), 
            Error(_) => Self::state_code_error(), 
        }
    }


    fn state_code_unknown() -> sqlx_types::U8 {
        0
    }
    fn state_code_refused()-> sqlx_types::U8 {
        1
    }
    fn state_code_applying() -> sqlx_types::U8 {
        2
    }
    fn state_code_preparing()-> sqlx_types::U8 {
        3
    }
    fn state_code_writing()-> sqlx_types::U8 {
        4
    } 
    fn state_code_storing()-> sqlx_types::U8 {
        5
    } 
    fn state_code_canceled() -> sqlx_types::U8 {
        6
    }
    fn state_code_error()-> sqlx_types::U8 {
        10
    }
}

impl TryInto<Contract> for ContractStateRow {
    type Error = DmcError;
    fn try_into(self) -> DmcResult<Contract> {
        let contract = Contract {
            order_id: self.order_id as u64, 
            process_id: self.process_id.map(|i| i as u32), 
            update_at: self.update_at as u64,
            miner_endpoint: self.miner_host.clone(),
            source_id: self.source_id as u64, 
            state: if self.state_code == Self::state_code_unknown() {
                ContractState::Unknown
            } else if self.state_code == Self::state_code_refused() {
                ContractState::Refused
            } else if self.state_code == Self::state_code_applying() {
                ContractState::Applying
            } else if self.state_code == Self::state_code_preparing() {
                ContractState::Preparing
            } else if self.state_code == Self::state_code_writing() {
                ContractState::Writing
            } else if self.state_code == Self::state_code_storing() {
                ContractState::Storing
            } else if self.state_code == Self::state_code_canceled() {
                ContractState::Canceled
            } else if self.state_code == Self::state_code_error() {
                let state_value = self.state_value.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, format!("invalid contract error {}", "NULL")))?;
                let error = String::from_utf8(state_value).map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid contract error {}", err)))?;
                ContractState::Error(error)
            } else {
                return Err(DmcError::new(DmcErrorCode::InvalidData, format!("invalid contract state code {}", self.state_code)));
            },
            challenge: None
        };
        Ok(contract)
    }
}


#[derive(sqlx::FromRow)]
struct OffchainChallengeRow {
    order_id: sqlx_types::U64,
    index: sqlx_types::U32,
    state_code: sqlx_types::U8,
    state_value: Option<Vec<u8>>,
    update_at: sqlx_types::U64
}

impl OffchainChallengeRow {
    fn state_code_creating() -> sqlx_types::U8 {
        0
    }

    fn state_code_requesting() -> sqlx_types::U8 {
        1
    }

    fn state_code_prooved() -> sqlx_types::U8 {
        2
    }

    fn state_code_error() -> sqlx_types::U8 {
        3
    }
}

impl TryInto<OffChainChallenge> for OffchainChallengeRow {
    type Error = DmcError;
    fn try_into(self) -> DmcResult<OffChainChallenge> {
        let state = if self.state_code == Self::state_code_creating() {
            OffChainChallengeState::Creating
        } else if self.state_code == Self::state_code_requesting() {
            let stub = serde_json::from_slice(&self.state_value.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "not state value"))?)?;
            OffChainChallengeState::Requesting(stub)
        } else if self.state_code == Self::state_code_prooved() {
            OffChainChallengeState::Prooved
        } else if self.state_code == Self::state_code_error() {
            OffChainChallengeState::Error
        } else {
            unreachable!()
        };

        Ok(OffChainChallenge {
            order_id: self.order_id as u64,
            index: self.index as u32,
            state,
            update_at: self.update_at as u64
        })
    }
}



#[derive(sqlx::FromRow)]
struct SourceChallengeRow {
    order_id: sqlx_types::U64,
    index: sqlx_types::U32,
    state_code: sqlx_types::U8,
    state_value: Option<Vec<u8>>,
    update_at: sqlx_types::U64, 
    block_number: sqlx_types::U64, 
    tx_index: sqlx_types::U32
}

impl SourceChallengeRow {
    fn state_code_waiting() -> sqlx_types::U8 {
        0
    }

    fn state_code_created() -> sqlx_types::U8 {
        1
    }

    fn state_code_commited() -> sqlx_types::U8 {
        2
    }

    fn state_code_prooved() -> sqlx_types::U8 {
        3
    }

    fn state_code_expired() -> sqlx_types::U8 {
        4
    }

    fn state_code_failed() -> sqlx_types::U8 {
        5
    }

    fn state_code_error() -> sqlx_types::U8 {
        10
    }
}

impl TryInto<OnChainChallenge> for SourceChallengeRow {
    type Error = DmcError;
    fn try_into(self) -> DmcResult<OnChainChallenge> {
        let state = if self.state_code == Self::state_code_waiting() {
            SourceChallengeState::Waiting
        } else if self.state_code == Self::state_code_created() {
            let stub = serde_json::from_slice(&self.state_value.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no state value"))?)
                .map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid state value {}", err)))?;
            SourceChallengeState::Created(stub)
        } else if self.state_code == Self::state_code_commited() {
            SourceChallengeState::Commited
        } else if self.state_code == Self::state_code_prooved() {
            SourceChallengeState::Prooved
        } else if self.state_code == Self::state_code_expired() {
            SourceChallengeState::Expired
        } else if self.state_code == Self::state_code_failed() {
            SourceChallengeState::Failed
        } else if self.state_code == Self::state_code_error() {
            let error = String::from_utf8(self.state_value.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no state value"))?)
                .map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid state value {}", err)))?;
            SourceChallengeState::Error(error)
        } else {
            unreachable!()
        };

        Ok(OnChainChallenge {
            order_id: self.order_id as u64,
            index: self.index as u32,
            state: OnChainChallengeState::SourceStub(state),
            update_at: self.update_at as u64
        })
    }
}



#[derive(Serialize, Deserialize, Clone)]
pub struct ContractsServerConfig {
    pub host: String, 
    pub sql_url: String, 
    pub source_client: SourceClientConfig, 
    pub journal_client: JournalClientConfig, 
    pub apply_atomic_interval: Duration, 
    pub apply_retry_interval: Duration,
    pub challenge_atomic_interval: Duration, 
    pub challenge_interval: Duration, 
}

struct ServerImpl<T: DmcChainAccountClient> {
    config: ContractsServerConfig, 
    sql_pool: sqlx_types::SqlPool, 
    journal_client: JournalClient, 
    source_client: SourceClient, 
    chain_client: T
}


#[derive(Clone)]
pub struct ContractsServer<T: DmcChainAccountClient> {
    inner: Arc<ServerImpl<T>>
}

impl<T: DmcChainAccountClient> std::fmt::Display for ContractsServer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserContractsServer {{process_id={}}}", std::process::id())
    }
}

impl<T: DmcChainAccountClient> ContractsServer<T> {
    pub async fn listen(self) -> DmcResult<()> {
        let host = self.config().host.clone();

        {
            let server = self.clone();
            task::spawn(async move {
                loop {
                    let _ = server.on_apply_time_escape().await;
                    task::sleep(server.config().apply_atomic_interval).await;
                }
            });
        }


        {
            let server = self.clone();
            task::spawn(async move {
                loop {
                    let _ = server.on_offchain_challenge_time_escape().await;
                    task::sleep(server.config().challenge_atomic_interval).await;
                }
            });
        }
                
        let mut http_server = tide::with_state(self);
    
        http_server.at("/contract").post(|mut req: Request<Self>| async move {
            let mut query = None;
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "order_id" => {
                        query = Some(QueryContract {order_id: u64::from_str_radix(&*value, 10)?});
                    }
                    _ => {

                    }
                }
            }
            // let query = req.query::<QueryContract>()?;

            if let Some(_) = query {
                let options = req.body_json().await?;
                let _ = req.state().update(options).await?;
                let resp = Response::new(200);
                Ok(resp)
            } else {
                let options = req.body_json().await?;
                let mut resp = Response::new(200);
                resp.set_body(Body::from_json(&req.state().apply(options).await?)?);
                Ok(resp)
            }
           
        });
    
        http_server.at("/contract").get(|req: Request<Self>| async move {
            let mut query = ContractFilterAndNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
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
            // let query = req.query::<ContractFilterAndNavigator>()?;
            
            let mut resp = Response::new(200);
            let contracts = req.state().get(query.filter, query.navigator).await?;
            resp.set_body(Body::from_json(&contracts)?);
            Ok(resp)
        });


        http_server.at("/contract/challenge").post(|mut req: Request<Self>| async move {
            let options = req.body_json().await?;
            let _ = req.state().challenge(options).await?;
            let resp = Response::new(200);
            Ok(resp)
        });

        let _ = http_server.listen(host.as_str()).await?;

        Ok(())
    }

    pub async fn new(chain_client: T, config: ContractsServerConfig) -> DmcResult<Self> {
        let sql_pool = sqlx_types::connect_pool(&config.sql_url).await?;
        Ok(Self {
            inner: Arc::new(ServerImpl {
                chain_client, 
                journal_client: JournalClient::new(config.journal_client.clone())?, 
                source_client: SourceClient::new(config.source_client.clone())?, 
                config, 
                sql_pool,  
            })
        })
    }

    pub async fn init(&self) -> DmcResult<()> {
        let _ = sqlx::query(&Self::sql_create_table_contract_state()).execute(&self.inner.sql_pool).await?;
        let _ = sqlx::query(&Self::sql_create_table_contract_challenge()).execute(&self.inner.sql_pool).await?;
        let _ = sqlx::query(&Self::sql_create_table_onchain_source_challenge()).execute(&self.inner.sql_pool).await?;
        
        for sql in Self::sql_update_table_contract_state() {
            let _ = sqlx::query(sql).execute(&self.inner.sql_pool).await;
        }
        
        Ok(())
    }

    pub async fn reset(self) -> DmcResult<Self> {
        self.init().await?;
        let mut trans = self.sql_pool().begin().await?;
        let _ = sqlx::query("DELETE FROM contract_state WHERE order_id > 0").execute(&mut trans).await?;
        let _ = sqlx::query("DELETE FROM contract_challenge WHERE order_id > 0").execute(&mut trans).await?;
        let _ = sqlx::query("DELETE FROM contract_onchain_source_challenge WHERE order_id > 0").execute(&mut trans).await?;
        trans.commit().await?;
        Ok(self)
    }
}



impl<T: DmcChainAccountClient> ContractsServer<T> {
    fn chain_client(&self) -> &T {
        &self.inner.chain_client
    }

    fn source_client(&self) -> &SourceClient {
        &self.inner.source_client
    }

    fn journal_client(&self) -> &JournalClient {
        &self.inner.journal_client
    }

    fn config(&self) -> &ContractsServerConfig {
        &self.inner.config
    }

    #[cfg(feature = "sqlite")]
    fn sql_pool(&self) -> &sqlx::Pool<sqlx::Sqlite> {
        &self.inner.sql_pool
    }

    #[cfg(feature = "mysql")]
    fn sql_pool(&self) -> &sqlx::Pool<sqlx::MySql> {
        &self.inner.sql_pool
    }

    #[cfg(feature = "mysql")]
    fn sql_create_table_contract_state() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `contract_state` (
            `order_id` BIGINT UNSIGNED NOT NULL PRIMARY KEY, 
            `miner_host` TEXT NOT NULL, 
            `source_id` BIGINT UNSIGNED NOT NULL, 
            `state_code` TINYINT UNSIGNED NOT NULL,  
            `process_id` INT UNSIGNED DEFAULT NULL, 
            `update_at` BIGINT UNSIGNED NOT NULL, 
            `block_number` BIGINT UNSIGNED NOT NULL DEFAULT 0, 
            `tx_index` INT UNSIGNED NOT NULL DEFAULT 0, 
            `last_challenge_at` BIGINT UNSIGNED, 
            INDEX (`update_at`),
            INDEX (`process_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
    }

    #[cfg(feature = "mysql")]
    fn sql_update_table_contract_state() -> Vec<&'static str> {
        vec![
            "ALTER TABLE `contract_state` ADD COLUMN `state_value` BLOB AFTER `process_id`;",
            "ALTER TABLE `contract_state` ADD COLUMN `onchain_challenge` INT UNSIGNED;",
        ]
    }


    #[cfg(feature = "sqlite")]
    fn sql_create_table_contract_state() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `contract_state` (
            `order_id` INTEGER NOT NULL PRIMARY KEY,
            `miner_host` TEXT NOT NULL,
            `source_id` INTEGER NOT NULL,
            `state_code` INTEGER NOT NULL,
            `process_id` INTEGER DEFAULT NULL,
            `update_at` INTEGER NOT NULL,
            `block_number` INTEGER NOT NULL DEFAULT 0,
            `tx_index` INTEGER NOT NULL DEFAULT 0,
            `last_challenge_at` INTEGER, 
            `state_value` BLOB
        );
        CREATE INDEX IF NOT EXISTS update_at ON contract_state (
            update_at
        );
        CREATE INDEX IF NOT EXISTS process_id ON contract_state (
            process_id
        );"#
    }

    #[cfg(feature = "sqlite")]
    fn sql_update_table_contract_state() -> Vec<&'static str> {
        vec![
            "ALTER TABLE `contract_state` ADD COLUMN `state_value` BLOB;", 
            "ALTER TABLE `contract_state` ADD COLUMN `onchain_challenge` INTEGER;",
        ]
    }
    

    #[cfg(feature = "mysql")]
    fn sql_create_table_contract_challenge() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `contract_challenge` (
            `order_id` BIGINT UNSIGNED NOT NULL, 
            `index` INT UNSIGNED NOT NULL, 
            `state_code` TINYINT UNSIGNED NOT NULL, 
            `state_value` BLOB,   
            `update_at` BIGINT UNSIGNED NOT NULL, 
            `process_id` INT UNSIGNED DEFAULT NULL, 
            PRIMARY KEY order_index (`order_id`, `index`),
            INDEX (`process_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
        
    }

    
    #[cfg(feature = "sqlite")]
    fn sql_create_table_contract_challenge() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `contract_challenge` (
            `order_id` INTEGER NOT NULL,
            `index` INTEGER NOT NULL,
            `state_code` INTEGER NOT NULL,
            `state_value` BLOB,
            `update_at` INTEGER NOT NULL,
            `process_id` INTEGER DEFAULT NULL,
            PRIMARY KEY(`order_id`, `index`)
        );
        CREATE INDEX IF NOT EXISTS process_id ON contract_challenge (
            process_id
        );"#
    }

    #[cfg(feature = "mysql")]
    fn sql_create_table_onchain_source_challenge() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `contract_onchain_source_challenge` (
            `order_id` BIGINT UNSIGNED NOT NULL, 
            `index` INT UNSIGNED NOT NULL, 
            `state_code` TINYINT UNSIGNED NOT NULL, 
            `state_value` BLOB,   
            `update_at` BIGINT UNSIGNED NOT NULL, 
            `process_id` INT UNSIGNED DEFAULT NULL, 
            `block_number` BIGINT UNSIGNED DEFAULT 0, 
            `tx_index` INT UNSIGNED DEFAULT 0, 
            PRIMARY KEY order_index (`order_id`, `index`),
            INDEX (`process_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
    }


    #[cfg(feature = "sqlite")]
    fn sql_create_table_onchain_source_challenge() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `contract_onchain_source_challenge` (
            `order_id` INTEGER NOT NULL,
            `index` INTEGER NOT NULL,
            `state_code` INTEGER NOT NULL,
            `state_value` BLOB,
            `update_at` INTEGER NOT NULL,
            `process_id` INTEGER DEFAULT NULL,
            `block_number` INTEGER DEFAULT 0, 
            `tx_index` INTEGER DEFAULT 0, 
            PRIMARY KEY(`order_id`, `index`)
        );
        CREATE INDEX IF NOT EXISTS process_id ON contract_onchain_source_challenge (
            process_id
        );"#
    }

    async fn get_contract_by_id(&self, order_id: u64) -> DmcResult<Option<Contract>> {
        let contract = sqlx::query_as::<_, ContractStateRow>("SELECT * FROM contract_state WHERE order_id=?")
            .bind(order_id as sqlx_types::U64).fetch_optional(self.sql_pool()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get contract by id={}, err={}", self, order_id, err))?;

        if let Some(contract) = contract {
            let challenge = if let Some(index) = contract.onchain_challenge {
                let challenge = if T::supported_challenge_type() == DmcChallengeTypeCode::SourceStub {
                    let challenge_row = sqlx::query_as::<_, SourceChallengeRow>("SELECT * FROM contract_onchain_source_challenge WHERE order_id=? AND `index`=?")
                        .bind(order_id as sqlx_types::U64).bind(index as sqlx_types::U32).fetch_one(self.sql_pool()).await
                        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get contract by id={}, err=query  on chain challenge {} {}", self, order_id, index, err))?;

                    challenge_row.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get contract by id={}, err={}", self, order_id, err))?
                } else if T::supported_challenge_type() == DmcChallengeTypeCode::MerklePath {
                    todo!()
                } else {
                    unimplemented!()
                };
                Some(challenge)
            } else {
                None
            };
            let mut contract: Contract = contract.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get contract by id={}, err={}", self, order_id, err))?;
            contract.challenge = challenge;

            Ok(Some(contract))
        } else {
            Ok(None)
        }  
    }

    #[async_recursion::async_recursion]
    async fn prepare_inner(&self, contract: Contract) -> DmcResult<()> {
        let update_at = contract.update_at;
        info!("{} prepare contract, contract={:?}, update_at={}", self, contract, update_at);
        let source = self.source_client().sources(SourceFilter::from_source_id(contract.source_id), 1).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
            .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
            .get(0).cloned().ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, "source not found"))?;

        if source.state != SourceState::Ready {
            return Err(dmc_err!(DmcErrorCode::InvalidData, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, "source not found"))?;
        }
        
        let merkle_stub = source.merkle_stub.unwrap();
        info!("{} prepare contract, contract={}, update_at={}, merkel_stub={:?}", self, contract.order_id, update_at, merkle_stub);
        let result = self.chain_client().prepare_order(DmcPrepareOrderOptions { order_id: contract.order_id, merkle_stub }).await
            .map_err(|err| dmc_err!(DmcErrorCode::InvalidData, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
        
        match result {
            Ok(_) => {
                info!("{} prepare contract, contract={}, update_at={}, set merkle root to order", self, contract.order_id, update_at);
    
                let update_at = dmc_time_now();
                if sqlx::query("UPDATE contract_state SET state_code=?, process_id=NULL, update_at=? WHERE order_id=? AND state_code=? AND process_id=? AND update_at=?")
                    .bind(ContractStateRow::state_code_writing()).bind(update_at as sqlx_types::U64)
                    .bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
                    .bind(std::process::id() as sqlx_types::U32).bind(contract.update_at as sqlx_types::U64)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, contract.update_at, err))?
                    .rows_affected() > 0 {
                    let _ = self.journal_client().append(JournalEvent { source_id: contract.source_id, order_id: Some(contract.order_id), event_type: JournalEventType::OrderPrepared, event_params: None }).await
                        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, contract.update_at, err))?;
                    info!("{} prepare contract, contract={:?}, update_at={}, update to writing", self, contract, update_at);
                    let mut contract = contract;
                    contract.state = ContractState::Writing;
                    contract.update_at = update_at;
                    let server = self.clone();
                    task::spawn(async move {
                        let _ = server.write_contract(contract).await;
                    });
                } else {
                    info!("{} prepare contract, contract={:?}, update_at={}, ignore", self, contract, update_at);
                }
            }, 
            Err(err) => {
                info!("{} prepare contract, contract={}, update_at={}, save error = {}", self, contract.order_id, update_at, err);
    
                let update_at = dmc_time_now();

                if sqlx::query("UPDATE contract_state SET state_code=?, state_value=?, process_id=NULL, update_at=? WHERE order_id=? AND state_code=? AND process_id=? AND update_at=?")
                    .bind(ContractStateRow::state_code_error()).bind(format!("prepare contract {}", err)).bind(update_at as sqlx_types::U64)
                    .bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
                    .bind(std::process::id() as sqlx_types::U32).bind(contract.update_at as sqlx_types::U64)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, contract.update_at, err))?
                    .rows_affected() > 0 {
                    let _ = self.journal_client().append(JournalEvent { source_id: contract.source_id, order_id: Some(contract.order_id), event_type: JournalEventType::OrderFailed, event_params: Some(err.msg().to_owned()) }).await
                        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, contract.update_at, err))?;
                    info!("{} prepare contract, contract={:?}, update_at={}, update to error", self, contract, update_at);
                } else {
                    info!("{} prepare contract, contract={:?}, update_at={}, ignore", self, contract, update_at);
                }
            }
        }
        
        Ok(())
    }   

    async fn prepare_contract(&self, contract: Contract) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} prepare contract, contract={:?}, update_at={}", self, contract, update_at);
        if sqlx::query("UPDATE contract_state SET process_id=?, update_at=? WHERE order_id=? AND state_code=? AND update_at=? AND (process_id IS NULL OR process_id!=?)")
                    .bind(std::process::id() as sqlx_types::U32).bind(update_at as sqlx_types::U64)
                    .bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
                    .bind(contract.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
                    .rows_affected() == 0 {
            info!("{} prepare contract, contract={:?}, update_at={}, ignore", self, contract, update_at);
            return Ok(());
        } 

        let mut contract = contract;
        contract.update_at = update_at;

        if self.prepare_inner(contract.clone()).await.is_err() {
            let _ = sqlx::query("UPDATE contract_state SET process_id=NULL, update_at=? WHERE order_id=? AND state_code=? AND process_id=? AND update_at=?")
                    .bind(dmc_time_now() as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64)
                    .bind(ContractStateRow::state_code_of(&contract.state)).bind(contract.update_at as sqlx_types::U64)
                    .bind(std::process::id() as sqlx_types::U32)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
        }
        Ok(())
    }

    async fn write_contract(&self, contract: Contract) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} write contract, contract={:?}, update_at={}", self, contract, update_at);
        if sqlx::query("UPDATE contract_state SET process_id=?, update_at=? WHERE order_id=? AND state_code=? AND update_at=? AND (process_id IS NULL OR process_id!=?)")
                    .bind(std::process::id() as sqlx_types::U32).bind(update_at as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64)
                    .bind(ContractStateRow::state_code_of(&contract.state)).bind(contract.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
                    .rows_affected() == 0 {
            info!("{} write contract, contract={:?}, update_at={}, ignore", self, contract, update_at);
            return Ok(());
        } 

        let mut contract = contract;
        contract.update_at = update_at;

        if self.write_inner(contract.clone()).await.is_err() {
            let _ = sqlx::query("UPDATE contract_state SET process_id=NULL, update_at=? WHERE order_id=? AND state_code=? AND process_id=? AND update_at=?")
                    .bind(dmc_time_now() as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
                    .bind(std::process::id() as sqlx_types::U32).bind(contract.update_at as sqlx_types::U64)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
        }
        Ok(())
    }

    async fn write_inner(&self, contract: Contract) -> DmcResult<()> {
        let update_at = contract.update_at;
        info!("{} write contract, contract={:?}, update_at={}", self, contract, update_at);
        use miner::*;

        let miner_client = ContractClient::new(ContractClientConfig {
            endpoint: Url::parse(&contract.miner_endpoint)?,
        })?;
        
        let source = self.source_client().sources(SourceFilter::from_source_id(contract.source_id), 1).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
            .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
            .get(0).cloned().ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} write contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, "source not found"))?;

        if source.state != SourceState::Ready {
            return Err(dmc_err!(DmcErrorCode::InvalidData, "{} write contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, "source not found"))?;
        }

        let source_url = Url::parse(&source.source_url)
            .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} write contract, contract={:?}, update_at={}, err=source url {}", self, contract, update_at, err))?;
        if source_url.scheme() == "file" {
            let source_path;
            #[cfg(target_os = "windows")] {
                source_path = &source_url.path()[1..];
            } 
            #[cfg(not(target_os = "windows"))] {
                source_path = source_url.path();
            }
            let source_file = fs::File::open(source_path).await
                .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} write contract, contract={:?}, update_at={}, err=open file {} {}", self, contract, update_at, source_path, err))?;
            
            let file_length = source_file.metadata().await
                .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} write contract, contract={:?}, update_at={}, err=open file {} {}", self, contract, update_at, source_path, err))?
                .len();
            let options = WriteContractOptions { navigator: ContractDataNavigator { order_id: contract.order_id, offset: 0, len: file_length, }};
            let miner_side = miner_client.write(source_file, options, self.chain_client()).await?;
            match miner_side.state {
                ContractState::Calculating {..} | ContractState::Calculated {..} | ContractState::Storing { .. } => {Ok(())}, 
                _ => unimplemented!()
            }
        } else {
            unimplemented!()
        }
    }

    async fn on_apply_time_escape(&self) -> DmcResult<()> {
        let now = dmc_time_now();
        let apply_expired = now - self.config().apply_retry_interval.as_micros() as u64;
        debug!("{} on apply time escape, when={}", self, now);
        {
            let mut contract_stream = sqlx::query_as::<_, ContractStateRow>("SELECT * FROM contract_state WHERE (state_code=? OR state_code=? OR state_code=?) AND update_at<? AND (process_id IS NULL OR process_id!=?)")
                .bind(ContractStateRow::state_code_applying()).bind(ContractStateRow::state_code_preparing()).bind(ContractStateRow::state_code_writing())
                .bind(apply_expired as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32).fetch(self.sql_pool());
            loop {
                let result = contract_stream.next().await;
                if result.is_none() {
                    debug!("{} on apply time escape, when={}, finish retry apply", self, now);
                    break;
                }
                let result = result.unwrap();
                match result {
                    Ok(contract) => {
                        match contract.try_into() {
                            Ok(contract) => {
                                let _ = self.retry_apply(contract).await;
                            },
                            Err(err) => {
                                error!("{} on apply time escape, err, when={}, err={}", self, now, err);
                                break;
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

        {
            let mut challenge_stream = sqlx::query_as::<_, OffchainChallengeRow>("SELECT * FROM contract_challenge WHERE (state_code=? OR state_code=?) AND update_at<? AND (process_id IS NULL OR process_id!=?)")
                .bind(OffchainChallengeRow::state_code_creating() as sqlx_types::U8).bind(OffchainChallengeRow::state_code_requesting() as sqlx_types::U8)
                .bind(apply_expired as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32).fetch(self.sql_pool());
            loop {
                let result = challenge_stream.next().await;
                if result.is_none() {
                    debug!("{} on apply time escape, when={}, finish retry process challenge", self, now);
                    break;
                }
                let result = result.unwrap();
                match result {
                    Ok(row) => {
                        let challenge = row.try_into().unwrap();
                        let _ = self.process_offchain_challenge(challenge, None).await;
                    }, 
                    Err(err) => {
                        error!("{} on apply time escape, err, when={}, err={}", self, now, err);
                        break;
                    }
                }
            }
        }
        Ok(())
    }



    async fn on_offchain_challenge_time_escape(&self) -> DmcResult<()> {
        let now = dmc_time_now();
        let challenge_expired = now - self.config().challenge_interval.as_micros() as u64;
        debug!("{} on challenge time escape, when={}", self, now);
        {
            let mut contract_stream = sqlx::query_as::<_, ContractStateRow>("SELECT * FROM contract_state WHERE state_code=? AND last_challenge_at<? ")
                .bind(ContractStateRow::state_code_storing()).bind(challenge_expired as sqlx_types::U64).fetch(self.sql_pool());
            loop {
                let result = contract_stream.next().await;
                if result.is_none() {
                    debug!("{} on challenge time escape, when={}, finished", self, now);
                    break;
                }
                let result = result.unwrap();
                match result {
                    Ok(contract) => {
                        match contract.try_into() {
                            Ok(contract) => {
                                let _ = self.new_offchain_challenge(contract).await;
                            },
                            Err(err) => {
                                error!("{} on challenge time escape, err, when={}, err={}", self, now, err);
                                break;
                            }   
                        }
                       
                    }, 
                    Err(err) => {
                        error!("{} on challenge time escape, err, when={}, err={}", self, now, err);
                        break;
                    }
                }
            }
        }
        Ok(())
    }


    async fn new_offchain_challenge(&self, contract: Contract) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} new challenge, order={}, update_at={}", self, contract.order_id, update_at);
        let row: Option<OffchainChallengeRow> = sqlx::query_as("SELECT * FROM contract_challenge WHERE order_id=? ORDER BY `index` DESC LIMIT 1")
            .bind(contract.order_id as sqlx_types::U64).fetch_optional(self.sql_pool()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} new challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
        let index = if let Some(last_challenge) = row {
            last_challenge.index + 1
        } else {
            0
        };

        let mut trans = self.sql_pool().begin().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} new challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
        if sqlx::query("INSERT INTO contract_challenge (order_id, `index`, state_code, update_at) SELECT ?, ?, ?, ? FROM contract_state WHERE order_id=? AND state_code=?")
            .bind(contract.order_id as sqlx_types::U64).bind(index as sqlx_types::U32).bind(OffchainChallengeRow::state_code_creating() as sqlx_types::U8)
            .bind(update_at as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
            .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} new challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
            .rows_affected() > 0 {
            if sqlx::query("UPDATE contract_state SET last_challenge_at=? WHERE order_id=? AND state_code=?")
                .bind(update_at as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
                .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} new challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
                .rows_affected() > 0 {
                trans.commit().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} new challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
                
                let challenge = OffChainChallenge {
                    order_id: contract.order_id,
                    index: index as u32,
                    state: OffChainChallengeState::Creating,
                    update_at
                };
                let server = self.clone();
                task::spawn(async move {
                    let _ = server.process_offchain_challenge(challenge, Some(contract)).await;
                });
                Ok(())
            } else {
                info!("{} new challenge, order={}, update_at={}, ignored", self, contract.order_id, update_at);
                Ok(())
            }
        } else {
            info!("{} new challenge, order={}, update_at={}, ignored", self, contract.order_id, update_at);
            Ok(())
        }
    }

    async fn process_offchain_challenge(&self, challenge: OffChainChallenge, contract: Option<Contract>) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} process challenge, challenge={:?}, update_at={}", self, challenge, update_at);
        
        if sqlx::query("UPDATE contract_challenge SET process_id=?, update_at=? WHERE order_id=? AND `index`=? AND (process_id IS NULL OR process_id!=?)")
            .bind(std::process::id() as sqlx_types::U32).bind(update_at as sqlx_types::U64).bind(challenge.order_id as sqlx_types::U64)
            .bind(challenge.index as sqlx_types::U32).bind(std::process::id() as sqlx_types::U32)
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} process challenge, challenge={:?}, update_at={}, err={}", self, challenge, update_at, err))?
            .rows_affected() == 0 {
            info!("{} process challenge, challenge={:?}, update_at={}, ignored", self, challenge, update_at);
            return Ok(());
        }

        let mut challenge = challenge;
        challenge.update_at = update_at;
        if match &challenge.state {
            OffChainChallengeState::Creating => self.create_offchain_challenge_inner(challenge.clone(), contract).await,
            OffChainChallengeState::Requesting(_) => self.request_offchain_challenge_inner(challenge.clone(), contract).await,
            _ => {
                Ok(())
            }
        }.is_err() {
            let _ = sqlx::query("UPDATE contract_challenge SET process_id=NULL, update_at=? WHERE order_id=? AND `index`=? AND update_at=? AND process_id=?")
                .bind(dmc_time_now() as sqlx_types::U64).bind(challenge.order_id as sqlx_types::U64).bind(challenge.index as sqlx_types::U32)
                .bind(challenge.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} process challenge, challenge={:?}, update_at={}, err={}", self, challenge, update_at, err))?;
        } 
        Ok(())
    }

    async fn create_offchain_challenge_inner(&self, challenge: OffChainChallenge, contract: Option<Contract>) -> DmcResult<()> {
        info!("{} create challenge, challenge={:?}, update_at={}", self, challenge, challenge.update_at);
        let contract = if let Some(contract) = contract {
            contract
        } else {
            self.get_contract_by_id(challenge.order_id).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, err))?
                .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} create challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, "contract not found"))?
        };
        let stub = self.source_client().random_stub(contract.source_id).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, err))?
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} create challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, "stub not found"))?;
        let raw_stub = serde_json::to_vec(&stub).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, err))?;

        let update_at = dmc_time_now();
        if sqlx::query("UPDATE contract_challenge SET process_id=NULL, state_code=?, state_value=?, update_at=? WHERE order_id=? AND `index`=? AND update_at=? AND process_id=?")
            .bind(OffchainChallengeRow::state_code_requesting() as sqlx_types::U8).bind(raw_stub).bind(update_at as sqlx_types::U64)
            .bind(challenge.order_id as sqlx_types::U64).bind(challenge.index as sqlx_types::U32).bind(challenge.update_at as sqlx_types::U64)
            .bind(std::process::id() as sqlx_types::U32)
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, err))?
            .rows_affected() > 0 {
            info!("{} create challenge, challenge={:?}, update_at={}, finished, stub={:?}", self, challenge, challenge.update_at, stub);

            let mut challenge = challenge;
            challenge.state = OffChainChallengeState::Requesting(stub);
            challenge.update_at = update_at;

            let server = self.clone();
            task::spawn(async move {
                let _ = server.request_offchain_challenge_inner(challenge, Some(contract)).await;
            });
        } else {
            info!("{} create challenge, challenge={:?}, update_at={}, ignored", self, challenge, challenge.update_at);
        }
        Ok(())
    }


    async fn request_offchain_challenge_inner(&self, challenge: OffChainChallenge, contract: Option<Contract>) -> DmcResult<()> {
        info!("{} request challenge, challenge={:?}, update_at={}", self, challenge, challenge.update_at);
        let contract = if let Some(contract) = contract {
            contract
        } else {
            self.get_contract_by_id(challenge.order_id).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} request challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, err))?
                .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} request challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, "contract not found"))?
        };

        if let OffChainChallengeState::Requesting(stub) = &challenge.state {
            let miner_client = miner::ContractClient::new(miner::ContractClientConfig {
                endpoint: Url::parse(&contract.miner_endpoint)?,
            })?;
            let result = match miner_client.challenge(miner::OffChainChallenge {
                order_id: challenge.order_id, 
                offset: stub.offset, 
                length: stub.length
            }).await {
                Ok(proof) => {
                    if proof.content == stub.content {
                        info!("{} request challenge, challenge={:?}, update_at={}, proof ok", self, challenge, challenge.update_at);
                        OffchainChallengeRow::state_code_prooved()
                    } else {
                        error!("{} create challenge, challenge={}, update_at={}, err={} {:?}", self, challenge.order_id, challenge.update_at, "result mismatch", proof.content);
                        OffchainChallengeRow::state_code_error()
                    }
                },
                Err(err) => {
                    error!("{} create challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, err);
                    OffchainChallengeRow::state_code_error()
                } 
            };

            let event_type = if result == OffchainChallengeRow::state_code_prooved() {
                JournalEventType::OffchainChallengeOk
            } else {
                JournalEventType::OffchainChallengeFailed
            };
            let _ = self.journal_client().append(JournalEvent { source_id: contract.source_id, order_id: Some(contract.order_id), event_type, event_params: None}).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} request challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, err))?;
            
            if sqlx::query("UPDATE contract_challenge SET state_code=?, process_id=NULL, update_at=? WHERE order_id=? AND `index`=? AND update_at=? AND process_id=?")
                .bind(result as sqlx_types::U8).bind(dmc_time_now() as sqlx_types::U64).bind(challenge.order_id as sqlx_types::U64)
                .bind(challenge.index as sqlx_types::U32).bind(challenge.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} request challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, challenge.update_at, err))?
                .rows_affected() > 0 {
                info!("{} request challenge, challenge={:?}, update_at={}, finished", self, challenge, challenge.update_at);
            } else {
                info!("{} request challenge, challenge={:?}, update_at={}, ignored", self, challenge, challenge.update_at);
            }
            Ok(())
        } else {
            unreachable!()
        }
    }

    async fn commit_onchain_source_challenge_inner(&self, contract: Contract) -> DmcResult<()> {
        info!("{} commit onchain challenge, contract={:?}", self, contract);

        let challenge = contract.challenge.as_ref().unwrap();
        match &challenge.state {
            OnChainChallengeState::SourceStub(state) => {
                if let SourceChallengeState::Created(stub) = state {
                  
                    let result = self.chain_client().challenge(DmcChallengeOptions {
                        order_id: contract.order_id, 
                        options: DmcChallengeTypedOptions::SourceStub {
                            piece_index: stub.piece_index, 
                            nonce: stub.nonce.clone(), 
                            hash: stub.hash.clone()
                        }  
                    }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} commit onchain challenge, contract={}, err={}", self, contract.order_id, err))?;
                    
                    let update_at = dmc_time_now();
                    match result {   
                        Ok(_) => {
                            info!("{} commit onchain challenge, contract={}, success", self, contract.order_id);
                            if sqlx::query("UPDATE contract_onchain_source_challenge SET process_id=NULL, state_code=?, state_value=NULL, update_at=? WHERE order_id=? AND `index`=? AND update_at=? AND process_id=?")
                                .bind(SourceChallengeRow::state_code_commited()).bind(update_at as sqlx_types::U64)
                                .bind(contract.order_id as sqlx_types::U64).bind(challenge.index as sqlx_types::U32)
                                .bind(challenge.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
                                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} commit onchain challenge, contract={}, err={}", self, contract.order_id, err))?
                                .rows_affected() > 0 {
                                let _ = self.journal_client().append(JournalEvent { source_id: contract.source_id, order_id: Some(contract.order_id), event_type: JournalEventType::OnchainChallengeCommited, event_params: None })
                                    .await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} commit onchain challenge, contract={}, err={}", self, contract.order_id, err))?;
                                info!("{} commit onchain challenge, contract={}, commited", self, contract.order_id);
                            } else {
                                info!("{} commit onchain challenge, contract={}, ignored", self, contract.order_id);
                            }
                        },
                        Err(err) => {
                            error!("{} commit onchain challenge, contract={}, failed, err={}", self, contract.order_id, err);
                            let mut trans = self.sql_pool().begin().await
                                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} commit onchain challenge, contract={}, err={}", self, contract.order_id, err))?;

                            sqlx::query("UPDATE contract_onchain_source_challenge SET process_id=NULL, state_code=?, state_value=?, update_at=? WHERE order_id=? AND `index`=? AND update_at=? AND process_id=?")
                                .bind(SourceChallengeRow::state_code_error()).bind(err.msg().as_bytes()).bind(update_at as sqlx_types::U64)
                                .bind(contract.order_id as sqlx_types::U64).bind(challenge.index as sqlx_types::U32)
                                .bind(challenge.update_at as sqlx_types::U64)
                                .bind(std::process::id() as sqlx_types::U32)
                                .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} commit onchain challenge, contract={}, err={}", self, contract.order_id, err))?;
                            sqlx::query("UPDATE contract_state SET onchain_challenge=? WHERE order_id=? AND state_code=?")
                                .bind(challenge.index as sqlx_types::U32).bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_storing())
                                .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} commit onchain challenge, contract={}, err={}", self, contract.order_id, err))?;
                            trans.commit().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} commit onchain challenge, contract={}, err={}", self, contract.order_id, err))?;
                            info!("{} commit onchain challenge, contract={}, saved error", self, contract.order_id);
                        }
                    };
                    Ok(())
                } else {
                    unreachable!()
                }
            },
            _ => {
                unreachable!()
            }
        }
    }

    async fn create_onchain_source_challenge_inner(&self, contract: Contract) -> DmcResult<()> {
        info!("{} create onchain challenge, contract={:?}", self, contract);
        
        let source = self.source_client().sources(SourceFilter::from_source_id(contract.source_id), 1).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create onchain challenge, contract={}, err={}", self, contract.order_id, err))?
            .next_page().await .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create onchain challenge, contract={}, err={}", self, contract.order_id, err))?
            .get(0).cloned() .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} create onchain challenge, contract={}, err={}", self, contract.order_id, "stub not found"))?;

        let source_stub = self.source_client().random_stub(contract.source_id).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create onchain challenge, contract={}, err={}", self, contract.order_id, err))?
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} create onchain challenge, contract={}, err={}", self, contract.order_id, "stub not found"))?;

        info!("{} create onchain challenge, contract={:?}, with source stub={:?}", self, contract.order_id, source_stub);
        let update_at = dmc_time_now();
        let nonce = format!("{}", update_at);

        let mut hasher = Sha256::new();
        hasher.update(source_stub.content.as_slice());
        hasher.update(nonce.as_bytes());
        let hash = HashValue::from(hasher.finalize_fixed());

        let mut hasher = Sha256::new();
        hasher.update(hash.as_slice());
        let hash = HashValue::from(hasher.finalize_fixed());

        let stub = SourceChallengeStub {
            piece_index: source_stub.offset / source.merkle_stub.unwrap().piece_size as u64, 
            nonce,  
            hash
        };

        let raw_stub = serde_json::to_vec(&stub).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create onchain challenge, contract={}, err={}", self, contract.order_id, err))?;


        if sqlx::query("UPDATE contract_onchain_source_challenge SET process_id=NULL, state_code=?, state_value=?, update_at=? WHERE order_id=? AND `index`=? AND update_at=? AND process_id=?")
            .bind(SourceChallengeRow::state_code_created()).bind(raw_stub).bind(update_at as sqlx_types::U64)
            .bind(contract.order_id as sqlx_types::U64).bind(contract.challenge.as_ref().unwrap().index as sqlx_types::U32).bind(contract.challenge.as_ref().unwrap().update_at as sqlx_types::U64)
            .bind(std::process::id() as sqlx_types::U32)
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create onchain challenge, contract={}, err={}", self, contract.order_id, err))?
            .rows_affected() > 0 {
            info!("{} create onchain challenge, contract={:?}, finished, stub={:?}", self, contract, stub);
                
            let mut contract = contract;
            let challenge = contract.challenge.as_mut().unwrap();
            challenge.state = OnChainChallengeState::SourceStub(SourceChallengeState::Created(stub));
            challenge.update_at = update_at;

            let server = self.clone();
            task::spawn(async move {
                let _ = server.process_onchain_challenge(contract).await;
            });
        } else {
            info!("{} create onchain challenge, contract={:?}, ignored", self, contract);
        }
        Ok(())
    }

    #[async_recursion::async_recursion]
    async fn process_onchain_challenge(&self, contract: Contract) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} process onchain challenge, contract={:?}, update_at={}", self, contract, update_at);
        let challenge = contract.challenge.as_ref().unwrap();
        match &challenge.state {
            OnChainChallengeState::SourceStub(state) => {
                if sqlx::query("UPDATE contract_onchain_source_challenge SET process_id=?, update_at=? WHERE order_id=? AND `index`=? AND (process_id IS NULL OR process_id!=?)")
                    .bind(std::process::id() as sqlx_types::U32).bind(update_at as sqlx_types::U64).bind(challenge.order_id as sqlx_types::U64)
                    .bind(challenge.index as sqlx_types::U32).bind(std::process::id() as sqlx_types::U32)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} process onchain challenge, challenge={:?}, update_at={}, err={}", self, challenge, update_at, err))?
                    .rows_affected() == 0 {
                    info!("{} process onchain challenge, challenge={:?}, update_at={}, ignored", self, challenge, update_at);
                    return Ok(());
                }

                let mut contract = contract.clone();
                contract.challenge.as_mut().unwrap().update_at = update_at;
                if match state {
                    SourceChallengeState::Waiting => self.create_onchain_source_challenge_inner(contract).await,
                    SourceChallengeState::Created(_) => self.commit_onchain_source_challenge_inner(contract).await,
                    _ => {
                        Ok(())
                    }
                }.is_err() {
                    let _ = sqlx::query("UPDATE contract_onchain_source_challenge SET process_id=NULL, update_at=? WHERE order_id=? AND `index`=? AND update_at=? AND process_id=?")
                        .bind(dmc_time_now() as sqlx_types::U64).bind(challenge.order_id as sqlx_types::U64).bind(challenge.index as sqlx_types::U32)
                        .bind(challenge.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
                        .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} process onchain challenge, challenge={:?}, update_at={}, err={}", self, challenge, update_at, err))?;
                } 
                Ok(())
            },
            OnChainChallengeState::MerklePath(_) => {
                todo!()
            }
        }
    }

    async fn apply_inner(&self, contract: Contract) -> DmcResult<()> {
        let update_at = contract.update_at;
        info!("{} apply contract, contract={:?}, update={}", self, contract, update_at);
        match contract.state {
            ContractState::Applying => {
                let (next_state, _) = {
                    let miner_client = miner::ContractClient::new(miner::ContractClientConfig {
                        endpoint: Url::parse(&contract.miner_endpoint)?,
                    })?;
                    let miner_side = miner_client.get(miner::ContractFilter::from_order_id(contract.order_id), 1).await?
                        .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
                        .get(0).cloned();

                    if miner_side.is_none() {
                        return Err(dmc_err!(DmcErrorCode::NotFound, "{} apply contract, contract={}, update_at={}, err=miner contract not found", self, contract.order_id, update_at));
                    }
                    
                    let contract = miner_side.unwrap();
                    info!("{} apply contract, contract={}, update_at={},  get miner contract state, miner_state={:?}", self, contract.order_id, update_at, contract);
                    match &contract.state {
                        miner::ContractState::Refused => (ContractState::Refused, None), 
                        miner::ContractState::Writing { sector, .. } => (ContractState::Preparing, Some(sector.clone())), 
                        _ => (ContractState::Storing, None)
                    }
                };

                if let ContractState::Preparing = next_state {
                    let update_at = dmc_time_now();
                    if sqlx::query("UPDATE contract_state SET state_code=?, process_id=NULL, update_at=? WHERE order_id=? AND state_code=? AND update_at=? AND process_id=?")
                        .bind(ContractStateRow::state_code_preparing()).bind(update_at as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64)
                        .bind(ContractStateRow::state_code_applying()).bind(contract.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
                        .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
                        .rows_affected() > 0 {
                        info!("{} apply contract, contract={:?}, update_at={}, update to preparing", self, contract, update_at);
                        let mut contract = contract;
                        contract.state = ContractState::Preparing;
                        contract.update_at = update_at;
    
                        let client = self.clone();
                        task::spawn(async move {
                            let _ = client.prepare_contract(contract).await;
                        });
                    } 
                }
                Ok(())
                
            },
            ContractState::Preparing => {
                self.prepare_inner(contract).await
            }, 
            ContractState::Writing => {
                self.write_inner(contract).await
            }, 
            ContractState::Refused => {
                let update_at = dmc_time_now();
                if sqlx::query("UPDATE contract_state SET state_code=?, process_id=NULL, update_at=? WHERE order_id=? AND state_code=? AND update_at=? AND process_id=?")
                    .bind(ContractStateRow::state_code_preparing()).bind(update_at as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64)
                    .bind(ContractStateRow::state_code_refused()).bind(contract.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
                    .rows_affected() > 0 {
                    let _ = self.journal_client().append(JournalEvent { source_id: contract.source_id, order_id: Some(contract.order_id), event_type: JournalEventType::OrderRefused, event_params: None }).await
                        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
                    info!("{} apply contract, contract={:?}, update_at={}, miner refused", self, contract, update_at);
                } 
                Ok(())
            }, 
            _ => unimplemented!()
        }
    }

    async fn retry_apply(&self, contract: Contract) -> DmcResult<()> {
        let update_at = contract.update_at;
        if sqlx::query("UPDATE contract_state SET process_id=? WHERE order_id=? AND state_code=? AND (process_id IS NULL OR process_id!=?)")
            .bind(std::process::id() as sqlx_types::U32).bind(contract.order_id as sqlx_types::U64)
            .bind(ContractStateRow::state_code_of(&contract.state)).bind(std::process::id() as sqlx_types::U32)
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} retry apply, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
            .rows_affected() > 0 {
            info!("{} retry apply, contract={:?}, update_at={}", self, contract, update_at);
            if self.apply_inner(contract.clone()).await.is_err() {
                let _ = sqlx::query("UPDATE contract_state SET process_id=NULL, update_at=? WHERE order_id=? AND state_code=? AND process_id=? AND update_at=?")
                    .bind(dmc_time_now() as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
                    .bind(std::process::id() as sqlx_types::U32).bind(contract.update_at as sqlx_types::U64)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} retry apply, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
            }
        } else {
            info!("{} retry apply, contract={:?}, update_at={}, ignored", self, contract, update_at);
        }
        Ok(()) 
    }
}

impl<T: DmcChainAccountClient> ContractsServer<T> {
    pub async fn update(&self, options: UpdateContractOptions) -> DmcResult<()> {
        let update_at = dmc_time_now();
        if let Some(challenge) = &options.challenge {
            info!("{} update challenge, challenge={:?}, update_at={}", self, options, update_at);
            match &challenge.state {
                DmcChallengeState::Prooved | DmcChallengeState::Failed => {
                    if T::supported_challenge_type() == DmcChallengeTypeCode::SourceStub {
                        let row: Option<SourceChallengeRow> = sqlx::query_as("SELECT * FROM contract_onchain_source_challenge WHERE order_id=? ORDER BY `index` DESC LIMIT 1")
                            .bind(challenge.order_id as sqlx_types::U64).fetch_optional(self.sql_pool()).await
                            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update challenge, order={}, update_at={}, err={}", self, challenge.order_id, update_at, err))?;
                        
                        if row.is_none() {
                            info!("{} update challenge, order={}, update_at={}, ignored", self, challenge.order_id, update_at);
                            return Ok(());
                        }
                        let row = row.unwrap();
                        if row.block_number as u64 > options.block_number || (row.block_number as u64 == options.block_number && row.tx_index as u32 >= options.tx_index) {
                            info!("{} update challenge, order={}, update_at={}, ignored", self, challenge.order_id, update_at);
                            return Ok(());
                        }
                        if row.state_code != SourceChallengeRow::state_code_commited() {
                            info!("{} update challenge, order={}, update_at={}, ignored", self, challenge.order_id, update_at);
                            return Ok(());
                        }

                        let mut trans = self.sql_pool().begin().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update challenge, order={}, update_at={}, err={}", self, challenge.order_id, update_at, err))?;
                        
                        sqlx::query("UPDATE contract_onchain_source_challenge SET state_code=?, update_at=?, process_id=NULL, block_number=?, tx_index=? WHERE order_id=? AND `index`=?")
                            .bind(SourceChallengeRow::state_code_prooved()).bind(update_at as sqlx_types::U64).bind(options.block_number as sqlx_types::U64)
                            .bind(options.tx_index as sqlx_types::U32).bind(row.order_id).bind(row.index)
                            .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update challenge, order={}, update_at={}, err={}", self, challenge.order_id, update_at, err))?;

                        sqlx::query("UPDATE contract_state SET onchain_challenge=NULL WHERE order_id=?")
                            .bind(row.order_id).execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update challenge, order={}, update_at={}, err={}", self, challenge.order_id, update_at, err))?;

                        trans.commit().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update challenge, order={}, update_at={}, err={}", self, challenge.order_id, update_at, err))?;

                        let contract = self.get_contract_by_id(options.order.order_id).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update challenge, order={}, update_at={}, err={}", self, challenge.order_id, update_at, err))?
                            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} update challenge, order={}, update_at={}, err={}", self, challenge.order_id, update_at, "contract not found"))?;

                        self.journal_client().append(JournalEvent { source_id: contract.source_id, order_id: Some(contract.order_id), event_type: JournalEventType::OnchainChallengeProoved, event_params: None }).await
                            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update challenge, order={}, update_at={}, err={}", self, challenge.order_id, update_at, err))?;

                        info!("{} update challenge, order={}, update_at={}, prooved", self, challenge.order_id, update_at);

                        Ok(())
                    } else if T::supported_challenge_type() == DmcChallengeTypeCode::MerklePath {
                        todo!()
                    } else {
                        unreachable!()
                    }
                },
                _ => {
                    // do nothing
                    Ok(())
                }
            }
        } else {
            info!("{} update contract, order={:?}, update_at={}", self, options, update_at);
            // order still waiting 
            match &options.order.state {
                DmcOrderState::Preparing {..} => {
                    // do nothing
                    Ok(())
                }, 
                DmcOrderState::Storing(..) => {
                    if sqlx::query("UPDATE contract_state SET state_code=?, process_id=NULL, last_challenge_at=?, block_number=?, tx_index=? WHERE order_id=? AND state_code=? AND (block_number<? OR (block_number=? AND tx_index<=?))")
                        .bind(ContractStateRow::state_code_storing()).bind(update_at as sqlx_types::U64).bind(options.block_number as sqlx_types::U64)
                        .bind(options.tx_index as sqlx_types::U32).bind(options.order.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_writing())
                        .bind(options.block_number as sqlx_types::U64).bind(options.block_number as sqlx_types::U64).bind(options.tx_index as sqlx_types::U32)
                        .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?
                        .rows_affected() > 0 {
                        let contract = self.get_contract_by_id(options.order.order_id).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?
                            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, "contract not found"))?;
                        let _ = self.journal_client().append(JournalEvent { source_id: contract.source_id, order_id: Some(contract.order_id), event_type: JournalEventType::OrderStored, event_params: None })
                            .await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;
                        info!("{} update contract storing, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                    } else {
                        info!("{} ignore update contract, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                    }
                    Ok(())
                }, 
                DmcOrderState::Canceled => {
                    let contract = self.get_contract_by_id(options.order.order_id).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;
                    if contract.is_none() {
                        info!("{} update contract, order={}, update_at={}, ignored", self, options.order.order_id, update_at);
                        return Ok(()); 
                    }
                    let contract = contract.unwrap();

                    let mut trans = self.sql_pool().begin().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;

                    if sqlx::query("UPDATE contract_state SET state_code=?, update_at=?, process_id=NULL, block_number=?, tx_index=? WHERE order_id=? AND (block_number<? OR (block_number=? AND tx_index<=?))")
                        .bind(ContractStateRow::state_code_canceled()).bind(update_at as sqlx_types::U64).bind(options.block_number as sqlx_types::U64)
                        .bind(options.tx_index as sqlx_types::U32).bind(options.order.order_id as sqlx_types::U64)
                        .bind(options.block_number as sqlx_types::U64).bind(options.block_number as sqlx_types::U64).bind(options.tx_index as sqlx_types::U32)
                        .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?
                        .rows_affected() > 0 {
                        if let Some(challenge) = contract.challenge {
                            if T::supported_challenge_type() == DmcChallengeTypeCode::SourceStub {
                                sqlx::query("UPDATE contract_onchain_source_challenge SET state_code=?, state_value=?, update_at=? WHERE order_id=? AND `index`=?")
                                    .bind(SourceChallengeRow::state_code_error()).bind("order canceled").bind(update_at as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64).bind(challenge.index as sqlx_types::U32)
                                    .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;
                            } else {
                                todo!()
                            }
                        };
                        trans.commit().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;
                        let _ = self.journal_client().append(JournalEvent { source_id: contract.source_id, order_id: Some(contract.order_id), event_type: JournalEventType::OrderCanceled, event_params: None })
                            .await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;
                        
                        info!("{} update contract canceled, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                    } else {
                        info!("{} ignore update contract, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                    }
                    Ok(())
                }
            }   
        }
    }

    pub async fn apply(&self, options: ApplyContractOptions) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} apply contract, contract={:?}, update_at={}", self, options, update_at);
        let order = self.chain_client().get_order_by_id(options.order_id).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err={}", self, options.order_id, update_at, err))?
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err=order not found", self, options.order_id, update_at))?;

        let miner_method = self.chain_client().get_apply_method(&order.miner).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err={}", self, options.order_id, update_at, err))?
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err=apply method not found", self, options.order_id, update_at))?;

        if sqlx::query("INSERT INTO contract_state (order_id, process_id, miner_host, source_id, state_code, update_at, block_number, tx_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(options.order_id as sqlx_types::U64).bind(std::process::id() as sqlx_types::U64).bind(&miner_method)
            .bind(options.source_id as sqlx_types::U64).bind(ContractStateRow::state_code_applying()).bind(update_at as sqlx_types::U64)
            .bind(options.block_number as sqlx_types::U64).bind(options.tx_index as sqlx_types::U32)
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err={}", self, options.order_id, update_at, err))?
            .rows_affected() == 0 {
            info!("{} apply contract, ignored, contract={:?}, update_at={}", self, options.order_id, update_at);
            return Ok(());
        } 

        let contract = Contract {
            order_id: options.order_id, 
            process_id: None, 
            update_at, 
            miner_endpoint: miner_method,
            source_id: options.source_id, 
            state: ContractState::Applying, 
            challenge: None
        };
        if self.apply_inner(contract.clone()).await.is_err() {
            let _ = sqlx::query("UPDATE contract_state SET process_id=NULL, update_at=? WHERE order_id=? AND state_code=? AND process_id=? AND update_at=?")
                    .bind(dmc_time_now() as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_applying())
                    .bind(std::process::id() as sqlx_types::U32).bind(contract.update_at as sqlx_types::U64)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} apply contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
        }

        Ok(())
    }

    pub async fn get(&self, filter: ContractFilter, navigator: ContractNavigator) -> DmcResult<Vec<Contract>> {
        debug!("{} get contract, filter={:?}, navigator={:?}", self, filter, navigator);
        if navigator.page_size == 0 {
            debug!("{} get contract returned, filter={:?}, navigator={:?}, results=0", self, filter, navigator);
            return Ok(vec![])
        }
        
        if let Some(order_id) = filter.order_id {
            let contracts = self.get_contract_by_id(order_id).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get contract, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?
                .map(|contract| vec![contract]).unwrap_or(vec![]);
            debug!("{} get contract returned, filter={:?}, navigator={:?}, results={}", self, filter, navigator, contracts.len());

            Ok(contracts)
        } else {
            unimplemented!()
        }
    }

    pub async fn challenge(&self, options: OnChainChallengeOptions) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} onchain challenge, options={:?}, update_at={}", self, options, update_at);

        let contract = self.get_contract_by_id(options.order_id).await
            .map_err(|err| dmc_err!(err.code(), "{} onchain challenge, options={:?}, update_at={}, err={}", self, options, update_at, err))?
            .ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} onchain challenge, options={:?}, update_at={}, err={}", self, options, update_at, "no contract"))?;

        if contract.state != ContractState::Storing {
            return Err(dmc_err!(DmcErrorCode::NotFound, "{} onchain challenge, options={:?}, update_at={}, err={}", self, options, update_at, "not storing"));
        }

        if contract.challenge.is_some() {
            return Err(dmc_err!(DmcErrorCode::NotFound, "{} onchain challenge, options={:?}, update_at={}, err={}", self, options, update_at, "existing challenge"));
        }

        if T::supported_challenge_type() == DmcChallengeTypeCode::SourceStub {
            let row: Option<SourceChallengeRow> = sqlx::query_as("SELECT * FROM contract_onchain_source_challenge WHERE order_id=? ORDER BY `index` DESC LIMIT 1")
                .bind(contract.order_id as sqlx_types::U64).fetch_optional(self.sql_pool()).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} onchain challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
            let index = if let Some(last_challenge) = row {
                last_challenge.index + 1
            } else {
                0
            };

            let mut challenge = OnChainChallenge {
                order_id: contract.order_id,
                index: index as u32,
                state: OnChainChallengeState::SourceStub(SourceChallengeState::Waiting),
                update_at
            };

            let mut trans = self.sql_pool().begin().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} onchain challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
            let query = if let Some(params) = options.challenge { 
                let stub = if let DmcChallengeParams::SourceStub { piece_index, nonce, hash} = params {
                    SourceChallengeStub {
                        piece_index, 
                        nonce, 
                        hash,  
                    }
                } else {
                    return Err(dmc_err!(DmcErrorCode::NotFound, "{} onchain challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, "invalid challenge type"));
                };
                
                let state_value = serde_json::to_vec(&stub).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} onchain challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
                challenge.state = OnChainChallengeState::SourceStub(SourceChallengeState::Created(stub));
                sqlx::query("INSERT INTO contract_onchain_source_challenge (order_id, `index`, state_code, state_value, update_at) SELECT ?, ?, ?, ?, ? FROM contract_state WHERE order_id=? AND state_code=? AND onchain_challenge IS NULL")
                    .bind(contract.order_id as sqlx_types::U64).bind(index as sqlx_types::U32).bind(SourceChallengeRow::state_code_created()).bind(state_value)
                    .bind(update_at as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
            } else {
                sqlx::query("INSERT INTO contract_onchain_source_challenge (order_id, `index`, state_code, update_at) SELECT ?, ?, ?, ? FROM contract_state WHERE order_id=? AND state_code=? AND onchain_challenge IS NULL")
                    .bind(contract.order_id as sqlx_types::U64).bind(index as sqlx_types::U32).bind(SourceChallengeRow::state_code_waiting())
                    .bind(update_at as sqlx_types::U64).bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
            };
            if query.execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} onchain challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
                .rows_affected() > 0 {
                if sqlx::query("UPDATE contract_state SET onchain_challenge=? WHERE order_id=? AND state_code=?")
                    .bind(index as sqlx_types::U32).bind(contract.order_id as sqlx_types::U64).bind(ContractStateRow::state_code_of(&contract.state))
                    .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} onchain challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
                    .rows_affected() > 0 {
                    trans.commit().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} onchain challenge, order={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
                    
                  
                    let mut contract = contract;
                    contract.challenge = Some(challenge);
                    let server = self.clone();
                    task::spawn(async move {
                        let _ = server.process_onchain_challenge(contract).await;
                    });
                    Ok(())
                } else {
                    info!("{} new challenge, order={}, update_at={}, ignored", self, contract.order_id, update_at);
                    Ok(())
                }
            } else {
                info!("{} new challenge, order={}, update_at={}, ignored", self, contract.order_id, update_at);
                Ok(())
            }
        } else if T::supported_challenge_type() == DmcChallengeTypeCode::MerklePath {
            todo!()
        } else {
            unimplemented!()
        }
    }
}