use log::*;
use sectors::ClearChunkNavigator;
use std::{
    sync::Arc,
    time::Duration,
    str::FromStr
};
use async_std::{task, stream::StreamExt};
use serde::{Serialize, Deserialize};
use sha2::{digest::FixedOutput, Digest, Sha256};
use sqlx::{ConnectOptions, Pool, Row};
use tide::{Request, Response, Body};
use dmc_tools_common::*;
use dmc_miner_journal::*;
use dmc_miner_sectors_client as sectors;
use crate::{
    types::*
};

#[derive(Serialize, Deserialize, Clone)]
pub struct ContractsHttpServerConfig {
    pub sql_url: String, 
    pub listen_address: String, 
    #[serde(flatten)]
    pub config: ContractsServerConfig
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ContractsServerConfig {
    pub prepare_retry_interval: Duration, 
    pub prepare_atomic_interval: Duration, 
    pub journal_client: JournalClientConfig, 
    pub sector_client: sectors::SectorClientConfig,
    #[serde(default)]
    pub prefix_table_name: bool
}

struct ServerImpl<T: DmcChainAccountClient> {
    config: ContractsServerConfig, 
    journal_client: JournalClient, 
    sector_client: sectors::SectorClient, 
    chain_client: T, 
    sql_pool: Pool<sqlx::MySql>,     
}



#[derive(Clone, sqlx::FromRow)]
pub struct ContractStateTableRow {
    pub order_id: u64, 
    pub bill_id: Option<u64>,
    pub update_at: u64, 
    pub state_code: u8, 
    pub state_value: Vec<u8>, 
    pub writen: u64, 
    pub process_id: Option<u32>, 
    pub block_number: u64, 
    pub tx_index: u32
}

impl ContractStateTableRow {
    fn state_code_unknown() -> u8 {
        0
    }

    fn state_code_applying() -> u8 {
        1
    }

    fn state_code_refused() -> u8 {
        2
    }

    fn state_code_writing() -> u8 {
        3
    }
   
    fn state_code_calculating() -> u8 {
        4
    } 

    fn state_code_calculated() -> u8 {
        5
    }

    fn state_code_prepare_error() -> u8 {
        7
    }

    fn state_code_storing() -> u8 {
        8
    }

    fn state_code_canceled() -> u8 {
        9
    }
}

#[derive(Clone, Debug, sqlx::FromRow)]
struct ContractSectorTableRow {
    order_id: u64,
    #[sqlx(flatten)]
    sector: ContractSector 
}

#[derive(Clone, sqlx::FromRow)]
struct ContractRow {
    #[sqlx(flatten)]
    state: ContractStateTableRow,
    #[sqlx(flatten)]
    sector: ContractSector 
}

impl TryInto<Contract> for ContractRow {
    type Error = DmcError;

    fn try_into(self) -> DmcResult<Contract> {
        let state = if self.state.state_code == ContractStateTableRow::state_code_unknown() {
            ContractState::Unknown
        } else if self.state.state_code == ContractStateTableRow::state_code_applying() {
            ContractState::Applying
        } else if self.state.state_code == ContractStateTableRow::state_code_refused() {
            ContractState::Refused
        } else if self.state.state_code == ContractStateTableRow::state_code_writing() {
            ContractState::Writing { sector: self.sector, writen: self.state.writen }
        } else if self.state.state_code == ContractStateTableRow::state_code_calculating() {
            ContractState::Calculating { sector: self.sector, writen: self.state.writen, calculated: 0 }
        } else if self.state.state_code == ContractStateTableRow::state_code_calculated() {
            let merkle = serde_json::from_slice(&self.state.state_value)
                .map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid merkle stub {}", err)))?;
            ContractState::Calculated { sector: self.sector, writen: self.state.writen, merkle }
        } else if self.state.state_code == ContractStateTableRow::state_code_prepare_error() {
            let error = String::from_utf8(self.state.state_value)
                .map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid prepare error {}", err)))?;
            ContractState::PrepareError(error)
        } else if self.state.state_code == ContractStateTableRow::state_code_storing() {
            let merkle = serde_json::from_slice(&self.state.state_value)
                .map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid merkle stub {}", err)))?;
            ContractState::Storing { sector: self.sector, writen: self.state.writen, merkle }
        } else if self.state.state_code == ContractStateTableRow::state_code_canceled() {
            ContractState::Canceled
        } else {
            return Err(DmcError::new(DmcErrorCode::InvalidData, format!("invalid state code {}", self.state.state_code)));
        };

        Ok(Contract {
            order_id: self.state.order_id, 
            update_at: self.state.update_at, 
            state,
            bill_id: self.state.bill_id,
        })
    }
}

#[derive(sqlx::FromRow)]
struct MerklePathStubRow {
    order_id: u64, 
    index: u32, 
    content: Vec<u8>
}



#[derive(sqlx::FromRow)]
struct OnChainChallengeRow {
    challenge_id: u64, 
    order_id: u64,
    challenge_params: Vec<u8>,
    state_code: u8,
    state_value: Option<Vec<u8>>,
    start_at: u64, 
    update_at: u64
}

impl OnChainChallengeRow {
    fn state_code_of(state: &OnChainChallengeState) -> u8 {
        use OnChainChallengeState::*;
        match state {
            Waiting => Self::state_code_waiting(),
            Calculating => Self::state_code_calculating(),
            Ready(_) => Self::state_code_ready(),
            Prooved => Self::state_code_prooved(),
            Expired => Self::state_code_expired(), 
            Error(_) => Self::state_code_error()
        }
    }

    fn state_code_waiting() -> u8 {
        0
    }

    fn state_code_calculating() -> u8 {
        1
    }

    fn state_code_ready() -> u8 {
        2
    }

    fn state_code_prooved() -> u8 {
        3
    }

    fn state_code_expired() -> u8 {
        4
    }

    fn state_code_error() -> u8 {
        10
    }
}

impl TryInto<OnChainChallenge> for OnChainChallengeRow {
    type Error = DmcError;
    fn try_into(self) -> DmcResult<OnChainChallenge> {
        let params = serde_json::from_slice(&self.challenge_params)
            .map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid params {}", err)))?;

        let state = if self.state_code == Self::state_code_waiting() {
            OnChainChallengeState::Waiting
        } else if self.state_code == Self::state_code_calculating() {
            OnChainChallengeState::Calculating
        } else if self.state_code == Self::state_code_ready() {
            let proof = serde_json::from_slice(&self.state_value.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no state value"))?)
                .map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid state value {}", err)))?;
            OnChainChallengeState::Ready(proof)
        } else if self.state_code == Self::state_code_prooved() {
            OnChainChallengeState::Prooved
        } else if self.state_code == Self::state_code_expired() {
            OnChainChallengeState::Expired
        } else if self.state_code == Self::state_code_error() {
            let error = String::from_utf8(self.state_value.ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no state value"))?)
                .map_err(|err| DmcError::new(DmcErrorCode::InvalidData, format!("invalid state value {}", err)))?;
            OnChainChallengeState::Error(error)
        } else {
            unreachable!()
        };

        Ok(OnChainChallenge {
            challenge_id: self.challenge_id, 
            order_id: self.order_id, 
            start_at: self.start_at, 
            update_at: self.update_at, 
            params,
            state
        })
    }
}


#[derive(Clone)]
pub struct ContractsServer<T: DmcChainAccountClient>(Arc<ServerImpl<T>>);

impl<T: DmcChainAccountClient> std::fmt::Display for ContractsServer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MinerContractsServer{{process_id={}}}", std::process::id())
    }
}
 
impl<T: DmcChainAccountClient> ContractsServer<T> {
    pub async fn start(&self) {
        loop {
            self.on_time_escape().await;
            async_std::task::sleep(self.config().prepare_atomic_interval).await;
        }
    }


    pub async fn listen(self, listen_address: String) -> DmcResult<()> {
        let server = self.clone();
        async_std::task::spawn(async move {
            server.start().await;
        });

        let mut http_server = tide::with_state(self);

        http_server.at("/contract").post(|mut req: Request<Self>| async move {
            let options = req.body_json().await?;
            let _ = req.state().update(options).await?;
            let resp = Response::new(200);
            Ok(resp)
        });

        http_server.at("/contract").get(|req: Request<Self>| async move {
            debug!("{} http get contract, url={}", req.state(), req.url());
            let mut query = ContractFilterAndNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "order_id" => {
                        query.filter.order_id = Some(u64::from_str_radix(&*value, 10)?);
                    },
                    "bill_id" => {
                        query.filter.bill_id = Some(u64::from_str_radix(&*value, 10)?);
                    },
                    "raw_sector_id" => {
                        query.filter.raw_sector_id = Some(u64::from_str_radix(&*value, 10)?);
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
            // let query = req.query ::<ContractFilterAndNavigator>()
            //     .map_err(|err| dmc_err!(DmcErrorCode::InvalidParam, "{} http get contract, url={}, err=query params {}", req.state(), req.url(), err))?;
            let mut resp = Response::new(200);
            let contracts = req.state().get(query.filter, query.navigator).await?;
            resp.set_body(Body::from_json(&contracts)?);
            Ok(resp)
        });

        http_server.at("/contract/occupy").get(|req: Request<Self>| async move {
            debug!("{} http get occupy, url={}", req.state(), req.url());
            let mut query: SectorOccupyOptions = SectorOccupyOptions::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "raw_sector_id" => {
                        query.raw_sector_id = Some(u64::from_str_radix(&*value, 10)?);
                    },
                    "bill_id" => {
                        query.bill_id = Some(u64::from_str_radix(&*value, 10)?);
                    }
                    _ => {}
                }
            }
            
            let mut resp = Response::new(200);
            let occupy = req.state().occupy(query).await?;
            resp.set_body(Body::from_json(&occupy)?);
            Ok(resp)
        });

        http_server.at("/contract/data").post(|mut req: Request<Self>| async move {
            debug!("{} http post contract data, url={}", req.state(), req.url());
            let mut query = ContractDataNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "order_id" => {
                        query.order_id = u64::from_str_radix(&*value, 10)?;
                    },
                    "offset" => {
                        query.offset = u64::from_str_radix(&*value, 10)?;
                    },
                    _ => {}
                }
            }
            let mut resp = Response::new(200);
            let body = req.take_body();
            query.len = body.len().ok_or_else(|| DmcError::new(DmcErrorCode::InvalidInput, "no length set"))? as u64;

            let sign = req.header("user-signature").and_then(|header| header.get(0))
                .ok_or_else(|| DmcError::new(DmcErrorCode::InvalidInput, "no signature"))?.as_str();

            let contract = req.state().write(body.into_reader(), &query, sign).await.map_err(|e| {
                error!("write order {}, len {}, offset {}, err {}", query.order_id, query.len, query.offset, e);
                e
            })?;
            resp.set_body(Body::from_json(&contract)?);
            Ok(resp)
        });

        http_server.at("/contract/data").get(|req: Request<Self>| async move {
            debug!("{} http get contract data, url={}", req.state(), req.url());
            let mut query = ContractDataNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "order_id" => {
                        query.order_id = u64::from_str_radix(&*value, 10)?;
                    },
                    "offset" => {
                        query.offset = u64::from_str_radix(&*value, 10)?;
                    }, 
                    "len" => {
                        query.len = u64::from_str_radix(&*value, 10)?;
                    }
                    _ => {}
                }
            }
            let mut resp = Response::new(200);
            // let total = query.len;
            let reader = async_std::io::BufReader::new(req.state().read(query).await?);
            resp.set_body(Body::from_reader(reader, None));
            Ok(resp)
        });

        let _ = http_server.listen(listen_address.as_str()).await?;

        Ok(())
    }

    pub fn with_sql_pool(chain_client: T, sql_pool: sqlx::MySqlPool, config: ContractsServerConfig) -> DmcResult<Self> {
        let journal_client = JournalClient::new(config.journal_client.clone())?;
        let sector_client = sectors::SectorClient::new(config.sector_client.clone())?;
        Ok(Self(Arc::new(ServerImpl {
            chain_client, 
            sql_pool,
            journal_client, 
            sector_client, 
            config: config.clone(), 
        })))
    }

    pub async fn with_sql_url(chain_client: T, sql_url: String, config: ContractsServerConfig) -> DmcResult<Self> {
        let mut options = sqlx::mysql::MySqlConnectOptions::from_str(&sql_url)?;
        options.log_statements(LevelFilter::Off);
        let sql_pool = sqlx::mysql::MySqlPoolOptions::new().connect_with(options).await?;

        Self::with_sql_pool(chain_client, sql_pool, config)
    }

    pub async fn init(&self) -> DmcResult<()> {
        let _ = sqlx::query(&self.sql_create_table_contract_sector()).execute(self.sql_pool()).await?;
        let _ = sqlx::query(&self.sql_create_table_contract_state()).execute(self.sql_pool()).await?;
        let _ = sqlx::query(&self.sql_create_table_merkle_stub()).execute(self.sql_pool()).await?;
        let _ = sqlx::query(&self.sql_create_table_onchain_challenge()).execute(self.sql_pool()).await?;
        
        for sql in self.sql_update_table_contract_state() {
            let _ = sqlx::query(&sql).execute(self.sql_pool()).await;
        }
        Ok(())
    }

    pub async fn reset(self) -> DmcResult<Self> {
        self.init().await?;
        let _ = sqlx::query(&format!("DELETE FROM {} WHERE order_id > 0", self.sql_table_name("contract_state"))).execute(self.sql_pool()).await?;
        let _ = sqlx::query(&format!("DELETE FROM {} WHERE order_id > 0;", self.sql_table_name("contract_sector"))).execute(self.sql_pool()).await?;
        let _ = sqlx::query(&format!("DELETE FROM {} WHERE order_id > 0;", self.sql_table_name("contract_stub"))).execute(self.sql_pool()).await?;
        let _ = sqlx::query(&format!("DELETE FROM {} WHERE challenge_id > 0;", self.sql_table_name("contract_onchain_challenge"))).execute(self.sql_pool()).await?;
        Ok(self)
    }
}

impl<T: DmcChainAccountClient> ContractsServer<T> {
    fn sql_pool(&self) -> &sqlx::Pool<sqlx::MySql> {
        &self.0.sql_pool
    }

    fn journal_client(&self) -> &JournalClient {
        &self.0.journal_client
    }

    fn sector_client(&self) -> &sectors::SectorClient {
        &self.0.sector_client
    }

    pub fn chain_client(&self) -> &T {
        &self.0.chain_client
    }

    fn config(&self) -> &ContractsServerConfig {
        &self.0.config
    }

    fn sql_table_name(&self, name: &str) -> String {
        if self.config().prefix_table_name {
            format!("{}_{}", self.chain_client().account(), name)
        } else {
            name.to_owned()
        }
    }

    fn sql_create_table_contract_state(&self) -> String {
        format!(r#"CREATE TABLE IF NOT EXISTS `{}` (
            `order_id` BIGINT UNSIGNED NOT NULL PRIMARY KEY, 
            `state_code` TINYINT UNSIGNED NOT NULL,  
            `state_value` BLOB, 
            `process_id` INT UNSIGNED DEFAULT NULL, 
            `writen` BIGINT UNSIGNED DEFAULT 0, 
            `update_at` BIGINT UNSIGNED NOT NULL, 
            `block_number` BIGINT UNSIGNED NOT NULL, 
            `tx_index` INT UNSIGNED NOT NULL, 
            `bill_id` BIGINT UNSIGNED NOT NULL, 
            INDEX (`update_at`), 
            INDEX (`process_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#, self.sql_table_name("contract_state"))
    }

    fn sql_create_table_contract_sector(&self) -> String {
        format!(r#"CREATE TABLE IF NOT EXISTS `{}` (
            `sector_id` BIGINT UNSIGNED NOT NULL, 
            `sector_offset_start` BIGINT UNSIGNED NOT NULL,
            `sector_offset_end` BIGINT UNSIGNED NOT NULL, 
            `order_id` BIGINT UNSIGNED NOT NULL UNIQUE,  
            `chunk_size` INT UNSIGNED NOT NULL, 
            PRIMARY KEY (`sector_id`, `sector_offset_start`, `sector_offset_end`), 
            INDEX (`order_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#, self.sql_table_name("contract_sector"))
    }

    fn sql_create_table_merkle_stub(&self) -> String {
        format!(r#"CREATE TABLE IF NOT EXISTS `{}` (
            `order_id` BIGINT UNSIGNED NOT NULL, 
            `index` INT UNSIGNED NOT NULL, 
            `content` BLOB, 
            INDEX (`order_id`),
            UNIQUE INDEX `source_index` (`order_id`, `index`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#, self.sql_table_name("contract_stub"))
    }

    fn sql_create_table_onchain_challenge(&self) -> String {
        format!(r#"CREATE TABLE IF NOT EXISTS `{}` (
            `challenge_id` BIGINT UNSIGNED NOT NULL PRIMARY KEY, 
            `order_id` BIGINT UNSIGNED NOT NULL,
            `challenge_params` BLOB NOT NULL,
            `state_code` TINYINT UNSIGNED NOT NULL,
            `state_value` BLOB,
            `start_at` BIGINT UNSIGNED NOT NULL,
            `update_at` BIGINT UNSIGNED NOT NULL,
            `process_id` INT UNSIGNED DEFAULT NULL,
            `block_number` BIGINT UNSIGNED DEFAULT 0, 
            `tx_index` INT UNSIGNED DEFAULT 0, 
            INDEX (`order_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#, self.sql_table_name("contract_onchain_challenge"))
    }

    fn sql_update_table_contract_state(&self) -> Vec<String> {
        vec![
            format!(r#"ALTER TABLE `{}` ADD COLUMN `bill_id` BIGINT UNSIGNED NOT NULL"#, self.sql_table_name("contract_state")), 
            format!(r#"DROP INDEX `chain_index` ON `{}`"#, self.sql_table_name("contract_state")), 
        ]
    }
}

impl<T: DmcChainAccountClient> ContractsServer<T> {
    async fn prepare_inner(&self, contract: Contract) -> DmcResult<()> {
        info!("{} prepare, contract={:?}, update_at={}", self, contract, contract.update_at);

        match &contract.state {
            ContractState::Calculated {
                merkle, 
                sector,
                ..
            } => {
                let result = self.chain_client().prepare_order(DmcPrepareOrderOptions { order_id: contract.order_id, merkle_stub: merkle.clone() }).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare, contract={}, update_at={}, err={}", self, contract.order_id, contract.update_at, err))?;
                let update_at = dmc_time_now();
                let update_storing_sql = format!("UPDATE {} SET process_id=NULL, state_code=?, update_at=? WHERE order_id=? AND state_code=?", self.sql_table_name("contract_state"));
                let update_error_sql = format!("UPDATE {} SET process_id=NULL, state_code=?, state_value=?, update_at=? WHERE order_id=? AND state_code=?", self.sql_table_name("contract_state"));
                let (qeury, journal) = match &result {
                    Ok(_) => {
                        let query = sqlx::query(&update_storing_sql)
                            .bind(ContractStateTableRow::state_code_storing()).bind(update_at).bind(contract.order_id).bind(ContractStateTableRow::state_code_calculated());
                        let journal = self.journal_client().append(JournalEvent { sector_id: sector.sector_id, order_id: Some(contract.order_id), event_type: JournalEventType::OrderStored, event_params: None });
                        (query, journal)
                    }, 
                    Err(err) => {
                        let query = sqlx::query(&update_error_sql)
                            .bind(ContractStateTableRow::state_code_prepare_error()).bind(err.msg().as_bytes().to_vec()).bind(update_at).bind(contract.order_id).bind(ContractStateTableRow::state_code_calculated());
                        let journal = self.journal_client().append(JournalEvent { sector_id: sector.sector_id, order_id: Some(contract.order_id), event_type: JournalEventType::OrderFailed, event_params: Some(err.msg().to_owned()) });
                        (query, journal)
                    }
                };
        
                if qeury.execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} sync prepare result, contract={}, update_at={}, result={:?}, err={}", self, contract.order_id, contract.update_at, result, err))?
                    .rows_affected() > 0 {
                    let _ = journal.await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} sync prepare result, contract={}, update_at={}, result={:?}, err={}", self, contract.order_id, contract.update_at, result, err))?;
                    info!("{} sync prepare result, contract={}, update_at={}, storing", self, contract.order_id, contract.update_at);
                } else {
                    info!("{} sync prepare result, contract={}, update_at={}, ignored", self, contract.order_id, contract.update_at);
                }
        
                Ok(())
                
            },
            _ => {
                Err(dmc_err!(DmcErrorCode::ErrorState, "{} create pending prepare, contract={}, update_at={}, err={}", self, contract.order_id, contract.update_at, "not calculated"))
            }
        }
    }

    async fn calculate_merkle_inner(&self, contract: Contract) -> DmcResult<()> {
        let update_at: u64 = contract.update_at;
        info!("{} calc contract, contract={:?}, update_at={}", self, contract, update_at);

        match &contract.state {
            ContractState::Calculating { sector, writen, .. } => {
                let order = self.chain_client().get_order_by_id(contract.order_id).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?
                    .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} calc contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, "order not found"))?;
                if let DmcOrderState::Preparing { user, .. } = order.state {
                    if let Some(user_merkle_stub) = user {
                        let stub_proc = MerkleStubProc::new(*writen, user_merkle_stub.piece_size, true);
                        let query_sql = format!("SELECT * FROM {} WHERE order_id=? ORDER BY `index`", self.sql_table_name("contract_stub"));
                        let mut row_stream = sqlx::query_as::<_, MerklePathStubRow>(&query_sql)
                            .bind(contract.order_id).fetch(self.sql_pool());

                        let mut stubs = vec![];
                        loop {
                            if let Some(stub) = row_stream.next().await {
                                let stub: MerklePathStubRow = stub.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
                                stubs.push(HashValue::try_from(stub.content).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?);
                            } else {
                                break;
                            }
                        }

                        if stubs.len() < stub_proc.stub_count() {
                            use std::io::SeekFrom;
                            use async_std::io::prelude::*;

                            let sector_reader = sectors::SectorReader::from_sector_id(self.sector_client().clone(), sector.sector_id, sector.sector_offset_start, *writen).await
                                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;

                            let mut sector_reader = stub_proc.wrap_reader(sector_reader);
                            sector_reader.seek(SeekFrom::Start(stub_proc.stub_chunk_size() as u64 * stubs.len() as u64)).await
                                .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} calc contract, contract={}, update_at={}, err=read sector {}", self, contract.order_id, update_at, err))?;
                            
                            for i in stubs.len()..stub_proc.stub_count() {
                                let content = stub_proc.calc_path_stub::<_, MerkleStubSha256>(i, &mut sector_reader).await
                                    .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} calc contract, contract={}, update_at={}, err=read sector {}", self, contract.order_id, update_at, err))?;
                                
                                sqlx::query(&format!("INSERT INTO {} (order_id, `index`, content) VALUES (?, ?, ?)", self.sql_table_name("contract_stub")))
                                    .bind(contract.order_id).bind(i as u32).bind(content.as_slice())
                                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc contract, contract={:?}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
                                
                                stubs.push(content);
                            }
                        }
                        
                        let root = stub_proc.calc_root_from_path_stub::<MerkleStubSha256>(stubs)
                            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc contract, contract={:?}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
                        
                        let merkle_stub = DmcMerkleStub {
                            leaves: stub_proc.leaves(), 
                            piece_size: stub_proc.piece_size(), 
                            root
                        };
                        
                        info!("{} calc contract, contract={}, update_at={}, calc merkle stub={:?}", self, contract.order_id, update_at, merkle_stub);

                        let merkle_stub_blob = serde_json::to_vec(&merkle_stub)
                            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc contract, contract={}, update_at={}, err=calc merkle stub {}", self, contract.order_id, update_at, err))?;

                        let update_at = dmc_time_now();
                        if sqlx::query(&format!("UPDATE {} SET state_code=?, update_at=?, state_value=?, process_id=NULL WHERE order_id=? AND process_id=? AND update_at=? AND state_code=?", self.sql_table_name("contract_state")))
                            .bind(ContractStateTableRow::state_code_calculated()).bind(update_at).bind(merkle_stub_blob)
                            .bind(contract.order_id).bind(std::process::id()).bind(contract.update_at).bind(ContractStateTableRow::state_code_calculating())
                            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc contract, source={}, update_at={}, err={}", self, contract.order_id, contract.update_at, err))?
                            .rows_affected() > 0 {
                            let mut contract = contract.clone();
                            contract.update_at = update_at;
                            contract.state = ContractState::Calculated {
                                merkle: merkle_stub, 
                                sector: sector.clone(),
                                writen: *writen
                            };
                            info!("{} calc contract, contract={}, update_at={}, finish", self, contract.order_id, contract.update_at);
                            let server = self.clone();
                            task::spawn(async move {
                                let _ = server.prepare_contract(contract).await;
                            });
                        } else {
                            info!("{} calc contract, contract={}, update_at={}, ignored", self, contract.order_id, contract.update_at);
                        }
                        Ok(())
                    } else {
                        Err(dmc_err!(DmcErrorCode::ErrorState, "{} calc contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, "user side not prepared"))
                    }
                } else {
                    Err(dmc_err!(DmcErrorCode::ErrorState, "{} calc contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, "user side not prepared"))
                }
            },
            _ => {
                Err(dmc_err!(DmcErrorCode::ErrorState, "{} calc contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, "not preparing"))
            }
        }
    }

    #[async_recursion::async_recursion]
    async fn prepare_contract(&self, contract: Contract) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} prepare contract, contract={:?}, update_at={}", self, contract, update_at);
        let state_code = match &contract.state {
            ContractState::Calculating { .. } => {
                ContractStateTableRow::state_code_calculating()
            }, 
            ContractState::Calculated { .. } => {
                ContractStateTableRow::state_code_calculated()
            },
            _ => {
                unreachable!()
            }
        };

        if sqlx::query(&format!("UPDATE {} SET process_id=?, update_at=? WHERE order_id=? AND state_code=? AND update_at=? AND (process_id IS NULL OR process_id!=?)", self.sql_table_name("contract_state")))
            .bind(std::process::id()).bind(update_at).bind(contract.order_id)
            .bind(state_code).bind(contract.update_at).bind(std::process::id())
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, contract.update_at, err))?
            .rows_affected() > 0 {
            let mut contract = contract;
            contract.update_at = update_at;
            let result = if state_code == ContractStateTableRow::state_code_calculating() {
                self.calculate_merkle_inner(contract.clone()).await
            } else if state_code == ContractStateTableRow::state_code_calculated() {
                self.prepare_inner(contract.clone(), ).await
            } else {
                unreachable!()
            };

            if result.is_err() {
                let _ = sqlx::query(&format!("UPDATE {} SET process_id=NULL, update_at=? WHERE order_id=? AND state_code=? AND update_at=?", self.sql_table_name("contract_state")))
                    .bind(dmc_time_now()).bind(contract.order_id).bind(state_code).bind(contract.update_at)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare contract, contract={}, update_at={}, err={}", self, contract.order_id, update_at, err))?;
            }
        } else {
            info!("{} prepare contract, contract={}, update_at={}, ignored", self, contract.order_id, update_at);
        }
        Ok(())
    }

    async fn commit_onchain_proof_inner(&self, challenge: OnChainChallenge) -> DmcResult<()> {
        info!("{} commit on chain proof, challenge={:?}", self, challenge);
        match &challenge.state {
            OnChainChallengeState::Ready(proof) => {
                let _ = self.chain_client().proof(DmcProofOptions {
                    order_id: challenge.order_id,  
                    challenge_id: challenge.challenge_id, 
                    proof: proof.clone()
                }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} commit on chain proof, challenge={}, err={}", self, challenge.challenge_id, err))?;

                info!("{} commit on chain proof, challenge={:?}, commited", self, challenge.challenge_id);

                if sqlx::query(&format!("UPDATE {} SET state_code=?, state_value=NULL, process_id=NULL, update_at=? WHERE challenge_id=? AND update_at=? AND state_code=?", self.sql_table_name("contract_onchain_challenge")))
                    .bind(OnChainChallengeRow::state_code_prooved()).bind(dmc_time_now())
                    .bind(challenge.challenge_id).bind(challenge.update_at).bind(OnChainChallengeRow::state_code_ready())
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} commit on chain proof, challenge={}, err={}", self, challenge.challenge_id, err))?
                    .rows_affected() > 0 {
                    info!("{} commit on chain proof, challenge={:?}, success", self, challenge.challenge_id);
                } else {
                    info!("{} commit on chain proof, challenge={:?}, ignored", self, challenge.challenge_id);
                }
                Ok(())
            },
            _ => unreachable!()
        }
    }

    async fn calc_onchain_source_challenge_inner(&self, contract: Contract, challenge: OnChainChallenge) -> DmcResult<()> {
        info!("{} calc on chain source challenge, challenge={:?}", self, challenge);
        
        if let ContractState::Storing { merkle, writen, .. } = &contract.state {
            if let DmcChallengeParams::SourceStub { piece_index, .. } = &challenge.params {
                let proof_proc = MerkleStubProc::new(*writen, merkle.piece_size, true);
                
                let stub_rows = sqlx::query_as::<_, MerklePathStubRow>(&format!("SELECT * FROM {} WHERE order_id=? ORDER BY `index`", self.sql_table_name("contract_stub")))
                    .bind(contract.order_id).fetch_all(self.sql_pool()).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc on chain source challenge, challenge={}, err={}", self, challenge.challenge_id, err))?;

                let stubs = stub_rows.into_iter().map(|row| HashValue::try_from(row.content).unwrap()).collect();
                
                let reader = self.read(ContractDataNavigator { order_id: contract.order_id, offset: 0, len: *writen }).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc on chain source challenge, challenge={}, err={}", self, challenge.challenge_id, err))?;

                let mut reader = proof_proc.wrap_reader(reader);

                let proof = proof_proc.proof_root::<_, MerkleStubSha256>(*piece_index, &mut reader, Some(stubs), None).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc on chain source challenge, challenge={}, err={}", self, challenge.challenge_id, err))?;
                
                let proof = DmcTypedProof::MerklePath { data: DmcData::from(proof.piece_content), pathes: proof.pathes };
                let update_at = dmc_time_now();
                if sqlx::query(&format!("UPDATE {} SET state_code=?, state_value=?, process_id=NULL, update_at=? WHERE challenge_id=? AND update_at=? AND state_code=?", self.sql_table_name("contract_onchain_challenge")))
                    .bind(OnChainChallengeRow::state_code_ready()).bind(serde_json::to_vec(&proof).unwrap()).bind(update_at)
                    .bind(challenge.challenge_id).bind(challenge.update_at).bind(OnChainChallengeRow::state_code_calculating())
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} calc on chain source challenge, challenge={}, err={}", self, challenge.challenge_id, err))?
                    .rows_affected() > 0 {
                    info!("{} calc on chain source challenge, challenge={:?}, ready", self, challenge.challenge_id);
                    let mut challenge = challenge.clone();
                    challenge.update_at = update_at;
                    challenge.state = OnChainChallengeState::Ready(proof);
                    let server = self.clone();
                    task::spawn(async move {
                        let _ = server.process_onchain_challenge(challenge).await;
                    });
                } else {
                    info!("{} proof on chain source challenge, challenge={:?}, ignored", self, challenge.challenge_id);
                }
                Ok(())
            } else {
                unreachable!()
            } 
        } else {
            unreachable!()
        }
    }

    async fn proof_onchain_source_challenge_inner(&self, contract: Contract, challenge: OnChainChallenge) -> DmcResult<()> {
        info!("{} proof on chain source challenge, challenge={:?}", self, challenge);
        if let ContractState::Storing { merkle, .. } = &contract.state {
            if let DmcChallengeParams::SourceStub { piece_index, nonce, hash } = &challenge.params {
                let mut piece = vec![0u8; merkle.piece_size as usize];
                let reader = self.read(ContractDataNavigator { order_id: contract.order_id, offset: *piece_index * merkle.piece_size as u64, len: merkle.piece_size as u64 }).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} proof on chain source challenge, challenge={}, err={}", self, challenge.challenge_id, err))?;
                let _ = async_std::io::copy(reader, async_std::io::Cursor::new(piece.as_mut_slice())).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} proof on chain source challenge, challenge={}, err={}", self, challenge.challenge_id, err))?;
                
                let mut hasher = Sha256::new();
                hasher.update(piece.as_slice());
                hasher.update(nonce.as_bytes());
                let source_hash: HashValue = HashValue::from(hasher.finalize_fixed());

                let mut hasher = Sha256::new();
                hasher.update(source_hash.as_slice());
                let proof: HashValue = HashValue::from(hasher.finalize_fixed());

                if proof.eq(hash) {
                    let proof = DmcTypedProof::SourceStub { hash: source_hash };
                    let update_at = dmc_time_now();
                    if sqlx::query(&format!("UPDATE {} SET state_code=?, state_value=?, process_id=NULL, update_at=? WHERE challenge_id=? AND update_at=? AND state_code=?", self.sql_table_name("contract_onchain_challenge")))
                        .bind(OnChainChallengeRow::state_code_ready()).bind(serde_json::to_vec(&proof).unwrap()).bind(update_at)
                        .bind(challenge.challenge_id).bind(challenge.update_at).bind(OnChainChallengeRow::state_code_waiting())
                        .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} proof on chain source challenge, challenge={}, err={}", self, challenge.challenge_id, err))?
                        .rows_affected() > 0 {
                        info!("{} proof on chain source challenge, challenge={:?}, ready", self, challenge.challenge_id);
                        let mut challenge = challenge.clone();
                        challenge.update_at = update_at;
                        challenge.state = OnChainChallengeState::Ready(proof);
                        let server = self.clone();
                        task::spawn(async move {
                            let _ = server.process_onchain_challenge(challenge).await;
                        });
                    } else {
                        info!("{} proof on chain source challenge, challenge={:?}, ignored", self, challenge.challenge_id);
                    }
                    Ok(())
                } else {
                    let update_at = dmc_time_now();
                    if sqlx::query(&format!("UPDATE {} SET state_code=?, process_id=NULL, update_at=? WHERE challenge_id=? AND update_at=? AND state_code=?", self.sql_table_name("contract_onchain_challenge")))
                        .bind(OnChainChallengeRow::state_code_calculating()).bind(update_at)
                        .bind(challenge.challenge_id).bind(challenge.update_at).bind(OnChainChallengeRow::state_code_waiting())
                        .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} proof on chain source challenge, challenge={}, err={}", self, challenge.challenge_id, err))?
                        .rows_affected() > 0 {
                        info!("{} proof on chain source challenge, challenge={:?}, calculating", self, challenge.challenge_id);
                        let mut challenge = challenge.clone();
                        challenge.update_at = update_at;
                        challenge.state = OnChainChallengeState::Calculating;
                        let server = self.clone();
                        task::spawn(async move {
                            let _ = server.process_onchain_challenge(challenge).await;
                        });
                    } else {
                        info!("{} proof on chain source challenge, challenge={:?}, ignored", self, challenge.challenge_id);
                    }
                    Ok(())
                }
            } else {
                unreachable!()
            } 
        } else {
            unreachable!()
        }
    }

    #[async_recursion::async_recursion]
    async fn process_onchain_challenge(&self, challenge: OnChainChallenge) -> DmcResult<()> {
        info!("{} process on chain challenge, challenge={:?}", self, challenge);
        let update_at = dmc_time_now();
        let contract = self.query_contract(challenge.order_id).await
            .map_err(|err|  dmc_err!(DmcErrorCode::Failed, "{} process on chain challenge, challenge={}, err={}", self, challenge.challenge_id, err))?
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} process on chain challenge, challenge={}, err={}", self, challenge.challenge_id, "contract not found"))?;
        if sqlx::query(&format!("UPDATE {} SET process_id=?, update_at=? WHERE challenge_id=? AND state_code=? AND update_at=? AND (process_id IS NULL OR process_id!=?)", self.sql_table_name("contract_onchain_challenge")))
            .bind(std::process::id()).bind(update_at).bind(challenge.challenge_id).bind(OnChainChallengeRow::state_code_of(&challenge.state)).bind(challenge.update_at).bind(std::process::id())
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} process on chain challenge, challenge={}, err={}", self, challenge.challenge_id, err))?
            .rows_affected() > 0 {
            let result = match &challenge.state {
                OnChainChallengeState::Waiting => {
                    let mut challenge = challenge.clone();
                    challenge.update_at = update_at;
                    if let ContractState::Storing { .. } = &contract.state {
                        match &challenge.params {
                            DmcChallengeParams::SourceStub { .. } => self.proof_onchain_source_challenge_inner(contract, challenge).await,
                            DmcChallengeParams::MerklePath { .. } => todo!(), 
                        } 
                    } else {
                        Ok(())
                    }
                }, 
                OnChainChallengeState::Calculating => {
                    let mut challenge = challenge.clone();
                    challenge.update_at = update_at;
                    if let ContractState::Storing { .. } = &contract.state {
                        match &challenge.params {
                            DmcChallengeParams::SourceStub { .. } => self.calc_onchain_source_challenge_inner(contract, challenge).await,
                            DmcChallengeParams::MerklePath { .. } => todo!(), 
                        } 
                    } else {
                        Ok(())
                    }
                },
                OnChainChallengeState::Ready(_) => {
                    let mut challenge = challenge.clone();
                    challenge.update_at = update_at;
                    self.commit_onchain_proof_inner(challenge).await
                },
                _ => {
                    Ok(())
                }
            };

            if result.is_err() {
                let _ = sqlx::query(&format!("UPDATE {} SET process_id=NULL, update_at=? WHERE challenge_id=? AND state_code=? AND update_at=?", self.sql_table_name("contract_onchain_challenge")))
                    .bind(dmc_time_now()).bind(challenge.challenge_id).bind(OnChainChallengeRow::state_code_of(&challenge.state)).bind(challenge.update_at)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} process on chain challenge, challenge={}, err={}", self, challenge.challenge_id, err))?;
            }
            Ok(())
        } else {
            info!("{} process on chain challenge, challenge={:?}, ignored", self, challenge);
            Ok(())
        }
    }

    pub async fn is_contract_applied(&self, order_id: u64) -> DmcResult<bool> {
        Ok(sqlx::query(&format!("SELECT order_id FROM {} WHERE order_id=? ", self.sql_table_name("contract_state")))
            .bind(order_id).fetch_optional(self.sql_pool()).await?.is_some())
    }


    pub async fn query_contract(&self, order_id: u64) -> DmcResult<Option<Contract>> {
        let row: Option<ContractRow> = sqlx::query_as::<_, ContractRow>(&format!("SELECT * FROM {} AS state RIGHT JOIN {} AS sector ON state.order_id=sector.order_id WHERE state.order_id=?", self.sql_table_name("contract_state"), self.sql_table_name("contract_sector")))
            .bind(order_id).fetch_optional(self.sql_pool()).await?;
        if let Some(row) = row {
            let mut contract: Contract = row.try_into()?;
            if let ContractState::Calculating { calculated, .. } = &mut contract.state {
                let last_row = sqlx::query_as::<_, MerklePathStubRow>(&format!("SELECT * FROM {} WHERE order_id=? ORDER BY `index` DESC LIMIT 1", self.sql_table_name("contract_stub"))).bind(order_id).fetch_optional(self.sql_pool()).await?;
                *calculated = if let Some(row) = last_row {
                    row.index + 1
                } else {
                    0
                }
            }
            Ok(Some(contract))
        } else {
            Ok(None)
        }
    }

    async fn query_contract_by_bill(&self, bill_id: u64) -> DmcResult<Vec<Contract>> {
        let rows: Vec<ContractRow> = sqlx::query_as::<_, ContractRow>(&format!("SELECT * FROM {} AS state RIGHT JOIN {} AS sector ON state.order_id=sector.order_id WHERE state.bill_id=?", self.sql_table_name("contract_state"), self.sql_table_name("contract_sector")))
            .bind(bill_id).fetch_all(self.sql_pool()).await?;

        let mut contracts = vec![];
        for row in rows {
            let order_id = row.state.order_id;
            let mut contract: Contract = row.try_into()?;
            if let ContractState::Calculating { calculated, .. } = &mut contract.state {
                let last_row = sqlx::query_as::<_, MerklePathStubRow>(&format!("SELECT * FROM {} WHERE order_id=? ORDER BY `index` DESC LIMIT 1", self.sql_table_name("contract_stub"))).bind(order_id).fetch_optional(self.sql_pool()).await?;
                *calculated = if let Some(row) = last_row {
                    row.index + 1
                } else {
                    0
                }
            }
            contracts.push(contract);
        }

        Ok(contracts)
    }

    async fn query_contract_by_raw_sector(&self, raw_sector_id: u64) -> DmcResult<Vec<Contract>> {
        let rows: Vec<ContractRow> = sqlx::query_as::<_, ContractRow>(&format!("SELECT * FROM {} AS state RIGHT JOIN {} AS sector ON state.order_id=sector.order_id WHERE sector.sector_id=?", self.sql_table_name("contract_state"), self.sql_table_name("contract_sector")))
            .bind(raw_sector_id).fetch_all(self.sql_pool()).await?;

        let mut contracts = vec![];
        for row in rows {
            let order_id = row.state.order_id;
            let mut contract: Contract = row.try_into()?;
            if let ContractState::Calculating { calculated, .. } = &mut contract.state {
                let last_row = sqlx::query_as::<_, MerklePathStubRow>(&format!("SELECT * FROM {} WHERE order_id=? ORDER BY `index` DESC LIMIT 1", self.sql_table_name("contract_stub"))).bind(order_id).fetch_optional(self.sql_pool()).await?;
                *calculated = if let Some(row) = last_row {
                    row.index + 1
                } else {
                    0
                }
            }
            contracts.push(contract);
        }

        Ok(contracts)
    }

    async fn on_time_escape(&self) {
        let now = dmc_time_now();
        info!("{} on time escape, when={}", self, now);

        let prepare_expired = now - self.config().prepare_retry_interval.as_micros() as u64;
        {
            let query_sql = format!("SELECT * FROM {} AS state RIGHT JOIN {} AS sector ON state.order_id=sector.order_id WHERE state.state_code>? AND state.state_code<? AND  state.update_at<? AND (state.process_id IS NULL OR state.process_id!=?)", self.sql_table_name("contract_state"), self.sql_table_name("contract_sector"));
            let mut stream = sqlx::query_as::<_, ContractRow>(&query_sql)
            .bind(ContractStateTableRow::state_code_writing()).bind(ContractStateTableRow::state_code_prepare_error()).bind(prepare_expired).bind(std::process::id()).fetch(self.sql_pool());
            loop {
                let result = stream.next().await;
                if result.is_none() {
                    debug!("{} on time escape, when={}, finish retry prepare", self, now);
                    break;
                }
                let result = result.unwrap();
                match result {
                    Ok(row) => {
                        let contract: DmcResult<Contract> = row.try_into();
                        match contract {
                            Ok(contract) => {
                                let server = self.clone();
                                async_std::task::spawn(async move {
                                    let _ = server.prepare_contract(contract).await;
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

impl<T: DmcChainAccountClient> ContractsServer<T> {
    pub async fn attach(&self, options: AttachContractOptions) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} attach contract, order={:?}, update_at={}", self, options, update_at);
        // order still waiting 
        match &options.order.state {
            DmcOrderState::Preparing {
                miner, 
                user, 
                ..
            } => {
                let mut trans = self.sql_pool().begin().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} attach contract, order={}, update_at={}, err={}", self, options.order.order_id, update_at, err))?;
                if miner.is_none() && user.is_none() {
                    if sqlx::query(&format!("INSERT INTO {} (order_id, state_code, state_value, writen, update_at, block_number, tx_index, bill_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", self.sql_table_name("contract_state")))
                        .bind(options.order.order_id).bind(ContractStateTableRow::state_code_calculating()).bind(vec![]).bind(options.sector.capacity).bind(std::process::id()).bind(update_at).bind(options.block_number).bind(options.tx_index).bind(options.order.bill_id)
                        .execute(&mut trans).await
                        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} attach contract, order={}, update_at={}, err={}", self, options.order.order_id, update_at, err))?
                        .rows_affected() > 0 {
                        let _ = sqlx::query(&format!("INSERT INTO {} (order_id, sector_id, sector_offset_start, sector_offset_end, chunk_size) VALUES (?, ?, ?, ?, ?)", self.sql_table_name("contract_sector")))
                            .bind(options.order.order_id).bind(options.sector.sector_id).bind(0).bind(options.sector.capacity).bind(options.sector.chunk_size as u32)
                            .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} attach contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;
                        let _ = trans.commit().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} attach contract, order={}, update_at={}, err={}", self, options.order.order_id, update_at, err))?;

                        {
                            let server = self.clone();
                            let contract = Contract {
                                order_id: options.order.order_id, 
                                bill_id: Some(options.order.bill_id),
                                update_at, 
                                state: ContractState::Calculating { 
                                    sector: ContractSector {
                                        sector_id: options.sector.sector_id, 
                                        sector_offset_start: 0, 
                                        sector_offset_end: options.sector.capacity,  
                                        chunk_size: options.sector.chunk_size, 
                                    }, 
                                    writen: options.sector.capacity, 
                                    calculated: 0 
                                },
                            };
                            async_std::task::spawn(async move {
                                let _ = server.prepare_contract(contract).await;
                            });
                        }
                        info!("{} attach contract, order={:?}, update_at={}, sucess", self, options, update_at);
                    } else {
                        info!("{} attach contract, order={:?}, update_at={}, ignored", self, options, update_at);
                    }
                } else {
                    info!("{} attach contract, order={:?}, update_at={}, sucess", self, options, update_at);
                }
            }, 
            _ => {
                info!("{} attach contract, order={:?}, update_at={}, sucess", self, options, update_at);
            }
        }
        Ok(())
    }

    pub async fn update(&self, options: UpdateContractOptions) -> DmcResult<()> {
        let update_at = dmc_time_now();
        if let Some(challenge) = options.challenge {
            info!("{} update onchain challenge, challenge={:?}, update_at={}", self, challenge, update_at);
            match &challenge.state {
                DmcChallengeState::Waiting => {
                    let result = sqlx::query(&format!("INSERT INTO {} (challenge_id, order_id, challenge_params, state_code, start_at, update_at, block_number, tx_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", self.sql_table_name("contract_onchain_challenge")))
                        .bind(challenge.challenge_id).bind(challenge.order_id).bind(serde_json::to_vec(&challenge.params).unwrap())
                        .bind(OnChainChallengeRow::state_code_waiting()).bind(challenge.start_at as u64).bind(update_at).bind(options.block_number).bind(options.tx_index)
                        .execute(self.sql_pool()).await
                        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update onchain challenge, challenge={}, update_at={}, err={}", self, challenge.order_id, update_at, err))?;
                    if result.rows_affected() > 0 {
                        info!("{} update onchain challenge, challenge={:?}, update_at={}, applied", self, challenge.order_id, update_at);
                        let challenge = OnChainChallenge {
                            challenge_id: challenge.challenge_id, 
                            order_id: challenge.order_id, 
                            start_at: challenge.start_at as u64, 
                            params: challenge.params.clone(),
                            state: OnChainChallengeState::Waiting,
                            update_at
                        };
                        {
                            let server = self.clone();
                            task::spawn(async move {
                                let _ = server.process_onchain_challenge(challenge).await;
                            });
                        }
                    } else {
                        info!("{} update onchain challenge, challenge={}, update_at={}, ignored", self, challenge.order_id, update_at);
                    }
                }, 
                _ => {
                    // do nothing
                }
            }
            Ok(())
        } else {
            info!("{} update contract, order={:?}, update_at={}", self, options, update_at);
            // order still waiting 
            match &options.order.state {
                DmcOrderState::Preparing {
                    miner, 
                    ..
                } => {
                    let mut trans = self.sql_pool().begin().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={}, update_at={}, err={}", self, options.order.order_id, update_at, err))?;
                    if miner.is_none() {
                        // already exists in table; ignore it 
                        if sqlx::query(&format!("INSERT INTO {} (order_id, state_code, state_value, process_id, update_at, block_number, tx_index, bill_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", self.sql_table_name("contract_state")))
                            .bind(options.order.order_id).bind(ContractStateTableRow::state_code_applying()).bind(vec![]).bind(std::process::id()).bind(update_at).bind(options.block_number).bind(options.tx_index).bind(options.order.bill_id)
                            .execute(&mut trans).await
                            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={}, update_at={}, err={}", self, options.order.order_id, update_at, err))?
                            .rows_affected() > 0 {
                            info!("{} begin applying contract, order={:?}, update_at={}", self, options.order.order_id, update_at);
                        } else {
                            // change process id to this
                            if sqlx::query(&format!("UPDATE {} SET (process_id=?, update_at=?, block_number=?, tx_index=?) WHERE order_id=? AND state_code=? AND (block_number<? OR (block_number=? AND tx_index <=?))", self.sql_table_name("contract_state")))
                                .bind(std::process::id()).bind(update_at).bind(options.block_number).bind(options.tx_index).bind(options.order.order_id).bind(ContractStateTableRow::state_code_applying()).bind(options.block_number).bind(options.block_number).bind(options.tx_index).execute(&mut trans).await
                                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?
                                .rows_affected() > 0 {
                                info!("{} begin applying contract, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                            } else {
                                // reenter processing, ignore it
                                info!("{} ignore update contract, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                                return Ok(());
                            }
                        }

                        if let Some(sector_info) = options.sector {
                            let _ = self.journal_client().append(JournalEvent { sector_id: sector_info.sector_id, order_id: Some(options.order.order_id), event_type: JournalEventType::OrderApplied, event_params: Some(serde_json::to_string(&options.order).unwrap()) })
                                .await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self, options.order.order_id, update_at, err))?;

                            let pre_contract: Option<ContractSectorTableRow>= sqlx::query_as(&format!("SELECT * FROM {} WHERE sector_id=? ORDER BY sector_offset_end DESC", self.sql_table_name("contract_sector")))
                                .bind(sector_info.sector_id).fetch_optional(&mut trans).await
                                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;
                            let sector_offset_start = pre_contract.map_or(0, |pre_contract| pre_contract.sector.sector_offset_end);
                            
                            if sector_info.capacity < sector_offset_start + options.order.capacity() {
                                // sector space not enough; refuse it 
                                unimplemented!()
                            } else {
                                // apply sector offset
                                let sector_row = ContractSectorTableRow {
                                    order_id: options.order.order_id,
                                    sector: ContractSector {
                                        sector_id: sector_info.sector_id, 
                                        sector_offset_start: sector_offset_start, 
                                        sector_offset_end: sector_offset_start + options.order.capacity(),  
                                        chunk_size: sector_info.chunk_size, 
                                    }
                                };   
                                sqlx::query(&format!("INSERT INTO {} (order_id, sector_id, sector_offset_start, sector_offset_end, chunk_size) VALUES (?, ?, ?, ?, ?)", self.sql_table_name("contract_sector")))
                                    .bind(sector_row.order_id).bind(sector_row.sector.sector_id).bind(sector_row.sector.sector_offset_start).bind(sector_row.sector.sector_offset_end).bind(sector_row.sector.chunk_size as u32)
                                    .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;
                                if sqlx::query(&format!("UPDATE {} SET state_code=?, process_id=NULL WHERE order_id=? AND state_code=? AND process_id=?", self.sql_table_name("contract_state")))
                                    .bind(ContractStateTableRow::state_code_writing()).bind( options.order.order_id).bind(ContractStateTableRow::state_code_applying()).bind(std::process::id())
                                    .execute(&mut trans).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?
                                    .rows_affected() > 0 {
                                    let _ = trans.commit().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?;
                                    info!("{} update contract writing, order={:?}, update_at={}, row={:?}", self,  options.order.order_id, update_at, sector_row);
                                    Ok(())
                                } else {
                                    let _ = trans.rollback().await;
                                    info!("{} ignore update contract, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                                    Ok(())
                                }
                            }
                        } else {
                            error!("{} update contract, order={:?}, update_at={}, err=sector not found", self,  options.order.order_id, update_at);
                            if sqlx::query(&format!("UPDATE {} SET state_code=?, process_id=NULL WHERE order_id=? AND state_code=? AND process_id=?", self.sql_table_name("contract_state")))
                                .bind(ContractStateTableRow::state_code_refused()).bind(options.order.order_id).bind(ContractStateTableRow::state_code_applying()).bind(std::process::id())
                                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?
                                .rows_affected() > 0 {
                                info!("{} update contract, order={:?}, update_at={}, refused", self,  options.order.order_id, update_at);
                                Ok(())
                            } else {
                                info!("{} ignore update contract, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                                Ok(())
                            }
                        }
                    } else {
                        Ok(())
                    }
                }, 
                DmcOrderState::Storing(..) => {
                    if sqlx::query(&format!("UPDATE {} SET state_code=?, process_id=NULL, block_number=?, tx_index=? WHERE order_id=?", self.sql_table_name("contract_state")))
                        .bind(ContractStateTableRow::state_code_storing()).bind(options.block_number).bind(options.tx_index).bind(options.order.order_id)
                        .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract, order={:?}, update_at={}, err={}", self,  options.order.order_id, update_at, err))?
                        .rows_affected() > 0 {
                        info!("{} update contract storing, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                    } else {
                        info!("{} ignore update contract, order={:?}, update_at={}", self,  options.order.order_id, update_at);
                    }
                    Ok(())
                }, 
                DmcOrderState::Canceled => {
                    let update_at = dmc_time_now();
                    info!("{} on chain order canceled, will change contract state to End, options={:?}, update_at={}", self, options, update_at);
                    let order_id = options.order.order_id;
                    let contract = self.get(ContractFilter::from_order_id(options.order.order_id), ContractNavigator { page_size: 1, page_index: 0 }).await?
                        .pop()
                        .ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} update contract for canceled order({}) failed for not found contract.", self, order_id))?;
            
                    if sqlx::query(&format!("UPDATE {} SET state_code=?, process_id=NULL, block_number=?, tx_index=? WHERE order_id=? AND state_code NOT IN (?,?)", self.sql_table_name("contract_state")))
                        .bind(ContractStateTableRow::state_code_canceled()).bind(options.block_number).bind(options.tx_index)
                        .bind(order_id).bind(ContractStateTableRow::state_code_refused()).bind(ContractStateTableRow::state_code_canceled())
                        .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract for canceled order failed, order={:?}, update_at={}, err={}", self,  order_id, update_at, err))?
                        .rows_affected() > 0 {
                        
                        let mut order = options.order.clone();
                        let sector = contract.sector().unwrap();
                        order.asset = sector.total_size() / (1 * 1024 * 1024 * 1024);
                        let _ = self.journal_client().append(JournalEvent { 
                            sector_id: sector.sector_id, 
                            order_id: Some(order_id), 
                            event_type: JournalEventType::OrderCanceled, 
                            event_params: Some(serde_json::to_string(&order).unwrap()) 
                        }).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} update contract for canceled order failed, order={:?}, update_at={}, err={}", self,  order_id, update_at, err))?;

                        self.sector_client().clear_chunk(ClearChunkNavigator { sector_id: sector.sector_id, offset: sector.sector_offset_start }).await?;
                        
                        info!("{} update contract for canceled order({}) success.", self,  order_id);
                    } else {
                        info!("{} ignore update contract for canceled order({}), update_at={}", self,  order_id, update_at);
                    }
            
                    Ok(())
                }
            }
        }
    }

    pub async fn get(&self, filter: ContractFilter, navigator: ContractNavigator) -> DmcResult<Vec<Contract>> {
        debug!("{} get contract, filter={:?}, navigator={:?}", self, filter, navigator);
        if navigator.page_size == 0 {
            debug!("{} get contract return, filter={:?}, navigator={:?}, results=0", self, filter, navigator);
            return Ok(vec![])
        }
        
        if let Some(order_id) = filter.order_id {
            let contracts = self.query_contract(order_id).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get contract, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?
                .map(|contract| vec![contract]).unwrap_or(vec![]);
            debug!("{} get contract, returns, filter={:?}, navigator={:?}, contracts={:?}", self, filter, navigator, contracts);
            Ok(contracts)
        } else if let Some(bill_id) = filter.bill_id {
            let contracts = self.query_contract_by_bill(bill_id).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get contract, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?;
            debug!("{} get contract, returns, filter={:?}, navigator={:?}, contracts={:?}", self, filter, navigator, contracts);
            Ok(contracts)
        } else if let Some(raw_sector_id) = filter.raw_sector_id {
            let contracts = self.query_contract_by_raw_sector(raw_sector_id).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get contract, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?;
            debug!("{} get contract, returns, filter={:?}, navigator={:?}, contracts={:?}", self, filter, navigator, contracts);
            Ok(contracts)
        } else {
            unimplemented!()
        }
    }

    pub async fn occupy(&self, options: SectorOccupyOptions) -> DmcResult<SectorOccupy> {
        debug!("{} get occupy, options={:?}", self, options);
        
        let sector_query_sql = format!("SELECT SUM(sector.sector_offset_start) AS start_sum, SUM(sector.sector_offset_end) AS end_sum FROM {} AS state INNER JOIN {} AS sector ON state.order_id=sector.order_id WHERE sector.sector_id=? AND state.state_code<>?", self.sql_table_name("contract_state"), self.sql_table_name("contract_sector"));
        let bill_query_sql = format!("SELECT SUM(sector.sector_offset_start) AS start_sum, SUM(sector.sector_offset_end) AS end_sum FROM {} AS state INNER JOIN {} AS sector ON state.order_id=sector.order_id WHERE state.bill_id=? AND state.state_code<>?", self.sql_table_name("contract_state"), self.sql_table_name("contract_sector"));
        let query = if let Some(raw_sector_id) = options.raw_sector_id {
            sqlx::query(&sector_query_sql).bind(raw_sector_id)
        } else if let Some(bill_id) = options.bill_id {
            sqlx::query(&bill_query_sql).bind(bill_id)
        } else {
            debug!("{} get occupy return, options={:?}", self, options);
            return Err(dmc_err!(DmcErrorCode::InvalidParam, "{} get occupy failed for no condition is not specified.", self));
        };

        let row = query.bind(ContractStateTableRow::state_code_refused())
            .fetch_optional(self.sql_pool()).await?;

        let occupy_size = match row {
            Some(row) => {
                let start_sum: u64 = row.try_get("start_sum").unwrap_or(0);
                let end_sum: u64 = row.try_get("end_sum").unwrap_or(0);
                end_sum - start_sum
            },
            None => 0
        };
        
        let occupy = SectorOccupy { occupy: occupy_size };
        info!("{} get occupy successfully, occupy: {:?}", self, occupy);
        Ok(occupy)
    }

    pub async fn read(&self, navigator: ContractDataNavigator) -> DmcResult<sectors::SectorReader> {
        let update_at = dmc_time_now();
        info!("{} read, navigator={:?}, update_at={}", self, navigator, update_at);
        let contract = self.query_contract(navigator.order_id).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, navigator={}, read={}, err={}", self, navigator.order_id, update_at, err))?
            .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} write, navigator={}, update_at={}, err={}", self, navigator.order_id, update_at, "contract not found"))?;
        if let ContractState::Storing { sector, .. } = contract.state {
            use async_std::io::prelude::*;
            use async_std::io::SeekFrom;
            let mut sector_reader = sectors::SectorReader::from_sector_id(self.sector_client().clone(), sector.sector_id, sector.sector_offset_start, navigator.len).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, contract={}, update_at={}, err={}", self, navigator.order_id, update_at, err))?;
            sector_reader.seek(SeekFrom::Start(navigator.offset)).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, contract={}, update_at={}, err={}", self, navigator.order_id, update_at, err))?;
            Ok(sector_reader)
        } else {
            Err(dmc_err!(DmcErrorCode::Failed, "{} write, navigator={}, update_at={}, err={}", self, navigator.order_id, update_at, "contract not storing"))
        }
    }

    pub async fn write<R: async_std::io::BufRead + Unpin + Send + Sync + 'static>(&self, source: R, navigator: &ContractDataNavigator, sign: &str) -> DmcResult<Contract> {
        let update_at = dmc_time_now();
        info!("{} write, navigator={:?}, update_at={}", self, navigator, update_at);
        let contract = self.query_contract(navigator.order_id).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, navigator={}, update_at={}, err={}", self, navigator.order_id, update_at, err))?;

        if let Some(contract) = contract {
            info!("{} write, got contract, navigator={:?}, update_at={}, contract={:?}", self, navigator, update_at, contract);
            let order = self.chain_client().get_order_by_id(contract.order_id).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, navigator={:?}, update_at={}, contract={:?}, err={}", self, navigator, update_at, contract, err))?
                .ok_or_else(|| dmc_err!(DmcErrorCode::Failed, "{} write, navigator={:?}, update_at={}, contract={:?}, err={}", self, navigator, update_at, contract, "order not found"))?;
            let verified = self.chain_client().verify(&order.user, serde_json::to_vec(&navigator).unwrap().as_slice(), sign).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, navigator={:?}, update_at={}, contract={:?}, err={}", self, navigator, update_at, contract, err))?;
            if !verified {
                return Err(dmc_err!(DmcErrorCode::Failed, "{} write, navigator={:?}, update_at={}, contract={:?}, err={}", self, navigator, update_at, contract, "signture verify failed"));
            }

            if let ContractState::Writing { sector, writen } = contract.state.clone() {
                if navigator.offset % sector.chunk_size as u64 != 0 || navigator.len > sector.chunk_size as u64 {
                    Err(dmc_err!(DmcErrorCode::InvalidData, "{} write, navigator={}, update_at={}, err=invalid offset", self, navigator.order_id, update_at))
                } else if writen < navigator.offset {
                    Err(dmc_err!(DmcErrorCode::InvalidData, "{} write, navigator={}, update_at={}, err=beyound offset", self, navigator.order_id, update_at))
                } else if writen > navigator.offset {
                    //verify navigtor.md and sector meta 
                    unimplemented!()
                } else {
                    if navigator.len > 0 {
                        let sector_navigator = sectors::WriteChunkNavigator {
                            sector_id: sector.sector_id, 
                            offset: sector.sector_offset_start + writen, 
                            len: navigator.len as usize, 
                            // md: navigator.md.clone()
                        };
                        let _ = self.sector_client().write_chunk(source, sector_navigator).await.map_err(|e| {
                            error!("write to sector service err {}", e);
                            e
                        })?;
                    }
                    if navigator.len as usize == sector.chunk_size {
                        if sqlx::query(&format!("UPDATE {} SET writen=?, update_at=? WHERE order_id=? AND state_code=? AND writen=?", self.sql_table_name("contract_state")))
                            .bind(writen + navigator.len as u64).bind(update_at).bind(navigator.order_id).bind(ContractStateTableRow::state_code_writing()).bind(writen)
                            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, navigator={}, update_at={}, err={}", self, navigator.order_id, update_at, err))?
                            .rows_affected() > 0 {
                            let contract = Contract {
                                order_id: navigator.order_id, 
                                update_at,
                                state: ContractState::Writing { sector, writen: writen + navigator.len as u64},
                                bill_id: contract.bill_id,
                            };
                            info!("{} write returned, navigator={:?}, update_at={}, contract={:?}", self, navigator, update_at, contract);
                            Ok(contract)
                        } else {
                            Err(dmc_err!(DmcErrorCode::AddrInUse, "{} write, navigator={}, update_at={}, err=reentering writing process", self, navigator.order_id, update_at))
                        }
                    } else {
                        if sqlx::query(&format!("UPDATE {} SET writen=?, update_at=?, state_code=? WHERE order_id=? AND state_code=? AND writen=?", self.sql_table_name("contract_state")))
                            .bind(writen + navigator.len as u64).bind(update_at).bind(ContractStateTableRow::state_code_calculating())
                            .bind(navigator.order_id).bind(ContractStateTableRow::state_code_writing()).bind(writen)
                            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, navigator={}, update_at={}, err={}", self, navigator.order_id, update_at, err))?
                            .rows_affected() > 0 {
                            let _ = self.journal_client().append(JournalEvent { sector_id: sector.sector_id, order_id: Some(navigator.order_id), event_type: JournalEventType::OrderWriten, event_params: Some(serde_json::to_string(&sector).unwrap()) })
                                .await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, navigator={}, update_at={}, err={}", self, navigator.order_id, update_at, err))?;
                            let writen = writen + navigator.len as u64;
                            let contract = Contract {
                                order_id: navigator.order_id, 
                                update_at,
                                state: ContractState::Calculating { sector: sector.clone(), writen, calculated: 0},
                                bill_id: contract.bill_id,
                            }; 
                            
                            {
                                let server = self.clone();
                                let contract = contract.clone();
                                async_std::task::spawn(async move {
                                    let _ = server.prepare_contract(contract).await;
                                });
                            }
                            info!("{} write returned, navigator={:?}, update_at={}, contract={:?}", self, navigator, update_at, contract);
                            Ok(contract)
                        } else {
                            Err(dmc_err!(DmcErrorCode::AddrInUse, "{} write, navigator={}, update_at={}, err=reentering writing process", self, navigator.order_id, update_at))
                        }
                    }
                   
                }
            } else {
                //verify navigtor.md and sector meta 
                unimplemented!()
            }
        } else {
            if navigator.offset == 0 && navigator.len == 0 {
                // sync order from chain
                unimplemented!()
            } else {
                Err(dmc_err!(DmcErrorCode::NotFound, "{} write, navigator={}, update_at={}, err=contract not exists", self, navigator.order_id, update_at))
            }
        }
    }
}