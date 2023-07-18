use log::*;
use rand::Rng;
use std::{
    sync::Arc,
    convert::TryInto
};
use async_std::{task, fs, stream::StreamExt};
use serde::{Serialize, Deserialize};
use sqlx::Row;
use tide::{Request, Response, Body, http::Url};
use dmc_tools_common::*;
use dmc_user_journal::*;
#[cfg(feature = "sqlite")]
use sqlx_sqlite as sqlx_types;
#[cfg(feature = "mysql")]
use sqlx_mysql as sqlx_types;

use crate::{
    types::*
};




#[derive(Serialize, Deserialize, Clone)]
pub struct SourceServerConfig {
    pub host: String, 
    pub sql_url: String, 
    pub journal_client: JournalClientConfig
}


#[derive(sqlx::FromRow)]
pub struct SourceStateRow {
    source_id: sqlx_types::U64, 
    source_url: String, 
    length: sqlx_types::U64,
    update_at: sqlx_types::U64, 
    state_code: sqlx_types::U8, 
    merkle_stub: Option<Vec<u8>>,   
    merkle_options: Option<Vec<u8>>, 
    stub_options: Vec<u8>
}

impl TryInto<DataSource> for SourceStateRow {
    type Error = DmcError;
    fn try_into(self) -> DmcResult<DataSource> {
        let merkle_stub = if let Some(value) = self.merkle_stub {
            serde_json::from_slice(&value)?
        } else {
            None
        };
        Ok(DataSource {
            source_id: self.source_id as u64,  
            length: self.length as u64, 
            update_at: self.update_at as u64, 
            source_url: self.source_url, 
            merkle_stub, 
            state: SourceState::try_from(self.state_code as u8)?
        })
    }
}

impl From<&CreateSourceOptions> for SourceStateRow {
    fn from(options: &CreateSourceOptions) -> Self {
        Self {
            source_id: 0, 
            source_url: options.source_url.clone(), 
            length: options.length as sqlx_types::U64,
            update_at: 0, 
            state_code: match &options.merkle {
                SourceMerkleOptions::Ready(_) => SourceState::PreparingStub, 
                SourceMerkleOptions::Prepare { .. } => SourceState::PreparingMerkle 
            } as u8 as sqlx_types::U8, 
            merkle_stub: match &options.merkle {
                SourceMerkleOptions::Ready(stub) => Some(serde_json::to_vec(stub).unwrap()), 
                SourceMerkleOptions::Prepare { .. } => None
            },   
            merkle_options:  match &options.merkle {
                SourceMerkleOptions::Ready(_) => None,  
                SourceMerkleOptions::Prepare { piece_size } => Some(serde_json::to_vec(piece_size).unwrap())
            }, 
            stub_options: serde_json::to_vec(&options.stub).unwrap()
        }
    }
}

impl TryInto<CreateSourceOptions> for &SourceStateRow {
    type Error = DmcError;
    fn try_into(self) -> DmcResult<CreateSourceOptions> {
        Ok(CreateSourceOptions {
            source_url: self.source_url.clone(), 
            length: self.length as u64, 
            merkle: if let Some(options) = &self.merkle_options {
                SourceMerkleOptions::Prepare {
                    piece_size: serde_json::from_slice(options)?
                }
            } else {
                let merle_stub = if let Some(value) = &self.merkle_stub {
                    serde_json::from_slice(value)?
                } else {
                    return Err(DmcError::new(DmcErrorCode::InvalidData, "no merkle stub but declared as ready"));
                };
                SourceMerkleOptions::Ready(merle_stub)
            }, 
            stub: serde_json::from_slice(&self.stub_options)?
        })
    }
}

#[derive(sqlx::FromRow)]
struct SourceStubRow {
    source_id: sqlx_types::U64, 
    index: sqlx_types::U32, 
    offset: sqlx_types::U64, 
    length: sqlx_types::U16, 
    content: Vec<u8>
}

impl Into<SourceStub> for SourceStubRow {
    fn into(self) -> SourceStub {
        SourceStub {
            source_id: self.source_id as u64, 
            index: self.index as u32, 
            offset: self.offset as u64, 
            length: self.length as u16, 
            content: DmcData::from(self.content)
        } 
    }
}

#[derive(sqlx::FromRow)]
pub struct MerklePathStubRow {
    source_id: sqlx_types::U64, 
    index: sqlx_types::U32, 
    content: Vec<u8>
}

impl Into<MerklePathStub> for MerklePathStubRow {
    fn into(self) -> MerklePathStub {
        MerklePathStub {
            source_id: self.source_id as u64, 
            index: self.index as u32, 
            content: self.content
        } 
    }
}


struct ServerImpl {
    config: SourceServerConfig, 
    sql_pool: sqlx_types::SqlPool,  
    journal_client: JournalClient, 
}


#[derive(Clone)]
pub struct SourceServer(Arc<ServerImpl>);

impl std::fmt::Display for SourceServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserSourceServer {{process_id={}}}", std::process::id())
    }
}

impl SourceServer {
    pub async fn listen(self) -> DmcResult<()> {
        let host = self.config().host.clone();
        
        let mut http_server = tide::with_state(self);
    
        http_server.at("/source").post(|mut req: Request<Self>| async move {
            let options = req.body_json().await?;
            let mut resp = Response::new(200);
            resp.set_body(Body::from_json(&req.state().create(options).await?)?);
            Ok(resp)
        });
    
        http_server.at("/source").get(|req: Request<Self>| async move {
            let mut query = SourceFilterAndNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "source_id" => {
                        query.filter.source_id = Some(u64::from_str_radix(&*value, 10)?);
                    },
                    "source_url" => {
                        query.filter.source_url = Some((&*value).to_owned());
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
            // let query = req.query::<SourceFilterAndNavigator>()?;
            
            let mut resp = Response::new(200);
            let sources = req.state().get(query.filter, query.navigator).await?;
            resp.set_body(Body::from_json(&sources)?);
            Ok(resp)
        });

        http_server.at("/source/stub").get(|req: Request<Self>| async move {
            let mut source_id = None;
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "source_id" => {
                        source_id = Some(u64::from_str_radix(&*value, 10)?);
                    }, 
                    _ => {}
                }
            }
            // let query = req.query::<SourceFilterAndNavigator>()?;
            let mut resp = Response::new(200);
            if let Some(source_id) = source_id {
                let stub = req.state().random_stub(source_id).await?;
                resp.set_body(Body::from_json(&stub)?);
                Ok(resp)
            } else {
                unreachable!()
            }
        });

        // http_server.at("/source/merkle_stub").get(|req: Request<Self>| async move {
        //     let mut source_id = None;
        //     for (key, value) in req.url().query_pairs() {
        //         match &*key {
        //             "source_id" => {
        //                 source_id = Some(u64::from_str_radix(&*value, 10)?);
        //             }, 
        //             _ => {}
        //         }
        //     }
        //     // let query = req.query::<SourceFilterAndNavigator>()?;
        //     let mut resp = Response::new(200);
        //     if let Some(source_id) = source_id {
        //         let stub = req.state().random_merkle_challenge(source_id).await?;
        //         resp.set_body(Body::from_json(&stub)?);
        //         Ok(resp)
        //     } else {
        //         unreachable!()
        //     }
        // });

        http_server.at("/source/detail").get(|req: Request<Self>| async move {
            let param = req.query::<QuerySource>()?;

            let detail = req.state().get_source_detail(param.source_id).await?;
            Ok(Response::from(Body::from_json(&detail)?))
        });

        let _ = http_server.listen(host.as_str()).await?;

        Ok(())
    }

    pub async fn new(config: SourceServerConfig) -> DmcResult<Self> {
        let sql_pool = sqlx_types::connect_pool(&config.sql_url).await?;
        let journal_client = JournalClient::new(config.journal_client.clone())?;
        Ok(Self(Arc::new(ServerImpl {
                config, 
                sql_pool,  
                journal_client
            })
        ))
    }

    pub async fn init(&self) -> DmcResult<()> {
        let _ = sqlx::query(&Self::sql_create_table_source_state()).execute(&self.0.sql_pool).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init failed, err={}", self, err))?;
        let _ = sqlx::query(&Self::sql_create_table_merkle_stub()).execute(&self.0.sql_pool).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init failed, err={}", self, err))?;
        let _ = sqlx::query(&Self::sql_create_table_source_stub()).execute(&self.0.sql_pool).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init failed, err={}", self, err))?;
        Ok(())
    }

    pub async fn reset(self) -> DmcResult<Self> {
        self.init().await?;
        let mut trans = self.sql_pool().begin().await?;
        let _ = sqlx::query("DELETE FROM source_state WHERE source_id > 0").execute(&mut trans).await?;
        let _ = sqlx::query("DELETE FROM merkle_stub WHERE source_id > 0").execute(&mut trans).await?;
        let _ = sqlx::query("DELETE FROM source_stub WHERE source_id > 0").execute(&mut trans).await?;
        trans.commit().await?;
        Ok(self)
    }
}



impl SourceServer {
    fn config(&self) -> &SourceServerConfig {
        &self.0.config
    }

    fn journal_client(&self) -> &JournalClient {
        &self.0.journal_client
    }

    fn sql_pool(&self) -> &sqlx_types::SqlPool {
        &self.0.sql_pool
    }

    #[cfg(feature = "mysql")]
    fn sql_create_table_source_state() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `source_state` (
            `source_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `source_url` VARCHAR(512) NOT NULL,  
            `length` BIGINT UNSIGNED NOT NULL,
            `state_code` TINYINT UNSIGNED NOT NULL, 
            `merkle_stub` BLOB,  
            `merkle_options` BLOB, 
            `stub_options` BLOB NOT NULL,
            `update_at` BIGINT UNSIGNED NOT NULL, 
            `process_id` INT UNSIGNED, 
            INDEX (`state_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
    }

    #[cfg(feature = "sqlite")]
    fn sql_create_table_source_state() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS source_state (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT NOT NULL,
            length  NOT NULL,
            state_code INTEGER NOT NULL, 
            merkle_stub BLOB,  
            merkle_options BLOB, 
            stub_options BLOB NOT NULL,
            update_at INTEGER NOT NULL,
            process_id INTEGER
        );
        CREATE INDEX IF NOT EXISTS state_code ON source_state (state_code);
        "#
    }

    #[cfg(feature = "mysql")]
    fn sql_create_table_merkle_stub() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `merkle_stub` (
            `source_id` BIGINT UNSIGNED NOT NULL, 
            `index` INT UNSIGNED NOT NULL, 
            `content` BLOB, 
            INDEX (`source_id`),
            UNIQUE INDEX `source_index` (`source_id`, `index`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
    }

    #[cfg(feature = "sqlite")]
    fn sql_create_table_merkle_stub() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS merkle_stub (
                source_id INTEGER NOT NULL,
                "index" INTEGER NOT NULL,
                content BLOB
            );
        CREATE UNIQUE INDEX IF NOT EXISTS source_index ON merkle_stub (
            source_id,
            "index"
        );
        CREATE INDEX IF NOT EXISTS source_id ON merkle_stub (
            source_id
        );"#
    }

    #[cfg(feature = "mysql")]
    fn sql_create_table_source_stub() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `source_stub` (
            `source_id` BIGINT UNSIGNED NOT NULL, 
            `index` INT UNSIGNED NOT NULL, 
            `offset` BIGINT UNSIGNED NOT NULL, 
            `length` SMALLINT UNSIGNED DEFAULT NULL,
            `content` BLOB, 
            INDEX (`source_id`),
            UNIQUE INDEX `source_index` (`source_id`, `index`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
    }

    #[cfg(feature = "sqlite")]
    fn sql_create_table_source_stub() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS source_stub (
                source_id INTEGER NOT NULL,
                "index" INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                length INTEGER DEFAULT NULL,
                content BLOB
            );
            CREATE UNIQUE INDEX IF NOT EXISTS source_index ON source_stub (
                source_id,
                "index"
            );
            CREATE INDEX IF NOT EXISTS source_id ON source_stub (
                source_id
            );"#
    }

    async fn prepare_stub_inner(&self, source: DataSource, options: CreateSourceOptions) -> DmcResult<()> {
        let update_at = source.update_at;
        info!("{} prepare source stub, source={:?}, update_at={}", self, source, update_at);

        let source_url = Url::parse(&source.source_url)
            .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=source {} {}", self, source.source_id, update_at, source.source_url, err))?;

        let last_row: Option<SourceStub> = sqlx::query_as::<_, SourceStubRow>("SELECT * FROM source_stub WHERE source_id=? ORDER BY `index` DESC LIMIT 1")
            .bind(source.source_id as sqlx_types::U64).fetch_optional(self.sql_pool()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source stub, source={:?}, update_at={}, err={}", self, source, update_at, err))?
            .map(|row| row.into());

        let count = if let Some(last_row) = last_row {
            if (last_row.index + 1) >= options.stub.count {
                0
            } else {
                options.stub.count - last_row.index - 1
            }
        } else {
            options.stub.count
        };

        if source_url.scheme() == "file" {
            let source_path;
            #[cfg(target_os = "windows")] {
                source_path = &source_url.path()[1..];
            } 
            #[cfg(not(target_os = "windows"))] {
                source_path = source_url.path();
            }

            let mut source_file = fs::File::open(source_path).await
                .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=source {} {}", self, source.source_id, update_at, source_path, err))?;
            let file_length = source_file.metadata().await
                .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=open file {} {}", self, source.source_id, update_at, source_path, err))?
                .len();


            for index in 0..count {
                let offset = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(0..file_length)
                };

                let offset = options.merkle.piece_size() as u64 * (offset / options.merkle.piece_size() as u64);

                use async_std::io::prelude::*;
                use async_std::io::SeekFrom;
                source_file.seek(SeekFrom::Start(offset)).await
                    .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=open file {} {}", self, source.source_id, update_at, source_path, err))?;
                let length = u64::min(options.merkle.piece_size() as u64, file_length - offset) as usize;
                let mut content = vec![0u8; length];
                source_file.read_exact(&mut content).await
                    .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=open file {} {}", self, source.source_id, update_at, source_path, err))?;
                // if read != length {
                //     return Err(dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=read file {} {}", self, source.source_id, update_at, source_path, "length mismatch"));
                // }
                // debug!("{} prepare source stub, source={:?}, update_at={}, create stub, offset={}, length={}, content={:?}", self, source, update_at, offset, length, content);
                if sqlx::query("INSERT INTO source_stub (source_id, `index`, offset, length, content) VALUES (?, ?, ?, ?, ?)")
                    .bind(source.source_id as sqlx_types::U64).bind(index as sqlx_types::U32).bind(offset as sqlx_types::U16).bind(length as u16).bind(content)
                    .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source stub, source={:?}, update_at={}, err={}", self, source, update_at, err))?
                    .rows_affected() == 0 {
                    return Err(dmc_err!(DmcErrorCode::Failed, "{} prepare source stub, source={:?}, update_at={}, err={}", self, source, update_at, "duplicated"));
                }
            }

            let update_at = dmc_time_now();
            if sqlx::query("UPDATE source_state SET state_code=?, update_at=?, process_id=NULL WHERE source_id=? AND process_id=? AND update_at=? AND state_code=?")
                .bind(SourceState::Ready as u8 as sqlx_types::U8).bind(update_at as sqlx_types::U64).bind(source.source_id as sqlx_types::U64)
                .bind(std::process::id() as sqlx_types::U32).bind(source.update_at as sqlx_types::U64).bind(source.state as u8 as sqlx_types::U8)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source stub, source={}, update_at={}, err={}", self, source.source_id, source.update_at, err))?
                .rows_affected() > 0 {
                let _ = self.journal_client().append(JournalEvent { source_id: source.source_id, order_id: None, event_type: JournalEventType::SourcePrepared, event_params: None}).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source stub, source={:?}, update_at={}, err={}", self, source, update_at, err))?;
                info!("{} prepare source stub, source={:?}, update_at={}, finish", self, source.source_id, source.update_at);
            } else {
                info!("{} prepare source stub, source={:?}, update_at={}, ignored", self, source.source_id, source.update_at);
            }
            Ok(())
        } else {
            unimplemented!()
        }

    }

    async fn prepare_merkle_inner(&self, source: DataSource, options: CreateSourceOptions) -> DmcResult<()> {
        let update_at = source.update_at;
        info!("{} prepare source merkle, source={:?}, update_at={}", self, source, update_at);

        let piece_size = if let SourceMerkleOptions::Prepare { piece_size } = options.merkle {
            Ok(piece_size)
        } else {
            Err(dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err={}", self, source.source_id, update_at, "invalid options"))
        }?;

        let source_url = Url::parse(&source.source_url)
            .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=source {} {}", self, source.source_id, update_at, source.source_url, err))?;

        if source_url.scheme() == "file" {
            let source_path;
            #[cfg(target_os = "windows")] {
                source_path = &source_url.path()[1..];
            } 
            #[cfg(not(target_os = "windows"))] {
                source_path = source_url.path();
            }

            let source_file = fs::File::open(source_path).await
                .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=source {} {}", self, source.source_id, update_at, source_path, err))?;
            let source_length = source_file.metadata().await
                .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=open file {} {}", self, source.source_id, update_at, source_path, err))?
                .len();
            
            let stub_proc = MerkleStubProc::new(source_length, piece_size, true);
            let mut source_reader = stub_proc.wrap_reader(source_file);

            let mut row_stream = sqlx::query_as::<_, MerklePathStubRow>("SELECT * FROM merkle_stub WHERE source_id=? ORDER BY `index`")
                .bind(source.source_id as sqlx_types::U64).fetch(self.sql_pool());

            let mut stubs = vec![];
            loop {
                if let Some(stub) = row_stream.next().await {
                    let stub: MerklePathStub = stub.map(|row| row.into()).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source merkle, source={:?}, update_at={}, err={}", self, source, update_at, err))?;
                    stubs.push(HashValue::try_from(stub.content).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source merkle, source={:?}, update_at={}, err={}", self, source, update_at, err))?);
                } else {
                    break;
                }
            }

            if stubs.len() < stub_proc.stub_count() {
                use std::io::SeekFrom;
                use async_std::io::prelude::*;
                source_reader.seek(SeekFrom::Start(stub_proc.stub_chunk_size() as u64 * stubs.len() as u64)).await
                    .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=open file {} {}", self, source.source_id, update_at, source_path, err))?;
                for i in stubs.len()..stub_proc.stub_count() {
                    let content = stub_proc.calc_path_stub::<_, MerkleStubSha256>(i, &mut source_reader).await
                        .map_err(|err| dmc_err!(DmcErrorCode::InvalidInput, "{} prepare source merkle, source={:?}, update_at={}, err=open file {} {}", self, source.source_id, update_at, source_path, err))?;

                    
                    sqlx::query("INSERT INTO merkle_stub (source_id, `index`, content) VALUES (?, ?, ?)")
                        .bind(source.source_id as sqlx_types::U64).bind(i as sqlx_types::U32).bind(content.as_slice())
                        .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source merkle, source={:?}, update_at={}, err={}", self, source, update_at, err))?;
                    
                    stubs.push(content);
                }
            }
            
            let root = stub_proc.calc_root_from_path_stub::<MerkleStubSha256>(stubs).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source merkle, source={:?}, update_at={}, err={}", self, source, update_at, err))?;
            
            let merkle_stub = DmcMerkleStub {
                leaves: stub_proc.leaves(), 
                piece_size: stub_proc.piece_size(), 
                root
            };

            info!("{} prepare source merkle, source={:?}, update_at={}, calc merkle stub={:?}", self, source.source_id, source.update_at, merkle_stub);
            let merkle_stub_blob = serde_json::to_vec(&merkle_stub).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source merkle, source={}, update_at={}, err=clac merkel {}", self, source.source_id, update_at, err))?;

            let update_at = dmc_time_now();
            if sqlx::query("UPDATE source_state SET state_code=?, update_at=?, merkle_stub=?, process_id=NULL WHERE source_id=? AND process_id=? AND update_at=? AND state_code=?")
                .bind(SourceState::PreparingStub as u8 as sqlx_types::U8).bind(update_at as sqlx_types::U64).bind(merkle_stub_blob)
                .bind(source.source_id as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32).bind(source.update_at as sqlx_types::U64)
                .bind(source.state as u8 as sqlx_types::U8)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source merkle, source={}, update_at={}, err={}", self, source.source_id, source.update_at, err))?
                .rows_affected() > 0 {
                let mut source = source;
                source.update_at = update_at;
                source.merkle_stub = Some(merkle_stub);
                source.state = SourceState::PreparingStub;
                info!("{} prepare source merkle, source={:?}, update_at={}, finish", self, source.source_id, source.update_at);
                let server = self.clone();
                task::spawn(async move {
                    let _ = server.prepare_source(source, options).await;
                });
            } else {
                info!("{} prepare source merkle, source={:?}, update_at={}, ignored", self, source.source_id, source.update_at);
            }
            Ok(())
        } else {
            unimplemented!()
        }
    }

    async fn prepare_inner(&self, source: DataSource, options: CreateSourceOptions) -> DmcResult<()> {
        let update_at = source.update_at;
        info!("{} prepare source, source={:?}, update_at={}", self, source, update_at);
        match source.state.clone() {
            SourceState::PreparingMerkle => {
                self.prepare_merkle_inner(source, options).await
            },
            SourceState::PreparingStub => {
                self.prepare_stub_inner(source, options).await
            },
            SourceState::Ready => {
                info!("{} prepare source, source={:?}, update_at={}, ignored", self, source, update_at);
                Ok(())
            }
        }
    }

    #[async_recursion::async_recursion]
    async fn prepare_source(&self, source: DataSource, options: CreateSourceOptions) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} prepare source, source={:?}, update_at={}", self, source, update_at);
        if sqlx::query("UPDATE source_state SET process_id=?, update_at=? WHERE source_id=? AND state_code=? AND (process_id IS NULL OR process_id!=?)")
                .bind(std::process::id() as sqlx_types::U32).bind(update_at as sqlx_types::U64).bind(source.source_id as sqlx_types::U64)
                .bind(source.state as u8 as sqlx_types::U8).bind(std::process::id() as sqlx_types::U32)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source, source={}, update_at={}, err={}", self, source.source_id, update_at, err))?
                .rows_affected() == 0 {
            info!("{} prepare source, contract={:?}, update_at={}, ignored", self, source.source_id, update_at);
            return Ok(());
        } 

        let mut source = source;
        source.update_at = update_at;

        if self.prepare_inner(source.clone(), options).await.is_err() {
            let _ = sqlx::query("UPDATE source_state SET process_id=NULL, update_at=? WHERE source_id=? AND state_code=? AND update_at=? AND process_id=?")
                .bind(dmc_time_now() as sqlx_types::U64).bind(source.source_id as sqlx_types::U64).bind(source.state as u8 as sqlx_types::U8)
                .bind(source.update_at as sqlx_types::U64).bind(std::process::id() as sqlx_types::U32)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} prepare source, source={}, update_at={}, err={}", self, source.source_id, update_at, err))?;
        }
        Ok(())
    }

    
}

impl SourceServer {
    pub async fn create(&self, options: CreateSourceOptions) -> DmcResult<DataSource> {
        let update_at = dmc_time_now();
        info!("{} create source, options={:?}, update_at={}", self, options, update_at);
        
        let mut conn = self.sql_pool().acquire().await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create source, options={:?}, update_at={}, err={}", self, options, update_at, err))?;

        let row = SourceStateRow::from(&options);
        let result = sqlx::query("INSERT INTO source_state (source_url, length, state_code, merkle_stub, merkle_options, stub_options, update_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(&row.source_url).bind(row.length).bind(row.state_code).bind(row.merkle_stub).bind(row.merkle_options).bind(row.stub_options).bind(update_at as sqlx_types::U64)
            .execute(&mut conn).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create source, options={:?}, update_at={}, err={}", self, options, update_at, err))?;
        if result.rows_affected() > 0 {
            let row: SourceStateRow = sqlx::query_as(&format!("SELECT * FROM source_state WHERE {}=?", sqlx_types::rowid_name("source_id")))
                .bind(sqlx_types::last_inersert(&result))
                .fetch_one(&mut conn).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create source, options={:?}, update_at={}, err={}", self, options, update_at, err))?;
            let _ = self.journal_client().append(JournalEvent { source_id: row.source_id as u64, order_id: None, event_type: JournalEventType::SourceCreated, event_params: None}).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} create source, options={:?}, update_at={}, err={}", self, options, update_at, err))?;
            let options: CreateSourceOptions = (&row).try_into().unwrap();
            let source: DataSource = row.try_into().unwrap();
            
            {
                let source = source.clone();
                let options = options.clone();
                let server = self.clone();
                task::spawn(async move {
                    let _ = server.prepare_source(source, options).await;
                });
            }
            info!("{} create source, options={:?}, update_at={}, finished, souce={:?}", self, options, update_at, source);
            Ok(source)
        } else {
            Err(dmc_err!(DmcErrorCode::AlreadyExists, "{} create source, options={:?}, update_at={}, ignored", self, options, update_at))
        }
    }

    pub async fn get(&self, filter: SourceFilter, navigator: SourceNavigator) -> DmcResult<Vec<DataSource>> {
        debug!("{} get source, filter={:?}, navigator={:?}", self, filter, navigator);
        if navigator.page_size == 0 {
            debug!("{} get source, filter={:?}, navigator={:?}, returns, results =0", self, filter, navigator);
            return Ok(vec![]);
        }
        
        if let Some(source_id) = filter.source_id {
            let row: Option<SourceStateRow> = sqlx::query_as("SELECT * FROM source_state WHERE source_id=?")
                .bind(source_id as sqlx_types::U64).fetch_optional(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get source, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?;
            let sources = if let Some(row) = row {
                vec![row.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get source, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?]
            } else {
                vec![]
            };
            debug!("{} get source returned, filter={:?}, navigator={:?}, results={:?}", self, filter, navigator, sources);
            Ok(sources)
        } else if let Some(source_url) = &filter.source_url {
            let row: Option<SourceStateRow> = sqlx::query_as("SELECT * FROM source_state WHERE source_url=?")
                .bind(source_url).fetch_optional(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get source, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?;
            let sources = if let Some(row) = row {
                vec![row.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get source, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?]
            } else {
                vec![]
            };
            debug!("{} get source returned, filter={:?}, navigator={:?}, results={:?}", self, filter, navigator, sources);
            Ok(sources)
        } else {
            unimplemented!()
        }
    }

    pub async fn random_stub(&self, source_id: u64) -> DmcResult<SourceStub> {
        let row: SourceStateRow = sqlx::query_as("SELECT * FROM source_state WHERE source_id=?")
            .bind(source_id as sqlx_types::U64).fetch_one(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} random stub, source={}, err={}", self, source_id, err))?;
        let options: CreateSourceOptions = (&row).try_into().unwrap();
        let source: DataSource = row.try_into().unwrap();

        if source.state != SourceState::Ready {
            return Err(DmcError::new(DmcErrorCode::ErrorState, "not ready"));
        }
        
        let index = {
            let mut rng = rand::thread_rng();
            rng.gen_range(0..options.stub.count)
        };

        let stub = sqlx::query_as::<_, SourceStubRow>("SELECT * FROM source_stub WHERE source_id=? AND `index`=?")
            .bind(source_id as sqlx_types::U64).bind(index as sqlx_types::U32).fetch_one(self.sql_pool()).await
            .map(|row| row.into())
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} random stub, source={}, err={}", self, source_id, err))?;
        Ok(stub)
    }

    // pub async fn random_merkle_challenge(&self, source_id: u64) -> DmcResult<MerkleStubChallenge> {
    //     let row: SourceStateRow = sqlx::query_as("SELECT * FROM source_state WHERE source_id=?")
    //         .bind(source_id as sqlx_types::U64).fetch_one(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} random stub, source={}, err={}", self, source_id, err))?;
    //     let source: DataSource = row.try_into().unwrap();

    //     if source.state != SourceState::Ready {
    //         return Err(DmcError::new(DmcErrorCode::ErrorState, "not ready"));
    //     }

    //     let merkle_stub = source.merkle_stub.unwrap();
    //     let stub_proc = MerkleStubProc::new(source.length, merkle_stub.piece_size);

    //     let stubs = sqlx::query_as::<_, MerklePathStubRow>("SELECT * FROM merkle_stub WHERE source_id=? ORDER BY `index`")
    //         .bind(source.source_id as sqlx_types::U64).fetch_all(self.sql_pool()).await
    //         .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} random merkle stub, source={:?}, err={}", self, source_id, err))?;
        
    //     if stubs.len() < stub_proc.stub_count() {
    //         return Err(dmc_err!(DmcErrorCode::Failed, "{} random merkle stub, source={:?}, err={}", self, source_id, "merkle stub not ready"));
    //     }

    //     let piece_index = {
    //         let mut rng = rand::thread_rng();
    //         rng.gen_range(0..stub_proc.leaves())
    //     };

    //     Ok(stub_proc.challenge_of_piece::<MerkleStubSha256>(piece_index, stubs.into_iter().map(|stub| HashValue::try_from(stub.content).unwrap()).collect()))
    // }

    pub async fn get_source_detail(&self, source_id: u64) -> DmcResult<SourceDetailState> {
        let row: SourceStateRow = sqlx::query_as("SELECT * FROM source_state WHERE source_id=?")
            .bind(source_id as sqlx_types::U64)
            .fetch_one(self.sql_pool()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "get source detail, id={}, err={}", source_id, err))?;
        let options: CreateSourceOptions = (&row).try_into().unwrap();
        let source: DataSource = row.try_into().unwrap();

        let (index, count) = {
            match source.state {
                SourceState::PreparingMerkle => {
                    let index: i64 = if let Ok(row) = sqlx::query("SELECT `index` FROM merkle_stub WHERE source_id=? ORDER BY `index` DESC LIMIT 1")
                        .bind(source_id as sqlx_types::U64)
                        .fetch_one(self.sql_pool()).await {
                        row.try_get(0)?
                    } else {
                        -1
                    };
                    let stub_proc = MerkleStubProc::new(source.length, options.merkle.piece_size(), true);
                    (index+1, stub_proc.stub_count() as u64)
                }
                SourceState::PreparingStub => {
                    let index: i64 = if let Ok(row) = sqlx::query("SELECT `index` FROM source_stub WHERE source_id=? ORDER BY `index` DESC LIMIT 1")
                        .bind(source_id as sqlx_types::U64)
                        .fetch_one(self.sql_pool()).await {
                        row.try_get(0)?
                    } else {
                        -1
                    };
                    (index+1, options.stub.count as u64)
                }
                SourceState::Ready => (0, 0)
            }
        };

        Ok(SourceDetailState {
            source_id,
            state: source.state,
            index: index as u64,
            count,
        })
    }
}