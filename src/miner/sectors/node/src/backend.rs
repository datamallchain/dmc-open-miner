use log::*;
use std::{
    sync::Arc, 
    path::Path
};
use std::path::PathBuf;
use std::str::FromStr;
use serde::Deserialize;
use sqlx::{ConnectOptions, Pool};
use sqlx::mysql::MySqlConnectOptions;
use tide::{Request, Response, Body};
use dmc_tools_common::*;
use dmc_miner_sectors_client::*;
use dmc_miner_journal::*;
use crate::{
    types::*
};

#[derive(Clone, Deserialize)]
pub struct SectorsNodeConfig {
    pub listen_address: String,
    pub node_id: SectorNodeId,
    pub sql_url: String, 
    pub journal_client: JournalClientConfig
}


struct ServerImpl {
    config: SectorsNodeConfig,  
    sql_pool: Pool<sqlx::MySql>, 
    journal_client: JournalClient
}

#[derive(Clone)]
pub struct SectorsNode(Arc<ServerImpl>);


impl SectorsNode {
    pub async fn listen(self) -> DmcResult<()> {
        let config = self.config();
        // if let Some(gateway) = config.gateway.as_ref() {
        //     let gateway = GatewaySectorAdmin::new(config.gateway.clone()).unwrap();
        //     gateway.register_node(NodeInfo {
        //         node_id: config.node_id,
        //         endpoint: Url::parse(self.endpoint().as_str()).unwrap(),
        //     }).await?;
        // }

        let listen_address = config.listen_address.clone();
        let mut http_server = tide::with_state(self);

        http_server.at("/sector").post(|mut req: Request<Self>| async move {
            let options = req.body_json().await?;
            let mut resp = Response::new(200);
            resp.set_body(Body::from_json(&req.state().add(options).await?)?);
            Ok(resp)
        });

        http_server.at("/sector").get(|req: Request<Self>| async move {
            debug!("{} http get sector, url={}", req.state(), req.url());
            let mut query = SectorFilterAndNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "sector_id" => {
                        query.filter.sector_id = Some(u64::from_str_radix(&*value, 10)?);
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
            // let query = req.query ::<SectorFilterAndNavigator>()
            //     .map_err(|err| dmc_err!(DmcErrorCode::InvalidParam, "{} http get sector, url={}, err=query params {}", req.state(), req.url(), err))?;
            let mut resp = Response::new(200);
            let contracts = req.state().get(query.filter, query.navigator).await?;
            resp.set_body(Body::from_json(&contracts)?);
            Ok(resp)
        });

        http_server.at("/sector/chunk").post(|mut req: Request<Self>| async move {
            let mut query = WriteChunkNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "sector_id" => {
                        query.sector_id = u64::from_str_radix(&*value, 10)?;
                    },
                    "offset" => {
                        query.offset = u64::from_str_radix(&*value, 10)?;
                    },
                    _ => {}
                }
            }
            // let query = req.query::<WriteChunkNavigator>()?;
            let body = req.take_body();
            query.len = body.len().ok_or_else(|| DmcError::new(DmcErrorCode::InvalidInput, "no length set"))?;
            req.state().write_chunk(body.into_reader(), query).await?;
            let resp = Response::new(200);
            Ok(resp)
        });


        http_server.at("/sector/chunk").get(|req: Request<Self>| async move {
            let mut query = ReadChunkNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "sector_id" => {
                        query.sector_id = u64::from_str_radix(&*value, 10)?;
                    },
                    "offset" => {
                        query.offset = u64::from_str_radix(&*value, 10)?;
                    },
                    _ => {}
                }
            }
            // let query = req.query::<ReadChunkNavigator>()?;
            let mut resp = Response::new(200);
            let (meta, chunk) = req.state().read_chunk(query).await?;
            resp.append_header("chunk_offset", format!("{}", meta.offset).as_str());
            if let Some(chunk) = chunk {
                resp.set_body(Body::from_reader(async_std::io::BufReader::new(chunk), Some(meta.len)));
            }
            Ok(resp)
        });

        http_server.at("/sector/chunk").delete(|req: Request<Self>| async move {
            let mut query = ClearChunkNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "sector_id" => {
                        query.sector_id = u64::from_str_radix(&*value, 10)?;
                    },
                    "offset" => {
                        query.offset = u64::from_str_radix(&*value, 10)?;
                    },
                    _ => {}
                }
            }

            let  resp = Response::new(200);
            req.state().clear_chunk(query).await?;
            Ok(resp)
        });

        let _ = http_server.listen(listen_address.as_str()).await?;

        Ok(())
    }

    pub async fn new(config: SectorsNodeConfig) -> DmcResult<Self> {
        let mut options = MySqlConnectOptions::from_str(&config.sql_url)?;
        options.log_statements(LevelFilter::Off);
        let sql_pool = sqlx::mysql::MySqlPoolOptions::new().connect_with(options).await?;
        let journal_client = JournalClient::new(config.journal_client.clone())?;
        Ok(Self(Arc::new(ServerImpl {
            config, 
            sql_pool, 
            journal_client
        })))
    }

    pub async fn init(&self) -> DmcResult<()> {
        let _ = sqlx::query(&self.sql_create_table_sector()).execute(self.sql_pool()).await?;
        Ok(())
    }

    pub async fn reset(&self) -> DmcResult<()> {
        self.init().await?;
        let _ = sqlx::query(format!("DELETE FROM {} WHERE sector_id>0", self.sector_table_name()).as_str()).execute(self.sql_pool()).await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct SectorTableRow {
    pub sector_id: u32,
    pub capacity: u64, 
    #[sqlx(try_from="u32")]
    pub chunk_size: usize, 
    pub local_path: String
}

impl SectorTableRow {
    fn into_meta(self, node: &SectorsNode) -> SectorMeta {
        SectorMeta {
            sector_id: SectorFullId::from((node.node_id(), self.sector_id)), 
            capacity: self.capacity,
            chunk_size: self.chunk_size, 
            local_path: self.local_path
        }
    }
}

impl std::fmt::Display for SectorsNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SectorsNode {{node_id={}}}", self.node_id())
    }
}

impl SectorsNode {
    fn node_id(&self) -> SectorNodeId {
        self.0.config.node_id
    }

    fn host(&self) -> &str {
        &self.0.config.listen_address
    }

    fn config(&self) -> &SectorsNodeConfig {
        &self.0.config
    }

    fn journal_client(&self) -> &JournalClient {
        &self.0.journal_client
    }

    fn sql_pool(&self) -> &sqlx::Pool<sqlx::MySql> {
        &self.0.sql_pool
    }

    fn sector_table_name(&self) -> String {
        format!("sector_node_{}", self.node_id())
    }

    fn sql_create_table_sector(&self) -> String {
        format!("CREATE TABLE IF NOT EXISTS `{}`", self.sector_table_name()) + r#"(
            `sector_id` INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
            `local_path` VARCHAR(512) NOT NULL UNIQUE,  
            `capacity` BIGINT UNSIGNED NOT NULL, 
            `chunk_size` INT UNSIGNED NOT NULL  
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"#
    }
}



impl SectorsNode {
    pub async fn get(&self, filter: SectorFilter, navigator: SectorNavigator) -> DmcResult<Vec<Sector>> {
        debug!("{} get sector, filter={:?}, navigator={:?}", self, filter, navigator);
        if navigator.page_size == 0 {
            debug!("{} get sector, returned, filter={:?}, navigator={:?}, results=0", self, filter, navigator);
            return Ok(vec![])
        }
        
        if let Some(sector_id) = filter.sector_id {
            let sector_id = SectorFullId::from(sector_id);
            if sector_id.node_id() != self.node_id() {
                return Err(dmc_err!(DmcErrorCode::TargetNotMatch, "{} try get sector from node({}), local is node({}), filter={:?}, navigator={:?}", self, sector_id.node_id(), self.node_id(), filter, navigator));
            }
            let row: Option<SectorTableRow> = sqlx::query_as(format!("SELECT * FROM {} WHERE sector_id=?", self.sector_table_name()).as_str())
                .bind(sector_id.local_id()).fetch_optional(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?;
            Ok(row.map(|row| vec![row.into_meta(self).into()]).unwrap_or(vec![]))
        } else {
            unimplemented!()
        }       
    }


    pub async fn add(&self, options: AddSectorOptions) -> DmcResult<SectorMeta> {
        info!("{} add, params={:?}", self, options);

        if let Some(node_id) = options.node_id.as_ref() {
            if *node_id != self.node_id() {
                return Err(dmc_err!(DmcErrorCode::TargetNotMatch, "{} try add sector to node({}), local is node({}), options={:?}", self, node_id, self.node_id(), options));
            }
        }

        let local_path = PathBuf::from(&options.local_path);

        if !local_path.exists() {
            info!("create path {}", local_path.display());
            let _ = std::fs::create_dir_all(local_path);
        }

        if sqlx::query(format!("INSERT INTO {} (local_path, capacity, chunk_size) VALUES (?, ?, ?)", self.sector_table_name()).as_str())
                .bind(&options.local_path).bind(options.capacity).bind(options.chunk_size as u32)
                .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} add failed, params={:?}, err=sql error {}", self, options, err))?
                .rows_affected() > 0 {
            let row: SectorTableRow = sqlx::query_as(format!("SELECT * FROM {} WHERE local_path=?", self.sector_table_name()).as_str())
                .bind(&options.local_path)
                .fetch_one(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} add failed, params={:?}, err=sql error {}", self, options, err))?;
            let sector = row.into_meta(self);
            let _ = self.journal_client().append(JournalEvent { sector_id: sector.sector_id.into(), order_id: None, event_type: JournalEventType::SectorCreated, event_params: None })
                .await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} add failed, params={:?}, err=sql error {}", self, options, err))?;
            info!("{} add success, params={:?}, sector={:?}", self, options, sector);
            Ok(sector)
        } else {
            Err(dmc_err!(DmcErrorCode::AlreadyExists, "{} add failed, params={:?}, err=local path already exists", self, options))
        }
    }

    pub async fn write_chunk<R: async_std::io::Read + Send + Unpin>(&self, source: R, navigator: WriteChunkNavigator) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} write chunk, params={:?}, update_at={}", self, navigator, update_at);

        let sector_id = SectorFullId::from(navigator.sector_id);
        if sector_id.node_id() != self.node_id() {
            return Err(dmc_err!(DmcErrorCode::TargetNotMatch, "{} try write chunk to node({}), local is node({}), navigator={:?}", self, sector_id.node_id(), self.node_id(), navigator));
        }

        let row: SectorTableRow = sqlx::query_as(format!("SELECT * FROM {} WHERE sector_id=?", self.sector_table_name()).as_str())
            .bind(sector_id.local_id()).fetch_one(self.sql_pool()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{}  write chunk, params={:?}, update_at={}, err={}", self, navigator, update_at, err))?;
        let tmp_path = Path::new(row.local_path.as_str()).join(format!("{}-{}-{}", navigator.sector_id, navigator.offset, update_at));
        let writen = {
            info!("{} write chunk,writing tmp file, params={:?}, update_at={}, tmp_path={:?}", self, navigator, update_at, tmp_path);
            let tmp_file = async_std::fs::OpenOptions::new().read(true).write(true).create(true).open(tmp_path.as_path()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{}  write chunk, params={:?}, update_at={}, err={}", self, navigator, update_at, err))?;
            async_std::io::copy(source, tmp_file).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{}  write chunk, params={:?}, update_at={}, err={}", self, navigator, update_at, err))?
        };
       
        if writen as usize != navigator.len {
            let _ = async_std::fs::remove_file(tmp_path.as_path()).await;
            Err(dmc_err!(DmcErrorCode::Failed, "{}  write chunk, params={:?}, update_at={}, err=writen mistach", self, navigator, update_at))
        } else {
            let chunk_path = Path::new(row.local_path.as_str()).join(format!("{}-{}", navigator.sector_id, navigator.offset));
            match async_std::fs::rename(tmp_path.as_path(), chunk_path.as_path()).await {
                Ok(_) => {
                    info!("{} write chunk, writen, params={:?}, update_at={}, path={:?}", self, navigator, update_at, chunk_path);
                    Ok(())
                },
                Err(err) => {
                    let _ = async_std::fs::remove_file(tmp_path).await;
                    Err(dmc_err!(DmcErrorCode::Failed, "{}  write chunk, params={:?}, update_at={}, err={}", self, navigator, update_at, err))
                }
            } 
        }
    }

    pub async fn read_chunk(&self, navigator: ReadChunkNavigator) -> DmcResult<(ChunkMeta, Option<async_std::fs::File>)> {
        let update_at = dmc_time_now();
        info!("{} read chunk, params={:?}, update_at={}", self, navigator, update_at);

        let sector_id = SectorFullId::from(navigator.sector_id);
        if sector_id.node_id() != self.node_id() {
            return Err(dmc_err!(DmcErrorCode::TargetNotMatch, "{} try read chunk from node({}), local is node({}), navigator={:?}", self, sector_id.node_id(), self.node_id(), navigator));
        }

        let row: SectorTableRow = sqlx::query_as(format!("SELECT * FROM {} WHERE sector_id=?", self.sector_table_name()).as_str())
            .bind(sector_id.local_id()).fetch_one(self.sql_pool()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{}  read chunk, params={:?}, update_at={}, err={}", self, navigator, update_at, err))?;
        let chunk_path = Path::new(row.local_path.as_str()).join(format!("{}-{}", navigator.sector_id, navigator.offset));
        if chunk_path.exists() {
            let chunk_file = async_std::fs::File::open(chunk_path.as_path()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{}  read chunk, params={:?}, update_at={}, err={}", self, navigator, update_at, err))?;
            let chunk_len = chunk_file.metadata().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{}  read chunk, params={:?}, update_at={}, err={}", self, navigator, update_at, err))?.len();
            Ok((ChunkMeta {
                sector_id: navigator.sector_id, 
                offset: navigator.offset, 
                len: chunk_len as usize
            }, Some(chunk_file)))
        
        } else {
            Ok((ChunkMeta {
                sector_id: navigator.sector_id, 
                offset: navigator.offset, 
                len: 0
            }, None))
        }
    }

    pub async fn clear_chunk(&self, navigator: ClearChunkNavigator) -> DmcResult<()> {
        let update_at = dmc_time_now();
        info!("{} clear chunk, params={:?}, update_at={}", self, navigator, update_at);

        let sector_id = SectorFullId::from(navigator.sector_id);
        if sector_id.node_id() != self.node_id() {
            return Err(dmc_err!(DmcErrorCode::TargetNotMatch, "{} try clear chunk from node({}), local is node({}), navigator={:?}", self, sector_id.node_id(), self.node_id(), navigator));
        }

        let row: SectorTableRow = sqlx::query_as(format!("SELECT * FROM {} WHERE sector_id=?", self.sector_table_name()).as_str())
            .bind(sector_id.local_id()).fetch_one(self.sql_pool()).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{}  read chunk, params={:?}, update_at={}, err={}", self, navigator, update_at, err))?;
        let chunk_path = Path::new(row.local_path.as_str()).join(format!("{}-{}", navigator.sector_id, navigator.offset));

        async_std::fs::remove_file(chunk_path).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} clear chunk failed, params={:?}, update_at={}, err={}", self, navigator, update_at, err))?;
            
        Ok(())
    }
}