use log::*;
use serde::{Deserialize, Serialize};
use sqlx::{mysql::MySqlConnectOptions, ConnectOptions, Pool, Row};
use std::{str::FromStr, sync::Arc};
use tide::http::Url;
use tide::{Body, Request, Response};
use dmc_tools_common::*;
use dmc_miner_sectors_client::*;
use dmc_miner_sectors_node::*;
use crate::types::*;


#[derive(Clone, Serialize, Deserialize)]
pub struct SectorsGatewayConfig {
    pub listen_address: String,
    pub sql_url: String,
}

struct ServerImpl {
    config: SectorsGatewayConfig,
    sql_pool: Pool<sqlx::MySql>,
}

#[derive(Clone)]
pub struct SectorsGateway(Arc<ServerImpl>);

impl SectorsGateway {
    pub async fn listen(self) -> DmcResult<()> {
        let listen_address = self.config().listen_address.clone();
        let mut http_server = tide::with_state(self);

        http_server
            .at("/sector")
            .get(|req: Request<Self>| async move {
                debug!("{} http get sector, url={}", req.state(), req.url());
                let mut query = SectorFilterAndNavigator::default();
                for (key, value) in req.url().query_pairs() {
                    match &*key {
                        "sector_id" => {
                            query.filter.sector_id = Some(u64::from_str_radix(&*value, 10)?);
                        }
                        "page_size" => {
                            query.navigator.page_size = usize::from_str_radix(&*value, 10)?;
                        }
                        "page_index" => {
                            query.navigator.page_index = usize::from_str_radix(&*value, 10)?;
                        }
                        _ => {}
                    }
                }
                let mut resp = Response::new(200);
                let sectors = req.state().sectors(query.filter, query.navigator).await?;
                resp.set_body(Body::from_json(&sectors)?);
                Ok(resp)
            });

        http_server
            .at("/node")
            .post(|mut req: Request<Self>| async move {
                info!("registering node request received.");

                let options = req.body_json().await?;

                info!("registering node {:?} request received.", options);

                let resp = Response::new(200);

                let _ = req.state().register_node(&options).await?;

                info!("registering node {:?} request executed.", options);

                Ok(resp)
            });

        http_server
            .at("/node")
            .get(|req: Request<Self>| async move {
                Ok(Response::from(Body::from_json(
                    &req.state().list_nodes().await?,
                )?))
            });

        let _ = http_server.listen(listen_address.as_str()).await?;

        Ok(())
    }

    pub async fn new(config: SectorsGatewayConfig) -> DmcResult<Self> {
        let mut options = MySqlConnectOptions::from_str(&config.sql_url)?;
        options.log_statements(LevelFilter::Off);
        let sql_pool = sqlx::mysql::MySqlPoolOptions::new()
            .connect_with(options)
            .await?;
        Ok(Self(Arc::new(ServerImpl { config, sql_pool })))
    }

    pub async fn init(&self) -> DmcResult<()> {
        let _ = sqlx::query(&self.sql_create_table_nodes())
            .execute(self.sql_pool())
            .await?;
        Ok(())
    }

    pub async fn reset(&self) -> DmcResult<()> {
        self.init().await?;
        let _ = sqlx::query("DELETE FROM `nodes_gateway`")
            .execute(self.sql_pool())
            .await?;
        Ok(())
    }
}


impl std::fmt::Display for SectorsGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SectorsGateway")
    }
}

#[derive(sqlx::FromRow)]
struct NodesTableRow {
    node_id: SectorNodeId, 
    host: String
}

impl TryInto<SectorNodeInfo> for NodesTableRow {
    type Error = DmcError;

    fn try_into(self) -> DmcResult<SectorNodeInfo> {
        Ok(SectorNodeInfo {
            node_id: self.node_id, 
            endpoint: Url::parse(&self.host)?
        })
    }
}

impl SectorsGateway {
    fn host(&self) -> &str {
        &self.0.config.listen_address
    }

    fn config(&self) -> &SectorsGatewayConfig {
        &self.0.config
    }

    fn sql_pool(&self) -> &sqlx::Pool<sqlx::MySql> {
        &self.0.sql_pool
    }

    fn sql_create_table_nodes(&self) -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `nodes_gateway` (
            `node_id` INT UNSIGNED NOT NULL UNIQUE,
            `host` VARCHAR(512) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"#
    }
}

impl SectorsGateway {
    pub async fn sectors(&self, filter: SectorFilter, navigator: SectorNavigator) -> DmcResult<Vec<Sector>> {
        debug!("{} get sector, filter={:?}, navigator={:?}", self, filter, navigator);
        if navigator.page_size == 0 {
            debug!("{} get sector, returned, filter={:?}, navigator={:?}, results=0", self, filter, navigator);
            return Ok(vec![])
        }
        
        if let Some(sector_id) = filter.sector_id {
            let sector_id = SectorFullId::from(sector_id);
            let node = self.node_of(sector_id.node_id()).await
                .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, returned, filter={:?}, navigator={:?}, failed, err={}", self, filter, navigator, err))?;
            if let Some(node) = node {
                let node = SectorAdmin::new(SectorAdminConfig { endpoint: node.endpoint })
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, returned, filter={:?}, navigator={:?}, failed, err={}", self, filter, navigator, err))?;
                node.get(filter.clone(), 1).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, returned, filter={:?}, navigator={:?}, failed, err={}", self, filter, navigator, err))?
                    .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get sector, returned, filter={:?}, navigator={:?}, failed, err={}", self, filter, navigator, err))
            } else {
                debug!("{} get sector, returned, filter={:?}, navigator={:?}, results=0", self, filter, navigator);
                return Ok(vec![])
            }
        } else {
            unimplemented!()
        }       
    }

    async fn node_of(&self, node_id: SectorNodeId) -> DmcResult<Option<SectorNodeInfo>> {
        let row = sqlx::query_as::<_, NodesTableRow>("SELECT * FROM nodes_gateway WHERE node_id=?").bind(node_id).fetch_optional(self.sql_pool())
            .await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} query node info node_id={}, failed, err={}", self, node_id, err))?;
        if let Some(row) = row {
            let info = row.try_into().map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} query node info node_id={}, failed, err={}", self, node_id, err))?;
            Ok(Some(info))
        } else {
            Ok(None)
        }
    }

    async fn list_nodes(&self) -> DmcResult<Vec<SectorNodeInfo>> {
        Ok(sqlx::query("select * from nodes_gateway")
            .fetch_all(self.sql_pool())
            .await?
            .iter()
            .map(|row| SectorNodeInfo {
                node_id: row.get("node_id"),
                endpoint: Url::parse(&row.get::<String, &str>("host")).unwrap(),
            })
            .collect())
    }

    async fn register_node(&self, options: &SectorNodeInfo) -> DmcResult<()> {
        info!("{} register sector, params={:?}", self, options);

        let result = sqlx::query("INSERT INTO nodes_gateway (node_id, host) VALUES (?, ?)")
            .bind(options.node_id)
            .bind(options.endpoint.as_str())
            .execute(self.sql_pool())
            .await
            .map_err(|err| {
                dmc_err!(
                    DmcErrorCode::Failed,
                    "{} add node host failed, params={:?}, err=sql error {}",
                    self,
                    options,
                    err
                )
            })?;

        if result.rows_affected() > 0 {
            let gateway_sector_id = result.last_insert_id();
            info!(
                "{} add node success, params={:?}, gateway-sector={:?}",
                self, options, gateway_sector_id
            );
        } else {
            warn!(
                "{} add node failed, params={:?}, err=local path already exists",
                self, options
            );
        }
        Ok(())
    }
}
