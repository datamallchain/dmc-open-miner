use log::*;
use std::{
    sync::Arc,
};
use async_std::{stream::StreamExt};
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use tide::{Request, Response, Body};

use dmc_tools_common::*;
#[cfg(feature = "sqlite")]
use sqlx_sqlite as sqlx_types;
#[cfg(feature = "mysql")]
use sqlx_mysql as sqlx_types;
use crate::{
    types::*
};



#[derive(Serialize, Deserialize, Clone)]
pub struct JournalServerConfig {
    pub host: String, 
    pub sql_url: String, 
}

struct ServerImpl {
    config: JournalServerConfig, 
    sql_pool: sqlx_types::SqlPool, 
}

#[derive(sqlx::FromRow)]
pub struct JournalLogRow {
    log_id: sqlx_types::U64, 
    source_id: sqlx_types::U64, 
    order_id: Option<sqlx_types::U64>, 
    event_type: sqlx_types::U16, 
    event_params: Option<String>, 
    timestamp: DateTime<Utc>
}

impl Into<JournalLog> for JournalLogRow {
    fn into(self) -> JournalLog {
        JournalLog {
            log_id: self.log_id as u64, 
            event: JournalEvent {
                source_id: self.source_id as u64, 
                order_id: self.order_id.map(|i| i as u64), 
                event_type: JournalEventType::try_from(self.event_type as u16).unwrap(), 
                event_params: self.event_params
            }, 
            timestamp: self.timestamp
        }
    }
}


#[derive(Clone)]
pub struct JournalServer(Arc<ServerImpl>);

impl std::fmt::Display for JournalServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserJournalServer")
    }
}

impl JournalServer {
    pub async fn listen(self) -> DmcResult<()> {
        let host = self.config().host.clone();        
        let mut http_server = tide::with_state(self);
    
        http_server.at("/journal").post(|mut req: Request<Self>| async move {
            let event = req.body_json().await?;
            let mut resp = Response::new(200);
            resp.set_body(Body::from_json(&req.state().append(event).await?)?);
            Ok(resp)
        });
    
        http_server.at("/journal").get(|req: Request<Self>| async move {
            debug!("{} http get {}", req.state(), req.url());

            let mut query = JournalFilterAndNavigator::default();
            for (key, value) in req.url().query_pairs() {
                match &*key {
                    "event_type" => {
                        let event_type = u16::from_str_radix(&*value, 10)
                            .map_err(|err| {
                                debug!("{} http get {}, value={}, err={}", req.state(), req.url(), &*value, err);
                                err
                            })?.try_into().map_err(|err| {
                                debug!("{} http get {}, value={}, err={}", req.state(), req.url(), &*value, err);
                                err
                            })?;
                        if query.filter.event_type.is_none() {
                            query.filter.event_type = Some(vec![event_type]);
                        } else {
                            query.filter.event_type.as_mut().unwrap().push(event_type);
                        }
                    },
                    "source_id" => {
                        query.filter.source_id = Some(u64::from_str_radix(&*value, 10)
                            .map_err(|err| {
                                debug!("{} http get {}, err={}", req.state(), req.url(), err);
                                err
                            })?);
                    }
                    "order_id" => {
                        query.filter.order_id = Some(u64::from_str_radix(&*value, 10)
                            .map_err(|err| {
                                debug!("{} http get {}, err={}", req.state(), req.url(), err);
                                err
                            })?);
                    }
                    "page_size" => {
                        query.navigator.page_size = usize::from_str_radix(&*value, 10)
                            .map_err(|err| {
                                debug!("{} http get {}, err={}", req.state(), req.url(), err);
                                err
                            })?;
                    },
                    "from_id" => {
                        query.navigator.from_id = Some(u64::from_str_radix(&*value, 10) .map_err(|err| {
                            debug!("{} http get {}, err={}", req.state(), req.url(), err);
                            err
                        })?);
                    }, 
                    _ => {}
                }
            }
            
            // let query = req.query::<ContractFilterAndNavigator>()?;
            
            let mut resp = Response::new(200);
            let logs = req.state().get(query.filter, query.navigator).await?;
            resp.set_body(Body::from_json(&logs)?);
            Ok(resp)
        });

        let _ = http_server.listen(host.as_str()).await?;

        Ok(())
    }

    pub async fn new(config: JournalServerConfig) -> DmcResult<Self> {
        let sql_pool = sqlx_types::connect_pool(&config.sql_url).await?;
        Ok(Self(Arc::new(ServerImpl {
            config, 
            sql_pool,  
        })))
    }

    pub async fn init(&self) -> DmcResult<()> {
        let _ = sqlx::query(&Self::sql_create_table_journal_log()).execute(&self.0.sql_pool).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} init failed, err={}", self, err))?;
        Ok(())
    }

    pub async fn reset(self) -> DmcResult<Self> {
        self.init().await?;
        let _ = sqlx::query("DELETE FROM journal_log WHERE log_id >= 0").execute(&self.0.sql_pool).await?;
        Ok(self)
    }
}



impl JournalServer {
    fn config(&self) -> &JournalServerConfig {
        &self.0.config
    }

    fn sql_pool(&self) -> &sqlx_types::SqlPool {
        &self.0.sql_pool
    }

    #[cfg(feature = "mysql")]
    fn sql_create_table_journal_log() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS `journal_log` (
            `log_id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
            `source_id` BIGINT UNSIGNED NOT NULL, 
            `order_id` BIGINT UNSIGNED, 
            `event_type` TINYINT UNSIGNED NOT NULL,  
            `event_params` TEXT, 
            `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#
    }

    #[cfg(feature = "sqlite")]
    fn sql_create_table_journal_log() -> &'static str {
        r#"CREATE TABLE IF NOT EXISTS journal_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT, 
            source_id INTEGER NOT NULL, 
            order_id INTEGER, 
            event_type INTEGER NOT NULL,  
            event_params TEXT, 
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );"#
    }
}

impl JournalServer {
    pub async fn append(&self, event: JournalEvent) -> DmcResult<JournalLog> {
        let update_at = dmc_time_now();
        info!("{} append, event={:?}, update_at={}", self, event, update_at);
        // order still waiting 
        let result = sqlx::query("INSERT INTO journal_log (source_id, order_id, event_type, event_params) VALUES (?, ?, ?, ?)")
            .bind(event.source_id as sqlx_types::U64).bind(event.order_id.map(|i| i as sqlx_types::U64)).bind(event.event_type as u16 as sqlx_types::U16).bind(event.event_params.as_ref())
            .execute(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} append, event={:?}, update_at={}, err={}", self, event, update_at, err))?;

        let log: JournalLog = sqlx::query_as::<_, JournalLogRow>(&format!("SELECT * FROM journal_log WHERE {}=?", sqlx_types::rowid_name("log_id"))).bind(sqlx_types::last_inersert(&result))
            .fetch_one(self.sql_pool()).await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} append, event={:?}, update_at={}, err={}", self, event, update_at, err))?
            .into();

        info!("{} append, event={:?}, update_at={}, finished, log={:?}", self, event, update_at, log);
        Ok(log)
    }


    pub async fn get(&self, filter: JournalFilter, navigator: JournalNavigator) -> DmcResult<Vec<JournalLog>> {
        let when = dmc_time_now();
        debug!("{} get log, filter={:?}, navigator={:?}, when={}", self, filter, navigator, when);
        if navigator.page_size == 0 {
            debug!("{} get log,  returned, filter={:?}, navigator={:?}, results=0", self, filter, navigator);
            return Ok(vec![])
        }
        
        let query_prefix = "SELECT * FROM journal_log WHERE ";

        let mut query_conditions = vec![];
        if let Some(events) = filter.event_type.as_ref() {
            for event in events {
                query_conditions.push(format!("event_type={}", *event as u16));
            }
        }

        if let Some(source_id) = filter.source_id {
            query_conditions.push(format!("source_id={}", source_id))
        }

        if let Some(order_id) = filter.order_id {
            query_conditions.push(format!("order_id={}", order_id))
        }

        if let Some(from_id) = navigator.from_id {
            query_conditions.push(format!("log_id>{}", from_id));
        }

        let query_str = format!("{} {} LIMIT {}", query_prefix, query_conditions.join(" AND "), navigator.page_size);

        let mut stream = sqlx::query_as::<_, JournalLogRow>(&query_str).fetch(self.sql_pool());
        let mut logs = vec![];
        loop {
            if let Some(log) = stream.next().await {
                let log = log.map(|l| l.into()).map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get log,  returned, filter={:?}, navigator={:?}, err={}", self, filter, navigator, err))?;
                logs.push(log);
            } else {
                break;
            }
        }
        debug!("{} get log,  returned, filter={:?}, navigator={:?}, results={}", self, filter, navigator, logs.len());
        Ok(logs)
    }
}