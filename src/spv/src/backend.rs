use std::collections::HashMap;
use std::str::FromStr;
use log::*;
use std::sync::Arc;
use std::time::Duration;
use async_std::prelude::StreamExt;
use serde::{Serialize, Deserialize};
use tide::{
    http::headers::HeaderValue, 
    {Request, Response},
    security::CorsMiddleware
};
use sqlx::{Row, Executor, mysql::{MySqlRow}, ConnectOptions};
use sqlx::mysql::MySqlConnectOptions;
use crate::types::FindBillFilter;
use dmc_tools_common::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpvServerConfig {
    pub host: String,
    pub sql_url: String
} 

struct ServerImpl<T: DmcChainClient> {
    config: SpvServerConfig, 
    chain_client: T,
    sql_pool: sqlx::Pool<sqlx::MySql>,
}

#[derive(Clone)]
pub struct SpvServer<T: DmcChainClient>(Arc<ServerImpl<T>>);

impl<T: DmcChainClient> SpvServer<T> {
    pub async fn new(chain_client: T, config: SpvServerConfig) -> DmcResult<Self> {
        let mut options =MySqlConnectOptions::from_str(&config.sql_url)?;
        options.log_statements(LevelFilter::Off);
        let sql_pool = sqlx::mysql::MySqlPoolOptions::new().connect_with(options).await?;

        let _ = sqlx::query(CREATE_CONFIG_TABLE).execute(&sql_pool).await?;
        let _ = sqlx::query(CREATE_BILLS_TABLE).execute(&sql_pool).await?;
        
        Ok(Self(Arc::new(ServerImpl {
                config, 
                chain_client, 
                sql_pool
        })))
    }

    pub async fn reset(self) -> DmcResult<Self> {
        let mut trans = self.sql_pool().begin().await?;
        let _ = sqlx::query(CREATE_CONFIG_TABLE).execute(&mut trans).await?;
        let _ = sqlx::query(CREATE_BILLS_TABLE).execute(&mut trans).await?;
        trans.commit().await?;
        Ok(self)
    }

    pub async fn listen(self) -> DmcResult<()> {
        {
            let server = self.clone();
            async_std::task::spawn(async move {
                let _ = server.listen_chain().await;
            });
        }
        let host = self.config().host.clone(); 
        let mut http_server = tide::with_state(self);
        let cors = CorsMiddleware::new()
            .allow_methods(
                "GET, POST, PUT, DELETE, OPTIONS"
                    .parse::<HeaderValue>()
                    .unwrap(),
            )
            .allow_origin("*")
            .allow_credentials(true)
            .allow_headers("*".parse::<HeaderValue>().unwrap())
            .expose_headers("*".parse::<HeaderValue>().unwrap());
        http_server.with(cors);

        
        // /bills?miner=&min_price=&max_price......
        http_server.at("/bills").get(|req: Request<Self>| async move {
            let filter = req.query::<FindBillFilter>()?;
            let ret = req.state().find_bill(filter).await;
            let mut resp = Response::new(tide::http::StatusCode::Ok);
            resp.set_body(serde_json::to_value(ret).unwrap());
            Ok(resp)
        });
        // /add
        http_server.at("/add").get(|req: Request<Self>| async move {
            let bill = req.query::<DmcBill>()?;
            req.state().save_bill(bill).await;
            Ok(Response::new(tide::http::StatusCode::Ok))
        });

        let _ = http_server.listen(host.as_str()).await?;
        Ok(())
    }
}

impl<T: DmcChainClient> SpvServer<T> {
    fn chain_client(&self) -> &T {
        &self.0.chain_client
    }

    fn config(&self) -> &SpvServerConfig {
        &self.0.config
    }

    fn sql_pool(&self) -> &sqlx::Pool<sqlx::MySql> {
        &self.0.sql_pool
    }

}

const CREATE_BILLS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS `bills` (
  `bill_id` INT UNSIGNED NOT NULL,
  `miner` VARCHAR(45) NOT NULL,
  `asset` BIGINT UNSIGNED NOT NULL,
  `price` BIGINT UNSIGNED NOT NULL,
  `pledge_rate` INT UNSIGNED NOT NULL,
  `min_asset` INT UNSIGNED NOT NULL,
  `expire_at` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`bill_id`),
  UNIQUE INDEX `bill_id_UNIQUE` (`bill_id` ASC) VISIBLE);
"#;

const CREATE_CONFIG_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS `config` (
  `key` VARCHAR(45) NOT NULL,
  `value` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`key`));
"#;

const INSERT_INTO_BILL: &str = r#"
INSERT INTO bills values (?, ?, ?, ?, ?, ?, ?) as t ON DUPLICATE KEY UPDATE asset=t.asset;
"#;

fn into_dmc_bill(row: MySqlRow) -> DmcBill {
    DmcBill {
        bill_id: row.get("bill_id"),
        miner: row.get("miner"),
        asset: row.get("asset"),
        price: row.get("price"),
        pledge_rate: row.get("pledge_rate"),
        min_asset: row.get("min_asset"),
        expire_at: row.get("expire_at")
    }
}

fn check_first(sql: &mut String, first: &mut bool) {
    if *first {
        *first = false;
        sql.push_str(" where ");
    } else {
        sql.push_str(" and ");
    }
}

impl<T: DmcChainClient> SpvServer<T> { 
    async fn get_config(&self, key: &str, default: &str) -> String {
        let row = sqlx::query("select value from config where 'key'=?")
            .bind(key).fetch_one(self.sql_pool()).await.unwrap();
        row.try_get(0).unwrap_or(default.to_owned())
    }

    async fn set_config(&self, key: &str, value: &str) {
        let sql = "insert into config values (?, ?) as t ON DUPLICATE KEY UPDATE value=t.value";
        self.sql_pool().execute(sqlx::query(sql).bind(key).bind(value)).await.unwrap();
        info!("set config {} to {}", key, value);
    }

    async fn save_bill(&self, bill: DmcBill) {
        let _ = sqlx::query(INSERT_INTO_BILL)
            .bind(bill.bill_id)
            .bind(&bill.miner)
            .bind(bill.asset)
            .bind(bill.price)
            .bind(bill.pledge_rate)
            .bind(bill.min_asset)
            .bind(bill.expire_at)
            .execute(self.sql_pool()).await.map_err(|e| {
                error!("insert into bill err {}", e);
                e
            });
    }

    async fn remove_bill(&self, bill_id: u64) {
        let _ = sqlx::query("delete from bills where bill_id = ?")
            .bind(bill_id)
            .execute(self.sql_pool()).await.map_err(|e| {
                error!("delete from bill err {}", e);
                e
            });
    }

    async fn get_bill(&self, bill_id: u64) -> DmcResult<DmcBill> {
        Ok(into_dmc_bill(sqlx::query("select * from bills where bill_id=?").bind(bill_id).fetch_one(self.sql_pool()).await?))
    }

    async fn find_bill(&self, filter: FindBillFilter) -> Vec<DmcBill> {
        let mut select_sql = "select * from bills".to_owned();
        let mut first = true;
        if filter.miner.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("miner = ?");
        }
        if filter.min_asset.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("asset >= ?");
        }
        if filter.max_asset.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("asset < ?");
        }
        if filter.min_price.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("price >= ?");
        }
        if filter.max_price.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("price < ?");
        }
        if filter.min_pledge_rate.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("pledge_rate >= ?");
        }
        if filter.max_pledge_rate.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("pledge_rate < ?");
        }
        if filter.min_min_asset.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("min_asset >= ?");
        }
        if filter.max_min_asset.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("min_asset < ?");
        }
        if filter.expire_after.is_some() {
            check_first(&mut select_sql, &mut first);
            select_sql.push_str("expire_at > ?");
        }

        select_sql.push_str(" ORDER BY expire_at");

        let mut query = sqlx::query(&select_sql);
        if let Some(miner) = filter.miner {
            query = query.bind(miner.clone());
        }
        if let Some(asset) = filter.min_asset {
            query = query.bind(asset);
        }
        if let Some(asset) = filter.max_asset {
            query = query.bind(asset);
        }
        if let Some(price) = filter.min_price {
            query = query.bind(price);
        }
        if let Some(price) = filter.max_price {
            query = query.bind(price);
        }
        if let Some(pledge_rate) = filter.min_pledge_rate {
            query = query.bind(pledge_rate);
        }
        if let Some(pledge_rate) = filter.max_pledge_rate {
            query = query.bind(pledge_rate);
        }
        if let Some(min_asset) = filter.min_min_asset {
            query = query.bind(min_asset);
        }
        if let Some(min_asset) = filter.max_min_asset {
            query = query.bind(min_asset);
        }
        if let Some(expire_after) = filter.expire_after {
            query = query.bind(expire_after);
        }

        let mut result = vec![];
        let mut stream = self.sql_pool().fetch(query);

        info!("will fetch bill from spv, use sql: {}", &select_sql);
        while let Some(Ok(row)) = stream.next().await {
            result.push(into_dmc_bill(row))
        }

        result
    }

    async fn get_all_bills(&self) -> DmcResult<Vec<u64>> {
        let mut bills = vec![];
        let rows = sqlx::query("select bill_id from bills").fetch_all(self.sql_pool()).await?;
        for row in rows {
            if let Ok(bill_id) = row.try_get(0) {
                bills.push(bill_id);
            }
        }

        Ok(bills)
    }

    async fn listen_chain(&self) {
        info!("start monitor");

        let start_block = if let Ok(row) = sqlx::query("select value from config where `key`=?")
            .bind("event_block").fetch_one(self.sql_pool()).await {
            let str: String = row.try_get(0).unwrap();
            Some(str.parse::<u64>().unwrap())
        } else {
            None
        };

        #[cfg(feature = "eos")]
        let spv = self.clone();
        async_std::task::spawn(async move {
            let mut bill_cache = HashMap::new();
            let mut timer = async_std::stream::interval(Duration::from_secs(60));
            while let Some(_) = timer.next().await {
                if let Ok(bills) = spv.get_all_bills().await {
                    for bill_id in bills {
                        if let Ok(bill) = spv.chain_client().get_bill_by_id(bill_id).await {
                            if let Some(bill) = bill {
                                let cached_bill = bill_cache.entry(bill_id).or_insert_with(||{
                                    async_std::task::block_on(async {
                                        info!("load bill {}", bill_id);
                                        spv.get_bill(bill_id).await.unwrap()
                                    })
                                });
                                if cached_bill.asset != bill.asset {
                                    info!("update bill {} asset from {} to {}", bill_id, cached_bill.asset, bill.asset);
                                    cached_bill.asset = bill.asset;
                                    spv.save_bill(bill).await;
                                }
                            } else {
                                info!("delete bill {}", bill_id);
                                spv.remove_bill(bill_id).await;
                                bill_cache.remove(&bill_id);
                            }
                        }
                    }
                }
            }
        });
        
        let listener = self.chain_client().event_listener(start_block).await;
        let mut block_number = 0;
        while let Ok(event) = listener.next().await {
            match event.event {
                DmcTypedEvent::BillChanged(bill) => {
                    info!("recv bill {} changed event at block {} tx {}", bill.bill_id, event.block_number, event.tx_index);
                    self.save_bill(bill).await;
                }
                DmcTypedEvent::BillCanceled(bill_id) => {
                    info!("recv bill {} cancel event at block {} tx {}", bill_id, event.block_number, event.tx_index);
                    self.remove_bill(bill_id).await;
                }
                _ => {}
            }
            if block_number != event.block_number {
                self.set_config("event_block", &event.block_number.to_string()).await;
                block_number = event.block_number;
            }
        }
        error!("listener next err!, stop monitor");
    }
}