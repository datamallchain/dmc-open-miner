pub mod sqlx_mysql {
    use crate::error::*;

    pub type SqlPool = sqlx::Pool<sqlx::MySql>;

    pub async fn connect_pool(sql_url: &str) -> DmcResult<SqlPool> {
        let pool = sqlx::mysql::MySqlPoolOptions::new().connect(sql_url).await?;
        Ok(pool)
    }

    pub fn rowid_name(primary: &str) -> &str {
        primary
    }

    pub fn last_inersert(result: &sqlx::mysql::MySqlQueryResult) -> u64 {
        result.last_insert_id()
    }

    pub type U8 = u8;

    pub type U16 = u16;

    pub type U32 = u32;

    pub type U64 = u64;

}


pub mod sqlx_sqlite {
    use crate::error::*;

    pub type SqlPool = sqlx::Pool<sqlx::Sqlite>;

    pub async fn connect_pool(sql_url: &str) -> DmcResult<SqlPool> {
        use std::str::FromStr;
        use sqlx::ConnectOptions;
        // creat if not exists
        let _ = sqlx::sqlite::SqliteConnectOptions::from_str(sql_url)?.create_if_missing(true).connect().await?;

        let pool = sqlx::sqlite::SqlitePoolOptions::new().connect(sql_url).await?;
        Ok(pool)
    }

    pub fn rowid_name(_: &str) -> &str {
        "ROWID"
    }

    pub fn last_inersert(result: &sqlx::sqlite::SqliteQueryResult) -> i64 {
        result.last_insert_rowid()
    }

    pub type U8 = i16;

    pub type U16 = i32;

    pub type U32 = i64;

    pub type U64 = i64;
}
