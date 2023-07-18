use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ServerConfig {
    pub sql_url: String,
    pub account_name: String,
    pub account_private_key: String,
    #[serde(default)]
    pub permission: String
}