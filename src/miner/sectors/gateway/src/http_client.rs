use log::*;
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use tide::{
    http::{self, Request, Url},
    Body,
};
use dmc_tools_common::*;
use crate::types::*;

#[derive(Clone, Serialize, Deserialize)]
pub struct GatewaySectorAdminConfig {
    pub endpoint: Url,
}

struct ClientImpl {
    config: GatewaySectorAdminConfig,
}

#[derive(Clone)]
pub struct GatewaySectorAdmin(Arc<ClientImpl>);


impl GatewaySectorAdmin {
    fn config(&self) -> &GatewaySectorAdminConfig {
        &self.0.config
    }
}

impl GatewaySectorAdmin {
    pub fn new(config: GatewaySectorAdminConfig) -> DmcResult<Self> {
        let mut config = config;
        config.endpoint.path_segments_mut().unwrap().pop_if_empty().push("");
        Ok(Self(Arc::new(ClientImpl { config })))
    }

     pub async fn register_node(&self, info: SectorNodeInfo) -> DmcResult<()> {
        info!(
            "registering node {:?} to gateway {}",
            info,
            self.config().endpoint
        );

        let url = Url::parse(&format!("{}node", self.config().endpoint))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&info)?);
        let _ = surf::client().send(req).await?;

        // let mut v = vec![];
        // resp.read_to_end(&mut v).await?;

        info!(
            "registering node {:?} to gateway {} success.",
            info,
            self.config().endpoint
        );

        Ok(())
    }

    pub async fn list_node(&self) -> DmcResult<Vec<SectorNodeInfo>> {
        let mut resp = surf::get(&format!("{}node", self.config().endpoint)).await?;

        Ok(resp.body_json().await?)
    }
}




