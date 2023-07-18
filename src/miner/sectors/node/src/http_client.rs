use async_std::sync::Arc;
use serde::{Serialize, Deserialize};
use tide::{Body, http::{Request, self, Url}};
use url_params_serializer::to_url_params;
use dmc_tools_common::*;
use dmc_miner_sectors_client::*;
use crate::{
    types::*
};

#[derive(Clone, Serialize, Deserialize)]
pub struct SectorAdminConfig {
    pub endpoint: Url
}

struct ClientImpl {
    config: SectorAdminConfig
}

#[derive(Clone)]
pub struct SectorAdmin(Arc<ClientImpl>);


pub struct SectorMetaIterator {
    client: SectorAdmin, 
    filter: SectorFilter, 
    page_size: usize, 
    next_page: Option<usize>
}

impl SectorMetaIterator {
    fn new(client: SectorAdmin, filter: SectorFilter, page_size: usize) -> Self {
        Self {
            client, 
            filter, 
            page_size, 
            next_page: Some(0)
        }
    }

    pub async fn next_page(&mut self) -> DmcResult<Vec<Sector>> {
        if self.next_page.is_none() {
            return Ok(vec![]);
        }
        let page_index = self.next_page.unwrap();
        let params = SectorFilterAndNavigator {
            filter: self.filter.clone(), 
            navigator: SectorNavigator {
                page_size: self.page_size, 
                page_index
            }
        };

        let url = Url::parse_with_params (&format!("{}sector", self.client.config().endpoint), to_url_params(params))?;
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;
        let sectors: Vec<Sector> = resp.body_json().await?;
        self.next_page = if sectors.len() < self.page_size {
            None
        } else {
            Some(page_index + 1)
        };
        Ok(sectors)
    }
}


impl SectorAdmin {
    fn config(&self) -> &SectorAdminConfig {
        &self.0.config
    }
}


impl SectorAdmin {
    pub fn new(config: SectorAdminConfig) -> DmcResult<Self> {
        let mut config = config;
        config.endpoint.path_segments_mut().unwrap().pop_if_empty().push("");
        Ok(Self(Arc::new(ClientImpl {
            config
        })))
    }

    pub async fn get(&self, filter: SectorFilter, page_size: usize) -> DmcResult<SectorMetaIterator> {
        Ok(SectorMetaIterator::new(self.clone(), filter, page_size))
    }

    pub async fn add(&self, options: AddSectorOptions) -> DmcResult<SectorMeta> {
        let url = Url::parse(&format!("{}sector", self.config().endpoint))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&options)?);
        let mut resp = surf::client().send(req).await?;
        let sector = resp.body_json().await?;
        Ok(sector)
    }
}
