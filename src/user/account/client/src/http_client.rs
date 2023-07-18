use async_std::sync::Arc;
use serde::{Serialize, Deserialize};
use tide::{Body, http::{Request, self, Url}};
use url_params_serializer::to_url_params;
use dmc_tools_common::*;
use crate::{
    types::*
};



pub struct SectorIterator {
    client: AccountClient, 
    filter: SectorFilter, 
    page_size: usize, 
    next_page: Option<usize>
}

impl SectorIterator {
    pub fn new(client: AccountClient, filter: SectorFilter, navigator: SectorNavigator) -> Self {
        Self {
            client, 
            filter, 
            page_size: navigator.page_size, 
            next_page: Some(navigator.page_index)
        }
    }

    pub async fn next_page(&mut self) -> DmcResult<Vec<SectorWithId>> {
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

        let url = Url::parse_with_params (&format!("http://{}/{}", self.client.config().host, "account/sector"), to_url_params(params))?;
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;
        let sectors: Vec<SectorWithId> = resp.body_json().await?;
        self.next_page = if sectors.len() < self.page_size {
            None
        } else {
            Some(page_index + 1)
        };
        Ok(sectors)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AccountClientConfig {
    pub host: String, 
}

struct ClientImpl {
    config: AccountClientConfig
}

#[derive(Clone)]
pub struct AccountClient(Arc<ClientImpl>);

impl AccountClient {
    fn config(&self) -> &AccountClientConfig {
        &self.0.config
    }
}

impl AccountClient {
    pub fn new(config: AccountClientConfig) -> DmcResult<Self> {
        Ok(Self(Arc::new(ClientImpl {
            config
        })))
    }

    pub async fn create_sector(&self, options: CreateSectorOptions) -> DmcResult<SectorWithId> {
        let url = Url::parse(&format!("http://{}/{}", self.config().host, "account/sector"))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&options)?);
        let mut resp = surf::client().send(req).await?;
        let brief = resp.body_json().await?;
        Ok(brief)
    }

    pub async fn remove_sector(&self, _: u64) -> DmcResult<()> {
        unimplemented!()
    }

    pub async fn sectors(&self, filter: SectorFilter, navigator: SectorNavigator) -> DmcResult<SectorIterator> {
        Ok(SectorIterator::new(self.clone(), filter, navigator))
    }

    pub async fn account(&self) -> DmcResult<String> {
        let url = format!("http://{}/account/address", self.config().host);
        println!("use url {}", url);
        let mut resp = surf::get(url).await?;
        Ok(resp.body_string().await?)
    }
}

