use log::*;
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
    pub fn new(client: AccountClient, filter: SectorFilter, page_size: usize) -> Self {
        Self {
            client, 
            filter, 
            page_size, 
            next_page: Some(0)
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

        let url = Url::parse_with_params(&format!("{}account/sector", self.client.config().endpoint), to_url_params(params))?;
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
    pub endpoint: Url,
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
        let mut config = config;
        config.endpoint.path_segments_mut().unwrap().pop_if_empty().push("");
        Ok(Self(Arc::new(ClientImpl {
            config
        })))
    }

    pub async fn name(&self) -> DmcResult<String> {
        let mut resp = surf::get(format!("{}account/name",self.config().endpoint).replace("//", "/")).await?;
        Ok(resp.body_string().await?)
    }

    pub async fn set_method(&self, method: String) -> DmcResult<()> {
        let resp = surf::post(format!("{}account/method",self.config().endpoint)).body_string(method).await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(DmcError::new(DmcErrorCode::Failed, format!("account server err status code {}", resp.status().to_string())))
        }
    }

    pub async fn get_method(&self) -> DmcResult<String> {
        let mut resp = surf::get(format!("{}account/method",self.config().endpoint)).await?;
        Ok(resp.body_string().await?)
    }

    pub async fn get_bill(&self, bill_id: u64) -> DmcResult<Option<DmcBill>> {
        let mut resp = surf::get(format!("{}account/bill?bill_id={}", self.config().endpoint, bill_id)).await?;
        let mut bill = resp.body_json::<Vec<DmcBill>>().await?;
        Ok(bill.pop())
    }

    pub async fn create_sector(&self, options: CreateSectorOptions) -> DmcResult<SectorWithId> {
        let url = Url::parse(&format!("{}account/sector", self.config().endpoint))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&options)?);
        let mut resp = surf::client().send(req).await?;
        let brief = resp.body_json().await?;
        Ok(brief)
    }

    pub async fn remove_sector(&self, options: RemoveSectorOptions) -> DmcResult<()> {
        let url = Url::parse(&format!("{}account/sector", self.config().endpoint))?;
        let mut req = Request::new(http::Method::Delete, url);
        req.set_body(Body::from_json(&options)?);
        let mut resp = surf::client().send(req).await.map_err(|err| {
            error!("send request to {} failed, err: {}.", self.config().endpoint, err);
            err
        })?;
        let _: SectorWithId = resp.body_json().await?;
        Ok(())
    }

    pub async fn sectors(&self, filter: SectorFilter, page_size: usize) -> DmcResult<SectorIterator> {
        Ok(SectorIterator::new(self.clone(), filter, page_size))
    }

    pub async fn get_available_assets(&self) -> DmcResult<Vec<DmcAsset>> {
        let mut resp = surf::get(format!("{}account/assets", self.config().endpoint)).await?;
        let assets = resp.body_json::<Vec<DmcAsset>>().await?;
        Ok(assets)
    }
}

