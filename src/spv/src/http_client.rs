use async_std::sync::Arc;
use log::{error, info};
use serde::{Serialize, Deserialize};
use tide::{http::{Request, self, Url}};
use url_params_serializer::to_url_params;
use dmc_tools_common::*;
use crate::{
    types::*
};

pub struct BillIterator {
    client: SpvClient, 
    filter: FindBillFilter, 
    page_size: usize, 
    next_page: Option<usize>
}

impl BillIterator {
    pub fn new(client: SpvClient, filter: FindBillFilter, page_size: usize) -> Self {
        Self {
            client, 
            filter, 
            page_size, 
            next_page: Some(0)
        }
    }

    pub async fn next_page(&mut self) -> DmcResult<Vec<DmcBill>> {
        if self.next_page.is_none() {
            return Ok(vec![]);
        }
        let page_index = self.next_page.unwrap();
        let params = FindBillFilterAndNavigator {
            filter: self.filter.clone(), 
            navigator: FindBillNavigator {
                page_size: self.page_size, 
                page_index
            }
        };

        let url = Url::parse_with_params (&format!("{}bills", self.client.config().host), to_url_params(params))?;
        info!("spv client request {}", url.to_string());
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;
        let bills: Vec<DmcBill> = resp.body_json().await?;
        self.next_page = if bills.len() < self.page_size {
            None
        } else {
            Some(page_index + 1)
        };
        Ok(bills)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SpvClientConfig {
    pub host: Url,
}

struct ClientImpl {
    config: SpvClientConfig
}

#[derive(Clone)]
pub struct SpvClient(Arc<ClientImpl>);

impl SpvClient {
    fn config(&self) -> &SpvClientConfig {
        &self.0.config
    }
}

impl SpvClient {
    pub fn new(config: SpvClientConfig) -> DmcResult<Self> {
        let mut config = config;
        config.host.path_segments_mut().unwrap().pop_if_empty().push("");
        Ok(Self(Arc::new(ClientImpl {
            config
        })))
    }

    pub async fn bills(&self, filter: FindBillFilter, page_size: usize) -> DmcResult<BillIterator> {
        Ok(BillIterator::new(self.clone(), filter, page_size))
    }

    pub async fn add_bill(&self, bill: DmcBill) -> DmcResult<()> {
        let resp = surf::get(format!("{}add", &self.0.config.host)).query(&bill).map_err(|e| {
            error!("seralize dmc bill err {}", e);
            DmcError::new(DmcErrorCode::InvalidParam, e.to_string())
        })?.await.map_err(|e| {
            error!("send add bill request err {}", e);
            DmcError::new(DmcErrorCode::NetworkError, e.to_string())
        })?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(DmcError::new(DmcErrorCode::Failed, format!("status code {}", resp.status().to_string())))
        }
    }
}

