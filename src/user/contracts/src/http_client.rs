use log::*;
use async_std::{sync::Arc};
use serde::{Serialize, Deserialize};
use tide::{Body, http::{Request, self, Url}};
use url_params_serializer::to_url_params;
use dmc_tools_common::*;
use dmc_miner_contracts as miner;
use crate::{
    types::*
};

#[derive(Serialize, Deserialize, Clone)]
pub struct ContractClientConfig {
    pub host: String, 
}


pub struct ContractIterator {
    client: ContractClient, 
    filter: ContractFilter, 
    page_size: usize, 
    next_page: Option<usize>
}

impl ContractIterator {
    pub fn new(client: ContractClient, filter: ContractFilter, page_size: usize) -> Self {
        Self {
            client, 
            filter, 
            page_size, 
            next_page: Some(0)
        }
    }

    pub async fn next_page(&mut self) -> DmcResult<Vec<Contract>> {
        if self.next_page.is_none() {
            return Ok(vec![]);
        }
        let page_index = self.next_page.unwrap();
        let params = ContractFilterAndNavigator {
            filter: self.filter.clone(), 
            navigator: ContractNavigator {
                page_size: self.page_size, 
                page_index
            }
        };

        let url = Url::parse_with_params(&format!("http://{}/{}", self.client.config().host, "contract"), to_url_params(params))?;
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;
        let sectors: Vec<Contract> = resp.body_json().await?;
        self.next_page = if sectors.len() < self.page_size {
            None
        } else {
            Some(page_index + 1)
        };
        Ok(sectors)
    }
}


struct ClientImpl {
    config: ContractClientConfig
}


#[derive(Clone)]
pub struct ContractClient(Arc<ClientImpl>);

impl ContractClient {
    fn config(&self) -> &ContractClientConfig {
        &self.0.config
    }
}

impl std::fmt::Display for ContractClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserContractsClient {{host={}}}", self.config().host)
    }
}

impl ContractClient {
    pub fn new(config: ContractClientConfig) -> DmcResult<Self> {
        Ok(Self(Arc::new(ClientImpl {
            config
        })))
    }

    pub async fn get(&self, filter: ContractFilter, page_size: usize) -> DmcResult<ContractIterator> {
        Ok(ContractIterator::new(self.clone(), filter, page_size))
    }

    pub async fn apply(&self, options: ApplyContractOptions) -> DmcResult<()> {
        let url = Url::parse(&format!("http://{}/{}", self.config().host, "contract"))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&options)?);
        let _ = surf::client().send(req).await?;
        Ok(())
    }

    pub async fn challenge(&self, options: OnChainChallengeOptions) -> DmcResult<()> {
        let url = Url::parse(&format!("http://{}/{}", self.config().host, "contract/challenge"))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&options)?);
        let _ = surf::client().send(req).await?;
        Ok(())
    }

    pub async fn update(&self, options: UpdateContractOptions) -> DmcResult<()> {
        let url = Url::parse_with_params(&format!("http://{}/{}", self.config().host, "contract"), to_url_params(QueryContract {order_id: options.order.order_id}))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&options)?);
        let _ = surf::client().send(req).await?;
        Ok(())
    }

    pub async fn restore(&self, options: miner::RestoreContractOptions) -> DmcResult<impl async_std::io::Read + Send + Unpin> {
        let update_at = dmc_time_now();
        info!("{} restore, options={:?}, update_at={}", self, options, update_at);
        let contract = self.get(ContractFilter::from_order_id(options.navigator.order_id), 1).await
            .map_err(|err| {
                error!("{} restore, options={:?}, update_at={}, err={}", self, options, update_at, err);
                err
            })?.next_page().await.map_err(|err| {
                error!("{} restore, options={:?}, update_at={}, err={}", self, options, update_at, err);
                err
            })?.get(0).cloned().ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} restore, options={:?}, update_at={}, err={}", self, options, update_at, "order not found"))?;

        if let ContractState::Storing = &contract.state {
            let miner_client = miner::ContractClient::new(miner::ContractClientConfig {
                endpoint: Url::parse(&contract.miner_endpoint)?,
            })?;
            miner_client.restore(options).await
        } else {
            Err(dmc_err!(DmcErrorCode::ErrorState, "{} restore, options={:?}, update_at={}, err={}", self, options, update_at, "contract not storing"))
        }
    }
}
