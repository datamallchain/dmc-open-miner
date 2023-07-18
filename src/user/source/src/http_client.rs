use async_std::{sync::Arc};
use serde::{Serialize, Deserialize};
use tide::{Body, http::{Request, self, Url}};
use url_params_serializer::to_url_params;
use dmc_tools_common::*;
use crate::{
    types::*
};

#[derive(Serialize, Deserialize, Clone)]
pub struct SourceClientConfig {
    pub host: String,    
}


pub struct SourceIterator {
    client: SourceClient, 
    filter: SourceFilter, 
    page_size: usize, 
    next_page: Option<usize>
}

impl SourceIterator {
    pub fn new(client: SourceClient, filter: SourceFilter, page_size: usize) -> Self {
        Self {
            client, 
            filter, 
            page_size, 
            next_page: Some(0)
        }
    }

    pub async fn next_page(&mut self) -> DmcResult<Vec<DataSource>> {
        if self.next_page.is_none() {
            return Ok(vec![]);
        }
        let page_index = self.next_page.unwrap();
        let params = SourceFilterAndNavigator {
            filter: self.filter.clone(), 
            navigator: SourceNavigator {
                page_size: self.page_size, 
                page_index
            }
        };

        let url = Url::parse_with_params(&format!("http://{}/{}", self.client.config().host, "source"), to_url_params(params))?;
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;
        let sources: Vec<DataSource> = resp.body_json().await?;
        self.next_page = if sources.len() < self.page_size {
            None
        } else {
            Some(page_index + 1)
        };
        Ok(sources)
    }
}


struct ClientImpl {
    config: SourceClientConfig
}


#[derive(Clone)]
pub struct SourceClient(Arc<ClientImpl>);

impl SourceClient {
    fn config(&self) -> &SourceClientConfig {
        &self.0.config
    }
}


impl SourceClient {
    pub fn new(config: SourceClientConfig) -> DmcResult<Self> {
        Ok(Self(Arc::new(ClientImpl {
            config
        })))
    }

    pub async fn create(&self, options: CreateSourceOptions) -> DmcResult<DataSource> {
        let url = Url::parse(&format!("http://{}/{}", self.config().host, "source"))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&options)?);
        let mut resp = surf::client().send(req).await?;
        let contract_state = resp.body_json().await?;
        Ok(contract_state)
    }

    pub async fn sources(&self, filter: SourceFilter, page_size: usize) -> DmcResult<SourceIterator> {
        Ok(SourceIterator::new(self.clone(), filter, page_size))
    }

    pub async fn random_stub(&self, source_id: u64) -> DmcResult<Option<SourceStub>> {
        let url = Url::parse_with_params(&format!("http://{}/{}", self.config().host, "source/stub"), to_url_params(QuerySource {source_id}))?;
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;
        let stub = resp.body_json().await?;
        Ok(stub)
    }

    // pub async fn random_merkle_challenge(&self, source_id: u64) -> DmcResult<MerkleStubChallenge<MerkleStubSha256>> {
    //     let url = Url::parse_with_params(&format!("http://{}/{}", self.config().host, "source/merkle_stub"), to_url_params(QuerySource {source_id}))?;
    //     let req = Request::new(http::Method::Get, url);
    //     let mut resp = surf::client().send(req).await?;
    //     let stub = resp.body_json().await?;
    //     Ok(stub)
    // }

    pub async fn get_source_detail(&self, source_id: u64) -> DmcResult<SourceDetailState> {
        let url = Url::parse_with_params(&format!("http://{}/{}", self.config().host, "source/detail"), to_url_params(QuerySource {source_id}))?;
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;
        let stub = resp.body_json().await?;
        Ok(stub)
    }
}
