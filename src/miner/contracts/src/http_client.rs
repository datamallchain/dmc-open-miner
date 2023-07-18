use log::*;
use async_std::{sync::Arc, io::ReadExt};
use serde::{Serialize, Deserialize};
use tide::{Body, http::{Request, self, Url}};
use url_params_serializer::to_url_params;
use dmc_tools_common::*;
use crate::{
    types::*
};

#[derive(Serialize, Deserialize, Clone)]
pub struct ContractClientConfig {
    pub endpoint: Url,
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

        let url = Url::parse_with_params(&format!("{}contract", self.client.config().endpoint), to_url_params(params))?;
        
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;
        let json_text = resp.body_string().await?;
        let sectors: Vec<Contract> = serde_json::from_str(&json_text).map_err(|e| {
            println!("decode json err {}, context: {}", e, &json_text);
            e
        })?;
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

impl std::fmt::Display for ContractClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MinerContractClient{{endpoint={}}}", self.config().endpoint)
    }
}

impl ContractClient {
    fn config(&self) -> &ContractClientConfig {
        &self.0.config
    }
}


impl ContractClient {
    pub fn new(config: ContractClientConfig) -> DmcResult<Self> {
        let mut config = config;
        config.endpoint.path_segments_mut().unwrap().pop_if_empty().push("");
        Ok(Self(Arc::new(ClientImpl {
            config
        })))
    }

    pub async fn update(&self, options: UpdateContractOptions) -> DmcResult<()> {
        let url = Url::parse(&format!("{}contract", self.config().endpoint))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&options)?);
        let _ = surf::client().send(req).await?;
        Ok(())
    }

    pub async fn get(&self, filter: ContractFilter, page_size: usize) -> DmcResult<ContractIterator> {
        Ok(ContractIterator::new(self.clone(), filter, page_size))
    }

    pub async fn write<R: async_std::io::Read + Send + Unpin, T: DmcChainAccountClient>(&self, source: R, options: WriteContractOptions, chain_client: &T) -> DmcResult<Contract> {
        let now = dmc_time_now();
        info!("{} write, contract={:?}, at={}", self, options, now);
        let mut source = source;
        let contract = self.get(ContractFilter::from_order_id(options.navigator.order_id), 1).await?
            .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, contract={:?}, at={}, err=get contract {}", self, options.navigator.order_id, now, err))?
            .get(0).ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} write, contract={:?}, at={}, err=contract not exists", self, options.navigator.order_id, now))?.clone();

        info!("{} write, miner returned, contract={:?}, at={}, miner_side={:?}", self, options.navigator.order_id, now, contract);
        if let ContractState::Writing { sector, writen } = &contract.state {
            if *writen < options.navigator.offset {
                Err(dmc_err!(DmcErrorCode::ErrorState, "{} write, contract={:?}, at={}, err=beyound offset", self, options.navigator.order_id, now))
            } else {
                let _ = async_std::io::copy(source.by_ref().take(*writen - options.navigator.offset), async_std::io::sink()).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, contract={:?}, at={}, err=read from source {}", self, options.navigator.order_id, now, err))?;
                debug!("{} write, skip bytes, contract={:?}, at={}, bytes={}", self, options.navigator.order_id, now, *writen - options.navigator.offset);

                let mut nav = ContractDataNavigator::next_chunk(&options.navigator, sector.chunk_size, *writen);
                loop {
                    let url = Url::parse_with_params (&format!("{}contract/data", self.config().endpoint), to_url_params(nav.clone()))?;
                    let mut req = Request::new(http::Method::Post, url);
                    let sign = chain_client.sign(serde_json::to_vec(&nav).unwrap().as_slice()).await.unwrap();
                    req.append_header("user-signature", sign.as_str());
                    if nav.len > 0 {
                        let mut chunk = vec![0u8; nav.len as usize];
                        let _ = source.read_exact(chunk.as_mut_slice()).await
                            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, contract={:?}, at={}, err=read from source {}", self, options.navigator.order_id, now, err))?;
                        
                        debug!("{} write, read bytes, contract={:?}, at={}, bytes={}", self, options.navigator.order_id, now, nav.len);
                        req.set_body(Body::from_bytes(chunk));
                    }
                   
                    debug!("{} write, writing to miner side, contract={:?}, at={}, navigator={:?}", self, options.navigator.order_id, now, nav);
                    let mut resp = surf::client().send(req).await
                        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, contract={:?}, at={}, err=get contract err {}", self, options.navigator.order_id, now, err))?;
                    if !resp.status().is_success() {
                        return Err(dmc_err!(DmcErrorCode::Failed, "{} write, contract={:?}, at={}, miner return status {}", self, options.navigator.order_id, now, resp.status()));
                    }
                    let contract_str = resp.body_string().await
                        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, contract={:?}, at={}, err=get contract err {}", self, options.navigator.order_id, now, err))?;
                    let contract = serde_json::from_str(&contract_str)
                        .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} write, contract={:?}, at={}, err=decode contract str {} err {}", self, options.navigator.order_id, now, contract_str, err))?;
                    debug!("{} write, miner side returned, contract={:?}, at={}, navigator={:?}, contract={:?}", self, options.navigator.order_id, now, nav, contract);

                    if nav.len < sector.chunk_size as u64 {
                        info!("{} write, finished, contract={:?}, at={}, miner_side={:?}", self, options.navigator.order_id, now, contract);
                        break Ok(contract);
                    }

                    nav = ContractDataNavigator::next_chunk(&options.navigator, sector.chunk_size, nav.end());
                }
            }
        } else {
            Err(dmc_err!(DmcErrorCode::ErrorState, "{} write, contract={:?}, at={}, err=miner side not writing", self, options.navigator.order_id, now))
        }
    }

    pub async fn restore(&self, options: RestoreContractOptions) -> DmcResult<impl async_std::io::Read + Unpin> {
        let now = dmc_time_now();
        info!("{} restore, contract={:?}, at={}", self, options, now);
       
        let contract = self.get(ContractFilter::from_order_id(options.navigator.order_id), 1).await?
            .next_page().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} restore, contract={:?}, at={}, err=get contract {}", self, options.navigator.order_id, now, err))?
            .get(0).ok_or_else(|| dmc_err!(DmcErrorCode::NotFound, "{} restore, contract={:?}, at={}, err=contract not exists", self, options.navigator.order_id, now))?.clone();

        if let ContractState::Storing { writen, .. } = contract.state {
            if writen < options.navigator.offset {
                Err(dmc_err!(DmcErrorCode::InvalidInput, "{} restore, contract={:?}, at={}, err=beyound offset", self, options.navigator.order_id, now))
            } else {
                let url = Url::parse_with_params(&format!("{}contract/data", self.config().endpoint), to_url_params(options.navigator.clone()))?;
                let req = Request::new(http::Method::Get, url);
                let mut resp = surf::client().send(req).await
                    .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} restore, contract={:?}, at={}, err=get contract {}", self, options.navigator.order_id, now, err))?;

                let body = resp.take_body();
                Ok(body)
            }   
        } else {
            Err(dmc_err!(DmcErrorCode::ErrorState, "{} restore, contract={:?}, at={}, err=miner side not storing", self, options.navigator.order_id, now))
        }       
    }

    pub async fn challenge(&self, challenge: OffChainChallenge) -> DmcResult<OffChainProof> {
        let now = dmc_time_now();
        let navigator = ContractDataNavigator {
            order_id: challenge.order_id, 
            offset: challenge.offset,  
            len: challenge.length as u64
        };
        let url = Url::parse_with_params(&format!("{}contract/data", self.config().endpoint), to_url_params(navigator))?;
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} challenge, challenge={:?}, at={}, err=get contract {}", self, challenge, now, err))?;
        let content = resp.body_bytes().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} challenge, challenge={:?}, at={}, err=get contract {}", self, challenge, now, err))?;
        Ok(OffChainProof {
            order_id: challenge.order_id, 
            offset: challenge.offset,  
            length: challenge.length,
            content: DmcData::from(content)
        })
    }

    pub async fn occupy(&self, options: SectorOccupyOptions) -> DmcResult<SectorOccupy> {
        let now = dmc_time_now();
        let url = Url::parse_with_params(&format!("{}contract/occupy", self.config().endpoint), to_url_params(options.clone()))?;
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await
            .map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get occupy, options={:?}, at={}, err={}", self, options, now, err))?;
        let occupy = resp.body_json().await.map_err(|err| dmc_err!(DmcErrorCode::Failed, "{} get occupy, options={:?}, at={}, err={}", self, options, now, err))?;
        Ok(occupy)
    }
}
