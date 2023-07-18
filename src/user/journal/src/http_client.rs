use std::{
    time::Duration, 
    collections::LinkedList
};
use async_std::{sync::Arc};
use serde::{Serialize, Deserialize};
use tide::{Body, http::{Request, self, Url}};
use url_params_serializer::to_url_params;
use dmc_tools_common::*;
use crate::{
    types::*
};

#[derive(Serialize, Deserialize, Clone)]
pub struct JournalClientConfig {
    pub host: String, 
}


pub struct JournalIterator {
    client: JournalClient, 
    filter: JournalFilter, 
    page_size: usize, 
    from_id: Option<Option<u64>>
}

impl JournalIterator {
    pub fn new(client: JournalClient, filter: JournalFilter, from_id: Option<u64>, page_size: usize) -> Self {
        Self {
            client, 
            filter, 
            page_size, 
            from_id: Some(from_id)
        }
    }

    pub async fn next_page(&mut self) -> DmcResult<Vec<JournalLog>> {
        if self.from_id.is_none() {
            return Ok(vec![]);
        }
        let from_id = self.from_id.unwrap();
        let params = JournalFilterAndNavigator {
            filter: self.filter.clone(), 
            navigator: JournalNavigator {
                from_id,  
                page_size: self.page_size
            }
        };

        let url = Url::parse_with_params(&format!("http://{}/{}", self.client.config().host, "journal"), to_url_params(params))?;
        
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;
        let logs: Vec<JournalLog> = resp.body_json().await?;
        self.from_id = if logs.len() < self.page_size {
            None
        } else {
            Some(logs.get(self.page_size - 1).map(|log| log.log_id))
        };
        Ok(logs)
    }
}


pub struct JournalListener {
    client: JournalClient, 
    filter: JournalFilter, 
    from_id: Option<u64>, 
    interval: Duration, 
    page_size: usize, 
    caches: LinkedList<JournalLog>
}


impl JournalListener {
    pub fn new(client: JournalClient, filter: JournalFilter, from_id: Option<u64>, interval: Duration, page_size: usize) -> Self {
        Self {
            client, 
            filter, 
            from_id, 
            interval, 
            page_size, 
            caches: LinkedList::new()
        }
    }

    pub async fn next(&mut self) -> DmcResult<JournalLog> {
        if let Some(log) = self.caches.pop_front() {
            return Ok(log);
        }

        loop {
            let params = JournalFilterAndNavigator {
                filter: self.filter.clone(), 
                navigator: JournalNavigator {
                    from_id: self.from_id,  
                    page_size: self.page_size
                }
            };
    
            let url = Url::parse_with_params(&format!("http://{}/{}", self.client.config().host, "journal"), to_url_params(params))?;
            
            let req = Request::new(http::Method::Get, url);
            let mut resp = surf::client().send(req).await?;
            let logs: Vec<JournalLog> = resp.body_json().await?;
            
            if logs.len() > 0 {
                self.from_id = Some(logs.get(logs.len() - 1).map(|log| log.log_id).unwrap());
                let mut logs = logs.into_iter();
                let top = logs.next().unwrap();
                for log in logs {
                    self.caches.push_back(log);
                }

                break Ok(top);
            } else {
                async_std::task::sleep(self.interval).await;
            }
        }
    }
}


struct ClientImpl {
    config: JournalClientConfig
}


#[derive(Clone)]
pub struct JournalClient(Arc<ClientImpl>);

impl std::fmt::Display for JournalClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserJournalClient{{host={}}}", self.config().host)
    }
}

impl JournalClient {
    fn config(&self) -> &JournalClientConfig {
        &self.0.config
    }
}


impl JournalClient {
    pub fn new(config: JournalClientConfig) -> DmcResult<Self> {
        Ok(Self(Arc::new(ClientImpl {
            config
        })))
    }

    pub async fn get(&self, filter: JournalFilter, from_id: Option<u64>, page_size: usize) -> DmcResult<JournalIterator> {
        Ok(JournalIterator::new(self.clone(), filter, from_id, page_size))
    }

    pub fn listener(&self, filter: JournalFilter, from_id: Option<u64>, interval: Duration, page_size: usize) -> JournalListener {
        JournalListener::new(self.clone(), filter, from_id, interval, page_size)
    }

    pub async fn append(&self, event: JournalEvent) -> DmcResult<JournalLog> {
        let url = Url::parse(&format!("http://{}/{}", self.config().host, "journal"))?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(Body::from_json(&event)?);
        let mut resp = surf::client().send(req).await?;
        let log = resp.body_json().await?;
        Ok(log)
    }
}
