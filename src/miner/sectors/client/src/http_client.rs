use log::*;
use std::{
    pin::Pin,
    task::{Poll, Context, Waker},
    sync::{Arc, RwLock}
};
use async_std::{io::ReadExt};
use serde::{Serialize, Deserialize};
use tide::{Body, http::{Request, self, Url}};
use url_params_serializer::to_url_params;
use dmc_tools_common::*;
use crate::{
    types::*
};

#[derive(Clone, Serialize, Deserialize)]
pub struct SectorClientConfig {
    pub endpoint: Url
}

struct ClientImpl {
    config: SectorClientConfig
}

#[derive(Clone)]
pub struct SectorClient(Arc<ClientImpl>);


pub struct SectorIterator {
    client: SectorClient, 
    filter: SectorFilter, 
    page_size: usize, 
    next_page: Option<usize>
}

impl SectorIterator {
    fn new(client: SectorClient, filter: SectorFilter, page_size: usize) -> Self {
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

        let url = Url::parse_with_params (&format!("{}sector", self.client.config().endpoint), to_url_params(params)).map_err(|e| {
            error!("parse url for sector next page err {}, endpoint {}", e, self.client.config().endpoint);
            e
        })?;
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

impl std::fmt::Display for SectorClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SectorClient{{endpoint={}}}", self.config().endpoint)
    }
}


impl SectorClient {
    fn config(&self) -> &SectorClientConfig {
        &self.0.config
    }
}


impl SectorClient {
    pub fn new(config: SectorClientConfig) -> DmcResult<Self> {
        Ok(Self(Arc::new(ClientImpl {
            config
        })))
    }

    pub async fn get(&self, filter: SectorFilter, page_size: usize) -> DmcResult<SectorIterator> {
        Ok(SectorIterator::new(self.clone(), filter, page_size))
    }

    pub async fn write_chunk<R: async_std::io::BufRead + Unpin + Send + Sync + 'static>(&self, source: R, navigator: WriteChunkNavigator) -> DmcResult<()> {
        let body = Body::from_reader(source, Some(navigator.len));
        let url = Url::parse_with_params(&format!("{}sector/chunk", self.config().endpoint), to_url_params(navigator)).map_err(|e| {
            error!("parse url for sector write err {}, endpoint {}", e, self.config().endpoint);
            e
        })?;
        let mut req = Request::new(http::Method::Post, url);
        req.set_body(body);
        let _ = surf::client().send(req).await?;
        Ok(())
    }

    pub fn chunk_meta(&self, _: ReadChunkNavigator) -> DmcResult<ChunkMeta> {
        unimplemented!()
    }

    pub async fn read_chunk(&self, navigator: ReadChunkNavigator, buffer: &mut [u8]) -> DmcResult<ChunkMeta> {
        let mut meta = ChunkMeta::default(); 
        meta.sector_id = navigator.sector_id;
        let url = Url::parse_with_params(&format!("{}sector/chunk", self.config().endpoint), to_url_params(navigator))?;
        let req = Request::new(http::Method::Get, url);
        let mut resp = surf::client().send(req).await?;

        meta.offset = u64::from_str_radix(resp.header("chunk_offset").ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no chunk_offset header value"))?
            .get(0).ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no chunk_offset header value"))?.as_str(), 10)
            .map_err(|_| DmcError::new(DmcErrorCode::InvalidData, "invalid chunk_offset header value"))?;
        let body = resp.take_body();
        meta.len = body.len().ok_or_else(|| DmcError::new(DmcErrorCode::InvalidData, "no body len"))?;
        let buffer_len = buffer.len();
        let _ = body.into_reader().read_exact(&mut buffer[0..usize::min(buffer_len, meta.len)]).await?;
        Ok(meta)
    } 

    pub async fn clear_chunk(&self, navigator: ClearChunkNavigator) -> DmcResult<()> {
        let url = Url::parse_with_params(&format!("{}sector/chunk", self.config().endpoint), to_url_params(navigator))?;
        let req = Request::new(http::Method::Delete, url);
        let mut resp = surf::client().send(req).await?;

        let mut buf = vec![];
        resp.read_to_end(&mut buf).await?;
        Ok(())        
    }
}


struct ReadingChunkStub {
    waker: Option<Waker>, 
    cached: Option<DmcResult<(ChunkMeta, Vec<u8>)>>
}

pub struct SectorReader {
    client: SectorClient, 
    sector: Sector, 
    sector_offset: u64, 
    offset: u64, 
    limit: u64, 
    read: u64, 
    stub: Arc<RwLock<ReadingChunkStub>>, 
}

impl SectorReader {
    pub async fn from_sector_id(client: SectorClient, sector_id: u64, sector_offset: u64, limit: u64) -> DmcResult<Self> {
        let sector = client.get(SectorFilter::from_sector_id(sector_id), 1).await?.next_page().await?.get(0).cloned()
            .ok_or_else(|| DmcError::new(DmcErrorCode::NotFound, "sector not found"))?;
        Ok(Self { client, sector, offset: sector_offset, sector_offset, limit, read: 0, stub: Arc::new(RwLock::new(ReadingChunkStub { waker: None, cached: None })) })
    }

    fn read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        debug!("{} poll read chunk, offset={}, len={}", self.client, self.offset, buf.len());
        if self.offset >= self.sector.capacity {
            debug!("{} poll read chunk, returned, offset={}, read={}", self.client, self.offset, 0);
            return Poll::Ready(Ok(0))
        }
        if self.read >= self.limit {
            debug!("{} poll read chunk, returned, offset={}, read={}", self.client, self.offset, 0);
            return Poll::Ready(Ok(0))
        }
        if self.stub.read().unwrap().waker.is_some() {
            debug!("{} poll read chunk, pending, offset={}", self.client, self.offset);
            return Poll::Pending;
        }

        {
            let stub_ref = self.stub.read().unwrap();
            if let Some(cached_result) = stub_ref.cached.as_ref() {
                match cached_result {
                    Err(err) => {
                        debug!("{} poll read chunk, returned, offset={}, err={}", self.client, self.offset, err.msg());
                        return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, err.msg())));
                    },
                    Ok((meta, chunk)) => {
                        if self.offset >= meta.offset && self.offset < meta.offset + self.sector.chunk_size as u64 {
                            let chunk_offset = (self.offset - meta.offset as u64) as usize;
                            let read = usize::min(meta.len - chunk_offset, buf.len());
                            let read = usize::min(read, (self.limit - self.read) as usize);
                            buf[..read].copy_from_slice(&chunk[chunk_offset..chunk_offset + read]);
                            debug!("{} poll read chunk, returned, offset={}, read={}", self.client, self.offset, read);
                            self.offset += read as u64;
                            self.read += read as u64;
                            return Poll::Ready(Ok(read));
                        } 
                    }
                }
            } 
        }
        
       
        {
            let mut stub_ref = self.stub.write().unwrap();
        
            if stub_ref.waker.is_some() {
                debug!("{} poll read chunk, pending, offset={}", self.client, self.offset);
                return Poll::Pending;
            } 
            stub_ref.waker = Some(cx.waker().clone());
        }
        let chunk_offset = self.sector_offset + self.sector.chunk_size as u64 * ((self.offset - self.sector_offset) / self.sector.chunk_size as u64);

        let client = self.client.clone();
        let sector = self.sector.clone();
        let stub = self.stub.clone();
        async_std::task::spawn(async move {
            let navigator = ReadChunkNavigator {
                sector_id: sector.sector_id, 
                offset: chunk_offset
            };
            let mut buf = vec![0u8; sector.chunk_size];
            let result = client.read_chunk(navigator, &mut buf).await;
            let waker = {
                let mut stub_ref = stub.write().unwrap();
                stub_ref.cached = Some(result.map(|meta| (meta, buf)));
                let waker = stub_ref.waker.clone();
                stub_ref.waker = None;
                waker
            }.unwrap();
            waker.wake();
        });
        Poll::Pending
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        use std::io::SeekFrom::*;
        let offset = match pos {
            Start(o) =>  u64::min(self.sector_offset + o, self.sector.capacity), 
            End(o) => {
                if o > 0 {
                    self.sector.capacity
                } else {
                    let pre = (-o) as u64;
                    if pre > self.sector.capacity {
                        0
                    } else {
                        self.sector.capacity - pre
                    }
                }
            },
            Current(o) => {
                if o > 0 {
                    u64::min(self.offset + o as u64, self.sector.capacity)
                } else {
                    let pre = (-o) as u64;
                    if pre > self.offset {
                        0
                    } else {
                        self.offset - pre
                    }
                }
            }
        };
        self.offset = offset;
        Ok(self.offset - self.sector_offset)
    }
}

impl async_std::io::Read for SectorReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        self.get_mut().read(cx, buf)
    }
}

impl async_std::io::Seek for SectorReader {
    fn poll_seek(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        pos: std::io::SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        Poll::Ready(self.get_mut().seek(pos))
    }
}
