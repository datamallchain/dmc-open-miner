use std::{
    time::{SystemTime, UNIX_EPOCH},
    fmt,
    str::FromStr, 
    collections::LinkedList
};
use async_std::future;
use serde::{Serialize, Deserialize};
use futures::future::{AbortHandle, AbortRegistration, Abortable};
use generic_array::{typenum::{U32}, GenericArray};
use base58::{FromBase58, ToBase58};
use crate::error::*;

static _TIME_TTO_MICROSECONDS_OFFSET: u64 = 11644473600_u64 * 1000 * 1000;

pub fn system_time_to_dmc_time(time: &SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64 + _TIME_TTO_MICROSECONDS_OFFSET
}

pub fn dmc_time_now() -> u64 {
    system_time_to_dmc_time(&SystemTime::now())
}

// js time以毫秒为单位
pub fn js_time_to_dmc_time(time: u64) -> u64 {
    time * 1000 + _TIME_TTO_MICROSECONDS_OFFSET
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Ord, PartialOrd, Default)]
#[serde(try_from="String", into="String")] 
pub struct HashValue(GenericArray<u8, U32>);
pub const HASH_VALUE_LEN: usize = 32;


impl std::fmt::Debug for HashValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HashValue: {}", hex::encode(self.0.as_slice()))
    }
}

impl From<GenericArray<u8, U32>> for HashValue {
    fn from(hash: GenericArray<u8, U32>) -> Self {
        Self(hash)
    }
}

impl From<HashValue> for GenericArray<u8, U32> {
    fn from(hash: HashValue) -> Self {
        hash.0
    }
}

impl AsRef<GenericArray<u8, U32>> for HashValue {
    fn as_ref(&self) -> &GenericArray<u8, U32> {
        &self.0
    }
}

impl Into<Vec<u8>> for HashValue {
    fn into(self) -> Vec<u8> {
        let slice: [u8; 32] = self.into();
        Vec::from(slice)
    }
}

impl Into<[u8;32]> for HashValue {
    fn into(self) -> [u8; 32] {
        self.0.into()
    }
}

impl From<&[u8; 32]> for HashValue {
    fn from(hash: &[u8; 32]) -> Self {
        Self(GenericArray::clone_from_slice(hash))
    }
}

impl TryFrom<&[u8]> for HashValue {
    type Error = DmcError;

    fn try_from(v: &[u8]) -> Result<Self, Self::Error> {
        if v.len() != 32 {
            let msg = format!(
                "HashValue expected bytes of length {} but it was {}",
                32,
                v.len()
            );
            return Err(DmcError::new(DmcErrorCode::InvalidData, msg));
        }

        let ar: [u8; 32] = v.try_into().unwrap();
        Ok(Self(GenericArray::from(ar)))
    }
}

impl TryFrom<Vec<u8>> for HashValue {
    type Error = DmcError;
    fn try_from(v: Vec<u8>) -> Result<Self, Self::Error> {
        if v.len() != 32 {
            let msg = format!(
                "HashValue expected bytes of length {} but it was {}",
                32,
                v.len()
            );
            return Err(DmcError::new(DmcErrorCode::InvalidData, msg));
        }

        let ar: [u8; 32] = v.try_into().unwrap();
        Ok(Self(GenericArray::from(ar)))
    }
}

impl HashValue {
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.0.as_mut_slice()
    }

    pub fn len() -> usize {
        HASH_VALUE_LEN
    }

    pub fn to_hex_string(&self) -> String {
        hex::encode(self.0.as_slice())
    }

    pub fn from_hex_string(s: &str) -> DmcResult<Self> {
        let ret = hex::decode(s).map_err(|e| {
            let msg = format!("invalid hash value hex string: {}, {}", s, e);
            DmcError::new(DmcErrorCode::InvalidFormat, msg)
        })?;

        Self::clone_from_slice(&ret)
    }

    pub fn to_base58(&self) -> String {
        self.0.to_base58()
    }

    pub fn from_base58(s: &str) -> DmcResult<Self> {
        let buf = s.from_base58().map_err(|e| {
            let msg = format!("convert base58 str to hashvalue failed, str={}, {:?}", s, e);
            DmcError::new(DmcErrorCode::InvalidFormat, msg)
        })?;

        if buf.len() != 32 {
            let msg = format!(
                "convert base58 str to hashvalue failed, len unmatch: str={}",
                s
            );
            return Err(DmcError::new(DmcErrorCode::InvalidFormat, msg));
        }

        Ok(Self::try_from(buf).unwrap())
    }

    pub fn clone_from_slice(hash: &[u8]) -> DmcResult<Self> {
        Self::try_from(hash)
    }
}

impl std::fmt::Display for HashValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex_string())
    }
}

impl FromStr for HashValue {
    type Err = DmcError;
    fn from_str(s: &str) -> DmcResult<Self> {
        if s.len() == 64 {
            Self::from_hex_string(s)
        } else {
            Self::from_base58(s)
        }
    }
}

impl Into<String> for HashValue {
    fn into(self) -> String {
        self.to_base58()
    }
}

impl TryFrom<String> for HashValue {
    type Error = DmcError;
    fn try_from(s: String) -> DmcResult<Self> {
        Self::from_str(s.as_str())
    }
}



pub struct StateWaiter {
    wakers: LinkedList<AbortHandle>,
}

impl StateWaiter {
    pub fn new() -> Self {
        Self { wakers: Default::default() }
    }

    pub fn transfer(&mut self) -> Self {
        let mut waiter = Self::new();
        self.transfer_into(&mut waiter);
        waiter
    }

    pub fn transfer_into(&mut self, waiter: &mut Self) {
        waiter.wakers.append(&mut self.wakers);
    }

    pub fn new_waiter(&mut self) -> AbortRegistration {
        let (waker, waiter) = AbortHandle::new_pair();
        self.wakers.push_back(waker);
        waiter
    }

    pub async fn wait<T, S: FnOnce() -> T>(waiter: AbortRegistration, state: S) -> T {
        let _ = Abortable::new(future::pending::<()>(), waiter).await;
        state()
    }

    pub async fn abort_wait<A: futures::Future<Output = DmcError>, T, S: FnOnce() -> T>(t: A, waiter: AbortRegistration, state: S) -> DmcResult<T> {
        match Abortable::new(t, waiter).await {
            Ok(err) => {
                //FIXME: remove waker 
                Err(err)
            }, 
            Err(_) =>  Ok(state())
        }
    }

    pub fn wake(self) {
        for waker in self.wakers {
            waker.abort();
        }
    }

    pub fn pop(&mut self) -> Option<AbortHandle> {
        self.wakers.pop_front()   
    }

    pub fn len(&self) -> usize {
        self.wakers.len()
    }
}

