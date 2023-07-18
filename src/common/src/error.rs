use int_enum::IntEnum;
use serde::{
    de::{self, Visitor},
    {Deserialize, Deserializer, Serialize, Serializer}
};
use std::{
    error::Error, 
    fmt::{self, Debug, Display},
    io::ErrorKind
};

// 系统的内置error code范围 [DMC_SYSTEM_ERROR_CODE_START, DMC_SYSTEM_ERROR_CODE_END)
pub const DMC_SYSTEM_ERROR_CODE_START: u16 = 0;
pub const DMC_SYSTEM_ERROR_CODE_END: u16 = 5000;

// MetaChain的error code范围
// [DMC_META_ERROR_CODE_START, DMC_META_ERROR_CODE_END)
pub const DMC_META_ERROR_CODE_START: u16 = 5000;
pub const DMC_META_ERROR_CODE_END: u16 = 6000;

pub const DMC_META_ERROR_CODE_MAX: u16 =
    DMC_META_ERROR_CODE_END - DMC_META_ERROR_CODE_START - 1;

// 应用扩展的错误码范围
// [DMC_DEC_ERROR_CODE_START, DMC_DEC_ERROR_CODE_END)
pub const DMC_DEC_ERROR_CODE_START: u16 = 15000;
pub const DMC_DEC_ERROR_CODE_END: u16 = u16::MAX;

// 应用扩展错误码DecError(code)中的code的最大取值
pub const DMC_DEC_ERROR_CODE_MAX: u16 = DMC_DEC_ERROR_CODE_END - DMC_DEC_ERROR_CODE_START;

pub fn is_system_error_code(code: u16) -> bool {
    code < DMC_SYSTEM_ERROR_CODE_END
}

pub fn is_meta_error_code(code: u16) -> bool {
    code >= DMC_META_ERROR_CODE_START && code < DMC_META_ERROR_CODE_END
}

pub fn is_dec_error_code(code: u16) -> bool {
    code >= DMC_DEC_ERROR_CODE_START
}

// CYFS框架的错误码定义
#[repr(u16)]
#[derive(
    Debug, Clone, Copy, Eq, IntEnum, PartialEq, Serialize, Deserialize,
)]
pub enum DmcSystemErrorCode {
    Ok = 0,

    Failed = 1,
    InvalidParam = 2,
    Timeout = 3,
    NotFound = 4,
    AlreadyExists = 5,
    NotSupport = 6,
    ErrorState = 7,
    InvalidFormat = 8,
    Expired = 9,
    OutOfLimit = 10,
    InternalError = 11,

    PermissionDenied = 12,
    ConnectionRefused = 13,
    ConnectionReset = 14,
    ConnectionAborted = 15,
    NotConnected = 16,
    AddrInUse = 18,
    AddrNotAvailable = 19,
    Interrupted = 20,
    InvalidInput = 21,
    InvalidData = 22,
    WriteZero = 23,
    UnexpectedEof = 24,
    BrokenPipe = 25,
    WouldBlock = 26,

    UnSupport = 27,
    Unmatch = 28,
    ExecuteError = 29,
    Reject = 30,
    Ignored = 31,
    InvalidSignature = 32,
    AlreadyExistsAndSignatureMerged = 33,
    TargetNotFound = 34,
    Aborted = 35,

    ConnectFailed = 40,
    ConnectInterZoneFailed = 41,
    InnerPathNotFound = 42,
    RangeNotSatisfiable = 43,
    UserCanceled = 44, 
    Conflict = 50,

    OutofSessionLimit = 60,

    Redirect = 66,

    MongoDBError = 99,
    MysqlError = 100,
    UrlError = 101,
    ZipError = 102,
    HttpError = 103,
    JsonError = 104,
    HexError = 105,
    RsaError = 106,
    CryptoError = 107,
    MpscSendError = 108,
    MpscRecvError = 109,
    IoError = 110,
    NetworkError = 111,

    SendTransactionError = 200,
    DmcClientError = 201,

    CodeError = 250, //TODO: cyfs-base的Code应该和DmcErrorCode整合，现在先搞个特殊Type让能编过
    UnknownBdtError = 253,
    UnknownIOError = 254,
    Unknown = 255,

    Pending = 256,
    NotChange = 257,

    NotMatch = 258,
    NotImplement = 259,
    NotInit = 260,
    ParseError = 261,
    NotHandled = 262,
    TargetNotMatch = 263,

    // 在system error code里面，meta_error默认值都取值5000
    MetaError = 5000,

    // 在system error code里面，dec_error默认都是取值15000
    DecError = 15000,
}

impl Into<u16> for DmcSystemErrorCode {
    fn into(self) -> u16 {
        unsafe { std::mem::transmute(self as u16) }
    }
}

impl From<u16> for DmcSystemErrorCode {
    fn from(code: u16) -> Self {
        match Self::from_int(code) {
            Ok(code) => code,
            Err(e) => {
                log::error!("unknown system error code: {} {}", code, e);
                if is_dec_error_code(code) {
                    Self::DecError
                } else if is_meta_error_code(code) {
                    Self::MetaError
                } else {
                    Self::Unknown
                }
            }
        }
    }
}

// DmcErrorCode的兼容性定义
#[repr(u16)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DmcErrorCode {
    Ok,

    Failed,
    InvalidParam,
    Timeout,
    NotFound,
    AlreadyExists,
    NotSupport,
    ErrorState,
    InvalidFormat,
    Expired,
    OutOfLimit,
    InternalError,

    PermissionDenied,
    ConnectionRefused,
    ConnectionReset,
    ConnectionAborted,
    NotConnected,
    AddrInUse,
    AddrNotAvailable,
    Interrupted,
    InvalidInput,
    InvalidData,
    WriteZero,
    UnexpectedEof,
    BrokenPipe,
    WouldBlock,

    UnSupport,
    Unmatch,
    ExecuteError,
    Reject,
    Ignored,
    InvalidSignature,
    AlreadyExistsAndSignatureMerged,
    TargetNotFound,
    Aborted,

    ConnectFailed,
    ConnectInterZoneFailed,
    InnerPathNotFound,
    RangeNotSatisfiable,
    UserCanceled,

    Conflict,

    OutofSessionLimit,

    Redirect,

    MongoDBError,
    MysqlError,
    SqlxError, 
    UrlError,
    ZipError,
    HttpError,
    JsonError,
    HexError,
    RsaError,
    CryptoError,
    MpscSendError,
    MpscRecvError,
    IoError,
    NetworkError,

    SendTransactionError,
    DmcClientError,

    CodeError, //TODO: cyfs-base的Code应该和DmcErrorCode整合，现在先搞个特殊Type让能编过
    UnknownBdtError,
    UnknownIOError,
    Unknown,

    Pending,
    NotChange,

    NotMatch,
    NotImplement,
    NotInit,

    ParseError,
    NotHandled,

    TargetNotMatch,

    // meta chain的error段，取值范围是[0, DMC_META_ERROR_CODE_MAX)
    MetaError(u16),

    // DEC自定义error，取值范围是[0, DMC_DEC_ERROR_CODE_MAX)
    DecError(u16),
}

impl Into<DmcSystemErrorCode> for DmcErrorCode {
    fn into(self) -> DmcSystemErrorCode {
        match self {
            Self::Ok => DmcSystemErrorCode::Ok,

            Self::Failed => DmcSystemErrorCode::Failed,
            Self::InvalidParam => DmcSystemErrorCode::InvalidParam,
            Self::Timeout => DmcSystemErrorCode::Timeout,
            Self::NotFound => DmcSystemErrorCode::NotFound,
            Self::AlreadyExists => DmcSystemErrorCode::AlreadyExists,
            Self::NotSupport => DmcSystemErrorCode::NotSupport,
            Self::ErrorState => DmcSystemErrorCode::ErrorState,
            Self::InvalidFormat => DmcSystemErrorCode::InvalidFormat,
            Self::Expired => DmcSystemErrorCode::Expired,
            Self::OutOfLimit => DmcSystemErrorCode::OutOfLimit,
            Self::InternalError => DmcSystemErrorCode::InternalError,

            Self::PermissionDenied => DmcSystemErrorCode::PermissionDenied,
            Self::ConnectionRefused => DmcSystemErrorCode::ConnectionRefused,
            Self::ConnectionReset => DmcSystemErrorCode::ConnectionReset,
            Self::ConnectionAborted => DmcSystemErrorCode::ConnectionAborted,
            Self::NotConnected => DmcSystemErrorCode::NotConnected,
            Self::AddrInUse => DmcSystemErrorCode::AddrInUse,
            Self::AddrNotAvailable => DmcSystemErrorCode::AddrNotAvailable,
            Self::Interrupted => DmcSystemErrorCode::Interrupted,
            Self::InvalidInput => DmcSystemErrorCode::InvalidInput,
            Self::InvalidData => DmcSystemErrorCode::InvalidData,
            Self::WriteZero => DmcSystemErrorCode::WriteZero,
            Self::UnexpectedEof => DmcSystemErrorCode::UnexpectedEof,
            Self::BrokenPipe => DmcSystemErrorCode::BrokenPipe,
            Self::WouldBlock => DmcSystemErrorCode::WouldBlock,

            Self::UnSupport => DmcSystemErrorCode::UnSupport,
            Self::Unmatch => DmcSystemErrorCode::Unmatch,
            Self::ExecuteError => DmcSystemErrorCode::ExecuteError,
            Self::Reject => DmcSystemErrorCode::Reject,
            Self::Ignored => DmcSystemErrorCode::Ignored,
            Self::InvalidSignature => DmcSystemErrorCode::InvalidSignature,
            Self::AlreadyExistsAndSignatureMerged => {
                DmcSystemErrorCode::AlreadyExistsAndSignatureMerged
            }
            Self::TargetNotFound => DmcSystemErrorCode::TargetNotFound,
            Self::Aborted => DmcSystemErrorCode::Aborted,

            Self::ConnectFailed => DmcSystemErrorCode::ConnectFailed,
            Self::ConnectInterZoneFailed => DmcSystemErrorCode::ConnectInterZoneFailed,
            Self::InnerPathNotFound => DmcSystemErrorCode::InnerPathNotFound,
            Self::RangeNotSatisfiable => DmcSystemErrorCode::RangeNotSatisfiable,
            Self::UserCanceled => DmcSystemErrorCode::UserCanceled, 
            Self::Conflict => DmcSystemErrorCode::Conflict,

            Self::OutofSessionLimit => DmcSystemErrorCode::OutofSessionLimit,
            Self::Redirect => DmcSystemErrorCode::Redirect,

            Self::MongoDBError => DmcSystemErrorCode::MongoDBError,
            Self::MysqlError => DmcSystemErrorCode::MysqlError,
            Self::SqlxError => DmcSystemErrorCode::MysqlError,
            Self::UrlError => DmcSystemErrorCode::UrlError,
            Self::ZipError => DmcSystemErrorCode::ZipError,
            Self::HttpError => DmcSystemErrorCode::HttpError,
            Self::JsonError => DmcSystemErrorCode::JsonError,
            Self::HexError => DmcSystemErrorCode::RsaError,
            Self::RsaError => DmcSystemErrorCode::InternalError,
            Self::CryptoError => DmcSystemErrorCode::CryptoError,
            Self::MpscSendError => DmcSystemErrorCode::MpscSendError,
            Self::MpscRecvError => DmcSystemErrorCode::MpscRecvError,
            Self::IoError => DmcSystemErrorCode::IoError,
            Self::NetworkError => DmcSystemErrorCode::NetworkError,

            Self::CodeError => DmcSystemErrorCode::CodeError,
            Self::UnknownBdtError => DmcSystemErrorCode::UnknownBdtError,
            Self::UnknownIOError => DmcSystemErrorCode::UnknownIOError,
            Self::Unknown => DmcSystemErrorCode::Unknown,

            Self::Pending => DmcSystemErrorCode::Pending,
            Self::NotChange => DmcSystemErrorCode::NotChange,

            Self::NotMatch => DmcSystemErrorCode::NotMatch,
            Self::NotImplement => DmcSystemErrorCode::NotImplement,
            Self::NotInit => DmcSystemErrorCode::NotInit,

            Self::ParseError => DmcSystemErrorCode::ParseError,
            Self::NotHandled => DmcSystemErrorCode::NotHandled,
            Self::TargetNotMatch => DmcSystemErrorCode::TargetNotMatch,

            Self::MetaError(_) => DmcSystemErrorCode::MetaError,
            Self::DecError(_) => DmcSystemErrorCode::DecError,
            Self::SendTransactionError => DmcSystemErrorCode::SendTransactionError,
            Self::DmcClientError => DmcSystemErrorCode::DmcClientError,
        }
    }
}

impl Into<DmcErrorCode> for DmcSystemErrorCode {
    fn into(self) -> DmcErrorCode {
        match self {
            Self::Ok => DmcErrorCode::Ok,

            Self::Failed => DmcErrorCode::Failed,
            Self::InvalidParam => DmcErrorCode::InvalidParam,
            Self::Timeout => DmcErrorCode::Timeout,
            Self::NotFound => DmcErrorCode::NotFound,
            Self::AlreadyExists => DmcErrorCode::AlreadyExists,
            Self::NotSupport => DmcErrorCode::NotSupport,
            Self::ErrorState => DmcErrorCode::ErrorState,
            Self::InvalidFormat => DmcErrorCode::InvalidFormat,
            Self::Expired => DmcErrorCode::Expired,
            Self::OutOfLimit => DmcErrorCode::OutOfLimit,
            Self::InternalError => DmcErrorCode::InternalError,

            Self::PermissionDenied => DmcErrorCode::PermissionDenied,
            Self::ConnectionRefused => DmcErrorCode::ConnectionRefused,
            Self::ConnectionReset => DmcErrorCode::ConnectionReset,
            Self::ConnectionAborted => DmcErrorCode::ConnectionAborted,
            Self::NotConnected => DmcErrorCode::NotConnected,
            Self::AddrInUse => DmcErrorCode::AddrInUse,
            Self::AddrNotAvailable => DmcErrorCode::AddrNotAvailable,
            Self::Interrupted => DmcErrorCode::Interrupted,
            Self::InvalidInput => DmcErrorCode::InvalidInput,
            Self::InvalidData => DmcErrorCode::InvalidData,
            Self::WriteZero => DmcErrorCode::WriteZero,
            Self::UnexpectedEof => DmcErrorCode::UnexpectedEof,
            Self::BrokenPipe => DmcErrorCode::BrokenPipe,
            Self::WouldBlock => DmcErrorCode::WouldBlock,

            Self::UnSupport => DmcErrorCode::UnSupport,
            Self::Unmatch => DmcErrorCode::Unmatch,
            Self::ExecuteError => DmcErrorCode::ExecuteError,
            Self::Reject => DmcErrorCode::Reject,
            Self::Ignored => DmcErrorCode::Ignored,
            Self::InvalidSignature => DmcErrorCode::InvalidSignature,
            Self::AlreadyExistsAndSignatureMerged => {
                DmcErrorCode::AlreadyExistsAndSignatureMerged
            }
            Self::TargetNotFound => DmcErrorCode::TargetNotFound,
            Self::Aborted => DmcErrorCode::Aborted,

            Self::ConnectFailed => DmcErrorCode::ConnectFailed,
            Self::ConnectInterZoneFailed => DmcErrorCode::ConnectInterZoneFailed,
            Self::InnerPathNotFound => DmcErrorCode::InnerPathNotFound,
            Self::RangeNotSatisfiable => DmcErrorCode::RangeNotSatisfiable,
            Self::UserCanceled => DmcErrorCode::UserCanceled, 

            Self::Conflict => DmcErrorCode::Conflict,

            Self::OutofSessionLimit => DmcErrorCode::OutofSessionLimit,

            Self::Redirect => DmcErrorCode::Redirect,
            
            Self::MongoDBError => DmcErrorCode::MongoDBError,
            Self::MysqlError => DmcErrorCode::MysqlError,
            Self::UrlError => DmcErrorCode::UrlError,
            Self::ZipError => DmcErrorCode::ZipError,
            Self::HttpError => DmcErrorCode::HttpError,
            Self::JsonError => DmcErrorCode::JsonError,
            Self::HexError => DmcErrorCode::RsaError,
            Self::RsaError => DmcErrorCode::InternalError,
            Self::CryptoError => DmcErrorCode::CryptoError,
            Self::MpscSendError => DmcErrorCode::MpscSendError,
            Self::MpscRecvError => DmcErrorCode::MpscRecvError,
            Self::IoError => DmcErrorCode::IoError,
            Self::NetworkError => DmcErrorCode::NetworkError,

            Self::CodeError => DmcErrorCode::CodeError,
            Self::UnknownBdtError => DmcErrorCode::UnknownBdtError,
            Self::UnknownIOError => DmcErrorCode::UnknownIOError,
            Self::Unknown => DmcErrorCode::Unknown,

            Self::Pending => DmcErrorCode::Pending,
            Self::NotChange => DmcErrorCode::NotChange,

            Self::NotMatch => DmcErrorCode::NotMatch,
            Self::NotImplement => DmcErrorCode::NotImplement,
            Self::NotInit => DmcErrorCode::NotInit,

            Self::ParseError => DmcErrorCode::ParseError,
            Self::NotHandled => DmcErrorCode::NotHandled,
            Self::TargetNotMatch => DmcErrorCode::TargetNotMatch,

            Self::MetaError => DmcErrorCode::MetaError(0),
            Self::DecError => DmcErrorCode::DecError(0),
            Self::SendTransactionError => DmcErrorCode::SendTransactionError,
            Self::DmcClientError => DmcErrorCode::DmcClientError,
        }
    }
}

impl Into<u32> for DmcErrorCode {
    fn into(self) -> u32 {
        let v: u16 = self.into();
        v as u32
    }
}

impl Into<i32> for DmcErrorCode {
    fn into(self) -> i32 {
        let v: u16 = self.into();
        v as i32
    }
}

impl Into<u16> for DmcErrorCode {
    fn into(self) -> u16 {
        match self {
            Self::MetaError(mut v) => {
                if v > DMC_META_ERROR_CODE_MAX {
                    log::error!("meta error code out of limit: {}", v);
                    v = DMC_META_ERROR_CODE_MAX;
                }

                DMC_META_ERROR_CODE_START + v
            }
            Self::DecError(mut v) => {
                if v > DMC_DEC_ERROR_CODE_MAX {
                    log::error!("dec error code out of limit: {}", v);
                    v = DMC_DEC_ERROR_CODE_MAX;
                }

                DMC_DEC_ERROR_CODE_START + v
            }
            _ => Into::<DmcSystemErrorCode>::into(self).into(),
        }
    }
}

impl From<u16> for DmcErrorCode {
    fn from(code: u16) -> Self {
        if is_system_error_code(code) {
            DmcSystemErrorCode::from(code).into()
        } else if is_meta_error_code(code) {
            let code = code - DMC_META_ERROR_CODE_START;
            Self::MetaError(code)
        } else if is_dec_error_code(code) {
            let code = code - DMC_DEC_ERROR_CODE_START;
            Self::DecError(code)
        } else {
            log::error!("unknown error code: {}", code);
            Self::Unknown
        }
    }
}

impl From<u32> for DmcErrorCode {
    fn from(code: u32) -> Self {
        if code < u16::MAX as u32 {
            Self::from(code as u16)
        } else {
            log::error!("u32 error code out of u16 limit: {}", code);
            Self::Unknown
        }
    }
}

impl Display for DmcErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u16())
    }
}

impl Serialize for DmcErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u16(self.as_u16())
    }
}

struct DmcErrorCodeVisitor {}
impl<'de> Visitor<'de> for DmcErrorCodeVisitor {
    type Value = DmcErrorCode;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("u16")
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v < u16::MAX as u64 {
            Ok(DmcErrorCode::from(v as u16))
        } else {
            log::error!("invalid DmcErrorCode int value: {}", v);
            Ok(DmcErrorCode::Unknown)
        }
    }
}

impl<'de> Deserialize<'de> for DmcErrorCode {
    fn deserialize<D>(deserializer: D) -> Result<DmcErrorCode, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u16(DmcErrorCodeVisitor {})
    }
}

impl DmcErrorCode {
    pub fn as_u8(&self) -> u8 {
        let v: u16 = self.clone().into();
        v as u8
    }

    pub fn into_u8(self) -> u8 {
        let v: u16 = self.into();
        v as u8
    }

    pub fn as_u16(&self) -> u16 {
        self.clone().into()
    }

    pub fn into_u16(self) -> u16 {
        self.into()
    }

    // 判断是不是DecError
    pub fn is_meta_error(&self) -> bool {
        match *self {
            Self::MetaError(_) => true,
            _ => false,
        }
    }

    // 判断是不是DecError
    pub fn is_dec_error(&self) -> bool {
        match *self {
            Self::DecError(_) => true,
            _ => false,
        }
    }
}

// DmcErrorCode的标准定义
pub enum DmcErrorCodeEx {
    System(DmcSystemErrorCode),
    MetaError(u16),
    DecError(u16),
}

impl Into<DmcErrorCode> for DmcErrorCodeEx {
    fn into(self) -> DmcErrorCode {
        match self {
            Self::System(code) => code.into(),
            Self::MetaError(v) => DmcErrorCode::MetaError(v),
            Self::DecError(v) => DmcErrorCode::DecError(v),
        }
    }
}

impl Into<DmcErrorCodeEx> for DmcErrorCode {
    fn into(self) -> DmcErrorCodeEx {
        match self {
            Self::MetaError(v) => DmcErrorCodeEx::MetaError(v),
            Self::DecError(v) => DmcErrorCodeEx::DecError(v),
            _ => self.into(),
        }
    }
}

impl Into<DmcErrorCodeEx> for DmcSystemErrorCode {
    fn into(self) -> DmcErrorCodeEx {
        DmcErrorCodeEx::System(self)
    }
}

// 第三方模块和std内部的Errror
#[derive(Debug)]
pub enum DmcOriginError {
    IoError(std::io::Error),
    SerdeJsonError(serde_json::error::Error),
    HttpError(http_types::Error),
    UrlError(url::ParseError),
    HttpStatusCodeError(http_types::StatusCode),
    SqlxError(sqlx::Error),
    HexError(hex::FromHexError),
    CodeError(u32),
    ParseIntError(std::num::ParseIntError),
    ParseFloatError(std::num::ParseFloatError),
    AddrParseError(std::net::AddrParseError),
    StripPrefixError(std::path::StripPrefixError),
    ParseUtf8Error(std::str::Utf8Error),
    ErrorMsg(String),
}

pub struct DmcError {
    code: DmcErrorCode,
    msg: String,

    origin: Option<DmcOriginError>,
}

pub type DmcResult<T> = Result<T, DmcError>;

impl Clone for DmcError {
    fn clone(&self) -> Self {
        DmcError::new(self.code(), self.msg())
    }
}

impl DmcError {
    pub fn new(code: impl Into<DmcErrorCode>, msg: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            msg: msg.into(),
            origin: None,
        }
    }

    pub fn set_code(&mut self, code: impl Into<DmcErrorCode>) {
        self.code = code.into();
    }

    pub fn code(&self) -> DmcErrorCode {
        self.code
    }

    pub fn with_code(mut self, code: impl Into<DmcErrorCode>) -> Self {
        self.code = code.into();
        self
    }

    pub fn set_msg(&mut self, msg: impl Into<String>) {
        self.msg = msg.into();
    }

    pub fn msg(&self) -> &str {
        self.msg.as_ref()
    }

    pub fn with_msg(mut self, msg: impl Into<String>) -> Self {
        self.msg = msg.into();
        self
    }

    pub fn origin(&self) -> &Option<DmcOriginError> {
        &self.origin
    }

    pub fn into_origin(self) -> Option<DmcOriginError> {
        self.origin
    }

    fn format(&self) -> String {
        format!("err: ({:?}, {}, {:?})", self.code, self.msg, self.origin)
    }

    pub fn error_with_log<T>(msg: impl Into<String> + std::fmt::Display) -> DmcResult<T> {
        log::error!("{}", msg);

        Err(DmcError::new(DmcErrorCode::Failed, msg))
    }
}

impl Error for DmcError {}

impl Display for DmcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.format(), f)
    }
}

impl Debug for DmcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.format(), f)
    }
}

impl DmcError {
    fn io_error_kind_to_code(kind: std::io::ErrorKind) -> DmcErrorCode {
        match kind {
            ErrorKind::NotFound => DmcErrorCode::NotFound,
            ErrorKind::PermissionDenied => DmcErrorCode::PermissionDenied,
            ErrorKind::ConnectionRefused => DmcErrorCode::ConnectionRefused,
            ErrorKind::ConnectionReset => DmcErrorCode::ConnectionReset,
            ErrorKind::ConnectionAborted => DmcErrorCode::ConnectionAborted,
            ErrorKind::NotConnected => DmcErrorCode::NotConnected,
            ErrorKind::AddrInUse => DmcErrorCode::AddrInUse,
            ErrorKind::AddrNotAvailable => DmcErrorCode::AddrNotAvailable,
            ErrorKind::BrokenPipe => DmcErrorCode::BrokenPipe,
            ErrorKind::AlreadyExists => DmcErrorCode::AlreadyExists,
            ErrorKind::WouldBlock => DmcErrorCode::WouldBlock,
            ErrorKind::InvalidInput => DmcErrorCode::InvalidInput,
            ErrorKind::InvalidData => DmcErrorCode::InvalidData,
            ErrorKind::TimedOut => DmcErrorCode::Timeout,
            ErrorKind::WriteZero => DmcErrorCode::WriteZero,
            ErrorKind::Interrupted => DmcErrorCode::Interrupted,
            ErrorKind::UnexpectedEof => DmcErrorCode::UnexpectedEof,
            _ => DmcErrorCode::UnknownIOError,
        }
    }

    fn dmc_error_to_io_error(e: DmcError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }

    fn io_error_to_dmc_error(e: std::io::Error) -> DmcError {
        let kind = e.kind();
        if kind == std::io::ErrorKind::Other && e.get_ref().is_some() {
            match e.into_inner().unwrap().downcast::<DmcError>() {
                Ok(e) => {
                    e.as_ref().clone()
                }
                Err(e) => {
                    DmcError {
                        code: Self::io_error_kind_to_code(kind),
                        msg: format!("io_error: {}", e),
                        origin: None,
                    }
                }
            }
        } else {
            DmcError {
                code: Self::io_error_kind_to_code(e.kind()),
                msg: format!("io_error: {}", e),
                origin: Some(DmcOriginError::IoError(e)),
            }
        }
    }
}

impl From<std::io::Error> for DmcError {
    fn from(err: std::io::Error) -> DmcError {
        DmcError::io_error_to_dmc_error(err)
    }
}

impl From<DmcError> for std::io::Error {
    fn from(err: DmcError) -> std::io::Error {
        DmcError::dmc_error_to_io_error(err)
    }
}

impl From<std::str::Utf8Error> for DmcError {
    fn from(err: std::str::Utf8Error) -> DmcError {
        DmcError {
            code: DmcErrorCode::InvalidFormat,
            msg: format!("io_error: {}", err),
            origin: Some(DmcOriginError::ParseUtf8Error(err)),
        }
    }
}

impl From<sqlx::Error> for DmcError {
    fn from(err: sqlx::Error) -> Self {
        Self {
            code: DmcErrorCode::MysqlError,
            msg: format!("sqlx error: {}", err),
            origin: Some(DmcOriginError::SqlxError(err))
        }
    }
}


impl From<http_types::Error> for DmcError {
    fn from(err: http_types::Error) -> DmcError {
        DmcError {
            code: DmcErrorCode::HttpError,
            msg: format!("http_error: {}", err),
            origin: Some(DmcOriginError::HttpError(err)),
        }
    }
}

impl From<std::num::ParseIntError> for DmcError {
    fn from(err: std::num::ParseIntError) -> DmcError {
        DmcError {
            code: DmcErrorCode::InvalidFormat,
            msg: format!("parse_int_error: {}", err),
            origin: Some(DmcOriginError::ParseIntError(err)),
        }
    }
}

impl From<std::num::ParseFloatError> for DmcError {
    fn from(err: std::num::ParseFloatError) -> DmcError {
        DmcError {
            code: DmcErrorCode::InvalidFormat,
            msg: format!("parse_int_error: {}", err),
            origin: Some(DmcOriginError::ParseFloatError(err)),
        }
    }
}

impl From<std::net::AddrParseError> for DmcError {
    fn from(err: std::net::AddrParseError) -> DmcError {
        DmcError {
            code: DmcErrorCode::InvalidFormat,
            msg: format!("parse_int_error: {}", err),
            origin: Some(DmcOriginError::AddrParseError(err)),
        }
    }
}

impl From<std::path::StripPrefixError> for DmcError {
    fn from(err: std::path::StripPrefixError) -> DmcError {
        DmcError {
            code: DmcErrorCode::InvalidFormat,
            msg: format!("strip_prefix_error: {}", err),
            origin: Some(DmcOriginError::StripPrefixError(err)),
        }
    }
}

impl From<async_std::future::TimeoutError> for DmcError {
    fn from(err: async_std::future::TimeoutError) -> DmcError {
        DmcError::new(DmcErrorCode::Timeout, format!("{}", err))
    }
}

impl From<u32> for DmcError {
    fn from(err: u32) -> DmcError {
        DmcError {
            code: DmcErrorCode::CodeError,
            msg: format!("base_code_error: {}", err),
            origin: Some(DmcOriginError::CodeError(err)),
        }
    }
}

pub struct CodeError(pub u32, pub String);

impl From<CodeError> for DmcError {
    fn from(err: CodeError) -> Self {
        DmcError {
            code: DmcErrorCode::CodeError,
            msg: err.1,
            origin: Some(DmcOriginError::CodeError(err.0)),
        }
    }
}


impl From<serde_json::error::Error> for DmcError {
    fn from(e: serde_json::error::Error) -> Self {
        DmcError {
            code: DmcErrorCode::JsonError,
            msg: format!("json_error: {}", e),
            origin: Some(DmcOriginError::SerdeJsonError(e)),
        }
    }
}

impl From<http_types::StatusCode> for DmcError {
    fn from(code: http_types::StatusCode) -> Self {
        DmcError {
            code: DmcErrorCode::HttpError,
            msg: format!("http status code: {}", code),
            origin: Some(DmcOriginError::HttpStatusCodeError(code)),
        }
    }
}

impl From<url::ParseError> for DmcError {
    fn from(e: url::ParseError) -> Self {
        DmcError {
            code: DmcErrorCode::UrlError,
            msg: format!("url_error: {}", e),
            origin: Some(DmcOriginError::UrlError(e)),
        }
    }
}


impl From<hex::FromHexError> for DmcError {
    fn from(e: hex::FromHexError) -> Self {
        DmcError {
            code: DmcErrorCode::HexError,
            msg: format!("hex error: {}", e),
            origin: Some(DmcOriginError::HexError(e)),
        }
    }
}


impl From<DmcErrorCode> for DmcError {
    fn from(code: DmcErrorCode) -> DmcError {
        DmcError {
            code,
            msg: "".to_owned(),
            origin: None,
        }
    }
}

impl From<&str> for DmcError {
    fn from(msg: &str) -> DmcError {
        DmcError {
            code: DmcErrorCode::Unknown,
            msg: msg.to_owned(),
            origin: None,
        }
    }
}

impl From<String> for DmcError {
    fn from(msg: String) -> DmcError {
        DmcError {
            code: DmcErrorCode::Unknown,
            msg,
            origin: None,
        }
    }
}

impl From<(DmcErrorCode, &str)> for DmcError {
    fn from(cm: (DmcErrorCode, &str)) -> DmcError {
        DmcError {
            code: cm.0,
            msg: cm.1.to_owned(),
            origin: None,
        }
    }
}

impl From<(DmcErrorCode, String)> for DmcError {
    fn from(cm: (DmcErrorCode, String)) -> DmcError {
        DmcError {
            code: cm.0,
            msg: cm.1,
            origin: None,
        }
    }
}



impl From<Box<dyn Error>> for DmcError {
    fn from(err: Box<dyn Error>) -> DmcError {
        if err.is::<DmcError>() {
            let be = err.downcast::<DmcError>().unwrap();
            *be
        } else {
            DmcError {
                code: DmcErrorCode::Unknown,
                msg: format!("{}", err),
                origin: None,
            }
        }
    }
}

impl Into<DmcErrorCode> for DmcError {
    fn into(self) -> DmcErrorCode {
        self.code
    }
}

impl Into<ErrorKind> for DmcErrorCode {
    fn into(self) -> ErrorKind {
        match self {
            Self::Reject | Self::PermissionDenied => ErrorKind::PermissionDenied,
            Self::NotFound => ErrorKind::NotFound,

            Self::ConnectionReset => ErrorKind::ConnectionReset,
            Self::ConnectionRefused => ErrorKind::ConnectionRefused,
            Self::ConnectionAborted => ErrorKind::ConnectionAborted,
            Self::AddrInUse => ErrorKind::AddrInUse,
            Self::AddrNotAvailable => ErrorKind::AddrNotAvailable,
            Self::NotConnected => ErrorKind::NotConnected,
            Self::AlreadyExists => ErrorKind::AlreadyExists,
            Self::Interrupted => ErrorKind::Interrupted,
            Self::WriteZero => ErrorKind::WriteZero,
            Self::UnexpectedEof => ErrorKind::UnexpectedEof,
            Self::UnSupport => ErrorKind::Unsupported,
            Self::BrokenPipe => ErrorKind::BrokenPipe,
            Self::WouldBlock => ErrorKind::WouldBlock,
            Self::Timeout => ErrorKind::TimedOut,
            Self::OutOfLimit => ErrorKind::OutOfMemory,

            _ => ErrorKind::Other,
        }
    }
}



#[macro_export]
macro_rules! dmc_err {
    ( $err: expr, $($arg:tt)*) => {
        {
            log::error!("{}", format!($($arg)*));
            DmcError::new($err, format!("{}", format!($($arg)*)))
        }
    };
}