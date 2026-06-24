//! `errors` 为错误处理模块，该工程设计为所有错误集中处理，处理过程为本模块内容
use actix_web::{
    error::BlockingError, http::header::InvalidHeaderValue, HttpResponse, ResponseError,
};
use bytes::Bytes;
// use derive_more::Display;
use log::{error as error_log, SetLoggerError};
use openssl::error::ErrorStack;
// use rdkafka::{error::KafkaError, message::OwnedMessage};
// use log4rs::config::runtime::ConfigErrors;
use serde_json::json;
use std::{collections::HashMap, fmt::Debug, net::AddrParseError, sync::Arc};
use trackable::{track, History, Location, Trackable};
use webrtc::data_channel::RTCDataChannel;

#[derive(Debug, Default, Serialize, Clone)]
pub struct VCError {
    #[serde(skip)]
    history: History<Location>,
    pub msg: String,
    pub code: i32,
}
impl Trackable for VCError {
    type Event = Location;
    fn history(&self) -> Option<&History<Self::Event>> {
        Some(&self.history)
    }
    fn history_mut(&mut self) -> Option<&mut History<Self::Event>> {
        Some(&mut self.history)
    }
}

impl VCError {
    pub fn new(msg: &str) -> Self {
        let mut e = track!(Self::default(), msg.to_owned());
        e.msg = msg.to_owned();
        e.code = -1;
        e
    }

    pub fn new_with_code(msg: &str, code: i32) -> Self {
        let mut e = track!(Self::default(), msg.to_owned());
        e.msg = msg.to_owned();
        e.code = code;
        e
    }

    pub fn log_tracks(&self) {
        error_log!("\n{}", self.history);
    }

    pub fn logger<T, E>(result: Result<T, E>) -> Result<T, E>
    where
        E: std::fmt::Debug,
    {
        result.map_err(|e| {
            error_log!("\n {:?}", e);
            e
        })
    }
    pub fn track_target<T, E>(target: &str, result: Result<T, E>)
    where
        E: Into<VCError>,
    {
        if let Err(e) = track!(result.map_err(|e| {
            let e1: VCError = e.into();
            e1
        })) {
            error_log!(target: target, "\n{}", e);
        }
    }
}

impl std::fmt::Display for VCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

unsafe impl Send for VCError {}

impl From<std::io::Error> for VCError {
    fn from(error: std::io::Error) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<std::env::VarError> for VCError {
    fn from(error: std::env::VarError) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<std::string::FromUtf8Error> for VCError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<std::num::ParseIntError> for VCError {
    fn from(error: std::num::ParseIntError) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<std::array::TryFromSliceError> for VCError {
    fn from(error: std::array::TryFromSliceError) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<serde_json::Error> for VCError {
    fn from(error: serde_json::Error) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<SetLoggerError> for VCError {
    fn from(error: SetLoggerError) -> Self {
        VCError::new(&error.to_string())
    }
}

// impl From<ConfigErrors> for VCError {
//     fn from(error: ConfigErrors) -> Self {
//         VCError::new(&error.to_string())
//     }
// }
impl From<base64::DecodeError> for VCError {
    fn from(error: base64::DecodeError) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<webrtc::Error> for VCError {
    fn from(error: webrtc::Error) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<tokio::sync::mpsc::error::SendError<VCError>> for VCError {
    fn from(error: tokio::sync::mpsc::error::SendError<VCError>) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<tokio::sync::broadcast::error::SendError<Arc<RTCDataChannel>>> for VCError {
    fn from(error: tokio::sync::broadcast::error::SendError<Arc<RTCDataChannel>>) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<tokio::sync::broadcast::error::SendError<Arc<std::os::raw::c_void>>> for VCError {
    fn from(error: tokio::sync::broadcast::error::SendError<Arc<std::os::raw::c_void>>) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<tokio::sync::broadcast::error::SendError<VCError>> for VCError {
    fn from(error: tokio::sync::broadcast::error::SendError<VCError>) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<tokio::sync::broadcast::error::SendError<Bytes>> for VCError {
    fn from(error: tokio::sync::broadcast::error::SendError<Bytes>) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<tokio::sync::broadcast::error::SendError<HashMap<String, Vec<u8>>>> for VCError {
    fn from(error: tokio::sync::broadcast::error::SendError<HashMap<String, Vec<u8>>>) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<tokio::task::JoinError> for VCError {
    fn from(error: tokio::task::JoinError) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<tokio::sync::TryLockError> for VCError {
    fn from(error: tokio::sync::TryLockError) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<actix_web::Error> for VCError {
    fn from(error: actix_web::Error) -> Self {
        VCError::new(&error.to_string())
    }
}

impl From<InvalidHeaderValue> for VCError {
    fn from(error: InvalidHeaderValue) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<BlockingError> for VCError {
    fn from(error: BlockingError) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<ErrorStack> for VCError {
    fn from(error: ErrorStack) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<webrtc::turn::Error> for VCError {
    fn from(error: webrtc::turn::Error) -> Self {
        VCError::new(&error.to_string())
    }
}
impl From<AddrParseError> for VCError {
    fn from(error: AddrParseError) -> Self {
        VCError::new(&error.to_string())
    }
}
// impl From<KafkaError> for VCError {
//     fn from(error: KafkaError) -> Self {
//         VCError::new(&error.to_string())
//     }
// }

// impl From<(KafkaError, OwnedMessage)> for VCError {
//     fn from(error: (KafkaError, OwnedMessage)) -> Self {
//         VCError::new(&error.0.to_string())
//     }
// }

impl ResponseError for VCError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::Ok().json(json!({"success": false, "message": self.msg, "code": self.code,}))
    }
}
impl std::error::Error for VCError {}

#[macro_export]
macro_rules! vcerr {
    ($($arg:tt)*) => {{
        let msg = format!($($arg)*);
        VCError::new(&msg)
    }}
}

#[macro_export]
macro_rules! vccode {
    ($x:expr, $($arg:tt)*) => {{
        let msg = format!($($arg)*);
        VCError::new_with_code(&msg, $x)
    }}
}

#[macro_export]
macro_rules! vclog {
    ( $x:expr ) => {{
        VCError::logger($x)
    }};
}
