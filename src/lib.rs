//! `vccplayer` 为video cloud codec player的简称，设计上提供多种形式可支持市面绝大多数直播/点播场景的播放器
//!
//! 该文档随代码生成，是一套全面介绍该系统功能的在线文档
//!
//! 对于web api文档，请参考[接口文档](./handlers/index.html)
//!
//! 对于接口，kafka消息或webrtc交互，请参考[流控文档](./VCCPlayerMessages.xlsx)
//!
//! 作者：[石奇峰](mailto:shiqifeng@boe.com.cn)

#[macro_use]
extern crate serde;
extern crate actix_web;
extern crate dotenv;
extern crate trackable;

#[macro_use]
pub mod errors;
// pub mod event;
// pub mod conn;
pub mod handler_ws;
pub mod handlers;
// pub mod message;
pub mod midware;
// pub mod minio;
// pub mod parser;
// pub mod mq;
pub mod processor;
pub mod routes;
pub mod stun;
// pub mod turn;

#[macro_use]
pub mod utils;
// pub mod workers;

// pub mod test;
