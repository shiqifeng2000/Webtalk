//! `stun` stun模块，用于webrtc的信令交换，同时也提供turn服务用于必要场景
//!
use crate::utils::STUN_ADDR;
use rustun::{
    message::{ErrorResponse, InvalidMessage, Request, Response, SuccessResponse},
    server::{Action, HandleMessage},
};
use std::net::SocketAddr;
use stun_codec::rfc5389;
use trackable::{error::MainError, track, track_any_err};

#[derive(Debug, Default, Clone)]
pub struct BindingHandler;
impl HandleMessage for BindingHandler {
    type Attribute = rfc5389::Attribute;

    fn handle_call(
        &mut self,
        peer: SocketAddr,
        request: Request<Self::Attribute>,
    ) -> Action<Response<Self::Attribute>> {
        if request.method() == rfc5389::methods::BINDING {
            let mut response = SuccessResponse::new(&request);
            response.add_attribute(rfc5389::attributes::XorMappedAddress::new(peer).into());
            Action::Reply(Ok(response))
        } else {
            let response = ErrorResponse::new(&request, rfc5389::errors::BadRequest.into());
            Action::Reply(Err(response))
        }
    }

    fn handle_invalid_message(
        &mut self,
        peer: SocketAddr,
        message: InvalidMessage,
    ) -> Action<Response<Self::Attribute>> {
        log::error!("[invalid] {peer} {:?}", message);
        Action::NoReply
    }

    fn handle_channel_error(&mut self, error: &rustun::Error) {
        log::error!("[ERROR] {}", error.to_string());
    }
}

pub fn start_udp() -> Result<(), MainError> {
    // let matches = App::new("binding_srv")
    //     .arg(
    //         Arg::with_name("PORT")
    //             .short("p")
    //             .long("port")
    //             .takes_value(true)
    //             .required(true)
    //             .default_value("3478"),
    //     )
    //     .get_matches();
    let stun_address = track_any_err!(STUN_ADDR.parse())?;

    let stun_server = track!(fibers_global::execute(rustun::server::UdpServer::start(
        fibers_global::handle(),
        stun_address,
        // rustun::server::BindingHandler
        BindingHandler
    )))?;

    fibers_global::spawn_monitor(stun_server);
    // let port = matches.value_of("PORT").unwrap();
    // if let Ok(stun_addr) = env::var("STUN_ADDR") {

    // }

    // TODO monitor skipped
    // if let Ok(turn_addr) = env::var("TURN_ADDR") {
    //     let username = env::var("TURN_USERNAME").unwrap_or("shiqifeng@boe.com.cn".to_string());
    //     let credential = env::var("TURN_PASSWORD").unwrap_or("Boe888888".to_string());
    //     let realm = env::var("TURN_REALM").unwrap_or("localhost".to_string());
    //     let turn_address = track_any_err!(turn_addr.parse())?;

    //     let turn_server = track!(fibers_global::execute(rusturn::server::UdpServer::start(
    //         turn_address,
    //         rusturn::auth::AuthParams::with_realm_and_nonce(&username, &credential, &realm, "qux")?
    //     )))?;

    //     // TODO monitor skipped
    //     fibers_global::spawn_monitor(turn_server);
    // }

    Ok(())
}

#[test]
pub fn test_stun() {
    start_udp().ok();
    assert!(true);
}
