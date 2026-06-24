use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use crate::errors::VCError;
use crate::utils::{HOST_CANDIDATE_IP, TURN_ADDR, TURN_USERS};
use log::info;
use tokio::net::UdpSocket;
use tokio::time::Duration;
use webrtc::turn::auth::*;
use webrtc::turn::relay::relay_range::RelayAddressGeneratorRanges;
use webrtc::turn::server::config::*;
use webrtc::turn::server::*;
use webrtc::turn::Error;
use webrtc::util::vnet::net::*;

struct MyAuthHandler {
    cred_map: HashMap<String, Vec<u8>>,
}

impl MyAuthHandler {
    fn new(cred_map: HashMap<String, Vec<u8>>) -> Self {
        MyAuthHandler { cred_map }
    }
}

impl AuthHandler for MyAuthHandler {
    fn auth_handle(
        &self,
        username: &str,
        _realm: &str,
        _src_addr: SocketAddr,
    ) -> Result<Vec<u8>, Error> {
        if let Some(pw) = self.cred_map.get(username) {
            //log::debug!("username={}, password={:?}", username, pw);
            Ok(pw.to_vec())
        } else {
            Err(Error::ErrFakeErr)
        }
    }
}

pub async fn start_udp() -> Result<Server, VCError> {
    let realm = "boe".to_owned();
    let mut cred_map = HashMap::new();
    let creds: Vec<&str> = TURN_USERS.split(',').collect();
    for user in creds {
        let cred: Vec<&str> = user.splitn(2, '=').collect();
        let key = generate_auth_key(cred[0], &realm, cred[1]);
        cred_map.insert(cred[0].to_owned(), key);
    }

    // Create a UDP listener to pass into pion/turn
    // turn itself doesn't allocate any UDP sockets, but lets the user pass them in
    // this allows us to add logging, storage or modify inbound/outbound traffic
    let conn = Arc::new(UdpSocket::bind(&*TURN_ADDR).await?);
    info!("turn starting {}...", conn.local_addr()?);

    let server = Server::new(ServerConfig {
        conn_configs: vec![ConnConfig {
            conn,
            relay_addr_generator: Box::new(RelayAddressGeneratorRanges {
                relay_address: IpAddr::from_str(&*HOST_CANDIDATE_IP)?,
                address: "0.0.0.0".to_owned(),
                net: Arc::new(Net::new(None)),
                min_port: 10200,
                max_port: 10200,
                max_retries: 3,
            }),
            // relay_addr_generator: Box::new(RelayAddressGeneratorStatic {
            //     relay_address: IpAddr::from_str(&*HOST_CANDIDATE_IP)?,
            //     address: "0.0.0.0".to_owned(),
            //     net: Arc::new(Net::new(None)),
            // }),
        }],
        realm: realm.to_owned(),
        auth_handler: Arc::new(MyAuthHandler::new(cred_map)),
        channel_bind_timeout: Duration::from_secs(0),
        alloc_close_notify: None,
    })
    .await?;
    Ok(server)
    // tokio::spawn(async move {
    //     // let public_ip = matches.value_of("public-ip").unwrap();
    //     // let port = matches.value_of("port").unwrap();
    //     // let users = matches.value_of("users").unwrap();
    //     // let realm = matches.value_of("realm").unwrap();

    //     // Cache -users flag for easy lookup later
    //     // If passwords are stored they should be saved to your DB hashed using turn.GenerateAuthKey

    //     server.
    //     println!("\nClosing connection now...");
    //     server.close().await?;

    //     Ok::<(), webrtc::turn::Error>(())
    // });
}
