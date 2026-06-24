use std::sync::Arc;

use actix_web::{middleware, web, App, HttpServer};
use dotenv::dotenv;
use tokio::sync::RwLock;
use webtalk::{
    midware::{self, SecureCheck},
    // mq::start_mq_actor,
    routes::routes,
    stun,
    utils::{
        self, cleaner, gen_webrtc_api, ConfSessions, P2PSessions, RtcSessions, RtcSocketPool,
        LOGGER, SERVER_PORT, SSL_SERVER_PORT,
    },
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    cleaner();
    // env_logger::init();
    log4rs::init_file(&*LOGGER, Default::default()).unwrap();
    let api = gen_webrtc_api(false);
    let api_data = web::Data::new(api);

    let sockets_pool = web::Data::from(Arc::new(RwLock::new(RtcSocketPool::new())));
    let p2p_sessions = web::Data::from(P2PSessions::init_arc());
    let rtc_sessions = web::Data::from(RtcSessions::init_arc());
    let conf_sessions = web::Data::from(ConfSessions::init_arc());
    stun::start_udp().expect("Stun server error!");
    // let turn = turn::start_udp().await.expect("Turn server error");
    // let mq_addr = web::Data::new(start_mq_actor().expect("start mq failed"));

    let ssl_api_data = api_data.clone();
    let ssl_p2p_sessions = p2p_sessions.clone();
    let ssl_rtc_sessions = rtc_sessions.clone();
    let ssl_conf_sessions = conf_sessions.clone();
    let ssl_sockets_pool = sockets_pool.clone();
    // let ssl_mq_addr = mq_addr.clone();
    let builder = utils::gen_ssl_builder().expect("Ssl builder error!");

    let result = futures::join!(
        HttpServer::new(move || {
            App::new()
                .wrap(middleware::Logger::default())
                .wrap(midware::default())
                .wrap(SecureCheck)
                .app_data(api_data.clone())
                .app_data(p2p_sessions.clone())
                .app_data(rtc_sessions.clone())
                .app_data(conf_sessions.clone())
                .app_data(sockets_pool.clone())
                // .app_data(mq_addr.clone())
                .configure(routes)
        })
        .bind(format!("0.0.0.0:{}", &*SERVER_PORT))?
        .run(),
        HttpServer::new(move || {
            App::new()
                .wrap(middleware::Logger::default())
                .wrap(midware::default())
                .wrap(SecureCheck)
                .app_data(ssl_api_data.clone())
                .app_data(ssl_p2p_sessions.clone())
                .app_data(ssl_rtc_sessions.clone())
                .app_data(ssl_sockets_pool.clone())
                .app_data(ssl_conf_sessions.clone())
                // .app_data(ssl_mq_addr.clone())
                .configure(routes)
        })
        .bind_openssl(format!("0.0.0.0:{}", &*SSL_SERVER_PORT), builder)?
        .run()
    )
    .0;

    // let _ = turn.close().await;
    result
    // HttpServer::new(move || {
    //     App::new()
    //         .wrap(middleware::Logger::default())
    //         .wrap(midware::default())
    //         .app_data(api_data.clone())
    //         // .app_data(thread_pool_data.clone())
    //         .configure(routes)
    // })
    // .bind(format!("0.0.0.0:{}", &*SERVER_PORT))?
    // .run()
    // .await
    // manager_clone.write().await.handle.replace(serv.handle());
    // serv.await
}
