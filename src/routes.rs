//! `routes` 为路由模块，负责工程中的路由匹配，路由保护等规则
//!
use actix_web::web;

use crate::{handler_ws, handlers};

pub fn routes(cfg: &mut web::ServiceConfig) {
    let mut factory = web::scope("")
        .service(web::resource("/ws").to(handler_ws::ws))
        .service(handlers::get_sessions)
        .service(handlers::peer)
        .service(handlers::publish::publish_stream)
        .service(handlers::publish::publish_simucast)
        .service(handlers::subscribe::subscribe_stream);

    factory = factory
        .service(handlers::http_conf::create)
        .service(handlers::http_conf::join)
        .service(handlers::http_conf::quit)
        .service(handlers::http_conf::upstream)
        .service(handlers::http_conf::downstream);

    #[cfg(feature = "fmp4")]
    {
        factory = factory.service(handlers::mp4_streamer::http_streamer);
    }
    #[cfg(feature = "es")]
    {
        factory = factory.service(handlers::es_streamer::http_streamer);
    }
    // factory = if *SOCEKT_MODE == 0 {
    //     factory
    //         .service(handlers::subscribe::publish_stream)
    //         .service(handlers::subscribe::subscribe_stream)
    //         .service(handlers::subscribe::publish_subscribe_stream)
    // } else {
    //     factory
    //         .service(handlers::restrict::publish_stream)
    //         .service(handlers::restrict::subscribe_stream)
    //         .service(handlers::restrict::publish_subscribe_stream)
    // };
    factory = factory
        .service(handlers::idx_file)
        .service(handlers::static_file());
    cfg.service(factory);
}

// pub fn static_routes(cfg: &mut web::ServiceConfig) {
//     cfg.service(
//         web::scope("")
//             .guard(guard::Get())
//             .service(handlerss::files::main)
//             .service(handlerss::files::static_file()),
//     );
// }
