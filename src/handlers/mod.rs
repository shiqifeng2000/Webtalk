use crate::{
    errors::VCError,
    tokio_read_lock,
    utils::{self, ConfSessions, RtcJob, RtcSessions, PEER_STUN_ADDRS},
};
use actix_files::{Files, NamedFile};
use actix_web::{get, http, post, web, HttpRequest, HttpResponse, Result};
use log::info;
use serde_json::json;
use std::{fs::File, io::BufReader, sync::Arc, time::Duration};
use tokio::{
    sync::{Notify, RwLock},
    time::Instant,
};
use webrtc::{
    api::{
        media_engine::{MIME_TYPE_H264, MIME_TYPE_OPUS},
        API,
    },
    ice_transport::{ice_connection_state::RTCIceConnectionState, ice_server::RTCIceServer},
    media::{
        io::{h264_reader::H264Reader, ogg_reader::OggReader},
        Sample,
    },
    peer_connection::{
        configuration::RTCConfiguration, peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription,
    },
    rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
    track::track_local::{track_local_static_sample::TrackLocalStaticSample, TrackLocal},
};
const OGG_PAGE_DURATION: Duration = Duration::from_millis(20);

// mod jitter;
#[cfg(feature = "es")]
pub mod es_streamer;
pub mod http_conf;
#[cfg(feature = "fmp4")]
pub mod mp4_streamer;
pub mod publish;
pub mod subscribe;
// pub mod restrict;
// mod vp9;

#[post("/peer")]
pub async fn peer(
    peer_job: web::Json<RtcJob>,
    api: web::Data<API>,
    // req: HttpRequest,
) -> Result<HttpResponse, VCError> {
    let offer_str = &peer_job.peer;
    let desc_data = utils::btoa(offer_str)?;
    let offer = serde_json::from_str::<RTCSessionDescription>(&desc_data)?;
    utils::check_sdp(&offer.sdp)?;
    let stuns = PEER_STUN_ADDRS
        .split(",")
        .filter(|v| *v != "")
        .map(|v| v.to_owned())
        .collect::<Vec<String>>();
    let ice_servers = if stuns.len() > 0 {
        vec![RTCIceServer {
            // urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            urls: stuns
                .iter()
                .map(|v| format!("stun:{}", v))
                .collect::<Vec<String>>(),
            ..Default::default()
        }]
    } else {
        vec![]
    };
    // let ice_servers = vec![RTCIceServer {
    //     urls: vec!["stun:stun.l.google.com:19302".to_owned()],
    //     // urls: stuns
    //     //     .iter()
    //     //     .map(|v| format!("stun:{}", v))
    //     //     .collect::<Vec<String>>(),
    //     ..Default::default()
    // }];
    let conf = RTCConfiguration {
        ice_servers,
        ..Default::default()
    };
    let connection = Arc::new(api.new_peer_connection(conf).await?);

    // let audio_track = Arc::new(TrackLocalStaticSample::new(
    //     RTCRtpCodecCapability {
    //         mime_type: MIME_TYPE_OPUS.to_owned(),
    //         ..Default::default()
    //     },
    //     "audio".to_owned(),
    //     "webrtc-rs".to_owned(),
    // ));
    // let audio_sender = connection
    //     .add_track(audio_track.clone() as Arc<dyn TrackLocal + Send + Sync>)
    //     .await?;

    let notify_tx = Arc::new(Notify::new());
    let notify_video = notify_tx.clone();
    let notify_audio = notify_tx.clone();

    // let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);
    // let video_done_tx = done_tx.clone();

    // Create a video track
    let video_track = Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_H264.to_owned(),
            ..Default::default()
        },
        "video".to_owned(),
        "webrtc-rs".to_owned(),
    ));

    // Add this newly created track to the PeerConnection
    let rtp_sender = connection
        .add_track(Arc::clone(&video_track) as Arc<dyn TrackLocal + Send + Sync>)
        .await?;

    // Read incoming RTCP packets
    // Before these packets are returned they are processed by interceptors. For things
    // like NACK this needs to be called.
    let peer_connection = connection.clone();
    tokio::spawn(async move {
        let mut rtcp_buf = vec![0u8; 1500];
        loop {
            let timeout = tokio::time::sleep(Duration::from_secs(4));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {
                    if peer_connection.connection_state() == RTCPeerConnectionState::Closed || peer_connection.connection_state() == RTCPeerConnectionState::Failed {
                        break;
                    }
                }
                m = rtp_sender.read(&mut rtcp_buf) => {
                    if m.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = rtp_sender.stop().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        if peer_connection.connection_state() != RTCPeerConnectionState::Closed {
            let _ = peer_connection.close().await;
        }
        info!("closing video rtcp");
    });

    let video_file_name = "./static/test.h264".to_owned();
    let peer_connection = connection.clone();
    tokio::spawn(async move {
        // Open a H264 file and start reading using our H264Reader
        let file = File::open(&video_file_name).unwrap();
        let reader = BufReader::new(file);
        let mut h264 = H264Reader::new(reader, 4096);

        // Wait for connection established
        let _ = notify_video.notified().await;
        let mut len = 0;
        println!("play video from disk file {}", video_file_name);

        // It is important to use a time.Ticker instead of time.Sleep because
        // * avoids accumulating skew, just calling time.Sleep didn't compensate for the time spent parsing the data
        // * works around latency issues with Sleep
        let mut ticker = tokio::time::interval(Duration::from_millis(40));
        loop {
            if peer_connection.connection_state() == RTCPeerConnectionState::Closed
                || peer_connection.connection_state() == RTCPeerConnectionState::Disconnected
            {
                break;
            }
            let time = Instant::now();
            let nal = match h264.next_nal() {
                Ok(nal) => nal,
                Err(err) => {
                    println!("All video frames parsed and sent: {}", err);
                    break;
                }
            };
            len += nal.data.len();
            println!(
                "PictureOrderCount={}, ForbiddenZeroBit={}, RefIdc={}, UnitType={}, data={}, total={} {}",
                nal.picture_order_count,
                nal.forbidden_zero_bit,
                nal.ref_idc,
                nal.unit_type,
                nal.data.len(),
                len,
                Instant::now().duration_since(time).as_millis()
            );

            video_track
                .write_sample(&Sample {
                    data: nal.data.freeze(),
                    duration: Duration::from_millis(40),
                    ..Default::default()
                })
                .await
                .unwrap();

            let _ = ticker.tick().await;
        }

        // let _ = video_done_tx.try_send(());
        info!("closing video rtp");
    });

    // Create a audio track
    let audio_track = Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_owned(),
            ..Default::default()
        },
        "audio".to_owned(),
        "webrtc-rs".to_owned(),
    ));

    // Add this newly created track to the PeerConnection
    let rtp_sender = connection
        .add_track(Arc::clone(&audio_track) as Arc<dyn TrackLocal + Send + Sync>)
        .await?;

    // Read incoming RTCP packets
    // Before these packets are returned they are processed by interceptors. For things
    // like NACK this needs to be called.
    let peer_connection = connection.clone();
    tokio::spawn(async move {
        let mut rtcp_buf = vec![0u8; 1500];
        loop {
            let timeout = tokio::time::sleep(Duration::from_secs(4));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {
                    if peer_connection.connection_state() == RTCPeerConnectionState::Closed || peer_connection.connection_state() == RTCPeerConnectionState::Failed {
                        break;
                    }
                }
                m = rtp_sender.read(&mut rtcp_buf) => {
                    if m.is_err() {
                        break;
                    }
                }
            }
        }
        // while let Ok((_, _)) = rtp_sender.read(&mut rtcp_buf).await {}
        info!("closing audio rtcp");
    });
    let peer_connection = connection.clone();
    tokio::spawn(async move {
        // Open a IVF file and start reading using our IVFReader
        let file = File::open("./static/test.ogg").unwrap();
        // let file = File::open("/data/workspace/boe/vccplayer/crates/webrtc/examples/examples/play-from-disk-h264/output.ogg").unwrap();

        let reader = BufReader::new(file);
        // Open on oggfile in non-checksum mode.
        let (mut ogg, _) = OggReader::new(reader, true).unwrap();

        // Wait for connection established
        let _ = notify_audio.notified().await;

        println!("play audio from disk file output.ogg");

        // It is important to use a time.Ticker instead of time.Sleep because
        // * avoids accumulating skew, just calling time.Sleep didn't compensate for the time spent parsing the data
        // * works around latency issues with Sleep
        let mut ticker = tokio::time::interval(OGG_PAGE_DURATION);

        // Keep track of last granule, the difference is the amount of samples in the buffer
        let mut last_granule: u64 = 0;

        while let Ok((page_data, page_header)) = ogg.parse_next_page() {
            if peer_connection.connection_state() == RTCPeerConnectionState::Closed
                || peer_connection.connection_state() == RTCPeerConnectionState::Disconnected
            {
                break;
            }
            // The amount of samples is the difference between the last and current timestamp
            let sample_count = page_header.granule_position - last_granule;
            last_granule = page_header.granule_position;
            let sample_duration = Duration::from_millis(sample_count * 1000 / 48000);

            println!(
                "sample_count {}, duration {} page_data {}",
                sample_count,
                sample_count * 1000 / 48000,
                page_data.len()
            );
            // let _time = Instant::now();
            audio_track
                .write_sample(&Sample {
                    data: page_data.freeze(),
                    duration: sample_duration,
                    ..Default::default()
                })
                .await
                .unwrap();
            // tokio::time::sleep_until(time.checked_add(sample_duration).unwrap()).await;
            let _ = ticker.tick().await;
        }
        info!("closing audio rtp");
    });

    // Set the handler for ICE connection state
    // This will notify you when the peer has connected/disconnected
    connection.on_ice_connection_state_change(Box::new(
        move |connection_state: RTCIceConnectionState| {
            println!("Connection State has changed {}", connection_state);
            if connection_state == RTCIceConnectionState::Connected {
                notify_tx.notify_waiters();
            }
            Box::pin(async {})
        },
    ));

    // Set the handler for Peer connection state
    // This will notify you when the peer has connected/disconnected
    // connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
    //     println!("Peer Connection State has changed: {}", s);

    //     if s == RTCPeerConnectionState::Failed {
    //         // Wait until PeerConnection has had no network activity for 30 seconds or another failure. It may be reconnected using an ICE Restart.
    //         // Use webrtc.PeerConnectionStateDisconnected if you are interested in detecting faster timeout.
    //         // Note that the PeerConnection may come back from PeerConnectionStateDisconnected.
    //         println!("Peer Connection has gone to failed exiting");
    //         let _ = done_tx.try_send(());
    //     }

    //     Box::pin(async {})
    // }));

    connection.set_remote_description(offer).await?;
    let answer = connection.create_answer(None).await?;
    let mut gather_complete = connection.gathering_complete_promise().await;
    connection.set_local_description(answer).await?;
    let _ = gather_complete.recv().await;
    let result = connection
        .local_description()
        .await
        .map(|v| serde_json::to_string(&v).map_err(|e| e.into()))
        .unwrap_or(Err(VCError::new("Error getting server sdp description")))?;

    let data = utils::atob(&result);
    Ok(HttpResponse::Ok().json(json!({"success": true, "data": data})))
}

#[get("/")]
pub async fn idx_file(req: HttpRequest) -> Result<HttpResponse> {
    let mut res: HttpResponse = NamedFile::open("./static/index.html")?.into_response(&req);
    (&mut res).head_mut().headers_mut().insert(
        http::header::HeaderName::from_static("cache-control"),
        http::header::HeaderValue::from_static("private, no-cache, no-store, must-revalidate"),
    );
    Ok(res)
}

#[get("/sessions")]
pub async fn get_sessions(
    sessions: web::Data<RwLock<RtcSessions>>,
    confs: web::Data<RwLock<ConfSessions>>,
) -> Result<HttpResponse> {
    let hash = {
        let s = tokio_read_lock!(sessions, 10)?;
        s.hash.clone()
    };
    let confs1 = {
        let s = tokio_read_lock!(confs, 10)?;
        s.hash.clone()
    };
    Ok(HttpResponse::Ok().json(json!({"success": true, "sessions":hash, "confs":confs1})))
}

pub fn static_file() -> Files {
    Files::new("/", "./static").use_last_modified(true)
}

pub fn storage_file() -> Files {
    Files::new("/storage", "./storage").use_last_modified(true)
}

pub fn doc_file() -> Files {
    Files::new("/document", "./target/doc").use_last_modified(true)
}
