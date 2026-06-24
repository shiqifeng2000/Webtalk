use crate::{
    errors::VCError,
    // mq::{MqActor, StreamDownEvent, StreamUnstableEvent, StreamUpEvent},
    tokio_watch_lock,
    tokio_write_lock,
    utils::{
        self, peer_closed, weak_peer_closed, RidPacket, RtcJob, RtcSessions, MAX_SESSION_TIME,
        PEER_STUN_ADDRS,
    },
};
use actix_web::{post, web, HttpResponse, Result};
use bytes::Bytes;
use log::{debug, info};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch, Notify, RwLock},
    time::Instant,
};
use webrtc::{api::media_engine::MIME_TYPE_OPUS, peer_connection::RTCPeerConnection};
use webrtc::{
    api::API,
    data_channel::RTCDataChannel,
    ice_transport::{
        ice_candidate_pair::RTCIceCandidatePair, ice_connection_state::RTCIceConnectionState,
        ice_server::RTCIceServer,
    },
    peer_connection::{
        configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
    },
    rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
    rtp_transceiver::{
        rtp_codec::RTPCodecType, rtp_receiver::RTCRtpReceiver,
        rtp_transceiver_direction::RTCRtpTransceiverDirection, RTCRtpTransceiver,
        RTCRtpTransceiverInit,
    },
    track::track_remote::TrackRemote,
};

/// 只发不收
#[post("/publish")]
pub async fn publish_stream(
    peer_job: web::Json<RtcJob>,
    api: web::Data<API>,
    sessions: web::Data<RwLock<RtcSessions>>,
    // mq_addr: web::Data<Addr<MqActor>>,
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
    let conf = RTCConfiguration {
        ice_servers,
        ..Default::default()
    };
    let connection = if let Some((vcodec, _acodec)) = peer_job.parse_codecs() {
        let myapi = utils::api_from_codecs(vcodec.map(|v| vec![v]).unwrap_or(vec![]), false);
        Arc::new(myapi.new_peer_connection(conf).await?)
    } else {
        Arc::new(api.new_peer_connection(conf).await?)
    };

    connection
        .add_transceiver_from_kind(
            RTPCodecType::Audio,
            Some(RTCRtpTransceiverInit {
                direction: RTCRtpTransceiverDirection::Recvonly,
                send_encodings: vec![],
            }),
            // None,
        )
        .await?;
    connection
        .add_transceiver_from_kind(
            RTPCodecType::Video,
            Some(RTCRtpTransceiverInit {
                direction: RTCRtpTransceiverDirection::Recvonly,
                send_encodings: vec![],
            }),
        )
        .await?;

    let (vsdr, _) = broadcast::channel(1024 * 1024);
    let (asdr, _) = broadcast::channel(1024 * 1024);
    let (dsdr, _) = broadcast::channel(1024 * 1024);
    let session = {
        let id = peer_job.token.unwrap_or(rand::random::<u32>());
        let mut s = tokio_write_lock!(sessions, 10)?;
        let result = s.create_session(id, vec![], &vsdr, &asdr, &dsdr, &connection)?;
        result
    };
    let sid = session.id;
    let announcer = session.announcer.clone();
    let (payload_pool_sdr, payload_pool_rcv) = watch::channel(None);
    let pc = Arc::downgrade(&connection);
    let pc1 = pc.clone();
    let sessions1 = sessions.into_inner();
    let notify = Arc::new(Notify::new());
    let notify1 = notify.clone();
    connection.on_track(Box::new(
        move |track: Arc<TrackRemote>,
              _receiver: Arc<RTCRtpReceiver>,
              _tranceiver: Arc<RTCRtpTransceiver>| {
            let kind = track.kind();

            if kind == RTPCodecType::Video {
                on_video_track(
                    sid,
                    track,
                    &notify1,
                    &announcer,
                    &vsdr,
                    &sessions1,
                    &payload_pool_rcv,
                    &pc1,
                );
            } else if kind == RTPCodecType::Audio {
                on_audio_track(
                    sid,
                    track,
                    &notify1,
                    &asdr,
                    &sessions1,
                    &payload_pool_rcv,
                    &pc1,
                );
            }
            Box::pin(async {})
        },
    ));

    // let connection1 = connection.clone();
    let notify1 = notify.clone();
    connection.on_ice_connection_state_change(Box::new(
        move |connection_state: RTCIceConnectionState| {
            info!("Session state changed {connection_state}",);
            // if connection_state == RTCIceConnectionState::Connected {
            //     mq_addr.do_send(StreamUpEvent::new(sid));
            // } else if connection_state == RTCIceConnectionState::Disconnected {
            //     mq_addr.do_send(StreamUnstableEvent::new(sid));
            // }
            if connection_state == RTCIceConnectionState::Closed
                || connection_state == RTCIceConnectionState::Failed
            {
                // if connection_state == RTCIceConnectionState::Closed {
                //     mq_addr.do_send(StreamDownEvent::new(sid));
                // }
                notify1.notify_waiters();
            }
            Box::pin(async {})
        },
    ));

    let pc1 = pc.clone();
    let dsdr1 = dsdr.clone();
    connection.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
        let dc1 = dc.clone();
        let pc2 = pc1.clone();
        let drcv2 = dsdr1.subscribe();
        info!("Session {sid} data_channel open");
        dc.on_open(Box::new(move || {
            Box::pin(async move {
                on_data_channel_open(sid, &dc1, drcv2, &pc2).await;
            })
        }));
        Box::pin(async move {})
    }));

    // close 需要放在事件外面
    let connection1 = connection.clone();
    tokio::spawn(async move {
        let timeout = tokio::time::sleep(Duration::from_secs(*MAX_SESSION_TIME));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {}
            _ = notify.notified() => {}
        }
        let closer = connection1.close().await;
        info!("[C->S] closing {sid} peer {:?}", closer);
    });

    connection.set_remote_description(offer).await?;
    if let Some(candidates) = &peer_job.candidates {
        for candidate in candidates
            .iter()
            .filter(|v| v.is_some())
            .map(|v| v.as_ref().unwrap())
        {
            let _ = connection.add_ice_candidate(candidate.clone()).await;
        }
    }
    let answer = connection.create_answer(None).await?;

    let payloads = utils::parse_payloads(&answer.sdp)?;
    let _ = payload_pool_sdr.send(Some(payloads));

    let mut gather_complete = connection.gathering_complete_promise().await;
    connection.set_local_description(answer).await?;
    let _ = gather_complete.recv().await;
    let result = connection
        .local_description()
        .await
        .map(|v| serde_json::to_string(&v).map_err(|e| e.into()))
        .unwrap_or(Err(VCError::new("Error getting server sdp description")))?;

    connection
        .sctp()
        .transport()
        .ice_transport()
        .on_selected_candidate_pair_change(Box::new(move |p: RTCIceCandidatePair| {
            info!("[C->S] candidate pair {p}");
            Box::pin(async {})
        }));

    let data = utils::atob(&result);
    Ok(HttpResponse::Ok().json(json!({"success": true, "sid":sid, "data": data})))
}

/// 只发不收
#[post("/publish_simucast")]
pub async fn publish_simucast(
    peer_job: web::Json<RtcJob>,
    sessions: web::Data<RwLock<RtcSessions>>,
    // mq_addr: web::Data<Addr<MqActor>>,
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
    let conf = RTCConfiguration {
        ice_servers,
        ..Default::default()
    };
    let connection = if let Some((vcodec, _acodec)) = peer_job.parse_codecs() {
        let myapi = utils::api_from_codecs(vcodec.map(|v| vec![v]).unwrap_or(vec![]), true);
        Arc::new(myapi.new_peer_connection(conf).await?)
    } else {
        let myapi = utils::gen_webrtc_api(true);
        Arc::new(myapi.new_peer_connection(conf).await?)
    };

    connection
        .add_transceiver_from_kind(
            RTPCodecType::Audio,
            Some(RTCRtpTransceiverInit {
                direction: RTCRtpTransceiverDirection::Recvonly,
                send_encodings: vec![],
            }),
            // None,
        )
        .await?;
    connection
        .add_transceiver_from_kind(
            RTPCodecType::Video,
            Some(RTCRtpTransceiverInit {
                direction: RTCRtpTransceiverDirection::Recvonly,
                send_encodings: vec![],
            }),
        )
        .await?;

    let (vsdr, _) = broadcast::channel(1024 * 1024);
    let (asdr, _) = broadcast::channel(1024 * 1024);
    let (dsdr, _) = broadcast::channel(1024 * 1024);
    let session = {
        let id = peer_job.token.unwrap_or(rand::random::<u32>());
        let mut s = tokio_write_lock!(sessions, 10)?;
        let result = s.create_session(id, vec![], &vsdr, &asdr, &dsdr, &connection)?;
        result
    };
    let sid = session.id;
    let announcer = session.announcer.clone();
    let (payload_pool_sdr, payload_pool_rcv) = watch::channel(None);
    let pc = Arc::downgrade(&connection);
    let pc1 = pc.clone();
    let sessions1 = sessions.clone().into_inner();
    let notify = Arc::new(Notify::new());
    let notify1 = notify.clone();
    connection.on_track(Box::new(
        move |track: Arc<TrackRemote>,
              _receiver: Arc<RTCRtpReceiver>,
              _tranceiver: Arc<RTCRtpTransceiver>| {
            let kind = track.kind();

            if kind == RTPCodecType::Video {
                on_video_simucast_track(
                    sid,
                    track,
                    &notify1,
                    &announcer,
                    &vsdr,
                    &sessions1,
                    &payload_pool_rcv,
                    &pc1,
                );
            } else if kind == RTPCodecType::Audio {
                on_audio_track(
                    sid,
                    track,
                    &notify1,
                    &asdr,
                    &sessions1,
                    &payload_pool_rcv,
                    &pc1,
                );
            }
            Box::pin(async {})
        },
    ));

    // let connection1 = connection.clone();
    let notify1 = notify.clone();
    connection.on_ice_connection_state_change(Box::new(
        move |connection_state: RTCIceConnectionState| {
            info!("Session state changed {connection_state}",);
            // if connection_state == RTCIceConnectionState::Connected {
            //     mq_addr.do_send(StreamUpEvent::new(sid));
            // } else if connection_state == RTCIceConnectionState::Disconnected {
            //     mq_addr.do_send(StreamUnstableEvent::new(sid));
            // }
            if connection_state == RTCIceConnectionState::Closed
                || connection_state == RTCIceConnectionState::Failed
            {
                // if connection_state == RTCIceConnectionState::Closed {
                //     mq_addr.do_send(StreamDownEvent::new(sid));
                // }
                notify1.notify_waiters();
            }
            Box::pin(async {})
        },
    ));

    let pc1 = pc.clone();
    let dsdr1 = dsdr.clone();
    connection.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
        let dc1 = dc.clone();
        let pc2 = pc1.clone();
        let drcv2 = dsdr1.subscribe();
        info!("Session {sid} data_channel open");
        dc.on_open(Box::new(move || {
            Box::pin(async move {
                on_data_channel_open(sid, &dc1, drcv2, &pc2).await;
            })
        }));
        Box::pin(async move {})
    }));

    // close 需要放在事件外面
    let connection1 = connection.clone();
    tokio::spawn(async move {
        let timeout = tokio::time::sleep(Duration::from_secs(*MAX_SESSION_TIME));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {}
            _ = notify.notified() => {}
        }
        let closer = connection1.close().await;
        info!("[C->S] closing {sid} peer {:?}", closer);
    });

    connection.set_remote_description(offer).await?;
    if let Some(candidates) = &peer_job.candidates {
        for candidate in candidates
            .iter()
            .filter(|v| v.is_some())
            .map(|v| v.as_ref().unwrap())
        {
            let _ = connection.add_ice_candidate(candidate.clone()).await;
        }
    }
    let answer = connection.create_answer(None).await?;

    let payloads = utils::parse_payloads(&answer.sdp)?;
    // 如果没有音频则设置音频标志
    if !payloads.contains_key(&*MIME_TYPE_OPUS) {
        let mut s = tokio_write_lock!(sessions, 10000)?;
        if let Some(t) = s.get_session_mut(sid) {
            t.set_audio(Some(HashMap::new()));
        }
    }
    let _ = payload_pool_sdr.send(Some(payloads));
    // {
    //     let mut pool = tokio_write_lock!(payload_pool, 10)?;
    //     pool.replace(payloads);
    // }

    let mut gather_complete = connection.gathering_complete_promise().await;
    connection.set_local_description(answer).await?;
    let _ = gather_complete.recv().await;
    let result = connection
        .local_description()
        .await
        .map(|v| serde_json::to_string(&v).map_err(|e| e.into()))
        .unwrap_or(Err(VCError::new("Error getting server sdp description")))?;

    connection
        .sctp()
        .transport()
        .ice_transport()
        .on_selected_candidate_pair_change(Box::new(move |p: RTCIceCandidatePair| {
            info!("[C->S] candidate pair {p}");
            Box::pin(async {})
        }));

    let data = utils::atob(&result);
    Ok(HttpResponse::Ok().json(json!({"success": true, "sid":sid, "data": data})))
}
pub fn on_video_track(
    sid: u32,
    track: Arc<TrackRemote>,
    notify1: &Arc<Notify>,
    announcer: &broadcast::Sender<Weak<String>>,
    vsdr: &broadcast::Sender<RidPacket>,
    sessions1: &Arc<RwLock<RtcSessions>>,
    payload_pool_rcv: &watch::Receiver<Option<HashMap<String, Vec<u8>>>>,
    pc1: &Weak<RTCPeerConnection>,
) {
    let media_ssrc = track.ssrc();
    debug!(target:"debug","[C->S] receiving {sid}-video track {media_ssrc}");
    let announcer2 = announcer.clone();
    let pc2 = pc1.clone();
    tokio::spawn(async move {
        let mut lrcv = announcer2.subscribe();
        let mut hb = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    hb = Instant::now();
                }
                _ = lrcv.recv() => {
                    let time_past = Instant::now().duration_since(hb).as_millis();
                    if time_past < 400 {
                        tokio::time::sleep(Duration::from_millis(400)).await;
                    }
                }
                // a = jitter_recv.recv() => {
                //     hb = Instant::now();
                //     a.unwrap_or(None)
                // }
            };
            if let Some(pc3) = pc2.upgrade() {
                if peer_closed(&pc3) {
                    break;
                }
                if let Err(_e) = pc3
                    .write_rtcp(&[Box::new(PictureLossIndication {
                        sender_ssrc: 0,
                        media_ssrc,
                    })])
                    .await
                {
                    break;
                }
                // if let Some(v) = nacks {
                //     log::debug!(target:"debug", "sending nacks {v:?}");
                //     if let Err(e) = pc3
                //         .write_rtcp(&[Box::new(TransportLayerNack {
                //             sender_ssrc: 0,
                //             media_ssrc,
                //             nacks: nack_pairs_from_sequence_numbers(&v),
                //         })])
                //         .await
                //     {
                //         break;
                //     }
                // } else {
                // }
            } else {
                break;
            }
        }
        info!("[C->S] closing {sid}-video rtcp thread");
    });

    let pc2 = pc1.clone();
    let sessions2 = sessions1.clone();
    let vsdr1 = vsdr.clone();
    let notify2 = notify1.clone();
    let mut payload_pool_rcv1 = payload_pool_rcv.clone();
    tokio::spawn(async move {
        let mut hb = Instant::now();
        let mut init = false;
        let rid = track.rid();
        payload_pool_rcv1.borrow_and_update();
        let payload_pool = match tokio_watch_lock!(payload_pool_rcv1, 20000) {
            Ok(_) => {
                let value = payload_pool_rcv1.borrow_and_update();
                if value.is_none() {
                    debug!("[C->S] {sid}-video payload_pool must not be none");
                    return;
                }
                value.as_ref().unwrap().to_owned()
            }
            Err(_) => {
                debug!("[C->S] {sid}-video read thread wait timeout");
                notify2.notify_waiters();
                return;
            }
        };
        // let mut jitter = JitterBuffer::<Packet, 50>::new(Some(&jitter_sndr));
        // let mut local_hash = HashSet::new();
        // let mut vp9 = Vp9Depacketizer::default();
        // use webrtc::media::io::Writer;
        // let mut h264_writer = H264Writer::new(std::fs::File::create("./test.264").unwrap());
        // let mut ivf_writer=
        //     IVFWriter::new(
        //         std::fs::File::create("./test.vp9").unwrap(),
        //         &IVFFileHeader {
        //             signature: *b"DKIF",                               // 0-3
        //             version: 0,                                        // 4-5
        //             header_size: 32,                                   // 6-7
        //             four_cc: *b"VP90", // 8-11
        //             width: 1280,                                        // 12-13
        //             height: 720,                                       // 14-15
        //             timebase_denominator: 30,                          // 16-19
        //             timebase_numerator: 1,                             // 20-23
        //             num_frames: 900,                                   // 24-27
        //             unused: 0,                                         // 28-31
        //         },
        //     ).unwrap();

        loop {
            let timeout = tokio::time::sleep(Duration::from_secs(4));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {
                    debug!("[C->S] {sid}-video read thread read timeout");
                    break;
                }
                m = track.read_rtp() => {
                    let now = Instant::now();
                    if now.duration_since(hb).as_secs() > 4 {
                        if weak_peer_closed(&pc2) {
                            debug!("[C->S] {sid}-video read thread timeout from peer close");
                            break;
                        }
                    }
                    if let Ok((rtp, _n)) = m {
                        if !init {
                            if let Ok(mut s) = sessions2.try_write() {
                                if s.set_session_video(sid, &rid, rtp.header.payload_type,media_ssrc, &payload_pool).is_some() {
                                    init = true;
                                } else {
                                    debug!("[C->S] {sid}-video read thread end from no such task");
                                    break;
                                }
                            }
                        }
                        let _ = vsdr1.send(RidPacket::new(rid, rtp));
                        hb = now;
                    } else if weak_peer_closed(&pc2) {
                        debug!("[C->S] {sid}-video read thread end from peer close");
                        break;
                    }
                }
            }
        }
        info!("[C->S] closing {sid}-video read thread");
    });
}

pub fn on_audio_track(
    sid: u32,
    track: Arc<TrackRemote>,
    notify1: &Arc<Notify>,
    asdr: &broadcast::Sender<RidPacket>,
    sessions1: &Arc<RwLock<RtcSessions>>,
    payload_pool_rcv: &watch::Receiver<Option<HashMap<String, Vec<u8>>>>,
    pc1: &Weak<RTCPeerConnection>,
) {
    let media_ssrc = track.ssrc();
    debug!(target:"debug","[C->S] receiving {sid}-audio track {media_ssrc}");
    let asdr1 = asdr.clone();
    let mut payload_pool_rcv1 = payload_pool_rcv.clone();
    let pc2 = pc1.clone();
    let sessions2 = sessions1.clone();
    let notify2 = notify1.clone();
    tokio::spawn(async move {
        let mut init = false;
        let mut hb = Instant::now();
        let rid = track.rid();
        payload_pool_rcv1.borrow_and_update();
        let payload_pool = match tokio_watch_lock!(payload_pool_rcv1, 20000) {
            Ok(_) => {
                let value = payload_pool_rcv1.borrow_and_update();
                if value.is_none() {
                    debug!("[C->S] {sid}-audio payload_pool must not be none");
                    return;
                }
                value.as_ref().unwrap().to_owned()
            }
            Err(_) => {
                debug!("[C->S] {sid}-audio read thread wait timeout");
                notify2.notify_waiters();
                return;
            }
        };
        loop {
            let timeout = tokio::time::sleep(Duration::from_secs(4));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {
                    debug!("[C->S] {sid}-audio read thread read timeout");
                    break;
                }
                m = track.read_rtp() => {
                    let now = Instant::now();
                    if now.duration_since(hb).as_secs() > 4 {
                        if weak_peer_closed(&pc2) {
                            debug!("[C->S] {sid}-audio read thread timeout from peer close");
                            break;
                        }
                    }
                    if let Ok((rtp, _n)) = m {
                        if !init {
                            if let Ok(mut s) = sessions2.try_write() {
                                if s.set_session_audio(sid,rid, rtp.header.payload_type, media_ssrc, &payload_pool).is_some() {
                                    init = true;
                                } else {
                                    debug!("[C->S] {sid}-audio thread end from no such task");
                                    break;
                                }
                            }
                        }
                        let _ = asdr1.send(RidPacket::new(rid, rtp));
                        hb = now;
                    } else if weak_peer_closed(&pc2) {
                        debug!("[C->S] {sid}-audio read thread end from peer close");
                        break;
                    }
                }
            }
        }
        info!("[C->S] closing {sid}-audio read thread");
    });
}

pub async fn on_data_channel_open(
    sid: u32,
    dc1: &Arc<RTCDataChannel>,
    mut drcv2: broadcast::Receiver<Bytes>,
    pc2: &Weak<RTCPeerConnection>,
) {
    loop {
        let timeout = tokio::time::sleep(Duration::from_secs(4));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {
                if weak_peer_closed(&pc2) {
                    break;
                }
            }
            m = drcv2.recv() => {
                if let Ok(d) = m {
                    let _ = dc1.send(&d).await;
                } else if weak_peer_closed(&pc2) {
                    break;
                }
            }
        }
    }
    info!("[C->S] closing {sid} data_channel");
}

pub fn on_video_simucast_track(
    sid: u32,
    track: Arc<TrackRemote>,
    notify1: &Arc<Notify>,
    announcer: &broadcast::Sender<Weak<String>>,
    vsdr: &broadcast::Sender<RidPacket>,
    sessions1: &Arc<RwLock<RtcSessions>>,
    payload_pool_rcv: &watch::Receiver<Option<HashMap<String, Vec<u8>>>>,
    pc1: &Weak<RTCPeerConnection>,
) {
    let media_ssrc = track.ssrc();
    let rid = track.rid().to_owned();
    debug!(target:"debug","[C->S] receiving {sid}-video track {media_ssrc} {rid}");
    let announcer2 = announcer.clone();
    let pc2 = pc1.clone();
    let rid1 = rid.to_owned();
    tokio::spawn(async move {
        let mut lrcv = announcer2.subscribe();
        let mut hb = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    hb = Instant::now();
                }
                _ = lrcv.recv() => {
                    let time_past = Instant::now().duration_since(hb).as_millis();
                    if time_past < 400 {
                        tokio::time::sleep(Duration::from_millis(400)).await;
                    }
                }
            };
            if let Some(pc3) = pc2.upgrade() {
                if peer_closed(&pc3) {
                    break;
                }
                if let Err(_e) = pc3
                    .write_rtcp(&[Box::new(PictureLossIndication {
                        sender_ssrc: 0,
                        media_ssrc,
                    })])
                    .await
                {
                    break;
                }
            } else {
                break;
            }
        }
        info!("[C->S] closing {sid}-{rid1}-video rtcp thread");
    });

    let pc2 = pc1.clone();
    let sessions2 = sessions1.clone();
    let vsdr1 = vsdr.clone();
    let notify1 = notify1.clone();
    let mut payload_pool_rcv1 = payload_pool_rcv.clone();
    tokio::spawn(async move {
        let mut hb = Instant::now();
        let mut init = false;
        let payload_pool = match tokio_watch_lock!(payload_pool_rcv1, 20000) {
            Ok(_) => {
                let value = payload_pool_rcv1.borrow_and_update();
                if value.is_none() {
                    debug!("[C->S] {sid}-video payload_pool must not be none");
                    return;
                }
                value.as_ref().unwrap().to_owned()
            }
            Err(_) => {
                debug!("[C->S] {sid}-video read thread wait timeout");
                notify1.notify_waiters();
                return;
            }
        };
        loop {
            let timeout = tokio::time::sleep(Duration::from_secs(4));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {
                    debug!("[C->S] {sid}-{rid}-video read thread read timeout");
                    break;
                }
                m = track.read_rtp() => {
                    let now = Instant::now();
                    if now.duration_since(hb).as_secs() > 4 {
                        if weak_peer_closed(&pc2) {
                            debug!("[C->S] {sid}-{rid}-video read thread timeout from peer close");
                            break;
                        }
                    }
                    if let Ok((rtp, _n)) = m {
                        if !init {
                            if let Ok(mut s) = sessions2.try_write() {
                                if s.set_session_video(sid, &rid, rtp.header.payload_type, media_ssrc, &payload_pool).is_some() {
                                    init = true;
                                } else {
                                    debug!("[C->S] {sid}-{rid}-video read thread end from no such task");
                                    break;
                                }
                            }
                        }
                        // log::debug!(target:"debug","sending rtp {} {} {} {}", rtp.header.payload_type, rtp.header.ssrc, rtp.header.sequence_number, rtp.payload.len());
                        let _ = vsdr1.send(RidPacket::new(&rid, rtp));
                        hb = now;
                    } else if weak_peer_closed(&pc2) {
                        debug!("[C->S] {sid}-{rid}-video read thread end from peer close");
                        break;
                    }
                }
            }
        }
        info!("[C->S] closing {sid}-{rid}-video read thread");
    });
}

#[test]
fn test_codec_type() {
    let a = RTPCodecType::Video;
    println!("@@{a} {}", a.to_string(),);
}
