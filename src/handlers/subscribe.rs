use crate::processor::jitter::{self, JitterBuffer};
use crate::processor::vp9::Vp9DepacketizerExt;
use crate::utils::RTCP_REPORT_INTERVAL;
use crate::{
    errors::VCError,
    processor::vp9::Vp9Depacketizer,
    tokio_any_lock, tokio_watch_lock, tokio_write_lock,
    utils::{
        self, peer_closed, RidPacket, RtcJob, RtcSession, RtcSessionRtpInfo, RtcSessions,
        MAX_SESSION_TIME, PEER_STUN_ADDRS, RTCP_FRACTION_THRESHOLD, RTCP_JITTER_THRESHOLD,
    },
};
use actix_web::{post, web, HttpRequest, HttpResponse, Result};
use log::{debug, info};
use serde_json::json;
use std::collections::VecDeque;
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};
use std::{u32, u8};
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        watch, Notify, RwLock,
    },
    time::Instant,
};
use webrtc::api::media_engine::{MIME_TYPE_H264, MIME_TYPE_VP9};
use webrtc::rtcp::reception_report::ReceptionReport;
use webrtc::rtp::codecs::h264::SPS_NALU_TYPE;
use webrtc::{
    api::API,
    data_channel::{data_channel_message::DataChannelMessage, RTCDataChannel},
    ice_transport::{
        ice_candidate_pair::RTCIceCandidatePair, ice_connection_state::RTCIceConnectionState,
        ice_server::RTCIceServer,
    },
    peer_connection::{
        configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
        RTCPeerConnection,
    },
    rtcp::{self, receiver_report::ReceiverReport},
    rtp::{
        codecs::h264::{NALU_TYPE_BITMASK, STAPA_NALU_TYPE},
        packet::Packet,
    },
    rtp_transceiver::{
        rtp_codec::{RTCRtpCodecCapability, RTPCodecType},
        rtp_sender::RTCRtpSender,
    },
    track::track_local::{
        track_local_static_rtp::TrackLocalStaticRTP, TrackLocal, TrackLocalWriter,
    },
};

/// 只收不发
#[post("/subscribe")]
pub async fn subscribe_stream(
    peer_job: web::Json<RtcJob>,
    _api: web::Data<API>,
    sessions: web::Data<RwLock<RtcSessions>>,
    req: HttpRequest,
) -> Result<HttpResponse, VCError> {
    let offer_str = &peer_job.peer;
    let target = peer_job
        .target
        .ok_or(vcerr!("target id must be provided"))?;
    let addr0 = req
        .peer_addr()
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or("".to_owned());
    let addr = Arc::new(addr0.clone());
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
    // 添加自选锁用于异步兼容推拉流同时访问的情况
    let (target_session, video_info, audio_info) = {
        let start = Instant::now();
        let mut dur = 10;
        loop {
            dur = std::cmp::min(dur * 2, 200);
            tokio::time::sleep(Duration::from_millis(dur)).await;
            // let s = tokio_read_lock!(sessions, 10)?;
            if Instant::now().duration_since(start).as_secs() > 4 {
                return Err(vcerr!("timeout wating for target session"));
            }
            if let Ok(s) = sessions.try_read() {
                if let Some(target_session) = s.get_session(target) {
                    if let Some((video_info, audio_info)) = target_session.get_media_infos() {
                        break (target_session, video_info, audio_info);
                    }
                }
            }
        }
    };
    if video_info.is_empty() {
        return Err(vcerr!("video stream must not be empty"));
    }
    // let connection = Arc::new(api.new_peer_connection(conf).await?);
    // let (video_mime, video_video_info) = target_session
    //     .video
    //     .iter()
    //     .next()
    //     .ok_or(vcerr!("video info not received yet"))?;
    // // Create Track that we send video back to browser on
    // let local_video_track = Arc::new(TrackLocalStaticRTP::new(
    //     RTCRtpCodecCapability {
    //         mime_type: video_mime.to_string(),
    //         ..Default::default()
    //     },
    //     "video".to_owned(),
    //     "webrtc-rs".to_owned(),
    // ));
    // let audio_info = target_session
    //     .audio
    //     .iter()
    //     .next()
    //     .ok_or((MIME_TYPE_OPUS.to_owned(), 111));
    // let local_audio_track = Arc::new(TrackLocalStaticRTP::new(
    //     RTCRtpCodecCapability {
    //         mime_type: audio_info.0.clone(),
    //         ..Default::default()
    //     },
    //     "audio".to_owned(),
    //     "webrtc-rs".to_owned(),
    // ));

    let myapi = utils::api_from_payloads(&video_info, &audio_info, false);
    let connection = Arc::new(myapi.new_peer_connection(conf).await?);
    // let rtp_sender = connection
    //     .add_track(Arc::clone(&local_video_track) as Arc<dyn TrackLocal + Send + Sync>)
    //     .await?;
    // video_rtcp_watcher(&addr, rtp_sender, &connection);

    // let rtp_sender = connection
    //     .add_track(Arc::clone(&local_audio_track) as Arc<dyn TrackLocal + Send + Sync>)
    //     .await?;
    // audio_rtcp_watcher(&addr, rtp_sender, &connection);

    let dsdr = target_session.dsdr.clone();
    connection.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
        info!("Session data_channel open");
        let dsdr1 = dsdr.clone();
        Box::pin(async move {
            let dsdr2 = dsdr1.clone();
            dc.on_message(Box::new(move |msg: DataChannelMessage| {
                let _ = dsdr2.send(msg.data);
                Box::pin(async {})
            }));
        })
    }));

    let addr1 = Arc::downgrade(&addr);
    let sessions1 = sessions.clone();
    let notify = Arc::new(Notify::new());
    let notify1 = notify.clone();
    let addr2 = addr0.clone();
    let target_announcer = target_session.announcer.clone();
    connection.on_ice_connection_state_change(Box::new(
        move |connection_state: RTCIceConnectionState| {
            info!("Session {addr2} state changed {connection_state}",);
            if connection_state == RTCIceConnectionState::Connected {
                let sessions1 = sessions1.clone();
                let addr1 = addr1.clone();
                let target_announcer1 = target_announcer.clone();
                Box::pin(async move {
                    let _ = target_announcer1.send(addr1.clone());
                    if let Ok(mut s) = tokio_write_lock!(sessions1, 10) {
                        s.set_listener(target, &addr1);
                    }
                })
            } else if connection_state == RTCIceConnectionState::Disconnected {
                Box::pin(async {})
            } else if connection_state == RTCIceConnectionState::Closed
                || connection_state == RTCIceConnectionState::Failed
            {
                notify1.notify_waiters();
                let addr2 = addr2.clone();
                let sessions1 = sessions1.clone();
                Box::pin(async move {
                    if let Ok(mut s) = tokio_write_lock!(sessions1, 10) {
                        s.drop_listener(target, &addr2);
                    }
                })
            } else {
                Box::pin(async move {})
            }
        },
    ));

    // close 需要放在事件外面
    let addr2 = addr0.clone();
    let connection1 = connection.clone();
    tokio::spawn(async move {
        let timeout = tokio::time::sleep(Duration::from_secs(*MAX_SESSION_TIME));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {}
            _ = notify.notified() => {}
        }
        let closer = connection1.close().await;
        info!("[S->C] closing {addr2} peer {:?}", closer);
    });

    let (expect_media_sdr, expect_media_rcv) = watch::channel(None);
    if audio_info.is_empty() {
        media_provider(
            &addr,
            RTPCodecType::Video,
            expect_media_rcv.clone(),
            &video_info,
            &target_session.vsdr,
            &connection,
        )
        .await?;
    } else {
        let (v, a) = futures::join!(
            media_provider(
                &addr,
                RTPCodecType::Video,
                expect_media_rcv.clone(),
                &video_info,
                &target_session.vsdr,
                &connection,
            ),
            media_provider(
                &addr,
                RTPCodecType::Audio,
                expect_media_rcv,
                &audio_info,
                &target_session.asdr,
                &connection,
            ),
        );
        v?;
        a?;
    }

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
    {
        let payload = utils::parse_payloads(&answer.sdp)?;
        let _ = expect_media_sdr.send(Some(payload));
    }

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
            info!("[S->C] candidate pair {p}");
            Box::pin(async {})
        }));

    let data = utils::atob(&result);
    Ok(HttpResponse::Ok().json(json!({"success": true, "data": data})))
}

pub async fn media_provider(
    addr: &Arc<String>,
    kind: RTPCodecType,
    mut expect_media_rcv: watch::Receiver<Option<HashMap<String, Vec<u8>>>>,
    codec_info: &HashMap<Cow<'static, str>, Vec<RtcSessionRtpInfo>>,
    msdr: &broadcast::Sender<RidPacket>,
    connection: &Arc<RTCPeerConnection>,
) -> Result<(), VCError> {
    let connection1 = connection.clone();
    let mut mrcv = msdr.subscribe();
    let addr1 = addr.clone();
    let codec_info1 = codec_info.clone();
    // 初始化值
    expect_media_rcv.borrow_and_update();
    let notify = Arc::new(Notify::new());
    let notify1 = notify.clone();
    tokio::spawn(async move {
        let mut check_time = Instant::now();
        let mut streamer_opt: Option<SubscribeStreamer<20>> = None;
        let (report_sndr, mut report_rcvr) = broadcast::channel(1024);
        let mut local_rid = "".to_owned();
        loop {
            let t = tokio::time::sleep(Duration::from_secs(4));
            tokio::pin!(t);
            tokio::select! {
                _ = t.as_mut() => {
                    if peer_closed(&connection1) {
                        debug!("[S->C] addr {addr1} {kind} listen break from close");
                        break;
                    }
                }
                r = report_rcvr.recv() => {
                    if let Ok(rid) = r {
                        log::debug!(target:"debug", "received expected rid {rid}");
                        if let Some(streamer) = &mut streamer_opt {
                            streamer.setup_extected_rid(rid);
                        }
                    }
                }
                m = mrcv.recv() => {
                    let now = Instant::now();
                    if now.duration_since(check_time).as_secs() > 4 {
                        if peer_closed(&connection1) {
                            debug!("[S->C] addr {addr1} {kind} listen timeout from close");
                            break;
                        }
                        check_time = now;
                    }
                    if let Ok(mut rpkt) = m {
                        // 接收rtp，当streamer为空时根据payload创建streamer，否则检查rtp payload，和streamer不一致则更新
                        if vclog!(media_validate_creater::<20u8>(&mut rpkt.packet, &mut streamer_opt, &codec_info1,&mut expect_media_rcv, &report_sndr,&mut local_rid, &addr1, kind, &connection1, &notify1).await).is_err() {
                            break;
                        }
                        let streamer = streamer_opt.as_mut().unwrap();
                        if vclog!(streamer.process_packet(rpkt).await).is_err() {
                            if peer_closed(&connection1) {
                                debug!("[S->C] addr {addr1} {kind} listen err from close");
                                break;
                            }
                            break;
                        }
                        // if kind == RTPCodecType::Video && current_rid == "s1"{
                        //         log::debug!(target:"debug", "sending rid {current_rid} {} {}", rtp.header.sequence_number, rtp.header.timestamp);
                        //         let result = streamer.track.write_rtp(&rtp).await;
                        //     if let Err(_err) = &result {
                        //         if peer_closed(&connection1) {
                        //             debug!("[S->C] addr {addr1} {kind} listen err from close");
                        //             break;
                        //         }
                        //     }
                        //     } else {
                        //     let result = streamer.track.write_rtp(&rtp).await;
                        //     if let Err(_err) = &result {
                        //         if peer_closed(&connection1) {
                        //             debug!("[S->C] addr {addr1} {kind} listen err from close");
                        //             break;
                        //         }
                        //     }
                        //     }

                        // if rid_checker(&rtp, &mut local_rid, &mut expected_rid, &current_rid, streamer) {
                        //     if kind == RTPCodecType::Video {
                        //         log::debug!(target:"debug", "sending rid {current_rid}");
                        //     }
                        //     let result = streamer.track.write_rtp(&rtp).await;
                        //     if let Err(_err) = &result {
                        //         if peer_closed(&connection1) {
                        //             debug!("[S->C] addr {addr1} {kind} listen err from close");
                        //             break;
                        //         }
                        //     }
                        // }
                    } else if let Err(RecvError::Closed) = m {
                        debug!("[S->C] addr {addr1} {kind} listen err");
                        break;
                    }
                }
            }
        }
        info!("[S->C] closing addr {addr1} {kind} provider");
    });
    tokio_any_lock!(notify.notified(), 10000)
}

/// return 的结果表示是否需要更新payload
pub async fn media_validate_creater<const S: u8>(
    rtp: &mut Packet,
    media_info: &mut Option<SubscribeStreamer<S>>,
    codec_info: &HashMap<Cow<'static, str>, Vec<RtcSessionRtpInfo>>,
    expect_media_rcv: &mut watch::Receiver<Option<HashMap<String, Vec<u8>>>>,
    reporter: &broadcast::Sender<String>,
    local_rid: &mut String,
    addr1: &Arc<String>,
    kind: RTPCodecType,
    connection: &Arc<RTCPeerConnection>,
    notify1: &Arc<Notify>,
) -> Result<(), VCError> {
    let new_payload = rtp.header.payload_type;
    if let Some(streamer) = &media_info {
        if new_payload != streamer.payload {
            rtp.header.payload_type = streamer.payload;
        }
    } else {
        let (media_mime, media_rids) = RtcSession::get_payload_info(new_payload, &codec_info)
            .ok_or(vcerr!("[S->C] addr {addr1} {kind} payload empty"))?;
        let new_media_info = SubscribeStreamer::new(
            &media_mime,
            kind,
            new_payload,
            media_rids,
            addr1,
            connection,
            reporter,
        )
        .await?;
        *local_rid = new_media_info.default_rid.clone();
        media_info.replace(new_media_info);

        // 通知外部线程放行，这样可以进行sdp握手
        notify1.notify_waiters();
        // 握手后等待sdp parser发过来握手后的payload type，并更新对应streamer
        payload_checker(media_info.as_mut().unwrap(), expect_media_rcv, &addr1, kind).await?;
    };
    Ok(())
}

pub async fn payload_checker<const S: u8>(
    streamer: &mut SubscribeStreamer<S>,
    expect_media_rcv: &mut watch::Receiver<Option<HashMap<String, Vec<u8>>>>,
    addr1: &Arc<String>,
    kind: RTPCodecType,
) -> Result<(), VCError> {
    let media_mime = &streamer.mime;
    let _ = tokio_watch_lock!(expect_media_rcv, 10000)
        .map_err(|_| vcerr!("[S->C] addr {addr1} {kind} sdp wait timeout"))?;
    let expect_medias = expect_media_rcv
        .borrow_and_update()
        .as_ref()
        .ok_or(vcerr!("[S->C] addr {addr1} {kind} sdp must not be none"))?
        .clone();
    let expect_payloads = expect_medias
        .get(media_mime)
        .ok_or(vcerr!("no such mime {media_mime}"))?;
    if expect_payloads.is_empty() {
        return Err(vcerr!("the {media_mime} payloads is empty"));
    }
    // 如果sdp 的payload和期待不同，则强制设成第一个，有可能造成真实不符的现象，几率很低
    if !expect_payloads.contains(&streamer.payload) {
        streamer.payload = expect_payloads[0];
    }
    Ok(())
}

pub fn media_rtcp_watcher(
    addr: &Arc<String>,
    kind: RTPCodecType,
    rtp_sender: Arc<RTCRtpSender>,
    connection: &Arc<RTCPeerConnection>,
    reporter: &Option<SubscribeStreamerLvlReporter>,
) {
    let addr1 = addr.clone();
    let connection1 = connection.clone();
    let mut reporter1 = reporter.clone();
    tokio::spawn(async move {
        // let mut rtcp_buf = vec![0u8; 1500];
        let mut hb = Instant::now();
        loop {
            let timeout = tokio::time::sleep(Duration::from_secs(4));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {
                    if peer_closed(&connection1){
                        break;
                    }
                    if Instant::now().duration_since(hb).as_secs() > 10 {
                        break;
                    }
                }
                // p = rtp_sender.read(&mut rtcp_buf)=> {
                p = rtp_sender.read_rtcp()=> {
                    if let Ok((receiver_report,_)) = p {
                        if let Some(r) = &mut reporter1 {
                            r.check_report(&receiver_report, kind);
                        }
                        hb = Instant::now();
                    } else {
                        if peer_closed(&connection1){
                            break;
                        }
                        if Instant::now().duration_since(hb).as_secs() > 4 {
                            break;
                        }
                    }
                }
            }
        }
        info!("[S->C] closing addr {addr1} {kind} rtcp watcher");
    });
}

pub struct SubscribeStreamer<const S: u8> {
    pub mime: String,
    pub kind: RTPCodecType,
    pub payload: u8,
    pub rid_jitters: HashMap<String, StreamJitter<S>>,
    pub track: Arc<TrackLocalStaticRTP>,
    pub level_reporter: Option<SubscribeStreamerLvlReporter>,
    pub default_rid: String,

    pub local_rid: String,
    expected_rid: Option<String>,
    sequence_number: u16,
    timestamp: u32,
    vp9_packetizer: Option<Vp9Depacketizer>,
}
impl<const S: u8> SubscribeStreamer<S> {
    pub async fn new(
        mime: &str,
        kind: RTPCodecType,
        payload: u8,
        rids: Vec<String>,
        addr: &Arc<String>,
        connection: &Arc<RTCPeerConnection>,
        reporter: &broadcast::Sender<String>,
    ) -> Result<Self, VCError> {
        let local_track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: mime.to_owned(),
                ..Default::default()
            },
            kind.to_string(),
            "webrtc-rs".to_owned(),
        ));
        let rtp_sender = connection
            .add_track(Arc::clone(&local_track) as Arc<dyn TrackLocal + Send + Sync>)
            .await?;
        // 目前只对video多播设计报告机制
        let (reporter, default_rid) = if kind == RTPCodecType::Video && rids.len() > 1 {
            let (reporter, default_rid) = SubscribeStreamerLvlReporter::new(reporter, &rids);
            (Some(reporter), default_rid)
        } else {
            (None, "".to_owned())
        };
        media_rtcp_watcher(addr, kind, rtp_sender, connection, &reporter);
        Ok(Self {
            mime: mime.to_owned(),
            kind,
            payload,
            rid_jitters: rids
                .into_iter()
                .map(|v| {
                    let jitter = StreamJitter::new(&v);
                    (v, jitter)
                })
                .collect(),
            track: local_track,
            level_reporter: reporter,
            local_rid: default_rid.clone(),
            default_rid,
            expected_rid: None,
            sequence_number: 0,
            timestamp: 0,
            vp9_packetizer: if mime == MIME_TYPE_VP9 {
                Some(Vp9Depacketizer::new())
            } else {
                None
            },
        })
    }
    fn is_key_frame(&mut self, rtp: &Packet) -> Result<bool, VCError> {
        if self.mime == MIME_TYPE_VP9 {
            if let Some(packetizer) = &mut self.vp9_packetizer {
                let (_, meta) = packetizer
                    .process_packet(&rtp.payload)
                    .map_err(|e| vcerr!("{e:?}",))?;
                return Ok(meta.is_keyframe);
            }
        } else if self.mime == MIME_TYPE_H264 {
            let data = &rtp.payload;
            if data.len() < 4 {
                return Ok(false);
            } else {
                let word = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let nalu_type = (word >> 24) as u8 & NALU_TYPE_BITMASK;
                return Ok((nalu_type == STAPA_NALU_TYPE
                    && (word & NALU_TYPE_BITMASK as u32) as u8 == SPS_NALU_TYPE)
                    || (nalu_type == SPS_NALU_TYPE));
            }
        }
        Err(vcerr!("not support yet for {}", self.mime))
    }
    fn setup_extected_rid(&mut self, expected_rid: String) {
        self.expected_rid.replace(expected_rid);
    }
    /// 如果是音频或者没有多播的视频，则直接代理发送，sequence_number/timestamp不会被更新
    async fn process_packet(&mut self, rpkt: RidPacket) -> Result<(), VCError> {
        let RidPacket { rid, packet } = rpkt;
        if self.kind == RTPCodecType::Audio || self.level_reporter.is_none() {
            self.track.write_rtp(&packet).await?;
            return Ok(());
        }
        // 否则根据rtcp的反馈实时更新
        if let Some(jitter) = self.rid_jitters.get_mut(&rid) {
            let mut packets = jitter.reorder_rtp(packet);
            for packet in &mut packets {
                if self.rid_checker(packet, &rid) {
                    // 对jitter输出的pkt进行更新，在一定的缓冲阶段设置seq_nb，并更新时间戳
                    packet.header.sequence_number = self.sequence_number;
                    self.sequence_number = self.sequence_number.wrapping_add(1);
                    self.timestamp = self.timestamp.wrapping_add(packet.header.timestamp);
                    packet.header.timestamp = self.timestamp;
                    // log::debug!(target:"debug", "sending {rid} {} {}", packet.header.sequence_number, packet.header.timestamp);
                    self.track.write_rtp(packet).await?;
                }
            }
        }
        Ok(())
    }

    // local_rid本地的rid， new_rid应该
    fn rid_checker(&mut self, rtp: &Packet, current_rid: &str) -> bool {
        let mut result = true;
        // 如果有reporter则表示多播，非多播自然放行
        if self.level_reporter.is_some() {
            // 如果有更新需求则进一步判断，否则判断本地rid和当前rid是否一样，不一样则阻止
            if self.expected_rid.is_some() {
                let expected_rid = self.expected_rid.as_ref().unwrap().to_owned();
                log::debug!(target:"debug", "expecteding {expected_rid} local {} current {current_rid}",self.local_rid,);
                // 如果有更新需求且期待rid等于当前rid，则进一步分析idr帧，否则同上判断
                if *expected_rid == *current_rid {
                    let is_key_frame = vclog!(self.is_key_frame(rtp)).unwrap_or(false);
                    log::debug!(target:"debug", "got expected {expected_rid}, is_idr: {is_key_frame}");
                    // 如果是idr帧则更新本地rid，并清空期待rid，放行，否则通上判断
                    if is_key_frame {
                        self.local_rid = self.expected_rid.take().unwrap();
                        log::debug!(target:"debug", "{:?} updated, now local {} current {current_rid}",self.expected_rid,self.local_rid,);
                        return true;
                    }
                }
            }
            if *self.local_rid != *current_rid {
                result = false;
            }
        }
        result
    }
}

// 用于监视rtcp反馈消息，并在合适的时机发送切流消息
#[derive(Clone)]
pub struct SubscribeStreamerLvlReporter {
    pub levels: HashMap<u8, String>,
    pub reporter: broadcast::Sender<String>,
    pub state: u8,
    min_level: u8,
    max_level: u8,
    hb: Instant,
    receptions: VecDeque<StreamerReporterReception>,
    check_time: Option<Instant>,
}
impl SubscribeStreamerLvlReporter {
    pub fn new(reporter: &broadcast::Sender<String>, rids: &Vec<String>) -> (Self, String) {
        let mut state = 0;
        let mut min_level = 1;
        let mut max_level = 0;
        let mut rid = "".to_owned();
        (
            Self {
                levels: rids
                    .iter()
                    .map(|v| {
                        let s = utils::rid2level(v);
                        if s > state {
                            state = s;
                            max_level = s;
                            rid = v.to_owned();
                        }
                        if s < min_level {
                            min_level = s;
                        }
                        (s, v.to_owned())
                    })
                    .collect(),
                state,
                reporter: reporter.clone(),
                min_level,
                max_level,
                hb: Instant::now(),
                receptions: VecDeque::new(),
                check_time: None,
            },
            rid,
        )
    }

    pub fn downgrade(&mut self) {
        if self.state <= self.min_level {
            return;
        }
        self.state -= 1;
        if let Some(rid1) = self.levels.get(&self.state) {
            if self.hb.elapsed().as_millis() >= *RTCP_REPORT_INTERVAL {
                log::debug!(target:"debug", "downgrading");
                let _ = self.reporter.send(rid1.to_owned());
            }
            self.hb = Instant::now();
        }
    }
    pub fn upgrade(&mut self) {
        if self.state >= self.max_level {
            return;
        }
        self.state += 1;
        if let Some(rid1) = self.levels.get(&self.state) {
            if self.hb.elapsed().as_millis() >= *RTCP_REPORT_INTERVAL {
                log::debug!(target:"debug", "upgrading");
                let _ = self.reporter.send(rid1.to_owned());
            }
            self.hb = Instant::now();
        }
    }

    fn check_report(
        &mut self,
        rtcp: &Vec<Box<dyn rtcp::packet::Packet + Send + Sync>>,
        kind: RTPCodecType,
    ) {
        if kind == RTPCodecType::Video {
            self.pop_expired_report();
            for report in rtcp {
                if let Some(c) = report.as_any().downcast_ref::<ReceiverReport>() {
                    for report in &c.reports {
                        self.receptions.push_back(report.into());
                        // let new_nb = if level_reporter.pkt_nb == usize::MAX - 1 {
                        //     usize::MAX / 2
                        // } else {
                        //     level_reporter.pkt_nb + 1
                        // };
                        // let avg_fraction_lost = (level_reporter.avg_fraction_lost as usize
                        //     * level_reporter.pkt_nb
                        //     + report.fraction_lost as usize)
                        //     / new_nb as usize;
                        // level_reporter.avg_fraction_lost =
                        //     cmp::min(avg_fraction_lost, u8::MAX as usize) as u8;
                        // let avg_deplay = (level_reporter.avg_delay as usize * level_reporter.pkt_nb
                        //     + report.delay as usize)
                        //     / new_nb as usize;
                        // level_reporter.avg_delay = cmp::min(avg_deplay, u32::MAX as usize) as u32;
                        // level_reporter.pkt_nb = new_nb;
                    }
                }
            }
            if self.check_time.is_none() {
                self.check_time.replace(Instant::now());
            }
            if self.check_time.as_ref().unwrap().elapsed().as_millis() >= *RTCP_REPORT_INTERVAL {
                let has_bad = self.receptions.iter().find(|v| v.is_bad()).is_some();
                log::debug!(target:"debug", "is_nack {has_bad}, reports: {}",serde_json::to_string(&self.receptions).unwrap_or("".to_owned()));
                if has_bad {
                    self.downgrade();
                } else {
                    let all_good = self.receptions.iter().any(|v| v.is_good());
                    log::debug!(target:"debug", "all_good: {all_good}, now upgrading");
                    self.upgrade();
                }
                self.check_time.replace(Instant::now());
            }

            // let avg_fraction_lost = level_reporter.avg_fraction_lost;
            // let avg_delay = level_reporter.avg_delay * 1000 / 90000;
            // log::debug!(target:"debug", "report {avg_fraction_lost}, {avg_delay}");
            // if avg_fraction_lost > *RTCP_FRACTION_THRESHOLD || avg_delay > *RTCP_JITTER_THRESHOLD {
            //     level_reporter.downgrade();
            // } else {
            //     level_reporter.upgrade();
            // }
        }
    }
    fn pop_expired_report(&mut self) {
        if self.check_time.is_none() {
            return;
        }
        loop {
            let expired = if let Some(front) = self.receptions.front() {
                front.hb.elapsed().as_millis() >= *RTCP_REPORT_INTERVAL
            } else {
                break;
            };
            if expired {
                self.receptions.pop_front();
            } else {
                break;
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct StreamerReporterReception {
    fraction_lost: u8,
    delay: u32,
    #[serde(skip_serializing)]
    hb: Instant,
}
impl StreamerReporterReception {
    pub fn new(fraction_lost: u8, delay: u32) -> Self {
        Self {
            fraction_lost,
            delay,
            hb: Instant::now(),
        }
    }
    pub fn is_bad(&self) -> bool {
        self.fraction_lost >= *RTCP_FRACTION_THRESHOLD || self.delay >= *RTCP_JITTER_THRESHOLD
    }
    pub fn is_good(&self) -> bool {
        self.fraction_lost < *RTCP_FRACTION_THRESHOLD / 2 && self.delay < *RTCP_JITTER_THRESHOLD / 2
    }
}
// webrtc默认视频都是90000采样率
impl From<&ReceptionReport> for StreamerReporterReception {
    fn from(report: &ReceptionReport) -> Self {
        Self::new(report.fraction_lost, report.delay * 1000 / 90000)
    }
}

pub struct StreamJitter<const S: u8> {
    pub rid: String,
    pub jitter: JitterBuffer<Packet, S>,
    last_timestamp: u32,
}

impl<const S: u8> StreamJitter<S> {
    pub fn new(rid: &str) -> Self {
        let jitter = JitterBuffer::new(None);
        Self {
            rid: rid.to_owned(),
            jitter,
            last_timestamp: 0,
        }
    }
    // 对乱序的上行包进行排序，并在pop阶段将时间戳改为包的时长，用于切流时单位一致
    pub fn reorder_rtp(&mut self, rtp: Packet) -> Vec<Packet> {
        self.jitter.push(rtp);
        let mut result = vec![];
        loop {
            if self.jitter.peek().is_some() {
                if let Some(mut pkt) = self.jitter.pop() {
                    let old_timestamp = pkt.header.timestamp;
                    if self.last_timestamp == 0 {
                        pkt.header.timestamp = 0
                    } else {
                        pkt.header.timestamp -= self.last_timestamp;
                    }
                    self.last_timestamp = old_timestamp;
                    result.push(pkt);
                    continue;
                }
            }
            break;
        }
        result
    }
    // pub fn process_to_pkt(&mut self, rtp: Packet) -> Option<(Vec<Packet>,)> {
    //     self.to_jitter.push(rtp);
    //     let mut result = vec![];
    //     let mut has_key_frame = false;
    //     let mut from_to_sequence_number_delta = 0;
    //     let mut from_to_timestamp_delta = 0;
    //     loop {
    //         if self.to_jitter.peek().is_some() {
    //             if let Some(pkt) = self.to_jitter.pop() {
    //                 if self.is_key_frame(&pkt).unwrap_or(false) {
    //                     pkt.header.sequence_number
    //                     result.push(pkt);
    //                     has_key_frame = true;
    //                 } else if has_key_frame {
    //                     result.push(pkt);
    //                 } else {
    //                     self.last_to_sequence_number = pkt.header.sequence_number;
    //                     self.last_to_timestamp = pkt.header.timestamp;
    //                 }
    //                 continue;
    //             }
    //         }
    //         break;
    //     }
    //     result
    // }
}
// const NALU_TTYPE_STAP_A: u32 = 24;
// const NALU_TTYPE_SPS: u32 = 7;
// const NALU_TYPE_BITMASK: u32 = 0x1F;

// fn is_key_frame(data: &[u8]) -> bool {
//     if data.len() < 4 {
//         false
//     } else {
//         let word = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
//         let nalu_type = (word >> 24) as u8 & NALU_TYPE_BITMASK;
//         (nalu_type == STAPA_NALU_TYPE && (word & NALU_TYPE_BITMASK) == NALU_TTYPE_SPS)
//             || (nalu_type == NALU_TTYPE_SPS)
//     }
// }
impl jitter::Packet for Packet {
    #[inline]
    fn sequence_number(&self) -> u16 {
        self.header.sequence_number
    }
}
