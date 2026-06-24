use crate::processor::es::h264::EsAvcStreamer;
use crate::processor::es::opus::EsOpusStreamer;
use crate::utils::TOKEN;
use crate::{errors::VCError, tokio_write_lock, utils::RtcSessions};
use actix_web::{
    get, http::header::ContentType, web, HttpRequest, HttpResponse, Result as ActixResult,
};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use log::info;
use std::any::Any;
use std::{collections::HashMap, sync::Arc, time::Duration};
use std::{u32, u8};
use tokio::sync::mpsc::{self, Sender};
use tokio::{
    sync::{broadcast, RwLock},
    time::Instant,
};
use tokio_stream::wrappers::ReceiverStream;
use webrtc::{rtp::packet::Packet, rtp_transceiver::rtp_codec::RTPCodecType};

/// 只收不发
#[get("/es_streamer/{target}")]
pub async fn http_streamer(
    config: web::Query<StreamerConfig>,
    target: web::Path<u32>,
    sessions: web::Data<RwLock<RtcSessions>>,
    req: HttpRequest,
) -> ActixResult<HttpResponse, VCError> {
    let target = *target.as_ref();
    let addr0 = req
        .peer_addr()
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or("".to_owned());
    let addr = Arc::new(addr0.clone());
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
    let video_ssrc = video_info
        .values()
        .next()
        .map(|v| v.first())
        .unwrap_or(None)
        .ok_or(vcerr!("video stream empty"))?
        .ssrc;
    let audio_ssrc = audio_info
        .values()
        .next()
        .map(|v| v.first())
        .unwrap_or(None)
        .map(|v| v.ssrc);
    let StreamerConfig { sep } = config.into_inner();
    let seperator = sep.unwrap_or(TOKEN.to_owned());

    let (mut streamer, es_rcv) =
        Webrtc2EsStreamer::new(video_ssrc, audio_ssrc, seperator.as_bytes(), None)
            .map_err(|e| vcerr!("{e:?}"))?;

    let mut vrcvr = target_session.vsdr.subscribe();
    let mut arcvr = target_session.asdr.subscribe();

    let target_announcer = target_session.announcer.clone();
    let sessions1 = sessions.into_inner();
    tokio::spawn(async move {
        info!("[S->ES] opening es listener {addr0}");
        let addr1 = Arc::downgrade(&addr);
        match tokio_write_lock!(sessions1, 10) {
            Ok(mut sessions) => {
                sessions.set_listener(target, &addr1);
            }
            Err(_) => {
                log::error!("[S->C] setting es listener {addr0} failed");
                return;
            }
        }
        let _ = target_announcer.send(addr1);
        let reason = loop {
            let timeout = tokio::time::sleep(Duration::from_secs(1));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {
                    if vrcvr.is_closed() || arcvr.is_closed() {
                        break "timeout".to_owned()
                    }
                }
                m = vrcvr.recv() => {
                    let Ok(rtp) = m else {continue;};
                    if let Err(e) = streamer.parse_rtp(rtp.packet, video_ssrc) {
                        break e.to_string();
                    }
                    // let Ok(_) = streamer.parse_rtp(&rtp.packet, video_ssrc) else {break "video parse err"};
                }
                m = arcvr.recv() => {
                    let Some(ssrc) = audio_ssrc.as_ref().map(|v|*v) else {break "audio ssrc empty".to_owned()};
                    let Ok(rtp) = m else {continue;};
                    if let Err(e) = streamer.parse_rtp(rtp.packet, ssrc) {
                        break e.to_string();
                    }
                }
            }
        };
        info!("[S->ES] closing es listener {addr0} from {reason}");
    });

    let stream = ReceiverStream::new(es_rcv);

    let mut builder = HttpResponse::Ok();
    builder.insert_header(("Cache-Control", "no-store"));
    builder.insert_header(("Connection", "keep-alive"));
    Ok(builder
        .content_type(ContentType::octet_stream())
        .streaming(stream))
}

// #[derive(Send)]
struct Webrtc2EsStreamer {
    // config: StreamerConfig,
    // header: Webrtc2EsStreamerHeader,
    sender: Sender<Result<Bytes>>,
    streamer: HashMap<u32, Box<dyn EsStreamer>>,
    // seg_dur: u32,
    // sps: Option<Sps>,
    // pps: Option<Pps>,
    // has_audio: bool,
    // avc_packetier: H264Packet,
    // opus_packetier: OpusPacket,
    // opus2aac_transcoder: Opus2AacTranscoder,
    // vp9_packetizer: Option<Vp9Depacketizer>,
}
impl Webrtc2EsStreamer {
    pub fn new(
        video_ssrc: u32,
        audio_ssrc: Option<u32>,
        seperator: &[u8],
        report: Option<&broadcast::Sender<Option<Vec<u16>>>>,
    ) -> Result<(Self, mpsc::Receiver<Result<Bytes>>)> {
        let (sender, rcvr) = mpsc::channel::<Result<Bytes>>(1024);
        let mut streamer: HashMap<u32, Box<dyn EsStreamer>> = HashMap::new();
        let avc_streamer: Box<EsAvcStreamer<255>> =
            Box::new(EsAvcStreamer::new(seperator, &sender, report));
        streamer.insert(video_ssrc, avc_streamer);
        if let Some(ssrc) = audio_ssrc {
            let opus_streamer: Box<EsOpusStreamer<10>> =
                Box::new(EsOpusStreamer::new(seperator, &sender, report)?);
            streamer.insert(ssrc, opus_streamer);
        }
        Ok((Self { sender, streamer }, rcvr))
    }

    pub fn parse_rtp(&mut self, rtp: Packet, ssrc: u32) -> Result<()> {
        let streamer = self
            .streamer
            .get_mut(&ssrc)
            .ok_or(anyhow!("no such ssrc {ssrc}"))?;
        streamer.feed(rtp)?;
        Ok(())
    }
}

pub trait EsStreamer: Send {
    // fn initialize(&mut self, rtp: &Packet) -> Option<TrackBox>;
    fn feed(&mut self, rtp: Packet) -> Result<()>;
    fn codec_type(&self) -> RTPCodecType;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Debug, Clone, Deserialize, Default)]
struct StreamerConfig {
    sep: Option<String>,
}
