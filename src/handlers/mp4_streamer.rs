use crate::processor::channel_io::ChannelWriter;
use crate::processor::jitter::{self, JitterBuffer};
use crate::processor::mp4::aac::Mp4boxOpus2AacStreamer;
use crate::processor::mp4::h264::Mp4boxAvcStreamer;
use crate::processor::opus2aac::Opus2AacTranscoder;
use crate::processor::vp9::Vp9DepacketizerExt;
use crate::tokio_rcv_lock;
use crate::utils::{MP4_TIMESCALE, RTCP_REPORT_INTERVAL};
use crate::{
    errors::VCError,
    processor::vp9::Vp9Depacketizer,
    tokio_any_lock, tokio_watch_lock, tokio_write_lock,
    utils::{
        self, peer_closed, RidPacket, RtcJob, RtcSession, RtcSessionRtpInfo, RtcSessions,
        MAX_SESSION_TIME, PEER_STUN_ADDRS, RTCP_FRACTION_THRESHOLD, RTCP_JITTER_THRESHOLD,
    },
};
use actix_web::http;
use actix_web::{
    get,
    http::{header::ContentType, Method, StatusCode},
    web, HttpRequest, HttpResponse, Result as ActixResult,
};
use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use log::{debug, info};
use mse_fmp4::aac::{AacProfile, ChannelConfiguration, SamplingFrequency};
use mse_fmp4::avc::AvcDecoderConfigurationRecord;
use mse_fmp4::fmp4::Mp4Box;
use mse_fmp4::fmp4::{
    AacSampleEntry, AvcConfigurationBox, AvcSampleEntry, InitializationSegment, MediaDataBox,
    MediaSegment, Mpeg4EsDescriptorBox, Sample, SampleEntry, SampleFlags, TrackBox,
    TrackExtendsBox, TrackFragmentBox,
};
use mse_fmp4::io::WriteTo;
use serde_json::json;
use std::any::Any;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::io::{BufWriter, Read, Write};
use std::sync::{Condvar, Mutex};
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};
use std::{u32, u8};
use tokio::sync::mpsc;
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        watch, Notify, RwLock,
    },
    time::Instant,
};
use tokio_stream::wrappers::ReceiverStream;
use webrtc::api::media_engine::{MIME_TYPE_H264, MIME_TYPE_VP9};
use webrtc::media::io::h264_reader::NAL;
use webrtc::rtcp::reception_report::ReceptionReport;
use webrtc::rtp::codecs::h264::{H264Packet, SPS_NALU_TYPE};
use webrtc::rtp::codecs::opus::OpusPacket;
use webrtc::rtp::packetizer::Depacketizer;
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
#[get("/fmp4/{target}")]
pub async fn http_streamer(
    // config: web::Query<StreamerConfig>,
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

    let (mut streamer,  mut fmp4_rcv) =
        // Webrtc2Fmp4Streamer::new(video_ssrc, audio_ssrc.clone()).map_err(|e| vcerr!("{e:?}"))?;
        Webrtc2Fmp4Streamer::new(video_ssrc, None).map_err(|e| vcerr!("{e:?}"))?;

    let mut vrcvr = target_session.vsdr.subscribe();
    let mut arcvr = target_session.asdr.subscribe();

    // connection.on_ice_connection_state_change(Box::new(
    //     move |connection_state: RTCIceConnectionState| {
    //         info!("Session {addr2} state changed {connection_state}",);
    //         if connection_state == RTCIceConnectionState::Connected {
    //             let sessions1 = sessions1.clone();
    //             let addr1 = addr1.clone();
    //             let target_announcer1 = target_announcer.clone();
    //             Box::pin(async move {
    //                 let _ = target_announcer1.send(addr1.clone());
    //                 if let Ok(mut s) = tokio_write_lock!(sessions1, 10) {
    //                     s.set_listener(target, &addr1);
    //                 }
    //             })

    let target_announcer = target_session.announcer.clone();
    let sessions1 = sessions.into_inner();
    tokio::spawn(async move {
        let addr1 = Arc::downgrade(&addr);
        match tokio_write_lock!(sessions1, 10) {
            Ok(mut sessions) => {
                sessions.set_listener(target, &addr1);
            }
            Err(_) => {
                log::error!("[S->C] setting http provider listener {addr0} failed");
                return;
            }
        }
        let _ = target_announcer.send(addr1);
        let reason = loop {
            let timeout = tokio::time::sleep(Duration::from_secs(10));
            tokio::pin!(timeout);
            tokio::select! {
                _ = timeout.as_mut() => {break "timeout".to_owned()}
                m = vrcvr.recv() => {
                    let Ok(rtp) = m else {continue;};
                    if let Err(e) = streamer.parse_rtp(&rtp.packet, video_ssrc) {
                        break e.to_string();
                    }
                    // let Ok(_) = streamer.parse_rtp(&rtp.packet, video_ssrc) else {break "video parse err"};
                }
                // m = arcvr.recv() => {
                //     let Some(ssrc) = audio_ssrc.as_ref().map(|v|*v) else {break "audio ssrc empty"};
                //     let Ok(rtp) = m else {continue;};
                //     let Ok(_) = streamer.parse_rtp(&rtp.packet, ssrc) else {break "audio parse err"};
                // }
            }
        };
        info!("[S->C] closing http provider {addr0} from {reason}");
    });

    let stream = ReceiverStream::new(fmp4_rcv);

    let mut builder = HttpResponse::Ok();
    builder.insert_header(("Content-Type", "video/mp4"));
    builder.insert_header(("Cache-Control", "no-store"));
    builder.insert_header(("Connection", "keep-alive"));
    Ok(builder
        // .content_type(ContentType::octet_stream())
        .streaming(stream))
}

#[derive(Debug, Clone, Deserialize, Default)]
struct StreamerConfig {
    group_nb: Option<usize>,
    video_mime: Option<String>,
    audio_mime: Option<String>,
}

// #[derive(Send)]
struct Webrtc2Fmp4Streamer {
    // config: StreamerConfig,
    streamer: Vec<Mp4boxStreamer>,
    init_segment: Option<InitializationSegment>,
    writer: ChannelWriter,
    // seg_dur: u32,
    // sps: Option<Sps>,
    // pps: Option<Pps>,
    // has_audio: bool,
    // avc_packetier: H264Packet,
    // opus_packetier: OpusPacket,
    // opus2aac_transcoder: Opus2AacTranscoder,
    // vp9_packetizer: Option<Vp9Depacketizer>,
}
impl Webrtc2Fmp4Streamer {
    pub fn new(
        video_ssrc: u32,
        audio_ssrc: Option<u32>,
    ) -> Result<(Self, mpsc::Receiver<Result<Bytes>>)> {
        // 暂定只有h264 + opus的流
        let h264_stremer = Mp4boxStreamer::new_h264(video_ssrc);
        let (mse_sdr, mse_rcv) = mpsc::channel::<Result<Bytes>>(1024);
        let writer = ChannelWriter::new(mse_sdr);
        let streamer = match audio_ssrc {
            Some(ssrc) => {
                vec![h264_stremer, Mp4boxStreamer::new_opus_aac(ssrc)?]
            }
            None => {
                vec![h264_stremer]
            }
        };
        Ok((
            Self {
                streamer,
                init_segment: None,
                writer,
            },
            mse_rcv,
        ))
    }

    pub fn parse_rtp(&mut self, rtp: &Packet, ssrc: u32) -> Result<()> {
        let (track_id, streamer) = self
            .streamer
            .iter_mut()
            .enumerate()
            .find(|(_, v)| v.ssrc == ssrc)
            .map(|(i, v)| (i + 1, v))
            .ok_or(anyhow!("no such ssrc {ssrc}"))?;
        let boxes = streamer.boxer.feed(rtp, track_id as u32);
        let mut samples = vec![];
        for tbox in boxes {
            match tbox {
                StreamerTrackBox::TrackBox(trak) => {
                    streamer.trak.replace(trak);
                    self.init_segment.take();
                    // 如果有新的track，则之前的traf则视为无效
                    samples.clear();
                }
                StreamerTrackBox::TrackFragmentBox(sample) => {
                    samples.push(sample);
                }
            }
        }

        if self.init_segment.is_none() && self.streamer.iter().any(|v| v.trak.is_some()) {
            #[cfg(feature = "log")]
            log::debug!(target:"debug","creating moov");

            self.make_initialization_segment()?;
            let Some(init_segment) = self.init_segment.as_ref() else {
                return Err(anyhow!("init_segment not set"));
            };
            init_segment.write_to(&mut self.writer)?;
        }

        // TODO暂定任何rtp帧都是一个segment，这样会很碎，增加头变相增加码率
        if samples.len() > 0 && self.init_segment.is_some() {
            #[cfg(feature = "log")]
            log::debug!(target:"debug","creating fragment");

            let segments = Self::make_media_segment(samples)?;
            segments.write_to(&mut self.writer)?;
        }
        Ok(())
    }

    fn make_initialization_segment(&mut self) -> Result<()> {
        let mut segment = InitializationSegment::default();
        segment.moov_box.mvhd_box.timescale = MP4_TIMESCALE;
        segment.moov_box.mvhd_box.duration = 0;

        for (i, streamer) in self.streamer.iter().enumerate() {
            let ssrc = streamer.ssrc;
            segment.moov_box.trak_boxes.push(
                streamer
                    .trak
                    .as_ref()
                    .ok_or(anyhow!("trak_boxe {ssrc} miss trak"))?
                    .clone(),
            );
            segment
                .moov_box
                .mvex_box
                .trex_boxes
                .push(TrackExtendsBox::new(i as u32 + 1));
        }
        self.init_segment.replace(segment);
        Ok(())
    }

    fn make_media_segment(segments: Vec<(TrackFragmentBox, Bytes)>) -> Result<MediaSegment> {
        let mut segment = MediaSegment::default();
        let datas = segments.into_iter().fold(vec![], |mut a, (f, d)| {
            segment.moof_box.traf_boxes.push(f);
            a.push(d);
            a
        });
        let mut counter = ByteCounter::with_sink();
        segment.moof_box.write_box(&mut counter)?;

        for (i, data) in datas.into_iter().enumerate() {
            segment.moof_box.traf_boxes[i].trun_box.data_offset = Some(counter.count() as i32 + 8);
            segment.mdat_boxes.push(MediaDataBox {
                data: data.to_vec(),
            });
            segment.mdat_boxes[i].write_box(&mut counter)?;
        }
        Ok(segment)
    }
}

struct Mp4boxStreamer {
    ssrc: u32,
    boxer: Box<dyn Mp4boxer>,
    trak: Option<TrackBox>,
    // buf: Vec<MediaSample>,
    // timestamp: u32,
}

impl Mp4boxStreamer {
    // streamer sample， 单位这里就要被调整为moov的sample rate
    // pub fn duration(&self) -> Result<u32> {
    //     let mut duration: u32 = 0;
    //     for sample in &self.buf {
    //         duration = duration
    //             .checked_add(sample.duration()?)
    //             .ok_or(anyhow!("sample duration overflow"))?;
    //     }
    //     let Some(trak) = self.trak.as_ref() else {
    //         return Err(anyhow!("track not set yet, duration unable to calculate"));
    //     };
    //     Ok(duration * MP4_TIMESCALE / trak.mdia_box.mdhd_box.timescale)
    // }
    pub fn new_h264(ssrc: u32) -> Self {
        Self {
            ssrc,
            boxer: Box::new(Mp4boxAvcStreamer::new()),
            trak: None,
        }
    }
    pub fn new_opus_aac(ssrc: u32) -> Result<Self> {
        Ok(Self {
            ssrc,
            boxer: Box::new(Mp4boxOpus2AacStreamer::new()?),
            trak: None,
        })
    }
}

pub trait Mp4boxer: Send {
    // fn initialize(&mut self, rtp: &Packet) -> Option<TrackBox>;
    fn feed(&mut self, rtp: &Packet, track_id: u32) -> Vec<StreamerTrackBox>;
    fn codec_type(&self) -> RTPCodecType;
    // fn duration(&self) -> u32;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub enum StreamerTrackBox {
    TrackBox(TrackBox),
    TrackFragmentBox((TrackFragmentBox, Bytes)),
}

#[derive(Debug, Default)]
struct MediaSample {
    samples: Vec<Sample>,
    data: Bytes,
    kind: RTPCodecType,
}
impl MediaSample {
    pub fn new(kind: RTPCodecType) -> Self {
        Self {
            kind,
            ..Self::default()
        }
    }
    /// media sample， 单位默认是该轨道下的sample rate
    pub fn duration(&self) -> Result<u32> {
        let mut duration: u32 = 0;
        for sample in &self.samples {
            let sample_duration = sample.duration.ok_or(anyhow!("sample duration not set"))?;
            duration = duration
                .checked_add(sample_duration)
                .ok_or(anyhow!("sample duration overflow"))?;
        }
        Ok(duration)
    }
    // pub fn add_video_sample(&mut self, nal: &NAL) -> Self {
    //     self.samples.push(Sample {});
    // }
}

// #[derive(Debug)]
// pub struct VideoStreamBuffer {
//     rtp_reader: std::sync::mpsc::Receiver<Bytes>,
//     cache: BytesMut,
// }
// impl VideoStreamBuffer {
//     pub fn new(rtp_reader: std::sync::mpsc::Receiver<Bytes>) -> Self {
//         Self {
//             rtp_reader,
//             cache: BytesMut::new(),
//         }
//     }
// }

// impl Read for VideoStreamBuffer {
//     fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
//         let reader = self.get_reader();
//         reader.read(buf)
//     }
// }

#[derive(Debug)]
pub(crate) struct ByteCounter<T> {
    inner: T,
    count: u64,
}
impl<T> ByteCounter<T> {
    pub fn new(inner: T) -> Self {
        ByteCounter { inner, count: 0 }
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}
impl ByteCounter<std::io::Sink> {
    pub fn with_sink() -> Self {
        Self::new(std::io::sink())
    }

    pub fn calculate<F>(f: F) -> Result<u64>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        let mut writer = ByteCounter::with_sink();
        f(&mut writer)?;
        Ok(writer.count() as u64)
    }
}
impl<T: Write> Write for ByteCounter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let size = self.inner.write(buf)?;
        self.count += size as u64;
        Ok(size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
