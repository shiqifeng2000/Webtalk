use crate::{
    errors::VCError,
    handler_ws::{CandidateEvent, WsSession},
    handlers::http_conf::{StreamerConfigAudio, StreamerConfigVideo},
};
use actix::Addr;
use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use crypto::{digest::Digest, md5};
use lazy_static::lazy_static;
use openssl::ssl::{SslAcceptor, SslAcceptorBuilder, SslFiletype, SslMethod};
use regex::Regex;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    env,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc, watch, Notify, RwLock};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors,
        media_engine::{
            MediaEngine, MIME_TYPE_AV1, MIME_TYPE_H264, MIME_TYPE_OPUS, MIME_TYPE_VP8,
            MIME_TYPE_VP9,
        },
        setting_engine::SettingEngine,
        APIBuilder, API,
    },
    ice::udp_network::{EphemeralUDP, UDPNetwork},
    ice_transport::{ice_candidate::RTCIceCandidateInit, ice_candidate_type::RTCIceCandidateType},
    interceptor::registry::Registry,
    peer_connection::{peer_connection_state::RTCPeerConnectionState, RTCPeerConnection},
    rtp::packet::Packet,
    rtp_transceiver::{
        rtp_codec::{
            RTCRtpCodecCapability, RTCRtpCodecParameters, RTCRtpHeaderExtensionCapability,
            RTPCodecType,
        },
        RTCPFeedback,
    },
};

lazy_static! {
    pub static ref EPHEMERAL_UDP_MIN_PORT: u32 = {
        env::var("EPHEMERAL_UDP_MIN_PORT")
            .unwrap_or("49152".to_string())
            .parse::<u32>()
            .unwrap_or(49152)
    };
    pub static ref EPHEMERAL_UDP_MAX_PORT: u32 = {
        env::var("EPHEMERAL_UDP_MAX_PORT")
            .unwrap_or("65535".to_string())
            .parse::<u32>()
            .unwrap_or(65535)
    };
    pub static ref EPHEMERAL_PORTS: Vec<u32> = {
        env::var("EPHEMERAL_PORTS")
            .map(|v| {
                v.split(",")
                    .filter(|s| *s != "")
                    .map(|s| s.parse::<u32>().unwrap_or(0))
                    .collect::<Vec<u32>>()
            })
            .unwrap_or(vec![])
    };
    pub static ref LOG_PATH: String = env::var("LOG_PATH").unwrap_or("./log".to_string());
    pub static ref LOGGER: String = env::var("LOGGER").unwrap_or("log4rs.yaml".to_string());
    pub static ref DEBUG_MODE: bool = env::var("DEBUG_MODE")
        .map(|v| v.parse::<i32>().map(|s| s == 1).unwrap_or(false))
        .unwrap_or(false);
    pub static ref SERVER_PORT: u32 = env::var("SERVER_PORT")
        .map(|v| v.parse::<u32>().unwrap_or(10000))
        .unwrap_or(10000);
    pub static ref SSL_SERVER_PORT: u32 = env::var("SSL_SERVER_PORT")
        .map(|v| v.parse::<u32>().unwrap_or(10043))
        .unwrap_or(10043);
    pub static ref STUN_ADDR: String = env::var("STUN_ADDR").unwrap_or("0.0.0.0:3478".to_string());
    pub static ref PEER_STUN_ADDRS: String =
        env::var("PEER_STUN_ADDRS").unwrap_or("0.0.0.0:3478".to_string());
    pub static ref TURN_ADDR: String = env::var("TURN_ADDR").unwrap_or("0.0.0.0:3479".to_string());
    pub static ref TURN_USERS: String = env::var("TURN_USERS").unwrap_or("".to_string());
    pub static ref HOST_CANDIDATE_IP: String =
        env::var("HOST_CANDIDATE_IP").unwrap_or("".to_owned());
    pub static ref STORAGE_PATH: String =
        std::env::var("STORAGE_PATH").unwrap_or("./storage".to_string());
    pub static ref SSL_KEY: String =
        std::env::var("SSL_KEY").unwrap_or("./cert/cert.key".to_string());
    pub static ref SSL_CERT: String =
        std::env::var("SSL_CERT").unwrap_or("./cert/cert.pem".to_string());
    pub static ref MAX_SESSION_TIME: u64 = env::var("MAX_SESSION_TIME")
        .map(|v| v.parse::<u64>().unwrap_or(604800))
        .unwrap_or(604800);
    pub static ref SOCEKT_MODE: i32 = env::var("SOCEKT_MODE")
        .map(|v| v.parse::<i32>().unwrap_or(0))
        .unwrap_or(0);
    pub static ref KAFKA_BROKER: String = std::env::var("KAFKA_BROKER").unwrap_or("".to_string());
    pub static ref KAFKA_TOPIC_STREAM: String =
        std::env::var("KAFKA_TOPIC_STREAM").unwrap_or("".to_string());
    pub static ref KAFKA_TOPIC_LISTENER: String =
        std::env::var("KAFKA_TOPIC_LISTENER").unwrap_or("".to_string());
    pub static ref TOKEN: String = std::env::var("TOKEN").unwrap_or("webtalk".to_string());
    pub static ref RTCP_FRACTION_THRESHOLD: u8 = std::env::var("RTCP_FRACTION_THRESHOLD")
        .unwrap_or("20".to_string())
        .parse::<u8>()
        .unwrap_or(20);
    // 40ms * 65535 / 1000
    pub static ref RTCP_JITTER_THRESHOLD: u32 = std::env::var("RTCP_JITTER_THRESHOLD")
        .unwrap_or("1000".to_string())
        .parse::<u32>()
        .unwrap_or(1000);
    pub static ref RTCP_REPORT_INTERVAL: u128 = std::env::var("RTCP_REPORT_INTERVAL")
        .unwrap_or("30000".to_string())
        .parse::<u128>()
        .unwrap_or(30000);

}

pub const MP4_VIDEO_SAMPLE_RATE: u32 = 90000;
pub const MP4_AUDIO_SAMPLE_RATE: u32 = 48000;
pub const MP4_AUDIO_CHANNELS: u32 = 2;
pub const MP4_AUDIO_SAMPLE_PER_FRAME: u32 = 1024;
pub const MP4_TIMESCALE: u32 = 1000;

#[derive(Serialize, Deserialize, Clone)]
pub struct RtcJob {
    pub token: Option<u32>,
    pub target: Option<u32>,
    pub sockets: Option<usize>,
    pub candidates: Option<Vec<Option<RTCIceCandidateInit>>>,
    pub prefer_codec: Option<String>,
    pub peer: String,
}

impl RtcJob {
    pub fn parse_codecs(&self) -> Option<(Option<String>, Option<String>)> {
        match &self.prefer_codec {
            Some(codec) => {
                let codecs = codec
                    .split(",")
                    .filter(|v| *v != "")
                    .map(|v| v.trim().to_owned())
                    .collect::<Vec<String>>();
                let codecs_len = codecs.len();
                if codecs_len >= 2 {
                    Some((Some(codecs[0].clone()), Some(codecs[1].clone())))
                } else if codecs_len == 1 {
                    Some((Some(codecs[0].clone()), None))
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

// #[derive(Serialize, Deserialize, Clone)]
// pub struct RtcSubscriber {
//     pub id: u32,
//     pub target: u32,
// }

pub fn gen_webrtc_api(simucast: bool) -> API {
    let mut m = MediaEngine::default();
    // m.register_default_codecs()
    //     .expect("Failed to register default codecs");
    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: vec![
                    RTCPFeedback {
                        typ: "goog-remb".to_owned(),
                        parameter: "".to_owned(),
                    },
                    RTCPFeedback {
                        typ: "ccm".to_owned(),
                        parameter: "fir".to_owned(),
                    },
                    RTCPFeedback {
                        typ: "nack".to_owned(),
                        parameter: "".to_owned(),
                    },
                    RTCPFeedback {
                        typ: "nack".to_owned(),
                        parameter: "pli".to_owned(),
                    },
                ],
            },
            payload_type: 102,
            ..Default::default()
        },
        RTPCodecType::Video,
    )
    .unwrap();

    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RTPCodecType::Audio,
    )
    .unwrap();

    let mut s = SettingEngine::default();
    s.set_ice_timeouts(
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_millis(200)),
    );
    let port_min = *EPHEMERAL_UDP_MIN_PORT as u16;
    let port_max = *EPHEMERAL_UDP_MAX_PORT as u16;
    let host_ips = HOST_CANDIDATE_IP
        .split(",")
        .filter(|v| *v != "")
        .map(|v| v.to_owned())
        .collect::<Vec<String>>();
    let udp_socket = EphemeralUDP::new(port_min, port_max).unwrap();
    s.set_udp_network(UDPNetwork::Ephemeral(udp_socket));
    if host_ips.len() > 0 {
        s.set_nat_1to1_ips(host_ips, RTCIceCandidateType::Host);
    }
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)
        .expect("Failed to register default interceptors");

    if simucast {
        for extension in [
            "urn:ietf:params:rtp-hdrext:sdes:mid",
            "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
            "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
        ] {
            m.register_header_extension(
                RTCRtpHeaderExtensionCapability {
                    uri: extension.to_owned(),
                },
                RTPCodecType::Video,
                None,
            )
            .unwrap();
        }
    }

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_setting_engine(s)
        .with_interceptor_registry(registry)
        .build();
    return api;
}

pub fn api_from_codecs(video_codecs: Vec<String>, simucast: bool) -> API {
    let mut m = MediaEngine::default();
    for codec in video_codecs {
        if codec == MIME_TYPE_H264 {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_H264.to_owned(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![
                            RTCPFeedback {
                                typ: "goog-remb".to_owned(),
                                parameter: "".to_owned(),
                            },
                            RTCPFeedback {
                                typ: "ccm".to_owned(),
                                parameter: "fir".to_owned(),
                            },
                            RTCPFeedback {
                                typ: "nack".to_owned(),
                                parameter: "".to_owned(),
                            },
                            RTCPFeedback {
                                typ: "nack".to_owned(),
                                parameter: "pli".to_owned(),
                            },
                        ],
                    },
                    payload_type: 102,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
            .unwrap();
        } else if codec == MIME_TYPE_VP8 {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_VP8.to_owned(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type: 96,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
            .unwrap();
        } else if codec == MIME_TYPE_VP9 {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_VP9.to_owned(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type: 98,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
            .unwrap();
        } else if codec == MIME_TYPE_AV1 {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_AV1.to_owned(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type: 41,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
            .unwrap();
        }
    }
    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "".to_owned(),
                // sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RTPCodecType::Audio,
    )
    .unwrap();
    let mut s = SettingEngine::default();
    s.set_ice_timeouts(
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_millis(200)),
    );
    let port_min = *EPHEMERAL_UDP_MIN_PORT as u16;
    let port_max = *EPHEMERAL_UDP_MAX_PORT as u16;

    let host_ips = HOST_CANDIDATE_IP
        .split(",")
        .filter(|v| *v != "")
        .map(|v| v.to_owned())
        .collect::<Vec<String>>();
    let udp_socket = EphemeralUDP::new(port_min, port_max).unwrap();
    s.set_udp_network(UDPNetwork::Ephemeral(udp_socket));
    if host_ips.len() > 0 {
        s.set_nat_1to1_ips(host_ips, RTCIceCandidateType::Host);
    }
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)
        .expect("Failed to register default interceptors");

    if simucast {
        for extension in [
            "urn:ietf:params:rtp-hdrext:sdes:mid",
            "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
            "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
        ] {
            m.register_header_extension(
                RTCRtpHeaderExtensionCapability {
                    uri: extension.to_owned(),
                },
                RTPCodecType::Video,
                None,
            )
            .unwrap();
        }
    }

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_setting_engine(s)
        .with_interceptor_registry(registry)
        .build();
    return api;
}

pub fn api_from_payloads(
    video_info: &HashMap<Cow<'_, str>, Vec<RtcSessionRtpInfo>>,
    audio_info: &HashMap<Cow<'_, str>, Vec<RtcSessionRtpInfo>>,
    simucast: bool,
) -> API {
    let mut m = MediaEngine::default();
    for (codec, infos) in video_info {
        if codec != MIME_TYPE_H264
            && codec != MIME_TYPE_VP8
            && codec != MIME_TYPE_VP9
            && codec != MIME_TYPE_AV1
        {
            continue;
        }
        let mut payloads = infos.iter().map(|v| v.payload).collect::<Vec<u8>>();
        payloads.dedup();
        for payload_type in payloads {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: codec.to_string(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
            .unwrap();
        }
    }
    for (codec, infos) in audio_info {
        if codec != MIME_TYPE_OPUS {
            continue;
        }
        let mut payloads = infos.iter().map(|v| v.payload).collect::<Vec<u8>>();
        payloads.dedup();
        for payload_type in payloads {
            m.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_OPUS.to_owned(),
                        clock_rate: 48000,
                        channels: 2,
                        sdp_fmtp_line: "".to_owned(),
                        // sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type,
                    ..Default::default()
                },
                RTPCodecType::Audio,
            )
            .unwrap();
        }
    }
    let mut s = SettingEngine::default();
    s.set_ice_timeouts(
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_millis(200)),
    );
    let port_min = *EPHEMERAL_UDP_MIN_PORT as u16;
    let port_max = *EPHEMERAL_UDP_MAX_PORT as u16;

    let host_ips = HOST_CANDIDATE_IP
        .split(",")
        .filter(|v| *v != "")
        .map(|v| v.to_owned())
        .collect::<Vec<String>>();
    let udp_socket = EphemeralUDP::new(port_min, port_max).unwrap();
    s.set_udp_network(UDPNetwork::Ephemeral(udp_socket));
    if host_ips.len() > 0 {
        s.set_nat_1to1_ips(host_ips, RTCIceCandidateType::Host);
    }
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)
        .expect("Failed to register default interceptors");

    if simucast {
        for extension in [
            "urn:ietf:params:rtp-hdrext:sdes:mid",
            "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
            "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
        ] {
            m.register_header_extension(
                RTCRtpHeaderExtensionCapability {
                    uri: extension.to_owned(),
                },
                RTPCodecType::Video,
                None,
            )
            .unwrap();
        }
    }

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_setting_engine(s)
        .with_interceptor_registry(registry)
        .build();
    return api;
}

pub fn api_from_list(sockets: &Vec<u32>) -> API {
    let mut m = MediaEngine::default();
    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 102,
            ..Default::default()
        },
        RTPCodecType::Video,
    )
    .unwrap();

    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RTPCodecType::Audio,
    )
    .unwrap();

    let mut s = SettingEngine::default();
    s.set_ice_timeouts(
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_millis(200)),
    );
    let port_min = sockets[0] as u16;
    let port_max = sockets[sockets.len() - 1] as u16;

    let host_ips = HOST_CANDIDATE_IP
        .split(",")
        .filter(|v| *v != "")
        .map(|v| v.to_owned())
        .collect::<Vec<String>>();
    let udp_socket = EphemeralUDP::new(port_min, port_max).unwrap();
    s.set_udp_network(UDPNetwork::Ephemeral(udp_socket));
    if host_ips.len() > 0 {
        s.set_nat_1to1_ips(host_ips, RTCIceCandidateType::Host);
    }
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)
        .expect("Failed to register default interceptors");
    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_setting_engine(s)
        .with_interceptor_registry(registry)
        .build();
    return api;
}

// alphabet to base 64
pub fn atob(b: &str) -> String {
    base64::encode(b)
}
// base64 to alphabet
pub fn btoa(s: &str) -> Result<String, VCError> {
    let b = base64::decode(s)?;
    let s = String::from_utf8(b)?;
    Ok(s)
}

pub fn check_sdp(sdp_str: &str) -> Result<(), VCError> {
    let b: Vec<u8> = sdp_str.as_bytes().into();
    let mut buff = std::io::Cursor::new(b);
    let sd = sdp::SessionDescription::unmarshal(&mut buff)
        .map_err(|e| VCError::new(&format!("error unmarsalling {}", e.to_string())))?;
    let mut mds = sd.media_descriptions.iter();
    let mv = mds
        .find(|v| (*v).media_name.media == "video")
        .ok_or(VCError::new("No video desc in offer"))?;
    mv.attributes
        .iter()
        .find(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sH264/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .ok_or(VCError::new("No h264 desc in offer"))?;

    let mut mds = sd.media_descriptions.iter();
    if let Some(ma) = mds.find(|v| (*v).media_name.media == "audio") {
        ma.attributes
            .iter()
            .find(|v| {
                if (*v).key == "rtpmap" {
                    (*v).value
                        .as_ref()
                        .map(|s| Regex::new(r"^\d+?\sopus/48000/2$").unwrap().is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            })
            .ok_or(VCError::new("No opus desc in offer"))?;
    };

    Ok(())
}

pub fn parse_payloads(sdp_str: &str) -> Result<HashMap<String, Vec<u8>>, VCError> {
    let b: Vec<u8> = sdp_str.as_bytes().into();
    let mut buff = std::io::Cursor::new(b);
    let sd = sdp::SessionDescription::unmarshal(&mut buff)
        .map_err(|e| VCError::new(&format!("error unmarsalling {}", e.to_string())))?;
    let mut mds = sd.media_descriptions.iter();
    let mv = mds
        .find(|v| (*v).media_name.media == "video")
        .ok_or(VCError::new("No video desc in offer"))?;
    let h264_payloads = mv
        .attributes
        .iter()
        .filter(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sH264/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|v| {
            v.value
                .as_ref()
                .map(|s| {
                    Regex::new(r"^(\d+)?\sH264/90000$")
                        .unwrap()
                        .replace_all(s, "$1")
                })
                .unwrap()
                .parse::<u8>()
                .unwrap()
        })
        .collect::<Vec<u8>>();
    let vp8_payloads = mv
        .attributes
        .iter()
        .filter(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sVP8/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|v| {
            v.value
                .as_ref()
                .map(|s| {
                    Regex::new(r"^(\d+)?\sVP8/90000$")
                        .unwrap()
                        .replace_all(s, "$1")
                })
                .unwrap()
                .parse::<u8>()
                .unwrap()
        })
        .collect::<Vec<u8>>();
    let vp9_payloads = mv
        .attributes
        .iter()
        .filter(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sVP9/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|v| {
            v.value
                .as_ref()
                .map(|s| {
                    Regex::new(r"^(\d+)?\sVP9/90000$")
                        .unwrap()
                        .replace_all(s, "$1")
                })
                .unwrap()
                .parse::<u8>()
                .unwrap()
        })
        .collect::<Vec<u8>>();
    let av1_payloads = mv
        .attributes
        .iter()
        .filter(|v| {
            if (*v).key == "rtpmap" {
                (*v).value
                    .as_ref()
                    .map(|s| Regex::new(r"^\d+?\sAV1/90000$").unwrap().is_match(s))
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|v| {
            v.value
                .as_ref()
                .map(|s| {
                    Regex::new(r"^(\d+)?\sAV1/90000$")
                        .unwrap()
                        .replace_all(s, "$1")
                })
                .unwrap()
                .parse::<u8>()
                .unwrap()
        })
        .collect::<Vec<u8>>();
    let mut map = [
        (MIME_TYPE_H264.to_owned(), h264_payloads),
        (MIME_TYPE_VP8.to_owned(), vp8_payloads),
        (MIME_TYPE_VP9.to_owned(), vp9_payloads),
        (MIME_TYPE_AV1.to_owned(), av1_payloads),
    ]
    .into_iter()
    .collect::<HashMap<String, Vec<u8>>>();
    let mut mds = sd.media_descriptions.iter();
    if let Some(ma) = mds.find(|v| (*v).media_name.media == "audio") {
        let opus_payloads = ma
            .attributes
            .iter()
            .filter(|v| {
                if (*v).key == "rtpmap" {
                    (*v).value
                        .as_ref()
                        .map(|s| Regex::new(r"^\d+?\sopus/48000/2$").unwrap().is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            })
            .map(|v| {
                v.value
                    .as_ref()
                    .map(|s| {
                        Regex::new(r"^(\d+)?\sopus/48000/2$")
                            .unwrap()
                            .replace_all(s, "$1")
                    })
                    .unwrap()
                    .parse::<u8>()
                    .unwrap()
            })
            .collect::<Vec<u8>>();
        map.insert(MIME_TYPE_OPUS.to_owned(), opus_payloads);
    }
    Ok(map)
}

pub fn gen_ssl_builder() -> Result<SslAcceptorBuilder, VCError> {
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    builder.set_private_key_file(&*SSL_KEY, SslFiletype::PEM)?;
    builder.set_certificate_chain_file(&*SSL_CERT)?;
    Ok(builder)
}

#[derive(Serialize, Clone)]
pub struct P2PSession {
    pub id: u32,
    pub offer: String,
    pub answer: Option<String>,
    pub offer_candidates: Vec<CandidateEvent>,
    pub answer_candidates: Vec<CandidateEvent>,
    #[serde(skip_serializing)]
    pub offer_addr: Addr<WsSession>,
    #[serde(skip_serializing)]
    pub answer_addr: Option<Addr<WsSession>>,
    #[serde(skip_serializing)]
    pub hb: Instant,
}
impl P2PSession {
    pub fn new(id: u32, offer: &str, offer_addr: &Addr<WsSession>) -> Self {
        Self {
            id,
            offer: offer.to_owned(),
            answer: None,
            offer_candidates: vec![],
            answer_candidates: vec![],
            offer_addr: offer_addr.clone(),
            answer_addr: None,
            hb: Instant::now(),
        }
    }
}

#[derive(Serialize, Clone, Default)]
pub struct P2PSessions {
    pub hash: HashMap<u32, P2PSession>,
}
impl P2PSessions {
    pub fn init_arc() -> Arc<RwLock<Self>> {
        let me = Arc::new(RwLock::new(Self::default()));
        let me1 = me.clone();
        tokio::spawn(async move {
            let mut ticker: tokio::time::Interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                if let Ok(mut m1) = me1.try_write() {
                    m1.prune();
                }
                ticker.tick().await;
            }
        });
        me
    }
    pub fn create_session(&mut self, offer: &str, offer_addr: &Addr<WsSession>) -> u32 {
        let id = rand::random::<u32>();
        self.hash.insert(id, P2PSession::new(id, offer, offer_addr));
        id
    }
    pub fn add_answer(
        &mut self,
        id: u32,
        answer: &str,
        offer_addr: &Addr<WsSession>,
    ) -> Option<Addr<WsSession>> {
        self.hash.get_mut(&id).map(|s| {
            s.answer.replace(answer.to_owned());
            s.answer_addr.replace(offer_addr.clone());
            s.offer_addr.clone()
        })
    }
    pub fn add_candidate(
        &mut self,
        id: u32,
        candidate: CandidateEvent,
        is_offer: bool,
    ) -> Option<Addr<WsSession>> {
        match self.hash.get_mut(&id) {
            Some(s) => {
                if is_offer {
                    s.offer_candidates.push(candidate);
                    s.answer_addr.clone()
                } else {
                    s.answer_candidates.push(candidate);
                    Some(s.offer_addr.clone())
                }
            }
            None => None,
        }
    }
    pub fn add_candidates(
        &mut self,
        id: u32,
        mut candidate: Vec<CandidateEvent>,
        is_offer: bool,
    ) -> Option<Addr<WsSession>> {
        self.hash.get_mut(&id).map(|s| {
            if is_offer {
                s.offer_candidates.append(&mut candidate);
            } else {
                s.answer_candidates.append(&mut candidate);
            }
            s.offer_addr.clone()
        })
    }
    pub fn get_candidates(&self, id: u32, is_offer: bool) -> Vec<CandidateEvent> {
        self.hash
            .get(&id)
            .map(|s| {
                if is_offer {
                    s.offer_candidates.clone()
                } else {
                    s.answer_candidates.clone()
                }
            })
            .unwrap_or(vec![])
    }
    pub fn get_target_candidates(&self, id: u32, is_offer: bool) -> Vec<CandidateEvent> {
        self.hash
            .get(&id)
            .map(|s| {
                if is_offer {
                    s.answer_candidates.clone()
                } else {
                    s.answer_candidates.clone()
                }
            })
            .unwrap_or(vec![])
    }
    pub fn get_target_addr(&self, id: u32, is_offer: bool) -> Option<Addr<WsSession>> {
        match self.hash.get(&id) {
            Some(s) => {
                if is_offer {
                    s.answer_addr.clone()
                } else {
                    Some(s.offer_addr.clone())
                }
            }
            None => None,
        }
    }
    pub fn hb(&mut self, id: u32, is_offer: bool) {
        if let Some(s) = self.hash.get_mut(&id) {
            if is_offer {
                s.hb = Instant::now()
            }
        }
    }
    pub fn prune(&mut self) {
        self.hash
            .retain(|_, v| Instant::now().duration_since(v.hb).as_secs() <= 10);
    }
    pub fn session_exist(&self, id: u32) -> bool {
        self.hash.contains_key(&id)
    }
    pub fn drop_session(&mut self, id: u32) -> Option<P2PSession> {
        self.hash.remove(&id)
    }
}
#[macro_export]
macro_rules! tokio_read_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_secs($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(VCError::new("tokio_read_lock timeout"))}
            m = $x.read()=> {Ok(m)}
        }
    }};
}

#[macro_export]
macro_rules! tokio_write_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_secs($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(VCError::new("tokio_write_lock timeout"))}
            m = $x.write()=> {Ok(m)}
        }
    }};
}

#[macro_export]
macro_rules! tokio_mutex_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_secs($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(VCError::new("tokio_mutex_lock timeout"))}
            m = $x.lock()=> {Ok(m)}
        }
    }};
}

#[macro_export]
macro_rules! tokio_watch_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_secs($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(VCError::new("tokio_watch_lock timeout"))}
            m = $x.changed()=> {Ok(m)}
        }
    }};
}

#[macro_export]
macro_rules! tokio_any_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_secs($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(VCError::new("tokio_any_lock timeout"))}
            m = $x=> {Ok(m)}
        }
    }};
}
#[macro_export]
macro_rules! tokio_rcv_lock {
    ( $x:expr, $y:expr ) => {{
        let timeout = tokio::time::sleep(Duration::from_millis($y));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {Err(VCError::new("tokio_any_lock timeout"))}
            m = $x.recv() => {Ok(m)}
        }
    }};
}

#[derive(Clone)]
pub struct RtcSession {
    pub id: u32,
    pub sockets: Vec<u32>,
    pub listener: Vec<Weak<String>>,
    // 用于快速拉起视频流I帧用
    pub announcer: broadcast::Sender<Weak<String>>,
    pub vsdr: broadcast::Sender<RidPacket>,
    pub asdr: broadcast::Sender<RidPacket>,
    // 暂时还没有设计抖动时的PLI反馈
    // pub jittersdr: broadcast::Sender<>,
    pub dsdr: broadcast::Sender<Bytes>,
    pub conn: Arc<RTCPeerConnection>,
    // None 表示握手没有完成，Some表示已完成，如果empty表示没有这路流
    pub video: Option<HashMap<Cow<'static, str>, Vec<RtcSessionRtpInfo>>>,
    pub audio: Option<HashMap<Cow<'static, str>, Vec<RtcSessionRtpInfo>>>,
    pub start: Instant,
}

impl RtcSession {
    pub fn new(
        id: u32,
        sockets: Vec<u32>,
        vsdr: &broadcast::Sender<RidPacket>,
        asdr: &broadcast::Sender<RidPacket>,
        dsdr: &broadcast::Sender<Bytes>,
        conn: &Arc<RTCPeerConnection>,
    ) -> Self {
        let (announcer, _) = broadcast::channel::<Weak<String>>(10);
        Self {
            id,
            sockets,
            listener: vec![],
            announcer,
            vsdr: vsdr.clone(),
            asdr: asdr.clone(),
            dsdr: dsdr.clone(),
            conn: conn.clone(),
            video: None,
            audio: None,
            start: Instant::now(),
        }
    }
    pub fn get_listeners(&self) -> Vec<String> {
        self.listener
            .iter()
            .map(|v| v.upgrade().map(|s| format!("{s}")))
            .filter(|v| v.is_some())
            .map(|v| v.unwrap())
            .collect::<Vec<String>>()
    }

    pub fn available_videos(&self) -> Option<Vec<String>> {
        self.video
            .as_ref()
            .map(|av| av.keys().map(|v| v.to_string()).collect())
    }
    // pub fn get_video_info(&self, payload: u8) -> Option<(String, Vec<String>)> {
    //     self.video
    //         .as_ref()
    //         .map(|i| {
    //             let mut result = None;
    //             for (k, v) in i {
    //                 let rids = v
    //                     .iter()
    //                     .filter(|r| r.payload == payload)
    //                     .map(|s| s.rid.to_string())
    //                     .collect();
    //                 result.replace((k.to_string(), rids));
    //                 break;
    //             }
    //             result
    //         })
    //         .unwrap_or(None)
    // }
    pub fn available_audios(&self) -> Option<Vec<String>> {
        self.audio
            .as_ref()
            .map(|aa| aa.keys().map(|v| v.to_string()).collect())
    }
    pub fn get_payload_info(
        payload: u8,
        info: &HashMap<Cow<'static, str>, Vec<RtcSessionRtpInfo>>,
    ) -> Option<(String, Vec<String>)> {
        for (k, v) in info {
            let rids = v
                .iter()
                .filter(|r| r.payload == payload)
                .map(|s| s.rid.to_string())
                .collect();
            return Some((k.to_string(), rids));
        }
        None
    }
    pub fn set_video(&mut self, video: Option<HashMap<Cow<'static, str>, Vec<RtcSessionRtpInfo>>>) {
        self.video = video;
    }
    pub fn set_audio(&mut self, audio: Option<HashMap<Cow<'static, str>, Vec<RtcSessionRtpInfo>>>) {
        self.audio = audio;
    }
    pub fn get_media_infos(
        &self,
    ) -> Option<(
        HashMap<Cow<'static, str>, Vec<RtcSessionRtpInfo>>,
        HashMap<Cow<'static, str>, Vec<RtcSessionRtpInfo>>,
    )> {
        if self.video.is_none() || self.audio.is_none() {
            return None;
        }
        Some((
            self.video.as_ref().unwrap().clone(),
            self.audio.as_ref().unwrap().clone(),
        ))
    }
}
// impl Drop for RtcSession {
//     fn drop(&mut self) {
//         let conn = self.conn.clone();
//         futures::executor::block_on(async move {
//             if !peer_closed(&conn) {
//                 let _ = conn.close().await;
//             }
//         });
//     }
// }

impl Serialize for RtcSession {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("RtcSession", 5)?;
        s.serialize_field("id", &self.id)?;
        s.serialize_field("listener", &self.get_listeners())?;
        s.serialize_field("video", &self.video)?;
        s.serialize_field("audio", &self.audio)?;
        s.serialize_field(
            "eclipsed",
            &Instant::now().duration_since(self.start).as_secs(),
        )?;
        s.end()
    }
}

#[derive(Clone)]
pub struct RidPacket {
    pub rid: String,
    pub packet: Packet,
}
impl RidPacket {
    pub fn new(rid: &str, packet: Packet) -> Self {
        Self {
            rid: rid.to_owned(),
            packet,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct RtcSessionRtpInfo {
    pub rid: Cow<'static, str>,
    pub payload: u8,
    pub ssrc: u32,
    pub level: u8,
}

impl RtcSessionRtpInfo {
    pub fn new(rid: &str, payload: u8, ssrc: u32) -> Self {
        let level = rid2level(rid);
        Self {
            rid: Cow::Owned(rid.to_owned()),
            payload,
            ssrc,
            level,
        }
    }
}

impl PartialEq for RtcSessionRtpInfo {
    fn eq(&self, other: &Self) -> bool {
        self.payload.eq(&other.payload) && self.ssrc.eq(&other.ssrc)
    }

    fn ne(&self, other: &Self) -> bool {
        self.payload.ne(&other.payload) || self.ssrc.ne(&other.ssrc)
    }
}

pub fn rid2level(rid: &str) -> u8 {
    if rid == "h" || rid.contains("2") {
        2
    } else if rid == "m" || rid.contains("1") {
        1
    } else {
        0
    }
}

#[derive(Serialize, Clone, Default)]
pub struct RtcSessions {
    pub hash: HashMap<u32, RtcSession>,
}

impl RtcSessions {
    pub fn init_arc() -> Arc<RwLock<Self>> {
        let me = Arc::new(RwLock::new(Self::default()));
        let me1 = me.clone();
        tokio::spawn(async move {
            let mut ticker: tokio::time::Interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                if let Ok(mut m1) = me1.try_write() {
                    m1.prune();
                }
                ticker.tick().await;
            }
        });
        me
    }
    pub fn create_session(
        &mut self,
        id: u32,
        sockets: Vec<u32>,
        vsdr: &broadcast::Sender<RidPacket>,
        asdr: &broadcast::Sender<RidPacket>,
        dsdr: &broadcast::Sender<Bytes>,
        conn: &Arc<RTCPeerConnection>,
    ) -> Result<RtcSession, VCError> {
        if self.hash.contains_key(&id) {
            return Err(vcerr!("dup session id {id}"));
        }
        let session = RtcSession::new(id, sockets, vsdr, asdr, dsdr, conn);
        self.hash.insert(id, session.clone());
        Ok(session)
    }
    pub fn prune(&mut self) {
        self.hash.retain(|_, v| {
            let state = v.conn.connection_state();
            let result =
                state != RTCPeerConnectionState::Closed && state != RTCPeerConnectionState::Failed;
            if result {
                v.listener.retain(|s| s.upgrade().is_some());
            }
            result
        });
    }
    pub fn get_session(&self, id: u32) -> Option<RtcSession> {
        self.hash.get(&id).map(|v| v.clone())
    }
    pub fn get_session_mut(&mut self, id: u32) -> Option<&mut RtcSession> {
        self.hash.get_mut(&id)
    }
    pub fn set_session_video(
        &mut self,
        id: u32,
        rid: &str,
        payload: u8,
        ssrc: u32,
        payloads: &HashMap<String, Vec<u8>>,
    ) -> Option<RtcSessionRtpInfo> {
        self.hash
            .get_mut(&id)
            .map(|v| {
                let mut result = None;
                if let Some(k) = payloads
                    .iter()
                    .find(|(k, v)| {
                        (*k == MIME_TYPE_H264
                            || *k == MIME_TYPE_AV1
                            || *k == MIME_TYPE_VP8
                            || *k == MIME_TYPE_VP9)
                            && v.contains(&payload)
                    })
                    .map(|(k, _)| k)
                {
                    if v.video.is_none()
                        || v.video
                            .as_ref()
                            .map(|s| !s.contains_key(k.as_str()))
                            .unwrap_or(true)
                    {
                        let mut mapper = HashMap::new();
                        mapper.insert(Cow::Owned(k.clone()), vec![]);
                        v.video.replace(mapper);
                    }
                    let video_infos = v.video.as_mut().unwrap().get_mut(k.as_str()).unwrap();
                    let new_info = RtcSessionRtpInfo::new(rid, payload, ssrc);
                    if !video_infos.contains(&new_info) {
                        result.replace(new_info.clone());
                        video_infos.push(new_info);
                    }
                }
                result
            })
            .unwrap_or(None)
    }
    pub fn set_session_audio(
        &mut self,
        id: u32,
        rid: &str,
        payload: u8,
        ssrc: u32,
        payloads: &HashMap<String, Vec<u8>>,
    ) -> Option<RtcSessionRtpInfo> {
        self.hash
            .get_mut(&id)
            .map(|v| {
                let mut result = None;
                if let Some(k) = payloads
                    .iter()
                    .find(|(k, v)| *k == MIME_TYPE_OPUS && v.contains(&payload))
                    .map(|(k, _)| k)
                {
                    if v.audio.is_none()
                        || v.audio
                            .as_ref()
                            .map(|s| !s.contains_key(k.as_str()))
                            .unwrap_or(true)
                    {
                        let mut mapper = HashMap::new();
                        mapper.insert(Cow::Owned(k.clone()), vec![]);
                        v.audio.replace(mapper);
                    }
                    let audio_infos = v.audio.as_mut().unwrap().get_mut(k.as_str()).unwrap();
                    let new_info = RtcSessionRtpInfo::new(rid, payload, ssrc);
                    if !audio_infos.contains(&new_info) {
                        result.replace(new_info.clone());
                        audio_infos.push(new_info);
                    }
                }
                result
            })
            .unwrap_or(None)
    }
    pub fn add_listener(&mut self, id: u32, addr: &Arc<String>) {
        if let Some(s) = self.hash.get_mut(&id) {
            s.listener.push(Arc::downgrade(addr));
        }
    }
    pub fn set_listener(&mut self, id: u32, addr: &Weak<String>) {
        if let Some(s) = self.hash.get_mut(&id) {
            if let Some(addr0) = addr.upgrade() {
                let listeners = s.get_listeners();
                if !listeners.contains(&addr0) {
                    s.listener.push(addr.clone());
                }
            }
        }
    }
    pub fn drop_listener(&mut self, id: u32, addr: &String) {
        if let Some(s) = self.hash.get_mut(&id) {
            s.listener.retain(|v| match v.upgrade() {
                Some(t) => format!("{t}") != *addr,
                None => false,
            });
        }
    }
    // pub async fn set_peek(&self, id: u32, target: u32) -> Result<(), VCError> {
    //     if let Some(s) = self.hash.get(&id) {
    //         if let Some(t) = self.hash.get(&target) {
    //             let mut peek = tokio_write_lock!(s.peek, 10)?;
    //             peek.replace(t.clone());
    //             // peek.replace(RtcPeeker::new(target, t.vsdr.clone(), t.asdr.clone()));
    //         }
    //         return Ok(());
    //     }
    //     Err(vcerr!("no such target {target}"))
    // }
    pub fn session_exist(&self, id: u32) -> bool {
        self.hash.contains_key(&id)
    }
    pub fn drop_session(&mut self, id: u32) -> Option<RtcSession> {
        let result = self.hash.remove(&id);
        result
    }
}

pub fn peer_closed(conn: &Arc<RTCPeerConnection>) -> bool {
    let state = conn.connection_state();
    state == RTCPeerConnectionState::Closed || state == RTCPeerConnectionState::Failed
}

pub fn weak_peer_closed(conn: &Weak<RTCPeerConnection>) -> bool {
    let mut result = false;
    if let Some(pc3) = conn.upgrade() {
        if peer_closed(&pc3) {
            result = true;
        }
    } else {
        result = true
    }
    result
}

// #[derive(Clone, Serialize)]
// pub struct RtcPeeker {
//     pub id: u32,
//     #[serde(skip_serializing)]
//     pub vsdr: broadcast::Sender<Packet>,
//     #[serde(skip_serializing)]
//     pub asdr: broadcast::Sender<Packet>,
// }

// impl RtcPeeker {
//     pub fn new(id: u32, vsdr: broadcast::Sender<Packet>, asdr: broadcast::Sender<Packet>) -> Self {
//         Self { id, vsdr, asdr }
//     }
// }

#[derive(Serialize, Clone, Default)]
pub struct RtcSocketPool {
    pub using: HashMap<String, Vec<u32>>,
    pub free: Vec<u32>,
    idx: u64,
}

impl RtcSocketPool {
    pub fn new() -> Self {
        Self {
            using: HashMap::new(),
            free: EPHEMERAL_PORTS.clone(),
            idx: 0,
        }
    }

    pub fn malloc_sockets(&mut self, nb: usize, host: &str) -> Result<Vec<u32>, VCError> {
        if self
            .using
            .contains_key(&format!("{host}:{}", std::cmp::min(self.idx as i32 - 1, 0)))
        {
            return Err(vcerr!("dup host {host}"));
        }
        self.free.sort();
        let mut result = vec![];
        for _ in 0..(std::cmp::min(self.free.len(), nb)) {
            let s = self.free.remove(0);
            result.push(s);
        }
        if result.len() > 0 {
            self.using
                .insert(format!("{host}:{}", self.idx), result.clone());
            self.idx += 1;
        }
        Ok(result)
    }

    pub fn release_sockets(&mut self, mut sockets: Vec<u32>) {
        self.using
            .retain(|_, v| v.iter().find(|s| sockets.contains(*s)).is_none());
        self.free.append(&mut sockets);
        self.free.dedup();
        self.free.sort();
    }
}

#[derive(Serialize, Clone, Default)]
pub struct ConfSessions {
    pub hash: HashMap<u32, ConfSession>,
}

impl ConfSessions {
    pub fn init_arc() -> Arc<RwLock<Self>> {
        let me = Arc::new(RwLock::new(Self::default()));
        let me1 = me.clone();
        tokio::spawn(async move {
            let mut ticker: tokio::time::Interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                if let Ok(mut m1) = me1.try_write() {
                    m1.prune();
                }
                ticker.tick().await;
            }
        });
        me
    }
    pub fn create_session(
        &mut self,
        sep: &Option<String>,
        vcnf: &Option<StreamerConfigVideo>,
        acnf: &Option<StreamerConfigAudio>,
    ) -> ConfSession {
        let conf = ConfSession::new(
            Some(self.hash.keys().map(|v| *v).collect::<Vec<u32>>()),
            sep,
            vcnf,
            acnf,
        );
        let conf_id = conf.id;
        self.hash.insert(conf_id, conf.clone());
        conf
    }
    pub fn join_session(
        &mut self,
        target: u32,
        sep: &Option<String>,
        vcnf: &Option<StreamerConfigVideo>,
        acnf: &Option<StreamerConfigAudio>,
    ) -> Option<u32> {
        let Some(target_session) = self.hash.get_mut(&target) else {
            return None;
        };
        Some(target_session.join(sep, vcnf, acnf))
    }
    pub fn quit_session(&mut self, target: u32, id: u32) -> Result<Option<u32>> {
        let Some(target_session) = self.hash.get_mut(&target) else {
            return Err(anyhow!("No such session"));
        };
        let host_opt = target_session.quit(id)?;
        if host_opt.is_none() {
            self.hash.remove(&target);
        }
        Ok(host_opt)
    }
    pub fn get_member(&self, target: u32, id: u32) -> Option<ConfSessionMember> {
        let Some(target_session) = self.hash.get(&target) else {
            return None;
        };
        target_session.members.get(&id).map(|v| v.clone())
    }

    pub fn get_session(&self, target: u32, id: u32) -> Option<ConfSessionMember> {
        let Some(target_session) = self.hash.get(&target) else {
            return None;
        };
        target_session.members.get(&id).map(|v| v.clone())
    }
    pub async fn get_receiver(
        &mut self,
        target: u32,
        id: u32,
    ) -> Option<(
        futures::stream::SelectAll<BroadcastStream<Bytes>>,
        broadcast::Receiver<ConfSessionMemberEvt>,
        Bytes,
    )> {
        let Some(target_session) = self.hash.get_mut(&target) else {
            return None;
        };
        let receiver = target_session.get_receiver(id).await;
        Some(receiver)
    }
    // pub fn feed(&mut self, target: u32, id: u32, data: Bytes) -> Result<()> {
    //     let Some(target_session) = self.hash.get_mut(&target) else {
    //         return Err(anyhow!("No such session"));
    //     };
    //     let _ = target_session.feed(id, data)?;
    //     Ok(())
    // }
    pub fn prune(&mut self) {
        for (_, sessions) in &mut self.hash {
            sessions
                .members
                .retain(|_, v| !v.timeout() && (v.vcnf.is_some() || v.acnf.is_some()));
        }
        self.hash.retain(|_, v| !v.members.is_empty());
        for (_, sessions) in &mut self.hash {
            sessions.check_host();
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct ConfSession {
    pub id: u32,
    pub host: u32,
    pub members: HashMap<u32, ConfSessionMember>,
    // 用于增减member时对外公布广播的频道
    #[serde(skip_serializing)]
    // pub stream_broadcast: broadcast::Sender<usize>,
    pub stream_broadcast: broadcast::Sender<ConfSessionMemberEvt>,
}

impl ConfSession {
    pub fn new(
        ids: Option<Vec<u32>>,
        sep: &Option<String>,
        vcnf: &Option<StreamerConfigVideo>,
        acnf: &Option<StreamerConfigAudio>,
    ) -> Self {
        let id = loop {
            let id = rand::random::<u32>();
            if !ids.as_ref().map(|v| v.contains(&id)).unwrap_or(false) {
                break id;
            }
        };
        let member = ConfSessionMember::new(None, sep, vcnf, acnf);
        let host = member.id;
        let mut members = HashMap::new();
        members.insert(host, member);
        let (stream_broadcast, _) = broadcast::channel::<ConfSessionMemberEvt>(1024);
        Self {
            id,
            host,
            members,
            stream_broadcast,
        }
    }
    pub fn join(
        &mut self,
        sep: &Option<String>,
        vcnf: &Option<StreamerConfigVideo>,
        acnf: &Option<StreamerConfigAudio>,
    ) -> u32 {
        let ids = self.members.keys().map(|v| *v).collect::<Vec<u32>>();
        let member = ConfSessionMember::new(Some(ids), sep, vcnf, acnf);
        let id = member.id;
        let _ = self.stream_broadcast.send((&member).into());
        self.members.insert(id, member);
        id
    }
    pub fn quit(&mut self, id: u32) -> Result<Option<u32>> {
        if !self.members.contains_key(&id) {
            return Err(anyhow!("No such member"));
        }
        self.members.remove(&id);
        Ok(self.check_host())
    }

    // fn get_senders(&self) -> Vec<broadcast::Sender<Bytes>> {
    //     self.members
    //         .values()
    //         .map(|v| v.sender.clone())
    //         .collect::<Vec<broadcast::Sender<Bytes>>>()
    // }
    // pub fn feed(&mut self, id: u32, data: Bytes) -> Result<()> {
    //     let Some(target_member) = self.members.get_mut(&id) else {
    //         return Err(anyhow!("No such member"));
    //     };
    //     let _ = target_member.feed(data)?;
    //     Ok(())
    // }
    pub fn check_host(&mut self) -> Option<u32> {
        if self.members.is_empty() {
            return None;
        }
        if self.members.get(&self.host).is_none() {
            let Some(host_candidate) = self
                .members
                .iter()
                .map(|(k, v)| (*k, v.create_time.elapsed().as_millis()))
                .min_by(|a, b| b.1.cmp(&a.1))
                .map(|a| a.0)
            else {
                return None;
            };
            self.host = host_candidate;
        }
        Some(self.host)
    }
    pub async fn get_receiver(
        &mut self,
        member: u32,
    ) -> (
        futures::stream::SelectAll<BroadcastStream<Bytes>>,
        broadcast::Receiver<ConfSessionMemberEvt>,
        Bytes,
    ) {
        let mut all = futures::stream::SelectAll::new();
        let mut data = BytesMut::new();
        // 过滤掉自己，所有其他人
        for (_, v) in self.members.iter_mut().filter(|(k, _)| **k != member) {
            data.extend_from_slice(&v.get_cnf());
            all.push(BroadcastStream::new(v.sender.subscribe()));
        }
        (all, self.stream_broadcast.subscribe(), data.freeze())
    }
}

#[derive(Debug, Clone)]
pub struct ConfSessionMember {
    pub id: u32,
    pub sep: String,
    pub sender: broadcast::Sender<Bytes>,
    pub create_time: Instant,
    pub hb: Arc<RwLock<Instant>>,
    pub vcnf: Option<Bytes>,
    pub acnf: Option<Bytes>,
    // pub has_video: bool,
    // pub has_audio: bool,
    // pub vcnf_rcvr: watch::Receiver<Option<Option<Bytes>>>,
    // pub vcnf_sndr: watch::Sender<Option<Option<Bytes>>>,
    // pub acnf_rcvr: watch::Receiver<Option<Option<Bytes>>>,
    // pub acnf_sndr: watch::Sender<Option<Option<Bytes>>>,
}
impl ConfSessionMember {
    pub fn new(
        ids: Option<Vec<u32>>,
        sep: &Option<String>,
        vconfig: &Option<StreamerConfigVideo>,
        aconfig: &Option<StreamerConfigAudio>,
    ) -> Self {
        let id = loop {
            let id = rand::random::<u32>();
            if !ids.as_ref().map(|v| v.contains(&id)).unwrap_or(false) {
                break id;
            }
        };
        let (sender, _) = broadcast::channel(1024);
        let sep = sep.as_ref().unwrap_or(&TOKEN).to_owned();
        let create_time = Instant::now();
        let vcnf = vconfig
            .as_ref()
            .map(|v| v.bin(&sep, id).ok())
            .unwrap_or(None);
        let acnf = aconfig
            .as_ref()
            .map(|v| v.bin(&sep, id).ok())
            .unwrap_or(None);
        // let (vcnf_sndr, vcnf_rcvr) =
        //     watch::channel::<Option<Option<Bytes>>>(if has_video { Some(None) } else { None });
        // let (acnf_sndr, acnf_rcvr) =
        //     watch::channel::<Option<Option<Bytes>>>(if has_audio { Some(None) } else { None });
        Self {
            id,
            sep,
            sender,
            create_time,
            hb: Arc::new(RwLock::new(create_time)), // cache: BytesMut::new(),
            vcnf,
            acnf,
        }
    }
    // pub fn feed(&self, data: Bytes) -> Result<()> {
    //     let _ = self.sender.send(data);
    //     Ok(())
    // }
    pub fn timeout(&self) -> bool {
        self.hb
            .try_read()
            .map(|v| v.elapsed().as_secs() > 20)
            .unwrap_or(false)
    }
    pub fn get_cnf(&mut self) -> Bytes {
        let mut data = BytesMut::new();
        if let Some(vcnf_bytes) = &self.vcnf {
            data.extend_from_slice(vcnf_bytes);
        }
        if let Some(acnf_bytes) = &self.acnf {
            data.extend_from_slice(&acnf_bytes);
        }
        data.freeze()
    }
    pub fn depacketize(
        data: &Bytes,
        sep: &str,
        cache: &mut BytesMut,
        sndr: &broadcast::Sender<Bytes>,
    ) -> Result<()> {
        cache.extend_from_slice(data);
        if cache.len() < sep.len() + 4 + 4 {
            return Ok(());
        }
        let mut box_starts =
            memchr::memmem::find_iter(&cache, sep.as_bytes()).collect::<Vec<usize>>();
        if box_starts.len() <= 1 {
            return Ok(());
        }
        let last_idx = box_starts.pop().unwrap();
        let mut local_cache = cache.split_to(last_idx);
        let mut sending_boxes = vec![];
        while let Some(idx) = box_starts.pop() {
            // log::debug!(target:"debug", "box_starts_len_minus_one {idx} {box_starts:?} {}", local_cache.len());
            let box_data = local_cache.split_off(idx);
            let mut cursor = sep.len();
            let _ssrc = u32::from_be_bytes(box_data[cursor..cursor + 4].try_into()?);
            cursor += 4;
            let box_name = String::from_utf8(box_data[cursor..cursor + 4].to_vec())?;
            if box_name != "vcnf" && box_name != "acnf" && box_name != "afrm" && box_name != "vfrm"
            {
                continue;
            }
            sending_boxes.push(box_data.freeze());
        }
        // let box_starts_len_minus_one = box_starts.len() - 1;
        // let mut local_cache = cache.split_to(box_starts[box_starts_len_minus_one]);
        // let mut sending_boxes = vec![];
        // for i in 1..box_starts_len_minus_one {
        //     log::debug!(target:"debug", "box_starts_len_minus_one {} {box_starts:?} {}", box_starts_len_minus_one - i, local_cache.len());
        //     let box_data = local_cache.split_to(box_starts[box_starts_len_minus_one - i]);
        //     log::debug!(target:"debug", "done");
        //     let mut cursor = sep.len();
        //     let _ssrc = u32::from_be_bytes(box_data[cursor..cursor + 4].try_into()?);
        //     cursor += 4;
        //     let box_name = String::from_utf8(box_data[cursor..cursor + 4].to_vec())?;
        //     if box_name != "vcnf" && box_name != "acnf" && box_name != "afrm" && box_name != "vfrm"
        //     {
        //         continue;
        //     }
        //     sending_boxes.push(box_data.freeze());
        //     // cursor += 4;
        //     // let payload_len = u32::from_be_bytes(box_data[cursor..cursor + 4].try_into()?);
        //     // cursor += 4;
        //     // let payload = Bytes::from(box_data[cursor..cursor + payload_len as usize].to_vec());
        //     // 非阻塞尝试，如果原子锁因为各种原因无法写，则fallback到本地变量
        //     // if box_name == "vcnf" {
        //     //     let _ = vcnf_sndr.send(Some(Some(payload)));
        //     // } else if box_name == "acnf" {
        //     //     let _ = acnf_sndr.send(Some(Some(payload)));
        //     // }
        // }

        while let Some(b) = sending_boxes.pop() {
            let _ = sndr.send(b);
        }
        Ok(())
    }
    // pub fn should_sdr_config(sndr: &watch::Sender<Option<Option<Bytes>>>) -> bool {
    //     let cnf = sndr.borrow();
    //     cnf.as_ref().map(|v| v.is_none()).unwrap_or(false)
    // }
    // pub fn probe(
    //     data: &Bytes,
    //     sep: &str,
    //     cache: &mut BytesMut,
    //     vcnf_sndr: &watch::Sender<Option<Option<Bytes>>>,
    //     acnf_sndr: &watch::Sender<Option<Option<Bytes>>>,
    //     configured: &mut bool,
    //     started: Instant,
    // ) -> Result<()> {
    //     if *configured {
    //         return Ok(());
    //     }
    //     if !Self::should_sdr_config(vcnf_sndr) && !Self::should_sdr_config(acnf_sndr) {
    //         *configured = true;
    //         return Ok(());
    //     }
    //     if started.elapsed().as_secs() > 10 {
    //         return Ok(());
    //     }
    //     cache.extend_from_slice(data);
    //     if cache.len() < sep.len() + 4 + 4 {
    //         return Ok(());
    //     }
    //     let box_starts = memchr::memmem::find_iter(&cache, sep.as_bytes()).collect::<Vec<usize>>();
    //     if box_starts.len() <= 1 {
    //         return Ok(());
    //     }
    //     let last_idx = box_starts.len() - 1;
    //     let mut local_cache = cache.split_to(box_starts[last_idx]);
    //     for i in 0..box_starts.len() - 1 {
    //         let box_data = local_cache.split_to(box_starts[last_idx - i]);
    //         let mut cursor = sep.len();
    //         let _ssrc = u32::from_be_bytes(box_data[cursor..cursor + 4].try_into()?);
    //         cursor += 4;
    //         let box_name = String::from_utf8(box_data[cursor..cursor + 4].to_vec())?;
    //         if box_name != "vcnf" && box_name != "acnf" {
    //             continue;
    //         }
    //         cursor += 4;
    //         let payload_len = u32::from_be_bytes(box_data[cursor..cursor + 4].try_into()?);
    //         cursor += 4;
    //         let payload = Bytes::from(box_data[cursor..cursor + payload_len as usize].to_vec());
    //         // 非阻塞尝试，如果原子锁因为各种原因无法写，则fallback到本地变量
    //         if box_name == "vcnf" {
    //             let _ = vcnf_sndr.send(Some(Some(payload)));
    //         } else if box_name == "acnf" {
    //             let _ = acnf_sndr.send(Some(Some(payload)));
    //         }
    //     }

    //     Ok(())
    // }
}

impl Serialize for ConfSessionMember {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("ConfSessionMember", 8)?;
        s.serialize_field("id", &self.id)?;
        s.serialize_field("sep", &self.sep)?;
        s.serialize_field("create_time", &self.create_time.elapsed().as_secs())?;
        s.serialize_field(
            "hb",
            &self.hb.try_read().map(|v| v.elapsed().as_secs()).ok(),
        )?;
        s.serialize_field("vcnf", &self.vcnf.as_ref().map(|v| base64::encode(v)))?;
        s.serialize_field("acnf", &self.acnf.as_ref().map(|v| base64::encode(v)))?;
        s.end()
    }
}

impl PartialEq for ConfSessionMember {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
    fn ne(&self, other: &Self) -> bool {
        self.id.ne(&other.id)
    }
}
#[derive(Debug, Clone)]
pub struct ConfSessionMemberEvt {
    pub sender: broadcast::Sender<Bytes>,
    pub cnf: Bytes,
}

impl From<&ConfSessionMember> for ConfSessionMemberEvt {
    fn from(member: &ConfSessionMember) -> Self {
        let mut cnf = BytesMut::new();
        if let Some(vcnf) = &member.vcnf {
            cnf.extend_from_slice(vcnf);
        }
        if let Some(acnf) = &member.acnf {
            cnf.extend_from_slice(acnf);
        }
        Self {
            sender: member.sender.clone(),
            cnf: cnf.freeze(),
        }
    }
}

pub fn md5(data: &str) -> String {
    let mut hash = md5::Md5::new();
    hash.input_str(data);
    hash.result_str()
}

pub fn cleaner() {
    if *DEBUG_MODE {
        if let Ok(results) = std::fs::read_dir(&*LOG_PATH) {
            for entry in results {
                if let Ok(de) = entry {
                    let _ = vclog!(std::fs::remove_file(de.path()));
                }
            }
        }
        if let Ok(results) = std::fs::read_dir(&*STORAGE_PATH) {
            for entry in results {
                if let Ok(de) = entry {
                    let _ = vclog!(std::fs::remove_file(de.path()));
                }
            }
        }
    }
    tokio::spawn(async move {
        log::info!("clearing up log/storage folder");
        log::debug!(target:"debug","clearing up log/storage folder");
        loop {
            if let Ok(results) = std::fs::read_dir(&*LOG_PATH) {
                for entry in results {
                    if let Ok(de) = entry {
                        if let Ok(meta) = de.metadata() {
                            let mut can_delete = false;
                            if let Ok(modified) = meta.modified() {
                                if let Ok(elisped) = modified.elapsed() {
                                    // 默认保留一周
                                    if elisped.as_secs() > 7 * 24 * 3600 {
                                        can_delete = true;
                                    }
                                }
                            }
                            if can_delete {
                                if meta.file_type().is_file() {
                                    let _ = std::fs::remove_file(de.path());
                                } else if meta.file_type().is_dir() {
                                    let _ = std::fs::remove_dir_all(de.path());
                                }
                            }
                        }
                    }
                }
            }
            if let Ok(results) = std::fs::read_dir(&*STORAGE_PATH) {
                for entry in results {
                    if let Ok(de) = entry {
                        if let Ok(meta) = de.metadata() {
                            let mut can_delete = false;
                            if let Ok(modified) = meta.modified() {
                                if let Ok(elisped) = modified.elapsed() {
                                    // 默认保留一周
                                    if elisped.as_secs() > 7 * 24 * 3600 {
                                        can_delete = true;
                                    }
                                }
                            }
                            if can_delete {
                                if meta.file_type().is_file() {
                                    let _ = std::fs::remove_file(de.path());
                                } else if meta.file_type().is_dir() {
                                    let _ = std::fs::remove_dir_all(de.path());
                                }
                            }
                        }
                    }
                }
            }
            // 默认1天检查一次
            tokio::time::sleep(Duration::from_secs(24 * 3600)).await;
        }
    });
}

#[test]
fn test_unit() {
    use std::io::{Read, Write};
    let mut data = vec![];
    let mut f = std::fs::File::open("/data/workspace/boe/webtalk/test.264").unwrap();
    let _ = f.read_to_end(&mut data);

    let mut f1 = std::fs::File::create("/data/workspace/boe/webtalk/test2.264").unwrap();
    let _ = f1.write_all(&data[..48945]);
}
#[test]
fn test_vp9() {
    use std::fs::File;
    use std::io::Read;
    let mut file = File::open("./test.vp9").expect("Cannot open file");
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).expect("Cannot read file");

    println!("File size: {} bytes", buffer.len());

    // Look for IVF headers and frame markers
    // IVF format: 32 byte header, then alternating 4 byte size + 8 byte timestamp + frame data

    // Check for scalability indicators in VP9 superframe
    let mut frame_count = 0;
    let mut pos = 0;

    while pos < buffer.len() {
        // Look for VP9 frame headers
        if pos + 10 < buffer.len() && buffer[pos] == 0x85 && buffer[pos + 1] == 0x49 {
            println!("Found VP9 frame signature at position {}", pos);

            // Extract frame type and scalability info
            let frame_marker = (buffer[pos + 2] >> 3) & 0x03;
            let show_frame = (buffer[pos + 2] >> 4) & 0x01;
            let error_resilient = (buffer[pos + 2] >> 5) & 0x01;

            println!(
                "Frame {}: marker={}, show_frame={}, error_resilient={}",
                frame_count, frame_marker, show_frame, error_resilient
            );

            frame_count += 1;
        }
        pos += 1;
    }

    println!("Analyzed {} potential VP9 frames", frame_count);
}
