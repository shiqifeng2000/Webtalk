use crate::utils::{ConfSessionMember, ConfSessionMemberEvt, ConfSessions};
use crate::{errors::VCError, tokio_read_lock, tokio_write_lock};
use actix_web::delete;
use actix_web::http::header::ContentType;
use actix_web::{get, post, put, web, HttpRequest, HttpResponse, Result as ActixResult};
use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use log::info;
use serde_json::json;
use std::time::{Duration, Instant};
use std::{u32, u8};
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream, UnboundedReceiverStream};
use tokio_stream::StreamExt;

#[post("/http_conf/create")]
pub async fn create(
    config: web::Json<StreamerConfig>,
    sessions: web::Data<RwLock<ConfSessions>>,
    _req: HttpRequest,
) -> ActixResult<HttpResponse, VCError> {
    let vcnf = &config.video;
    let acnf = &config.audio;
    let sep = &config.sep;
    let session = {
        let mut sessions = tokio_write_lock!(sessions, 10)?;
        sessions.create_session(sep, vcnf, acnf)
    };
    Ok(HttpResponse::Ok().json(json!({"success": true, "session": session})))
}

#[post("/http_conf/join/{target}")]
pub async fn join(
    target: web::Path<u32>,
    config: web::Json<StreamerConfig>,
    sessions: web::Data<RwLock<ConfSessions>>,
    _req: HttpRequest,
) -> ActixResult<HttpResponse, VCError> {
    let vcnf = &config.video;
    let acnf = &config.audio;
    let sep = &config.sep;
    let mid = {
        let mut sessions = tokio_write_lock!(sessions, 10)?;
        sessions.join_session(target.into_inner(), sep, vcnf, acnf)
    };
    Ok(HttpResponse::Ok().json(json!({"success": true, "mid": mid})))
}

#[delete("/http_conf/quit/{sid}/{mid}")]
pub async fn quit(
    param: web::Path<(u32, u32)>,
    sessions: web::Data<RwLock<ConfSessions>>,
    _req: HttpRequest,
) -> ActixResult<HttpResponse, VCError> {
    let (sid, mid) = param.into_inner();
    let host = {
        let mut sessions = tokio_write_lock!(sessions, 10)?;
        sessions
            .quit_session(sid, mid)
            .map_err(|e| vcerr!("{e:?}"))?
    };
    Ok(HttpResponse::Ok().json(json!({"success": true, "host": host})))
}

#[post("/http_conf/upstream/{sid}/{mid}")]
pub async fn upstream(
    param: web::Path<(u32, u32)>,
    mut body: web::Payload,
    sessions: web::Data<RwLock<ConfSessions>>,
    _req: HttpRequest,
) -> ActixResult<HttpResponse, VCError> {
    let (sid, mid) = param.into_inner();
    let ConfSessionMember {
        sender: media_sender,
        hb: remote_hb,
        sep,
        ..
    } = {
        let sessions = tokio_read_lock!(sessions, 10)?;
        sessions
            .get_member(sid, mid)
            .ok_or(vcerr!("no such session"))?
    };
    let weak_media_sender = media_sender.downgrade();
    let mut cache: BytesMut = BytesMut::new();
    drop(media_sender);
    // 本地心跳，每10秒更新一次原子心跳
    let mut local_hb = Instant::now();
    // 分割符
    let reason = loop {
        let timeout = tokio::time::sleep(Duration::from_secs(4));
        tokio::pin!(timeout);
        tokio::select! {
            _ = timeout.as_mut() => {break "timeout".to_owned()}
            m = body.next() => {
                if let Some(Ok(b)) = m {
                    let Some(msender) = weak_media_sender.upgrade() else {
                        break "quit".to_owned();
                    };
                    let _ = vclog!(ConfSessionMember::depacketize(
                        &b,
                        &sep,
                        &mut cache,
                        &msender,
                    ));
                    // let _ = msender.send(b);
                    // 10设置超时，用于定时清理，非阻塞
                    if local_hb.elapsed().as_secs() > 1 {
                        if let Ok(mut remote_hb) = remote_hb.try_write() {
                            local_hb = Instant::now();
                            *remote_hb = local_hb;
                        }
                    }
                } else {
                    break "eof".to_owned();
                }
            }
        }
    };
    // // 嗅探过程，用于更新视频音频配置
    // if !configured {
    //     let _ = vclog!(ConfSessionMember::probe(
    //         &b,
    //         sep,
    //         &mut cache,
    //         &vcnf_sndr,
    //         &acnf_sndr,
    //         &mut configured,
    //         session_start
    //     ));
    // }
    info!("[PUp->P] closing streamer {sid}-{mid} from {reason}");
    Ok(HttpResponse::Ok().finish())
}

#[get("/http_conf/downstream/{sid}/{mid}")]
pub async fn downstream(
    param: web::Path<(u32, u32)>,
    sessions: web::Data<RwLock<ConfSessions>>,
    _req: HttpRequest,
) -> ActixResult<HttpResponse, VCError> {
    let (sid, mid) = param.into_inner();
    let (sndr, rcvr) = mpsc::channel::<Result<Bytes>>(1024 * 1024);
    let (mut streamer, mut listener, weak_sender, sep, cnfs) = {
        let mut sessions = tokio_write_lock!(sessions, 10)?;
        let member = sessions
            .get_member(sid, mid)
            .ok_or(vcerr!("no stream founded"))?;
        let weak_sender = member.sender.downgrade();
        let sep = member.sep.to_owned();
        let (streamer, listener, cnfs) = sessions
            .get_receiver(sid, mid)
            .await
            .ok_or(vcerr!("no stream founded"))?;
        (streamer, listener, weak_sender, sep, cnfs)
    };
    tokio::spawn(async move {
        let _ = sndr.send(Ok(cnfs)).await;
        let reason = 'outer: loop {
            if weak_sender.upgrade().is_none() {
                break 'outer "quit".to_owned();
            }
            if streamer.is_empty() {
                let timeout = tokio::time::sleep(Duration::from_secs(1));
                tokio::pin!(timeout);
                tokio::select! {
                    _ = timeout.as_mut() => {
                        log::debug!(target:"debug","sending {sid}-{mid} dummy");
                        let _ = sndr.send(Ok(dummy(&sep, mid))).await;
                        continue 'outer;
                    }
                    s = listener.recv() => {
                        if let Ok(ConfSessionMemberEvt{sender: member_media_sndr, cnf}) = s {
                            streamer.push(BroadcastStream::new(member_media_sndr.subscribe()));
                            let _ = sndr.send(Ok(cnf)).await;
                        }
                    }
                }
            }
            'inner: loop {
                if weak_sender.upgrade().is_none() {
                    break 'outer "quit".to_owned();
                }
                let timeout = tokio::time::sleep(Duration::from_secs(4));
                tokio::pin!(timeout);
                tokio::select! {
                    _ = timeout.as_mut() => {break 'outer "timeout".to_owned()}
                    s = listener.recv() => {
                        if let Ok(ConfSessionMemberEvt{sender: member_media_sndr, cnf}) = s {
                            streamer.push(BroadcastStream::new(member_media_sndr.subscribe()));
                            let _ = sndr.send(Ok(cnf)).await;
                        }
                    }
                    m = streamer.next() => {
                        match m {
                            Some(Ok(b))=>{
                                // log::debug!(target:"debug","sending {sid}-{mid}");
                                let _ = sndr.send(Ok(b)).await;
                            }
                            Some(Err(e))=>{
                                log::warn!("down streamer {sid}-{mid}: {e:?}");
                            }
                            None=> {
                                break 'inner;
                            }
                        }
                    }
                }
            }
        };
        info!("[P->PDown] closing streamer {sid}-{mid} from {reason}");
    });
    let stream = ReceiverStream::new(rcvr);
    let mut builder = HttpResponse::Ok();
    builder.insert_header(("Cache-Control", "no-store"));
    builder.insert_header(("Connection", "keep-alive"));
    Ok(builder
        .content_type(ContentType::octet_stream())
        .streaming(stream))
}

// #[post("/http_conf/create")]
// pub async fn http_conf(
//     config: web::Query<StreamerConfig>,
//     target: web::Path<u32>,
//     sessions: web::Data<RwLock<ConfSessions>>,
//     req: HttpRequest,
// ) -> ActixResult<HttpResponse, VCError> {
// }

#[derive(Debug, Clone, Deserialize, Default)]
struct StreamerConfig {
    sep: Option<String>,
    video: Option<StreamerConfigVideo>,
    audio: Option<StreamerConfigAudio>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StreamerConfigVideo {
    width: u16,
    height: u16,
    codec: String,
}
impl StreamerConfigVideo {
    pub fn bin(&self, sep: &str, ssrc: u32) -> Result<Bytes> {
        let payload_len = 2 + 2 + self.codec.len();
        if payload_len > u32::MAX as usize {
            return Err(anyhow!("vcodec too long"));
        }
        let boxname = b"vcnf";
        let mut data = BytesMut::with_capacity(sep.len() + 4 + boxname.len() + 4 + payload_len);
        data.extend_from_slice(sep.as_bytes());
        data.put_u32(ssrc);
        data.extend_from_slice(boxname);
        data.put_u32(payload_len as u32);
        data.put_u16(self.width);
        data.put_u16(self.height);
        data.extend_from_slice(self.codec.as_bytes());
        Ok(data.freeze())
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StreamerConfigAudio {
    samplerate: u16,
    channels: u8,
    codec: String,
}
impl StreamerConfigAudio {
    pub fn bin(&self, sep: &str, ssrc: u32) -> Result<Bytes> {
        let payload_len = 2 + 1 + self.codec.len();
        if payload_len > u32::MAX as usize {
            return Err(anyhow!("acodec too long"));
        }
        let boxname = b"acnf";
        let mut data = BytesMut::with_capacity(sep.len() + 4 + boxname.len() + 4 + payload_len);
        data.extend_from_slice(sep.as_bytes());
        data.put_u32(ssrc);
        data.extend_from_slice(boxname);
        data.put_u32(payload_len as u32);
        data.put_u16(self.samplerate);
        data.put_u8(self.channels);
        data.extend_from_slice(self.codec.as_bytes());
        Ok(data.freeze())
    }
}

pub fn dummy(sep: &str, ssrc: u32) -> Bytes {
    let mut data = BytesMut::with_capacity(sep.len() + 4 + 4);
    data.extend_from_slice(sep.as_bytes());
    data.put_u32(ssrc);
    data.extend_from_slice(b"dumy");
    data.freeze()
}
// #[test]
// fn test_base64() {
//     let mut a = base64::decode("d2VidGFsa2gdkr1hY25mAAAAB7uAAm9wdXM=").unwrap();
//     let ssrc = u32::from_be_bytes(a[7..11].try_into().unwrap());
//     let boxname = String::from_utf8(a[11..15].to_vec()).unwrap();
//     let payload_len = u32::from_be_bytes(a[15..19].try_into().unwrap());
//     let samplerate = u16::from_be_bytes(a[19..21].try_into().unwrap());
//     let channels = a[21];
//     println!("{a:?} ssrc:{ssrc} boxname:{boxname} payload_len:{payload_len} samplerate:{samplerate} channels:{channels}",);
//     let mut f = std::fs::File::create("./temp").unwrap();
//     f.write_all(&mut a);
// }
