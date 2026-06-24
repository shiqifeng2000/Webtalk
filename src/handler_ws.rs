use crate::{
    errors::VCError,
    tokio_read_lock, tokio_write_lock,
    utils::{P2PSession, P2PSessions, DEBUG_MODE},
};
use actix::{prelude::*, StreamHandler};
use actix_web::{web, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use log::{debug, error as error_log, info};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SdpEvent {
    pub sid: Option<u32>,
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_: String,
    pub sdp: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CandidateEvent {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_: String,
    pub candidate: Option<RTCIceCandidateInit>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TalkstopEvent {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_: String,
    pub sid: u32,
}

impl PartialEq for CandidateEvent {
    fn eq(&self, other: &CandidateEvent) -> bool {
        self.type_ == other.type_ && self.candidate == other.candidate
    }
}

impl Eq for CandidateEvent {}

#[derive(Serialize, Deserialize, Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct AnswerMessage {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_: String,
    pub sdp: String,
}

impl AnswerMessage {
    pub fn new(sdp: &str) -> Self {
        Self {
            type_: "answer".to_owned(),
            sdp: sdp.to_owned(),
        }
    }
}

#[derive(Serialize, Deserialize, Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct SessionMessage {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_: String,
    pub id: u32,
}

impl SessionMessage {
    pub fn new(id: u32) -> Self {
        Self {
            type_: "init".to_owned(),
            id,
        }
    }
}

#[derive(Serialize, Deserialize, Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct CandidateMessage {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_: String,
    pub candidate: Option<RTCIceCandidateInit>,
}

impl CandidateMessage {
    pub fn new(candidate: Option<RTCIceCandidateInit>) -> Self {
        Self {
            type_: "iceCandidate".to_owned(),
            candidate,
        }
    }
}
impl From<CandidateEvent> for CandidateMessage {
    fn from(evt: CandidateEvent) -> Self {
        CandidateMessage::new(evt.candidate)
    }
}

#[derive(Serialize, Message, Clone)]
#[rtype(result = "()")]
pub struct BroadcastMessage {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_: String,
    pub sessions: HashMap<u32, P2PSession>,
}

impl BroadcastMessage {
    pub fn new(sessions: &HashMap<u32, P2PSession>) -> Self {
        Self {
            type_: "sessions".to_owned(),
            sessions: sessions.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct TalkstopMessage {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_: String,
    pub sid: u32,
    pub message: Option<String>,
}

impl TalkstopMessage {
    pub fn new(sid: u32, message: Option<String>) -> Self {
        Self {
            type_: "talkstop".to_owned(),
            sid,
            message,
        }
    }
}
impl From<TalkstopEvent> for TalkstopMessage {
    fn from(evt: TalkstopEvent) -> Self {
        TalkstopMessage::new(evt.sid, None)
    }
}

pub async fn ws(
    req: HttpRequest,
    stream: web::Payload,
    sessions: web::Data<RwLock<P2PSessions>>,
) -> Result<HttpResponse, VCError> {
    let ip = req
        .peer_addr()
        .as_ref()
        .map(|v| v.ip().to_string())
        .unwrap_or("".to_owned());
    let session = WsSession::new(&ip, sessions.into_inner());
    let (_addr, mut res) = ws::WsResponseBuilder::new(session, &req, stream).start_with_addr()?;
    let _headers = res.head_mut().headers_mut();
    Ok(res)
}

pub struct WsSession {
    pub ip: String,
    pub p2p_session: Option<u32>,
    pub p2p_role: Option<u32>,
    pub p2p_candidates: Vec<CandidateEvent>,
    pub sessions: Arc<RwLock<P2PSessions>>,
}

impl WsSession {
    fn new(ip: &str, sessions: Arc<RwLock<P2PSessions>>) -> Self {
        Self {
            ip: ip.to_owned(),
            p2p_session: None,
            p2p_role: None,
            p2p_candidates: vec![],
            sessions,
        }
    }

    pub fn close(&self, reason: &str, ctx: &mut ws::WebsocketContext<WsSession>) {
        ctx.close(Some(ws::CloseReason {
            code: ws::CloseCode::Normal,
            description: Some(reason.to_owned()),
        }));
        ctx.stop();
    }

    pub fn handle_text(&mut self, text_str: String, ctx: &mut ws::WebsocketContext<WsSession>) {
        // debug!(target:"debug", "in text {} self.connection.is_none() {} ", text_str, self.connection.is_none());
        let addr = ctx.address();
        if let Ok(msg) = serde_json::from_str::<SdpEvent>(&text_str) {
            if msg.type_ == "offer" {
                if *DEBUG_MODE {
                    debug!("offer evt from {}", self.ip,);
                }
                let sessions = self.sessions.clone();
                let result = futures::executor::block_on(async move {
                    match tokio_write_lock!(sessions, 10) {
                        Ok(mut s) => {
                            let id = s.create_session(&msg.sdp, &addr);
                            info!("session {id} offered");
                            Ok(id)
                        }
                        Err(e) => Err(e),
                    }
                });
                // 原子性，先注册后替换
                if let Ok(id) = result {
                    self.p2p_role.replace(0);
                    self.p2p_session.replace(id);
                    if let Ok(data) = serde_json::to_string(&SessionMessage::new(id)) {
                        info!("sending init sid to {}", self.ip);
                        ctx.text(data);
                    }
                }
            } else {
                if *DEBUG_MODE {
                    debug!("answer evt from {}", self.ip,);
                }
                let sessions = self.sessions.clone();
                let answer_sdp = msg.sdp.clone();
                if let Some(sid) = msg.sid {
                    let result = futures::executor::block_on(async move {
                        match tokio_write_lock!(sessions, 10) {
                            Ok(mut s) => {
                                let result = s.add_answer(sid, &answer_sdp, &addr);
                                info!("session {sid} offered");
                                result.ok_or(vcerr!("no such sid"))
                            }
                            Err(e) => Err(e),
                        }
                    });
                    if let Ok(offer_addr) = result {
                        self.p2p_role.replace(1);
                        self.p2p_session.replace(sid);
                        offer_addr.do_send(AnswerMessage::new(&msg.sdp));
                        if let Ok(data) = serde_json::to_string(&SessionMessage::new(sid)) {
                            info!("sending answer sid to {}", self.ip);
                            ctx.text(data);
                        }
                    }
                }
            }
        } else if let Ok(evt) = serde_json::from_str::<TalkstopEvent>(&text_str) {
            if *DEBUG_MODE {
                debug!("talkstop evt from {}", self.ip);
            }
            if self.p2p_role.is_none() || self.p2p_session.is_none() {
                return;
            }
            let role = self.p2p_role.unwrap();
            let sid = self.p2p_session.unwrap();
            let is_offer = role == 0;
            let sessions = self.sessions.clone();
            let result: Result<(), VCError> = futures::executor::block_on(async move {
                let s = tokio_read_lock!(sessions, 10)?;
                let target_addr = s
                    .get_target_addr(sid, is_offer)
                    .ok_or(vcerr!("not answer yet"))?;
                target_addr.do_send::<TalkstopMessage>(evt.into());
                // s.drop_session(sid);
                Ok(())
            });
            if result.is_ok() {
                self.p2p_session.take();
                self.p2p_role.take();
                self.p2p_candidates.clear();
            }
        } else if let Ok(evt) = serde_json::from_str::<CandidateEvent>(&text_str) {
            if *DEBUG_MODE {
                debug!("candidate evt from {}", self.ip);
            }
            self.p2p_candidates.push(evt);
            // 如果内部sdp还没就绪，则临时存起来
            if self.p2p_role.is_none() || self.p2p_session.is_none() {
                return;
            }
            let role = self.p2p_role.unwrap();
            let sid = self.p2p_session.unwrap();
            let candidates = self.p2p_candidates.clone();
            let sessions = self.sessions.clone();
            let result = futures::executor::block_on(async move {
                let mut s = tokio_write_lock!(sessions, 10)?;
                let is_offer = role == 0;
                let target_addr = s
                    .get_target_addr(sid, is_offer)
                    .ok_or(vcerr!("not answer yet"))?;
                let registered_candidates = s.get_candidates(sid, is_offer);
                let diff_candidates = candidates
                    .iter()
                    .filter(|v| !registered_candidates.contains(*v))
                    .map(|v| v.clone())
                    .collect::<Vec<CandidateEvent>>();
                if diff_candidates.len() > 0 {
                    // let target_addr = s
                    //     .add_candidates(sid, diff_candidates, is_offer)
                    //     .ok_or(vcerr!("no answer yet"))?;
                    for c in &diff_candidates {
                        target_addr.do_send::<CandidateMessage>(c.clone().into());
                    }
                    s.add_candidates(sid, diff_candidates, is_offer);
                    return Ok(());
                }
                Err(vcerr!("no new candidates"))
                // match tokio_write_lock!(sessions, 10) {
                //     Ok(mut s) => {}
                //     Err(e) => Err(e),
                // }
            });
            // let candidates = if self.p2p_candidates.len() > 0 {
            //     self.p2p_candidates.push(evt);
            //     self.p2p_candidates.split_off(0)
            // } else {
            //     vec![evt]
            // };

            if let Ok(_) = result {
                self.p2p_candidates.clear();
                // for item in candidates {
                //     target_addr.do_send::<CandidateMessage>(item.clone().into());
                // }
            }
        }
    }
}

impl Actor for WsSession {
    // type Context = Context<Self>;
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        info!("Starting websocket streaming thread for ip {} ", &self.ip,);
        ctx.run_interval(
            Duration::from_millis(1000),
            move |act, ctx: &mut ws::WebsocketContext<WsSession>| {
                if let Ok(s) = act.sessions.try_read() {
                    let msg = BroadcastMessage::new(&s.hash);
                    if let Ok(data) = serde_json::to_string(&msg) {
                        ctx.text(data);
                    }
                }
                let c_len = act.p2p_candidates.len();
                if act.p2p_role.is_none() || act.p2p_session.is_none() {
                    return;
                }
                let role = act.p2p_role.unwrap();
                let sid = act.p2p_session.unwrap();
                let is_offer = role == 0;

                if let Ok(mut s) = act.sessions.try_write() {
                    if !s.session_exist(sid) {
                        act.p2p_candidates.clear();
                        act.p2p_role.take();
                        act.p2p_session.take();
                        return;
                    }
                    s.hb(sid, is_offer);
                    if c_len == 0 {
                        return;
                    }
                    let target_addr_opt = s.get_target_addr(sid, is_offer);
                    if target_addr_opt.is_none() {
                        return;
                    }
                    let target_addr = target_addr_opt.unwrap();
                    let registered = s.get_candidates(sid, is_offer);
                    let candidates = act.p2p_candidates.split_off(0);
                    let diff_candidates = candidates
                        .iter()
                        .filter(|v| !registered.contains(*v))
                        .map(|v| v.clone())
                        .collect::<Vec<CandidateEvent>>();
                    if diff_candidates.len() > 0 {
                        for item in &diff_candidates {
                            target_addr.do_send::<CandidateMessage>(item.clone().into());
                        }
                        s.add_candidates(sid, diff_candidates, is_offer);
                    }
                }
            },
        );
    }
    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        info!("Client ip: {}, is now dropping", self.ip,);
        if let Some(sid) = self.p2p_session {
            if let Ok(mut s) = self.sessions.try_write() {
                s.drop_session(sid);
            }
        }
        Running::Stop
    }
    fn stopped(&mut self, _ctx: &mut Self::Context) {}
}

impl Handler<AnswerMessage> for WsSession {
    type Result = ();
    fn handle(&mut self, msg: AnswerMessage, ctx: &mut Self::Context) -> Self::Result {
        if let Ok(data) = serde_json::to_string(&msg) {
            info!("sending answer to {} role {:?}", self.ip, self.p2p_role);
            ctx.text(data);
        }
    }
}
impl Handler<CandidateMessage> for WsSession {
    type Result = ();
    fn handle(&mut self, msg: CandidateMessage, ctx: &mut Self::Context) -> Self::Result {
        if let Ok(data) = serde_json::to_string(&msg) {
            info!(
                "sending candidate {data} to {} role {:?}",
                self.ip, self.p2p_role
            );
            ctx.text(data);
        }
    }
}
impl Handler<BroadcastMessage> for WsSession {
    type Result = ();
    fn handle(&mut self, msg: BroadcastMessage, ctx: &mut Self::Context) -> Self::Result {
        if let Ok(data) = serde_json::to_string(&msg) {
            ctx.text(data);
        }
    }
}
impl Handler<TalkstopMessage> for WsSession {
    type Result = ();
    fn handle(&mut self, mut msg: TalkstopMessage, ctx: &mut Self::Context) -> Self::Result {
        if self.p2p_role.is_none() || self.p2p_session.is_none() {
            return;
        }
        let sid = self.p2p_session.unwrap();
        let sessions = self.sessions.clone();
        let result: Result<(), VCError> = futures::executor::block_on(async move {
            let mut s = tokio_write_lock!(sessions, 10)?;
            s.drop_session(sid);
            Ok(())
        });
        if result.is_ok() {
            self.p2p_session.take();
            self.p2p_role.take();
            self.p2p_candidates.clear();
        }
        msg.message.replace("remote stop".to_owned());
        if let Ok(data) = serde_json::to_string(&msg) {
            ctx.text(data);
        }
    }
}
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        let msg = match msg {
            Err(_) => {
                error_log!("Client ip: {} is sending a unknown message", self.ip,);
                ctx.stop();
                return;
            }
            Ok(msg) => msg,
        };
        match msg {
            ws::Message::Ping(msg) => {
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {}
            ws::Message::Text(text) => self.handle_text(text.into(), ctx),
            ws::Message::Binary(_bin) => {}
            ws::Message::Close(reason) => {
                ctx.close(reason);
                ctx.stop();
            }
            ws::Message::Continuation(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }
}

#[test]
pub fn test_candidate_parse() {
    let str = "{\"type\":\"candidate\",\"candidate\":\"candidate:1301188454 1 udp 2122260223 172.18.0.1 45545 typ host generation 0 ufrag K+SM network-id 1\",\"sdpMid\":\"0\",\"sdpMLineIndex\":0}";
    let a = serde_json::from_str::<CandidateEvent>(str);
    println!("parse result {:?}", a);
}
