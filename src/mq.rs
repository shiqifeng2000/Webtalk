use crate::errors::VCError;
use crate::utils::{KAFKA_TOPIC_LISTENER, KAFKA_TOPIC_STREAM};
use actix::{prelude::*, Addr};
// use kafka::producer::{Producer, Record, RequiredAcks};
use log::debug;
// use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Message, Serialize, Debug, Clone)]
#[rtype(result = "()")]
pub struct StreamUpEvent {
    pub token: u32,
}

impl StreamUpEvent {
    pub fn new(token: u32) -> Self {
        Self { token }
    }
}

#[derive(Message, Serialize, Debug, Clone)]
#[rtype(result = "()")]
pub struct StreamUnstableEvent {
    pub token: u32,
}

impl StreamUnstableEvent {
    pub fn new(token: u32) -> Self {
        Self { token }
    }
}

#[derive(Message, Serialize, Debug, Clone)]
#[rtype(result = "()")]
pub struct StreamDownEvent {
    pub token: u32,
}

impl StreamDownEvent {
    pub fn new(token: u32) -> Self {
        Self { token }
    }
}

#[derive(Message, Serialize, Debug, Clone)]
#[rtype(result = "()")]
pub struct ListenerUpEvent {
    pub token: u32,
    pub addr: String,
}

impl ListenerUpEvent {
    pub fn new(token: u32, addr: &str) -> Self {
        Self {
            token,
            addr: addr.to_owned(),
        }
    }
}

#[derive(Message, Serialize, Debug, Clone)]
#[rtype(result = "()")]
pub struct ListenerUnstableEvent {
    pub token: u32,
    pub addr: String,
}

impl ListenerUnstableEvent {
    pub fn new(token: u32, addr: &str) -> Self {
        Self {
            token,
            addr: addr.to_owned(),
        }
    }
}

#[derive(Message, Serialize, Debug, Clone)]
#[rtype(result = "()")]
pub struct ListenerDownEvent {
    pub token: u32,
    pub addr: String,
}

impl ListenerDownEvent {
    pub fn new(token: u32, addr: &str) -> Self {
        Self {
            token,
            addr: addr.to_owned(),
        }
    }
}

pub fn start_mq_actor() -> Result<Addr<MqActor>, VCError> {
    Ok(MqActor::new()?.start())
}

pub struct MqActor {
    pub producer: Option<Arc<FutureProducer>>,
    pub start: Instant,
}

impl MqActor {
    pub fn new() -> Result<Self, VCError> {
        // let producer = ClientConfig::new()
        //     .set("bootstrap.servers", &*KAFKA_BROKER)
        //     .set("allow.auto.create.topics", "true")
        //     .set("message.timeout.ms", "10000")
        //     .create()
        //     .map(|v| Arc::new(v))
        //     .ok();
        Ok(Self {
            producer: None,
            start: Instant::now(),
        })
    }
}

impl Actor for MqActor {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Self::Context) {
        // 超过一个月的日志及其备份将被清除，超过一周的临时文件将被清除
        // self.hb(ctx);
        // ctx.run_interval(Duration::from_millis(*HEAL_CHECK_INTERVAL), |act, ctx| {
        //     act.hb(ctx);
        // });
    }
}

impl Handler<StreamUpEvent> for MqActor {
    type Result = ResponseFuture<()>;
    fn handle(&mut self, msg: StreamUpEvent, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Stream up evt {:#?}", msg);
        let producer = self.producer.clone();
        Box::pin(async move {
            if let Some(producer) = producer {
                if let Ok(payload) = serde_json::to_vec(&msg) {
                    let _ = vclog!(
                        producer
                            .send(
                                FutureRecord::to(&KAFKA_TOPIC_STREAM)
                                    .payload(&payload)
                                    .key("up"),
                                Duration::from_secs(1),
                            )
                            .await
                    );
                }
            }
        })
    }
}

impl Handler<StreamUnstableEvent> for MqActor {
    type Result = ResponseFuture<()>;
    fn handle(&mut self, msg: StreamUnstableEvent, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Stream unstable evt {:#?}", msg);
        let producer = self.producer.clone();
        Box::pin(async move {
            if let Some(producer) = producer {
                if let Ok(payload) = serde_json::to_vec(&msg) {
                    let _ = vclog!(
                        producer
                            .send(
                                FutureRecord::to(&KAFKA_TOPIC_STREAM)
                                    .payload(&payload)
                                    .key("unstable"),
                                Duration::from_secs(1),
                            )
                            .await
                    );
                }
            }
        })
    }
}

impl Handler<StreamDownEvent> for MqActor {
    type Result = ResponseFuture<()>;
    fn handle(&mut self, msg: StreamDownEvent, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Stream down evt {:#?}", msg);
        let producer = self.producer.clone();
        Box::pin(async move {
            if let Some(producer) = producer {
                if let Ok(payload) = serde_json::to_vec(&msg) {
                    let _ = vclog!(
                        producer
                            .send(
                                FutureRecord::to(&KAFKA_TOPIC_STREAM)
                                    .payload(&payload)
                                    .key("down"),
                                Duration::from_secs(1),
                            )
                            .await
                    );
                }
            }
        })
    }
}

impl Handler<ListenerUpEvent> for MqActor {
    type Result = ResponseFuture<()>;
    fn handle(&mut self, msg: ListenerUpEvent, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Listener up evt {:#?}", msg);

        let producer = self.producer.clone();
        Box::pin(async move {
            if let Some(producer) = producer {
                if let Ok(payload) = serde_json::to_vec(&msg) {
                    let _ = vclog!(
                        producer
                            .send(
                                FutureRecord::to(&KAFKA_TOPIC_LISTENER)
                                    .payload(&payload)
                                    .key("up"),
                                Duration::from_secs(1),
                            )
                            .await
                    );
                }
            }
        })
    }
}

impl Handler<ListenerUnstableEvent> for MqActor {
    type Result = ResponseFuture<()>;
    fn handle(&mut self, msg: ListenerUnstableEvent, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Listener unstable evt {:#?}", msg);

        let producer = self.producer.clone();
        Box::pin(async move {
            if let Some(producer) = producer {
                if let Ok(payload) = serde_json::to_vec(&msg) {
                    let _ = vclog!(
                        producer
                            .send(
                                FutureRecord::to(&KAFKA_TOPIC_LISTENER)
                                    .payload(&payload)
                                    .key("unstable"),
                                Duration::from_secs(1),
                            )
                            .await
                    );
                }
            }
        })
    }
}

impl Handler<ListenerDownEvent> for MqActor {
    type Result = ResponseFuture<()>;
    fn handle(&mut self, msg: ListenerDownEvent, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Listener down evt {:#?}", msg);

        let producer = self.producer.clone();
        Box::pin(async move {
            if let Some(producer) = producer {
                if let Ok(payload) = serde_json::to_vec(&msg) {
                    let _ = vclog!(
                        producer
                            .send(
                                FutureRecord::to(&KAFKA_TOPIC_LISTENER)
                                    .payload(&payload)
                                    .key("down"),
                                Duration::from_secs(1),
                            )
                            .await
                    );
                }
            }
        })
    }
}
