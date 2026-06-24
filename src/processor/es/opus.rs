use crate::handlers::es_streamer::EsStreamer;
use crate::processor::jitter::JitterBuffer;
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use std::any::Any;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use webrtc::rtp::codecs::opus::OpusPacket;
use webrtc::rtp::packet::Packet;
use webrtc::rtp::packetizer::Depacketizer;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;

// #[derive(Default)]
pub struct EsOpusStreamer<const S: u8> {
    packetier: OpusPacket,
    last_timestamp: u32,
    // cur_timestamp: u32,
    seperator: Bytes,
    sender: mpsc::Sender<Result<Bytes>>,
    jitter: JitterBuffer<Packet, S>,
    rfc6381: Option<String>,
}
impl<const S: u8> EsOpusStreamer<S> {
    pub fn new(
        seperator: &[u8],
        sender: &mpsc::Sender<Result<Bytes>>,
        report: Option<&broadcast::Sender<Option<Vec<u16>>>>,
    ) -> Result<Self> {
        Ok(Self {
            packetier: OpusPacket::default(),
            last_timestamp: 0,
            // cur_timestamp: 0,
            sender: sender.clone(),
            seperator: Bytes::copy_from_slice(seperator),
            jitter: JitterBuffer::new(report),
            rfc6381: None,
        })
    }
    fn send(&self, type_: &str, raw_data: Bytes) -> Result<()> {
        #[cfg(feature = "log")]
        log::debug!(target:"debug","sending: {type_} {}", raw_data.len());
        let mut data = BytesMut::with_capacity(self.seperator.len() + 4 + 4 + raw_data.len());
        data.extend_from_slice(&self.seperator);
        data.extend_from_slice(type_.as_bytes());
        data.extend_from_slice((raw_data.len() as u32).to_be_bytes().as_slice());
        data.put(raw_data);
        futures::executor::block_on(async {
            self.sender
                .send_timeout(Ok(data.freeze()), Duration::from_secs(10))
                .await
        })?;
        Ok(())
    }
}

impl<const S: u8> EsStreamer for EsOpusStreamer<S> {
    fn feed(&mut self, rtp: Packet) -> Result<()> {
        if self.rfc6381.is_none() {
            let codec = "Opus".to_owned();
            self.send("acnf", Bytes::copy_from_slice(codec.as_bytes()))?;
            self.rfc6381.replace(codec);
        }
        self.jitter.push(rtp);
        loop {
            if self.jitter.peek().is_some() {
                if let Some(pkt) = self.jitter.pop() {
                    if let Ok(data) = self.packetier.depacketize(&pkt.payload) {
                        let duration = if self.last_timestamp == 0 {
                            0
                        } else {
                            pkt.header.timestamp.wrapping_sub(self.last_timestamp)
                        };
                        self.last_timestamp = pkt.header.timestamp;
                        let mut timestamp_data = BytesMut::with_capacity(4 + data.len());
                        timestamp_data.put_u32(duration);
                        timestamp_data.put(data);
                        self.send("afrm", timestamp_data.freeze())?;
                    }
                    continue;
                }
            }
            break;
        }
        // let Some(data) = self.packetier.depacketize(&rtp.payload).ok() else {
        //     return;
        // };
        // self.annex_b_reader.push(&data);
        Ok(())
    }
    fn codec_type(&self) -> RTPCodecType {
        RTPCodecType::Audio
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
