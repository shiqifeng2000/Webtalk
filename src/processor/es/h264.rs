use crate::handlers::es_streamer::EsStreamer;
use crate::processor::jitter::JitterBuffer;
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use h264_reader::nal::Nal;
use h264_reader::{
    annexb::AnnexBReader,
    nal::{pps::PicParameterSet, sps::SeqParameterSet, RefNal, UnitType},
    push::{AccumulatedNalHandler, NalAccumulator, NalInterest},
};
use std::any::Any;
use std::collections::VecDeque;
use std::io::Read;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use webrtc::rtp::codecs::h264::{H264Packet, ANNEXB_NALUSTART_CODE};
use webrtc::rtp::packet::Packet;
use webrtc::rtp::packetizer::Depacketizer;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;

// #[derive(Default)]
pub struct EsAvcStreamer<const S: u8> {
    packetier: H264Packet,
    annex_b_reader: AnnexBReader<NalAccumulator<EsAvcStreamerNalHander>>,
    jitter: JitterBuffer<Packet, S>,
    // fs: BufWriter<std::fs::File>,
}
impl<const S: u8> EsAvcStreamer<S> {
    pub fn new(
        seperator: &[u8],
        sender: &mpsc::Sender<Result<Bytes>>,
        report: Option<&broadcast::Sender<Option<Vec<u16>>>>,
    ) -> Self {
        Self {
            packetier: H264Packet::default(),
            // last_timestamp: 0,
            // cur_duration: 0,
            annex_b_reader: AnnexBReader::accumulate(EsAvcStreamerNalHander::new(
                seperator, sender,
            )),
            jitter: JitterBuffer::new(report),
            // fs: BufWriter::new(std::fs::File::create("./test.h264").unwrap()),
        }
    }
}

impl<const S: u8> EsStreamer for EsAvcStreamer<S> {
    fn feed(&mut self, rtp: Packet) -> Result<()> {
        self.jitter.push(rtp);
        loop {
            if self.jitter.peek().is_some() {
                if let Some(pkt) = self.jitter.pop() {
                    if let Ok(data) = self.packetier.depacketize(&pkt.payload) {
                        self.annex_b_reader
                            .fragment_handler_mut()
                            .handler_mut()
                            .timestamp = pkt.header.timestamp;

                        self.annex_b_reader.push(&data);
                        // self.fs.write(&data);
                    }
                    continue;
                }
            }
            break;
        }
        Ok(())
    }
    fn codec_type(&self) -> RTPCodecType {
        RTPCodecType::Video
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
struct EsAvcStreamerNalHander {
    last_timestamp: u32,
    timestamp: u32,
    annexb_ctx: h264_reader::Context,
    sender: mpsc::Sender<Result<Bytes>>,
    sps: Option<Bytes>,
    pps: Option<Bytes>,
    seperator: Bytes,
    avc_data: VecDeque<Bytes>,
    extradata: Option<Bytes>,
}
impl EsAvcStreamerNalHander {
    pub fn new(seperator: &[u8], sender: &mpsc::Sender<Result<Bytes>>) -> Self {
        Self {
            last_timestamp: 0,
            timestamp: 0,
            annexb_ctx: h264_reader::Context::new(),
            sps: None,
            pps: None,
            sender: sender.clone(),
            seperator: Bytes::copy_from_slice(seperator),
            avc_data: VecDeque::new(),
            extradata: None,
        }
    }
    // pub fn rfc_6381_codec(&self) -> Option<Bytes> {
    //     let Some(sps) = self.annexb_ctx.sps().next() else {
    //         return None;
    //     };
    //     let Ok((width, height)) = sps.pixel_dimensions() else {
    //         return None;
    //     };
    //     let profile_idc: u8 = sps.profile_idc.into();
    //     let constraint_set_flag: u8 = sps.constraint_flags.into();
    //     let level_idc = sps.level_idc;
    //     let rfc6381_str = format!(
    //         "avc1.{:X}{:X}{:X}",
    //         profile_idc, constraint_set_flag, level_idc
    //     );
    //     let mut data = BytesMut::with_capacity(rfc6381_str.len() + 4);
    //     data.put_u16(width as u16);
    //     data.put_u16(height as u16);
    //     data.put(rfc6381_str.as_bytes());
    //     Some(data.freeze())
    // }
    pub fn gen_configuration(&self) -> Option<Bytes> {
        let Some(sps) = self.annexb_ctx.sps().next() else {
            return None;
        };
        let Some(sps_data) = self.sps.as_ref() else {
            return None;
        };
        let Some(pps_data) = self.pps.as_ref() else {
            return None;
        };
        let Ok((width, height)) = sps.pixel_dimensions() else {
            return None;
        };
        let profile_idc: u8 = sps.profile_idc.into();
        let constraint_set_flag: u8 = sps.constraint_flags.into();
        let level_idc = sps.level_idc;
        let rfc6381_str = format!(
            "avc1.{:X}{:X}{:X}",
            profile_idc, constraint_set_flag, level_idc
        );
        let rfc6381_str_len = rfc6381_str.len();
        let extradata = build_avcc_extradata(sps_data, pps_data);
        let extradata_len = extradata.len();
        let mut data = BytesMut::with_capacity(2 + 2 + 4 + rfc6381_str_len + 4 + extradata_len);
        data.put_u16(width as u16);
        data.put_u16(height as u16);
        data.put_u32(rfc6381_str_len as u32);
        data.put(rfc6381_str.as_bytes());
        data.put_u32(extradata_len as u32);
        data.put(extradata.as_slice());
        Some(data.freeze())
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
impl AccumulatedNalHandler for EsAvcStreamerNalHander {
    fn nal(&mut self, nal: RefNal<'_>) -> NalInterest {
        // We only ever want to parse complete NALs.
        // You can filter for the specific types of NALs you're
        // interested in and NalInterest::Ignore the rest here.
        //
        // If a NAL is incomplete, trying to read its data will result in a WouldBlock.
        if !nal.is_complete() {
            return NalInterest::Buffer;
        }
        // Parse the NAL header, so we know what the NAL type is
        let nal_header = nal.header().unwrap();
        let nal_unit_type = nal_header.nal_unit_type();
        // Decode the NAL types that we're interested in
        match nal_unit_type {
            UnitType::SeqParameterSet => {
                let Ok(sps) = SeqParameterSet::from_bits(nal.rbsp_bits()) else {
                    return NalInterest::Ignore;
                };
                // Don't forget to tell stream_context that we have a new SPS.
                // If you want to handle it separately, you can clone the struct before passing along,
                // But if you only care about it when a slice calls for it, you don't have to handle it here.

                if self
                    .annexb_ctx
                    .sps()
                    .next()
                    .map(|v| *v != sps)
                    .unwrap_or(true)
                {
                    let mut sps_data = Vec::new();
                    let mut reader = nal.reader();
                    if reader.read_to_end(&mut sps_data).is_ok() {
                        self.annexb_ctx.put_seq_param_set(sps);
                        #[cfg(feature = "log")]
                        log::debug!(target:"debug", "sps_data {sps_data:?}");
                        self.sps.replace(Bytes::from_iter(sps_data));
                        self.extradata.take();
                    }
                }

                if self.extradata.is_none() {
                    if let Some(codec) = self.gen_configuration() {
                        if self.send("vcnf", codec.clone()).is_ok() {
                            self.extradata.replace(codec);
                        }
                    }
                }
            }
            UnitType::PicParameterSet => {
                if self.annexb_ctx.pps().next().is_none() {
                    // Same as when parsing an SPS, except it borrows the stream context so it can pick out
                    // the SPS that this PPS references
                    let Ok(pps) = PicParameterSet::from_bits(&self.annexb_ctx, nal.rbsp_bits())
                    else {
                        return NalInterest::Ignore;
                    };
                    // Same as with an SPS, tell the context that we've found a PPS
                    let mut pps_data = Vec::new();
                    let mut reader = nal.reader();
                    if reader.read_to_end(&mut pps_data).is_ok() {
                        #[cfg(feature = "log")]
                        log::debug!(target:"debug", "pps_data {pps_data:?}");
                        self.annexb_ctx.put_pic_param_set(pps);
                        self.pps.replace(Bytes::from_iter(pps_data));
                    }
                }

                if self.extradata.is_none() {
                    if let Some(codec) = self.gen_configuration() {
                        if self.send("vcnf", codec.clone()).is_ok() {
                            self.extradata.replace(codec);
                        }
                    }
                }
            }
            UnitType::SliceLayerWithoutPartitioningIdr
            | UnitType::SliceLayerWithoutPartitioningNonIdr => {
                if self.extradata.is_none() {
                    if let Some(codec) = self.gen_configuration() {
                        if self.send("vcnf", codec.clone()).is_ok() {
                            self.extradata.replace(codec);
                        }
                    }
                }

                let mut data = Vec::new();
                let mut reader = nal.reader();

                if reader.read_to_end(&mut data).is_ok() {
                    let is_key = nal_unit_type == UnitType::SliceLayerWithoutPartitioningIdr;
                    let mut key_data = BytesMut::with_capacity(data.len() + 4 + 1);
                    key_data.put_u8(is_key as u8);
                    // key_data.put_u32(data.len() as u32);
                    let duration = if self.last_timestamp == 0 {
                        0
                    } else {
                        self.timestamp.wrapping_sub(self.last_timestamp)
                    };
                    self.last_timestamp = self.timestamp;
                    key_data.put_u32(duration);

                    key_data.put(Bytes::from_iter(data));
                    self.avc_data.push_back(key_data.freeze());
                    log::debug!(target:"debug", "nal_unit_type {nal_unit_type:?}");
                }
                while let Some(data) = self.avc_data.pop_front() {
                    let _ = self.send("vfrm", data);
                }
            }
            _ => {
                #[cfg(feature = "log")]
                log::debug!(target:"debug","Unhandled: {:?}", nal_unit_type);
            }
        }
        NalInterest::Ignore
    }
}

pub fn build_avcc_extradata(sps_data: &[u8], pps_data: &[u8]) -> Vec<u8> {
    // assert!(sps.len() <= 4, "SPS too short");
    // assert!(pps.len() <= 1, "PPS too short");
    let profile_idc = sps_data[1];
    let profile_compat = sps_data[2];
    let level_idc = sps_data[3];
    let mut avcc = Vec::with_capacity(7 + 2 + sps_data.len() + 1 + 2 + sps_data.len() + 1);
    // configurationVersion
    avcc.push(1);
    // AVCProfileIndication
    avcc.push(profile_idc);
    // profile_compatibility
    avcc.push(profile_compat);
    // AVCLevelIndication
    avcc.push(level_idc);
    // lengthSizeMinusOne (4 bytes NAL length → 3)
    avcc.push(0b11111100 | 3);
    // numOfSequenceParameterSets (1 SPS)
    avcc.push(0b11100000 | 1);
    // SPS length (big-endian)
    avcc.extend_from_slice(&(sps_data.len() as u16).to_be_bytes());
    avcc.extend_from_slice(sps_data);
    // numOfPictureParameterSets (1 PPS)
    avcc.push(1);
    // PPS length
    avcc.extend_from_slice(&(pps_data.len() as u16 + 1).to_be_bytes());
    avcc.extend_from_slice(pps_data);
    avcc.put_u8(0x00);
    avcc
}

pub fn build_annexb_extradata(sps_data: &[u8], pps_data: &[u8]) -> Vec<u8> {
    let mut avcc = vec![];
    avcc.extend_from_slice(&ANNEXB_NALUSTART_CODE);
    avcc.extend_from_slice(sps_data);
    avcc.extend_from_slice(&ANNEXB_NALUSTART_CODE);
    avcc.extend_from_slice(pps_data);
    avcc
}
