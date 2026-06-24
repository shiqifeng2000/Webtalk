use bytes::{BufMut, Bytes, BytesMut};
use h264_reader::nal::Nal;
use h264_reader::{
    annexb::AnnexBReader,
    nal::{pps::PicParameterSet, sps::SeqParameterSet, RefNal, UnitType},
    push::{AccumulatedNalHandler, NalAccumulator, NalFragmentHandler, NalInterest},
};
use mse_fmp4::{
    avc::AvcDecoderConfigurationRecord,
    fmp4::{
        AvcConfigurationBox, AvcSampleEntry, Sample, SampleEntry, SampleFlags, TrackBox,
        TrackFragmentBox,
    },
};
use std::any::Any;
use std::collections::{vec_deque, VecDeque};
use std::fmt;
use std::io::Read;
use webrtc::rtp::codecs::h264::H264Packet;
use webrtc::rtp::packet::Packet;
use webrtc::rtp::packetizer::Depacketizer;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;

use crate::{
    handlers::mp4_streamer::{Mp4boxer, StreamerTrackBox},
    utils::MP4_VIDEO_SAMPLE_RATE,
};

/// NAL H.264 Network Abstraction Layer
// pub struct NAL {
//     pub picture_order_count: u32,

//     /// NAL header
//     pub forbidden_zero_bit: bool,
//     pub ref_idc: u8,
//     pub unit_type: NalUnitType,

//     /// header byte + rbsp
//     pub data: BytesMut,
// }

// impl NAL {
//     fn new(data: Bytes) -> Self {
//         NAL {
//             picture_order_count: 0,
//             forbidden_zero_bit: false,
//             ref_idc: 0,
//             unit_type: NalUnitType::Unspecified,
//             data: BytesMut::from(data),
//         }
//     }

//     fn parse_header(&mut self) {
//         let first_byte = self.data[0];
//         self.forbidden_zero_bit = ((first_byte & 0x80) >> 7) == 1; // 0x80 = 0b10000000
//         self.ref_idc = (first_byte & 0x60) >> 5; // 0x60 = 0b01100000
//         self.unit_type = NalUnitType::from(first_byte & 0x1F); // 0x1F = 0b00011111
//     }
// }

// const NAL_PREFIX_3BYTES: [u8; 3] = [0, 0, 1];
// const NAL_PREFIX_4BYTES: [u8; 4] = [0, 0, 0, 1];

// #[derive(Default)]
// pub struct NALList {
//     pub nals: Vec<NAL>,
// }

// impl NALList {
//     // webrtc depacketer fill nal with 0001 instead of 001
//     pub fn new(d: Bytes) -> Self {
//         let mut data = BytesMut::from(d);
//         let mut me = Self::default();
//         let nal_finder = memchr::memmem::Finder::new(&NAL_PREFIX_4BYTES);
//         let mut offset_starts = nal_finder.find_iter(&data).collect::<Vec<usize>>();
//         if offset_starts.len() == 0 {
//             return me;
//         }
//         // reverse first, split off all the nal slice, then reverse back
//         let rev_offset_starts = offset_starts.as_mut_slice();
//         rev_offset_starts.reverse();
//         let mut nals = rev_offset_starts
//             .into_iter()
//             .map(|s| {
//                 let mut nal = NAL::new(data.split_off(*s + 4).freeze());
//                 nal.parse_header();
//                 nal
//             })
//             .collect::<Vec<NAL>>();
//         nals.reverse();
//         me.nals = nals;
//         me
//     }
// }

// #[derive(Default)]
pub struct Mp4boxAvcStreamer {
    packetier: H264Packet,
    last_timestamp: u32,
    cur_timestamp: u32,
    annex_b_reader: AnnexBReader<NalAccumulator<Mp4boxAvcStreamerNalHander>>,
}
impl Mp4boxAvcStreamer {
    pub fn new() -> Self {
        Self {
            packetier: H264Packet::default(),
            last_timestamp: 0,
            cur_timestamp: 0,
            annex_b_reader: AnnexBReader::accumulate(Mp4boxAvcStreamerNalHander::new()),
        }
    }
}
impl Mp4boxer for Mp4boxAvcStreamer {
    fn feed(&mut self, rtp: &Packet, track_id: u32) -> Vec<StreamerTrackBox> {
        let mut result = vec![];
        let Some(data) = self.packetier.depacketize(&rtp.payload).ok() else {
            return result;
        };
        self.annex_b_reader.push(&data);

        // let nal_list = NALList::new(data);
        let duration = if self.last_timestamp == 0 {
            0
        } else {
            rtp.header.timestamp - self.last_timestamp
        };

        let handler = self.annex_b_reader.fragment_handler_mut().handler_mut();
        if handler.should_moov {
            let sps_opt = handler.annexb_ctx.sps().next();
            let pps_opt = handler.annexb_ctx.pps().next();
            let sps_data_opt = handler.sps_data.as_ref();
            let pps_data_opt = handler.pps_data.as_ref();
            if sps_opt.is_some()
                && pps_opt.is_some()
                && sps_data_opt.is_some()
                && pps_data_opt.is_some()
            {
                let sps = sps_opt.as_ref().unwrap();
                // let pps = pps_opt.as_ref().unwrap();
                let sps_data = sps_data_opt.unwrap();
                let pps_data = pps_data_opt.unwrap();
                let mut track = TrackBox::new(true, track_id);

                let width = sps.pic_width_in_mbs() * 16;
                let height = sps.pic_height_in_map_units() * 16;
                track.tkhd_box.width = width << 16;
                track.tkhd_box.height = height << 16;
                track.tkhd_box.duration = 0;
                track.edts_box.elst_box.media_time = 0;
                track.mdia_box.mdhd_box.timescale = MP4_VIDEO_SAMPLE_RATE;
                track.mdia_box.mdhd_box.duration = 0;

                let profile_idc: u8 = sps.profile_idc.into();
                let constraint_set_flag: u8 = sps.constraint_flags.into();
                let level_idc = sps.level_idc;
                let avc_sample_entry = AvcSampleEntry {
                    width: width as u16,
                    height: height as u16,
                    avcc_box: AvcConfigurationBox {
                        configuration: AvcDecoderConfigurationRecord {
                            profile_idc,
                            constraint_set_flag,
                            level_idc,
                            sequence_parameter_set: sps_data.to_vec(),
                            picture_parameter_set: pps_data.to_vec(),
                        },
                    },
                };
                track
                    .mdia_box
                    .minf_box
                    .stbl_box
                    .stsd_box
                    .sample_entries
                    .push(SampleEntry::Avc(avc_sample_entry));
                result.push(StreamerTrackBox::TrackBox(track));
                handler.should_moov = false;
            }
        }
        while let Some((nal_type, data)) = handler.avc_data.pop_front() {
            let (sample_depends_on, sample_is_depdended_on) =
                if UnitType::SliceLayerWithoutPartitioningIdr == nal_type {
                    (3, 1)
                } else {
                    (1, 2)
                };
            // 1
            let mut traf = TrackFragmentBox::new(track_id, self.cur_timestamp);
            traf.tfhd_box.default_sample_flags = Some(SampleFlags {
                is_leading: 0,
                sample_depends_on,
                sample_is_depdended_on,
                sample_has_redundancy: 0,
                sample_padding_value: 0,
                sample_is_non_sync_sample: true,
                sample_degradation_priority: 0,
            });
            traf.trun_box.data_offset = Some(0); // dummy
            traf.trun_box.first_sample_flags = Some(SampleFlags {
                is_leading: 0,
                sample_depends_on,
                sample_is_depdended_on,
                sample_has_redundancy: 0,
                sample_padding_value: 0,
                sample_is_non_sync_sample: false,
                sample_degradation_priority: 0,
            });
            let mut sample_data = BytesMut::new();
            let nal_len = data.len() as u32;
            sample_data.put_u32(nal_len);
            sample_data.extend_from_slice(&data);
            let sample_size = 4 + nal_len;
            traf.trun_box.samples.push(Sample {
                duration: Some(duration), // dummy
                size: Some(sample_size),
                flags: None,
                composition_time_offset: Some(0),
            });
            result.push(StreamerTrackBox::TrackFragmentBox((
                traf,
                sample_data.freeze(),
            )));
            self.last_timestamp = rtp.header.timestamp;
            self.cur_timestamp = self.cur_timestamp.wrapping_add(duration);
        }

        result
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

struct Mp4boxAvcStreamerNalHander {
    annexb_ctx: h264_reader::Context,
    sps_data: Option<Bytes>,
    pps_data: Option<Bytes>,
    avc_data: VecDeque<(UnitType, BytesMut)>,
    should_moov: bool,
}
impl Mp4boxAvcStreamerNalHander {
    pub fn new() -> Self {
        Self {
            annexb_ctx: h264_reader::Context::new(),
            sps_data: None,
            pps_data: None,
            avc_data: VecDeque::new(),
            should_moov: false,
        }
    }
}
impl AccumulatedNalHandler for Mp4boxAvcStreamerNalHander {
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
                    reader.read_to_end(&mut sps_data);
                    log::debug!(target:"debug", "sps_data {sps_data:?}");
                    self.sps_data.replace(Bytes::from(sps_data));
                    self.annexb_ctx.put_seq_param_set(sps);
                    self.should_moov = true;
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
                    reader.read_to_end(&mut pps_data);
                    self.pps_data.replace(Bytes::from(pps_data));
                    self.annexb_ctx.put_pic_param_set(pps);
                    self.should_moov = true;
                }
            }
            UnitType::SliceLayerWithoutPartitioningIdr
            | UnitType::SliceLayerWithoutPartitioningNonIdr => {
                let mut data = Vec::new();
                let mut reader = nal.reader();
                reader.read_to_end(&mut data);
                self.avc_data
                    .push_back((nal_unit_type, BytesMut::from_iter(data)));
            }
            _ => {
                log::debug!(target:"debug","Unhandled: {:?}", nal_unit_type);
            }
        }
        NalInterest::Ignore
    }
}
// fn is_avc_key_frame(&mut self, payload: &[u8]) -> Result<bool> {
//     let data = &payload;
//     if data.len() < 4 {
//         return Ok(false);
//     } else {
//         let word = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
//         let nalu_type = (word >> 24) as u8 & NALU_TYPE_BITMASK;
//         return Ok((nalu_type == STAPA_NALU_TYPE
//             && (word & NALU_TYPE_BITMASK as u32) as u8 == SPS_NALU_TYPE)
//             || (nalu_type == SPS_NALU_TYPE));
//     }
//     // Err(anyhow!("not support yet for avc"))
// }

// Wrapper class around reading buffer
// struct ReadBuffer {
//     buffer: Box<[u8]>,
//     read_end: usize,
//     filled_end: usize,
// }

// impl ReadBuffer {
//     fn new(capacity: usize) -> ReadBuffer {
//         Self {
//             buffer: vec![0u8; capacity].into_boxed_slice(),
//             read_end: 0,
//             filled_end: 0,
//         }
//     }

//     #[inline]
//     fn in_buffer(&self) -> usize {
//         self.filled_end - self.read_end
//     }

//     fn consume(&mut self, consume: usize) -> &[u8] {
//         debug_assert!(self.read_end + consume <= self.filled_end);
//         let result = &self.buffer[self.read_end..][..consume];
//         self.read_end += consume;
//         result
//     }

//     pub(crate) fn fill_buffer(&mut self, reader: &mut impl Read) -> Result<()> {
//         debug_assert_eq!(self.read_end, self.filled_end);

//         self.read_end = 0;
//         self.filled_end = reader.read(&mut self.buffer)?;

//         Ok(())
//     }
// }

// /// H264Reader reads data from stream and constructs h264 nal units
// pub struct H264Reader<R: Read> {
//     reader: R,
//     // reading buffers
//     buffer: ReadBuffer,
//     // for reading
//     nal_prefix_parsed: bool,
//     count_of_consecutive_zero_bytes: usize,
//     nal_buffer: BytesMut,
// }

// impl<R: Read> H264Reader<R> {
//     /// new creates new `H264Reader` with `capacity` sized read buffer.
//     pub fn new(reader: R, capacity: usize) -> H264Reader<R> {
//         H264Reader {
//             reader,
//             nal_prefix_parsed: false,
//             buffer: ReadBuffer::new(capacity),
//             count_of_consecutive_zero_bytes: 0,
//             nal_buffer: BytesMut::new(),
//         }
//     }

//     fn read4(&mut self) -> Result<([u8; 4], usize)> {
//         let mut result = [0u8; 4];
//         let mut result_filled = 0;
//         loop {
//             let in_buffer = self.buffer.in_buffer();

//             if in_buffer + result_filled >= 4 {
//                 let consume = 4 - result_filled;
//                 result[result_filled..].copy_from_slice(self.buffer.consume(consume));
//                 return Ok((result, 4));
//             }

//             result[result_filled..][..in_buffer].copy_from_slice(self.buffer.consume(in_buffer));
//             result_filled += in_buffer;

//             self.buffer.fill_buffer(&mut self.reader)?;

//             if self.buffer.in_buffer() == 0 {
//                 return Ok((result, result_filled));
//             }
//         }
//     }

//     fn read1(&mut self) -> Result<Option<u8>> {
//         if self.buffer.in_buffer() == 0 {
//             self.buffer.fill_buffer(&mut self.reader)?;

//             if self.buffer.in_buffer() == 0 {
//                 return Ok(None);
//             }
//         }

//         Ok(Some(self.buffer.consume(1)[0]))
//     }

//     fn bit_stream_starts_with_h264prefix(&mut self) -> Result<usize> {
//         let (prefix_buffer, n) = self.read4()?;

//         if n == 0 {
//             return Err(Error::ErrIoEOF);
//         }

//         if n < 3 {
//             return Err(Error::ErrDataIsNotH264Stream);
//         }

//         let nal_prefix3bytes_found = NAL_PREFIX_3BYTES[..] == prefix_buffer[..3];
//         if n == 3 {
//             if nal_prefix3bytes_found {
//                 return Err(Error::ErrIoEOF);
//             }
//             return Err(Error::ErrDataIsNotH264Stream);
//         }

//         // n == 4
//         if nal_prefix3bytes_found {
//             self.nal_buffer.put_u8(prefix_buffer[3]);
//             return Ok(3);
//         }

//         let nal_prefix4bytes_found = NAL_PREFIX_4BYTES[..] == prefix_buffer;
//         if nal_prefix4bytes_found {
//             Ok(4)
//         } else {
//             Err(Error::ErrDataIsNotH264Stream)
//         }
//     }

//     /// next_nal reads from stream and returns then next NAL,
//     /// and an error if there is incomplete frame data.
//     /// Returns all nil values when no more NALs are available.
//     pub fn next_nal(&mut self) -> Result<NAL> {
//         if !self.nal_prefix_parsed {
//             self.bit_stream_starts_with_h264prefix()?;

//             self.nal_prefix_parsed = true;
//         }

//         loop {
//             let Some(read_byte) = self.read1()? else {
//                 break;
//             };

//             let nal_found = self.process_byte(read_byte);
//             if nal_found {
//                 let nal_unit_type = NalUnitType::from(self.nal_buffer[0] & 0x1F);
//                 if nal_unit_type == NalUnitType::SEI {
//                     self.nal_buffer.clear();
//                     continue;
//                 } else {
//                     break;
//                 }
//             }

//             self.nal_buffer.put_u8(read_byte);
//         }

//         if self.nal_buffer.is_empty() {
//             return Err(Error::ErrIoEOF);
//         }

//         let mut nal = NAL::new(self.nal_buffer.split());
//         nal.parse_header();

//         Ok(nal)
//     }

//     fn process_byte(&mut self, read_byte: u8) -> bool {
//         let mut nal_found = false;

//         match read_byte {
//             0 => {
//                 self.count_of_consecutive_zero_bytes += 1;
//             }
//             1 => {
//                 if self.count_of_consecutive_zero_bytes >= 2 {
//                     let count_of_consecutive_zero_bytes_in_prefix =
//                         if self.count_of_consecutive_zero_bytes > 2 {
//                             3
//                         } else {
//                             2
//                         };
//                     let nal_unit_length =
//                         self.nal_buffer.len() - count_of_consecutive_zero_bytes_in_prefix;
//                     if nal_unit_length > 0 {
//                         let _ = self.nal_buffer.split_off(nal_unit_length);
//                         nal_found = true;
//                     }
//                 }
//                 self.count_of_consecutive_zero_bytes = 0;
//             }
//             _ => {
//                 self.count_of_consecutive_zero_bytes = 0;
//             }
//         }

//         nal_found
//     }
// }
