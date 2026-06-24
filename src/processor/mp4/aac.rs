use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use mse_fmp4::aac::{AacProfile, SamplingFrequency};
use mse_fmp4::fmp4::{
    AacSampleEntry, Mpeg4EsDescriptorBox, Sample, SampleEntry, TrackBox, TrackFragmentBox,
};
use std::any::Any;
use webrtc::rtp::codecs::opus::OpusPacket;
use webrtc::rtp::packet::Packet;
use webrtc::rtp::packetizer::Depacketizer;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;

use crate::handlers::mp4_streamer::{Mp4boxer, StreamerTrackBox};
use crate::processor::opus2aac::Opus2AacTranscoder;
use crate::utils::{MP4_AUDIO_CHANNELS, MP4_AUDIO_SAMPLE_RATE};

pub struct Mp4boxOpus2AacStreamer {
    asc: Bytes,
    channels: u32,
    sample_rate: u32,
    should_moov: bool,
    packetier: OpusPacket,
    transcoder: Opus2AacTranscoder,
    last_timestamp: u32,
}
impl Mp4boxOpus2AacStreamer {
    pub fn new() -> Result<Self> {
        let channels = MP4_AUDIO_CHANNELS;
        let sample_rate = MP4_AUDIO_SAMPLE_RATE;
        // 默认都是LC
        let asc_audio_object_type = 2u16;
        let asc_sampling_frequency_index = match sample_rate {
            48000 => 0x3,
            44100 => 0x4,
            16000 => 0x8,
            8000 => 0xb,
            _ => return Err(anyhow!("sample_rate {sample_rate} not yet supported")),
        } as u16;
        let (asc_channels, decode_channels, encode_channels) = match channels {
            1 => (
                0x1u16,
                audiopus::Channels::Stereo,
                fdk_aac::enc::ChannelMode::Stereo,
            ),
            2 => (
                0x2u16,
                audiopus::Channels::Stereo,
                fdk_aac::enc::ChannelMode::Stereo,
            ),
            _ => return Err(anyhow!("channels {channels} not yet supported")),
        };
        let asc =
            asc_audio_object_type << 11 | asc_sampling_frequency_index << 7 | asc_channels << 3;
        Ok(Self {
            asc: Bytes::from(asc.to_be_bytes().to_vec()),
            should_moov: true,
            channels,
            sample_rate,
            packetier: OpusPacket::default(),
            transcoder: Opus2AacTranscoder::new(
                sample_rate as i32,
                decode_channels,
                sample_rate,
                encode_channels,
            )?,
            last_timestamp: 0,
        })
    }
}
impl Mp4boxer for Mp4boxOpus2AacStreamer {
    fn feed(&mut self, rtp: &Packet, track_id: u32) -> Vec<StreamerTrackBox> {
        let mut result = vec![];
        if self.should_moov {
            let mut track = TrackBox::new(false, track_id);
            track.tkhd_box.duration = 0;
            track.mdia_box.mdhd_box.timescale = MP4_AUDIO_SAMPLE_RATE;
            track.mdia_box.mdhd_box.duration = 0;
            let aac_sample_entry = AacSampleEntry {
                esds_box: Mpeg4EsDescriptorBox {
                    profile: AacProfile::Lc,
                    frequency: SamplingFrequency::Hz48000,
                    channel_configuration: mse_fmp4::aac::ChannelConfiguration::TwoChannels,
                },
            };
            track
                .mdia_box
                .minf_box
                .stbl_box
                .stsd_box
                .sample_entries
                .push(SampleEntry::Aac(aac_sample_entry));
            result.push(StreamerTrackBox::TrackBox(track));
            self.should_moov = true;
        }

        let Some(opus_data) = self.packetier.depacketize(&rtp.payload).ok() else {
            return result;
        };
        let Ok((aac_datas, sample_nb)) = self.transcoder.transcode(&opus_data) else {
            return result;
        };

        // TODO
        let mut traf = TrackFragmentBox::new(2, 0);
        traf.tfhd_box.default_sample_duration = Some(sample_nb as u32);
        traf.trun_box.data_offset = Some(0); // dummy
                                             // traf.trun_box.samples = aac_stream.samples;
        let mut data = BytesMut::new();
        for aac_data in aac_datas {
            let sample = Sample {
                duration: Some(sample_nb as u32),
                size: Some(aac_data.len() as u32),
                flags: None,
                composition_time_offset: None,
            };
            traf.trun_box.samples.push(sample);
            data.extend_from_slice(&aac_data);
        }
        result.push(StreamerTrackBox::TrackFragmentBox((traf, data.freeze())));
        vec![]
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
