// Custom error types replacing str0m's PacketError
#[derive(Debug)]
pub enum Vp9Error {
    ShortPacket,
    TooManyPDiff,
    TooManySpatialLayers,
    CorruptedPacket,
    InvalidData,
}

// Constants from str0m
const VP9HEADER_SIZE: usize = 3;
const MAX_SPATIAL_LAYERS: usize = 3;
const MAX_VP9REF_PICS: usize = 3;

// Replacing CodecExtra with our own metadata structure
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Vp9Metadata {
    pub temporal_id: Option<u8>,
    pub spatial_id: Option<u8>,
    pub picture_id: u16,
    pub is_keyframe: bool,
    pub tl0_picture_id: Option<u8>,
}

// BitRead trait - kept mostly the same since it's fundamental
pub trait BitRead {
    fn get_u8(&mut self) -> Option<u8>;
    fn get_u16(&mut self) -> Option<u16>;
    fn remaining(&self) -> usize;
}

// Simple implementation of BitRead
pub struct SimpleBitReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> SimpleBitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn consumed(&self) -> usize {
        self.pos
    }
}

impl<'a> BitRead for SimpleBitReader<'a> {
    fn get_u8(&mut self) -> Option<u8> {
        if self.pos < self.data.len() {
            let byte = self.data[self.pos];
            self.pos += 1;
            Some(byte)
        } else {
            None
        }
    }

    fn get_u16(&mut self) -> Option<u16> {
        if self.pos + 1 < self.data.len() {
            let bytes = &self.data[self.pos..self.pos + 2];
            self.pos += 2;
            Some(u16::from_be_bytes([bytes[0], bytes[1]]))
        } else {
            None
        }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }
}

// Main depacketizer struct - cleaned up from str0m dependencies
#[derive(PartialEq, Eq, Debug, Default, Clone)]
pub struct Vp9Depacketizer {
    // Basic descriptor flags
    pub i: bool, // picture ID present
    pub p: bool, // inter-picture predicted
    pub l: bool, // layer indices present
    pub f: bool, // flexible mode
    pub b: bool, // start of frame
    pub e: bool, // end of frame
    pub v: bool, // scalability structure present
    pub z: bool, // not reference for upper layers
    pub picture_id: u16,

    // Layer information
    pub tid: u8, // temporal layer ID
    pub u: bool, // switching up point
    pub sid: u8, // spatial layer ID
    pub d: bool, // inter-layer dependency used

    // Reference indices (flexible mode)
    pub pdiff: [u8; MAX_VP9REF_PICS],
    pub tl0picidx: u8,

    // Scalability structure
    pub ns: u8,
    pub y: bool,
    pub g: bool,
    pub ng: u8,
    pub width: [Option<u16>; MAX_SPATIAL_LAYERS],
    pub height: [Option<u16>; MAX_SPATIAL_LAYERS],
    pub pgtid: Vec<u8>,
    pub pgu: Vec<bool>,
    pub pgpdiff: Vec<Vec<u8>>,
}

impl Vp9Depacketizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// check if it got svc mode
    pub fn metadata(&mut self, packet: &[u8]) -> Result<Vp9Metadata, Vp9Error> {
        if packet.is_empty() {
            return Err(Vp9Error::ShortPacket);
        }

        let mut reader = SimpleBitReader::new(packet);
        let b = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;

        self.i = (b & 0x80) != 0;
        self.p = (b & 0x40) != 0;
        self.l = (b & 0x20) != 0;
        self.f = (b & 0x10) != 0;
        self.b = (b & 0x08) != 0;
        self.e = (b & 0x04) != 0;
        self.v = (b & 0x02) != 0;
        self.z = (b & 0x01) != 0;

        let mut payload_index = 1;

        if self.i {
            payload_index = self.parse_picture_id(&mut reader, payload_index)?;
        }

        if self.l {
            payload_index = self.parse_layer_info(&mut reader, payload_index)?;
        }

        if self.f && self.p {
            payload_index = self.parse_ref_indices(&mut reader, payload_index)?;
        }

        if self.v {
            payload_index = self.parse_ssdata(&mut reader, payload_index)?;
        }

        let mut metadata = Vp9Metadata::default();
        self.update_metadata(&mut metadata, 0, packet.len(), payload_index)?;

        Ok(metadata)
    }

    /// Main depacketization method - replaces str0m's version
    pub fn depacketize(
        &mut self,
        packet: &[u8],
        out: &mut Vec<u8>,
        metadata: &mut Vp9Metadata,
    ) -> Result<(), Vp9Error> {
        if packet.is_empty() {
            return Err(Vp9Error::ShortPacket);
        }

        let mut reader = SimpleBitReader::new(packet);
        let b = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;

        self.i = (b & 0x80) != 0;
        self.p = (b & 0x40) != 0;
        self.l = (b & 0x20) != 0;
        self.f = (b & 0x10) != 0;
        self.b = (b & 0x08) != 0;
        self.e = (b & 0x04) != 0;
        self.v = (b & 0x02) != 0;
        self.z = (b & 0x01) != 0;

        let mut payload_index = 1;

        if self.i {
            payload_index = self.parse_picture_id(&mut reader, payload_index)?;
        }

        if self.l {
            payload_index = self.parse_layer_info(&mut reader, payload_index)?;
        }

        if self.f && self.p {
            payload_index = self.parse_ref_indices(&mut reader, payload_index)?;
        }

        if self.v {
            payload_index = self.parse_ssdata(&mut reader, payload_index)?;
        }

        self.update_metadata(metadata, out.len(), packet.len(), payload_index)?;

        out.extend_from_slice(&packet[payload_index..]);

        Ok(())
    }

    /// Check if this is the start of a VP9 partition
    pub fn is_partition_head(&self, payload: &[u8]) -> bool {
        if payload.is_empty() {
            false
        } else {
            (payload[0] & 0x08) != 0
        }
    }

    /// Check if this is the end of a VP9 partition  
    pub fn is_partition_tail(&self, marker: bool, _payload: &[u8]) -> bool {
        marker
    }

    fn update_metadata(
        &self,
        metadata: &mut Vp9Metadata,
        out_len: usize,
        packet_len: usize,
        payload_index: usize,
    ) -> Result<(), Vp9Error> {
        metadata.temporal_id = Some(self.tid);
        metadata.spatial_id = Some(self.sid);
        metadata.picture_id = self.picture_id;
        metadata.is_keyframe |= !self.p && (self.sid == 0 || !self.l) && self.b;

        if self.l && !self.f {
            metadata.tl0_picture_id = Some(self.tl0picidx);
        }
        Ok(())
    }

    fn parse_picture_id(
        &mut self,
        reader: &mut dyn BitRead,
        mut payload_index: usize,
    ) -> Result<usize, Vp9Error> {
        if reader.remaining() == 0 {
            return Err(Vp9Error::ShortPacket);
        }

        let b = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;
        payload_index += 1;

        if (b & 0x80) != 0 {
            if reader.remaining() == 0 {
                return Err(Vp9Error::ShortPacket);
            }

            let x = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;
            self.picture_id = (((b & 0x7f) as u16) << 8) | (x as u16);
            payload_index += 1;
        } else {
            self.picture_id = (b & 0x7F) as u16;
        }

        Ok(payload_index)
    }

    fn parse_layer_info(
        &mut self,
        reader: &mut dyn BitRead,
        mut payload_index: usize,
    ) -> Result<usize, Vp9Error> {
        payload_index = self.parse_layer_info_common(reader, payload_index)?;

        if self.f {
            Ok(payload_index)
        } else {
            self.parse_layer_info_non_flexible_mode(reader, payload_index)
        }
    }

    fn parse_layer_info_common(
        &mut self,
        reader: &mut dyn BitRead,
        mut payload_index: usize,
    ) -> Result<usize, Vp9Error> {
        if reader.remaining() == 0 {
            return Err(Vp9Error::ShortPacket);
        }

        let b = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;
        payload_index += 1;

        self.tid = b >> 5;
        self.u = (b & 0x10) != 0;
        self.sid = (b >> 1) & 0x7;
        self.d = (b & 0x01) != 0;

        if self.sid as usize >= MAX_SPATIAL_LAYERS {
            Err(Vp9Error::TooManySpatialLayers)
        } else {
            Ok(payload_index)
        }
    }

    fn parse_layer_info_non_flexible_mode(
        &mut self,
        reader: &mut dyn BitRead,
        mut payload_index: usize,
    ) -> Result<usize, Vp9Error> {
        if reader.remaining() == 0 {
            return Err(Vp9Error::ShortPacket);
        }

        self.tl0picidx = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;
        payload_index += 1;
        Ok(payload_index)
    }

    fn parse_ref_indices(
        &mut self,
        reader: &mut dyn BitRead,
        mut payload_index: usize,
    ) -> Result<usize, Vp9Error> {
        let mut b = 1u8;
        let mut num_ref_pics = 0;

        while (b & 0x1) != 0 {
            if num_ref_pics == MAX_VP9REF_PICS {
                return Err(Vp9Error::TooManyPDiff);
            }

            if reader.remaining() == 0 {
                return Err(Vp9Error::ShortPacket);
            }

            b = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;
            payload_index += 1;

            self.pdiff[num_ref_pics] = b >> 1;
            num_ref_pics += 1;
        }

        Ok(payload_index)
    }

    fn parse_ssdata(
        &mut self,
        reader: &mut dyn BitRead,
        mut payload_index: usize,
    ) -> Result<usize, Vp9Error> {
        if reader.remaining() == 0 {
            return Err(Vp9Error::ShortPacket);
        }

        let b = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;
        payload_index += 1;

        self.ns = b >> 5;
        self.y = (b & 0x10) != 0;
        self.g = ((b >> 1) & 0x7) != 0;

        let ns = (self.ns + 1) as usize;
        self.ng = 0;

        if ns > MAX_SPATIAL_LAYERS {
            return Err(Vp9Error::CorruptedPacket);
        }

        if self.y {
            if reader.remaining() < 4 * ns {
                return Err(Vp9Error::ShortPacket);
            }

            for i in 0..ns {
                self.width[i] = Some(reader.get_u16().ok_or(Vp9Error::ShortPacket)?);
                self.height[i] = Some(reader.get_u16().ok_or(Vp9Error::ShortPacket)?);
            }
            payload_index += 4 * ns;
        }

        if self.g {
            if reader.remaining() == 0 {
                return Err(Vp9Error::ShortPacket);
            }

            self.ng = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;
            payload_index += 1;
        }

        for i in 0..self.ng as usize {
            if reader.remaining() == 0 {
                return Err(Vp9Error::ShortPacket);
            }

            let b = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;
            payload_index += 1;

            self.pgtid.push(b >> 5);
            self.pgu.push((b & 0x10) != 0);

            let r = ((b >> 2) & 0x3) as usize;

            if reader.remaining() < r {
                return Err(Vp9Error::ShortPacket);
            }

            self.pgpdiff.push(vec![]);
            for _ in 0..r {
                let b = reader.get_u8().ok_or(Vp9Error::ShortPacket)?;
                payload_index += 1;

                self.pgpdiff[i].push(b);
            }
        }

        Ok(payload_index)
    }
}

// Extension trait for easier usage
pub trait Vp9DepacketizerExt {
    fn process_packet(&mut self, packet: &[u8]) -> Result<(Vec<u8>, Vp9Metadata), Vp9Error>;
}

impl Vp9DepacketizerExt for Vp9Depacketizer {
    fn process_packet(&mut self, packet: &[u8]) -> Result<(Vec<u8>, Vp9Metadata), Vp9Error> {
        let mut frame_data = Vec::new();
        let mut metadata = Vp9Metadata::default();

        self.depacketize(packet, &mut frame_data, &mut metadata)?;

        Ok((frame_data, metadata))
    }
}

// Helper function to quickly depacketize a single packet
pub fn quick_depacketize(packet: &[u8]) -> Result<(Vec<u8>, Vp9Metadata), Vp9Error> {
    let mut depacketizer = Vp9Depacketizer::new();
    depacketizer.process_packet(packet)
}
