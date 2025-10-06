use crate::kafka::{KAFKA_RECORDTYPE_FEATURE, KAFKA_RECORDTYPE_PARTITION, KAFKA_RECORDTYPE_TOPIC};

use super::{errors, metadata, parser, writer};
use core::fmt;
use crc32c::crc32c;
use std::fmt::Write;
use std::io::{BufWriter, Read};

fn size_of<T: Sized>(_v: &T) -> usize {
    std::mem::size_of::<T>()
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub(crate) struct RecordsBatch {
    pub base_offset: u64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: u64,
    pub max_timestamp: u64,
    pub producer_id: u64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub rec_length: i32,
    pub records: Vec<KafkaRecord>,
}

impl std::fmt::Display for RecordsBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut msg: String = String::new();
        write!(
            &mut msg,
            "base_offset: {}, batch_length: {}\n",
            self.base_offset, self.batch_length
        )
        .expect("filed to write 1");
        write!(
            &mut msg,
            "leader epoch: {}, magic: {}, crc: {:0x}\n",
            self.partition_leader_epoch, self.magic, self.crc
        )
        .expect("failed to write! 2");
        write!(
            &mut msg,
            "Attributes: {:0x}, last offset delta: {}\n",
            self.attributes, self.last_offset_delta
        )
        .expect("filed to write 3!");
        write!(
            &mut msg,
            "Base timestamp: {:0x}, Max timestamp: {:0x}\n",
            self.base_timestamp, self.max_timestamp
        )
        .expect("filed to write 4!");
        write!(
            &mut msg,
            "produced ID: {:0x}, producer epoch: {:0x}, num records: {}\n",
            self.producer_id, self.producer_epoch, self.rec_length
        )
        .expect("filed to write 5!");
        self.records.iter().enumerate().for_each(|(i, record)| {
            write!(&mut msg, "record {}: \n{:?}", i, record).expect("failed to write record!");
        });
        write!(f, "{}", msg)
    }
}

impl RecordsBatch {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            records: vec![KafkaRecord::new()],
            ..Default::default()
        }
    }

    pub fn deserialize<R: Read>(buffer: &mut R) -> errors::Result<Self> {
        let mut rec = RecordsBatch {
            ..Default::default()
        };

        rec.base_offset = parser::read_u64(buffer)?;
        println!("base offset: {:?}", rec.base_offset);

        rec.batch_length = parser::read_int(buffer)?;
        println!("batch length: {:?}", rec.batch_length);

        rec.partition_leader_epoch = parser::read_int(buffer)?;
        println!("partition_leader_epoch: {:?}", rec.partition_leader_epoch);

        rec.magic = parser::read_byte(buffer)?;
        println!("magic: {}", rec.magic);

        rec.crc = parser::read_int(buffer)?;
        println!("----------- CRC from disk: {:#x} -----------", rec.crc);

        rec.attributes = parser::read_short(buffer)?;
        println!("attributes: {:?}", rec.attributes);

        rec.last_offset_delta = parser::read_int(buffer)?;
        rec.base_timestamp = parser::read_u64(buffer)?;
        rec.max_timestamp = parser::read_u64(buffer)?;
        rec.producer_id = parser::read_u64(buffer)?;
        rec.producer_epoch = parser::read_short(buffer)?;
        rec.base_sequence = parser::read_int(buffer)?;
        rec.rec_length = parser::read_int(buffer)?;

        for _i in 0..rec.rec_length {
            match KafkaRecord::deserialize(buffer) {
                Ok(r) => rec.records.push(r),
                Err(e) => {
                    println!("Unable to parse KafkaRecord!! Err: {e:?}");
                    println!(
                        "Original Records length: {}, updated: {}",
                        rec.rec_length,
                        rec.records.len()
                    );
                    rec.rec_length = rec.records.len() as i32;
                }
            }
        }

        Ok(rec)
    }

    fn calc_meta(&self) -> errors::Result<(u32, i32)> {
        let mut buf = vec![0_u8; 1500];
        let mut copybuf = BufWriter::new(&mut buf);
        writer::write_bytes(&mut copybuf, &self.attributes)?;
        writer::write_bytes(&mut copybuf, &self.last_offset_delta)?;
        writer::write_bytes(&mut copybuf, &self.base_timestamp)?;
        writer::write_bytes(&mut copybuf, &self.max_timestamp)?;
        writer::write_bytes(&mut copybuf, &self.producer_id)?;
        writer::write_bytes(&mut copybuf, &self.producer_epoch)?;
        writer::write_bytes(&mut copybuf, &self.base_sequence)?;
        writer::write_bytes(&mut copybuf, &(self.rec_length as u32))?;
        self.records
            .iter()
            .try_for_each(|record| record.serialize(&mut copybuf))?;
        let batch_length = copybuf.buffer().len() as i32;
        drop(copybuf);
        Ok((crc32c(&buf[0..batch_length as usize]), batch_length))
    }

    pub fn serialize<W: std::io::Write>(&self, resp: &mut W) -> errors::Result<()> {
        let (crc, batch_length) = self.calc_meta()?;
        println!(
            "---------- crc: {crc:#x} vs {:#x}, batch length: {batch_length} vs {} ----------",
            self.crc, self.batch_length
        );
        writer::write_bytes(resp, &self.base_offset)?;
        writer::write_bytes(resp, &batch_length)?;
        writer::write_bytes(resp, &self.partition_leader_epoch)?;
        writer::write_bytes(resp, &self.magic)?;
        writer::write_bytes(resp, &crc)?;
        writer::write_bytes(resp, &self.attributes)?;
        writer::write_bytes(resp, &self.last_offset_delta)?;
        writer::write_bytes(resp, &self.base_timestamp)?;
        writer::write_bytes(resp, &self.max_timestamp)?;
        writer::write_bytes(resp, &self.producer_id)?;
        writer::write_bytes(resp, &self.producer_epoch)?;
        writer::write_bytes(resp, &self.base_sequence)?;
        // this length is 32-bit
        writer::write_bytes(resp, &(self.rec_length as u32))?;
        self.records
            .iter()
            .try_for_each(|record| record.serialize(resp))?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct KafkaRecord {
    pub length: i8,
    pub attributes: i8,
    pub timestamp_delta: i8,
    pub offset_delta: i8,
    pub key_length: i8,
    pub key: Vec<u8>,
    pub value_length: i8,
    pub value: KafkaRecordValue,
    pub header_count: i8,
    pub headers: Vec<KafkaRecordHeader>,
}

impl std::fmt::Display for KafkaRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut msg: String = String::new();
        write!(
            &mut msg,
            "length: {}, attributes: {}, timestamp_delta: {}, offset_delta: {}\n",
            self.length, self.attributes, self.timestamp_delta, self.offset_delta
        )
        .expect("KR write failed 1!");
        write!(
            &mut msg,
            "key length: {}, key: {:?}\n",
            self.key_length, self.key
        )
        .expect("KR write failed 2!");
        write!(
            &mut msg,
            "value length: {}, value: {:?}\n",
            self.value_length, self.value
        )
        .expect("KR write failed 3!");
        write!(&mut msg, "num headers: {}\n", self.header_count).expect("KR write failed 4!");
        for i in 0..self.header_count {
            write!(&mut msg, "Header {} - {:?}", i, self.headers[i as usize])
                .expect("KR Write failed 5!");
        }

        write!(f, "{}", msg)
    }
}

impl KafkaRecord {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            headers: vec![KafkaRecordHeader::new()],
            ..Default::default()
        }
    }

    pub fn deserialize<R: Read>(buffer: &mut R) -> errors::Result<Self> {
        let mut rec = Self {
            ..Default::default()
        };

        rec.length = parser::read_varint(buffer)?;
        println!("record len: {:?}", rec.length);

        rec.attributes = parser::read_byte(buffer)?;
        rec.timestamp_delta = parser::read_byte(buffer)?;
        rec.offset_delta = parser::read_byte(buffer)?;
        rec.key_length = parser::read_varint(buffer)?;
        if rec.key_length > 0 {
            let mut key = vec![0_u8; rec.key_length as usize];
            buffer.read_exact(&mut key)?;
            rec.key = key;
        }

        assert_eq!(rec.key_length, -1);
        rec.value_length = parser::read_varint(buffer)?;
        if rec.value_length > 0 {
            rec.value = KafkaRecordValue::deserialize(buffer, rec.value_length as usize)?;
        }
        Ok(rec)
    }

    fn size(&self) -> usize {
        size_of(&self.attributes)
            + size_of(&self.timestamp_delta)
            + size_of(&self.offset_delta)
            + 1
            + self.key.len()
            + self.value_length as usize
            + self.value.size()
            + 1
            + self.headers.iter().fold(0, |acc, h| acc + h.size())
    }

    pub fn serialize<W: std::io::Write>(&self, resp: &mut W) -> errors::Result<()> {
        // need to determine length dynamically
        let len = self.size();
        println!("------ length {} vs new length: {len} -----", self.length);
        writer::write_varint_main(resp, len as i32)?;
        writer::write_bytes(resp, &self.attributes)?;
        writer::write_varint(resp, self.timestamp_delta as usize)?;
        writer::write_varint_main(resp, self.offset_delta as i32)?;
        writer::write_compact_string(resp, &self.key)?;
        self.value.serialize(resp)?;
        //writer::write_compact_string(resp, &self.value)?;
        writer::write_bytes(resp, &(self.headers.len() as u8))?;
        self.headers.iter().try_for_each(|h| h.serialize(resp))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct KafkaRecordHeader {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl std::fmt::Display for KafkaRecordHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut msg: String = String::new();
        write!(&mut msg, "key: {:?}, value: {:?}\n", self.key, self.value)
            .expect("KRH write failed 1!");
        write!(f, "{}", msg)
    }
}

impl KafkaRecordHeader {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            //key: "key-header".into(),
            //value: "value-header".into(),
            ..Default::default()
        }
    }

    #[allow(dead_code)]
    pub fn deserialize<R: Read>(_data: &R) -> errors::Result<Self> {
        let d = Self {
            ..Default::default()
        };
        Ok(d)
    }

    fn size(&self) -> usize {
        self.key.len() + 1 + self.value.len() + 1
    }

    pub fn serialize<W: std::io::Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_compact_string(resp, &self.key)?;
        writer::write_compact_string(resp, &self.value)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub enum KafkaRecordValue {
    #[default]
    Invalid,
    KafkaRecordFeatureType(KafkaRecordFeature),
    KafkaRecordTopicRecordType(KafkaRecordTopicRecord),
    KafkaRecordPartitionType(KafkaRecordPartitionRecord),
}

impl std::fmt::Display for KafkaRecordValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid => write!(f, "Invalid!"),
            Self::KafkaRecordFeatureType(v) => write!(f, "{}", v),
            Self::KafkaRecordTopicRecordType(v) => write!(f, "{}", v),
            Self::KafkaRecordPartitionType(v) => write!(f, "{}", v),
        }
    }
}

impl KafkaRecordValue {
    pub fn deserialize<R: Read>(
        buffer: &mut R,
        data_len: usize,
    ) -> errors::Result<KafkaRecordValue> {
        let frame_version = parser::read_byte(buffer)?;
        let frame_type = parser::read_byte(buffer)?;
        let version = parser::read_byte(buffer)?;
        println!(
            "frame version: {}, frame type: {}, version: {}, data_len: {}\n",
            frame_version, frame_type, version, data_len
        );
        match frame_type {
            KAFKA_RECORDTYPE_FEATURE => {
                // feature record
                let mut rec = KafkaRecordFeature::deserialize(buffer, data_len - 3)?;
                rec.frame_version = frame_version;
                rec.frame_type = frame_type;
                rec.version = version;
                Ok(Self::KafkaRecordFeatureType(rec))
            }
            KAFKA_RECORDTYPE_TOPIC => {
                // topic record
                let mut rec = KafkaRecordTopicRecord::deserialize(buffer, data_len - 3)?;
                rec.frame_version = frame_version;
                rec.frame_type = frame_type;
                rec.version = version;
                Ok(Self::KafkaRecordTopicRecordType(rec))
            }
            KAFKA_RECORDTYPE_PARTITION => {
                // partition record
                let mut rec = KafkaRecordPartitionRecord::deserialize(buffer, data_len - 3)?;
                rec.frame_version = frame_version;
                rec.frame_type = frame_type;
                rec.version = version;
                Ok(Self::KafkaRecordPartitionType(rec))
            }
            _ => todo!("Kafka Record type {} not implemented!", frame_type),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Invalid => 0,
            Self::KafkaRecordFeatureType(v) => v.size(),
            Self::KafkaRecordTopicRecordType(v) => v.size(),
            Self::KafkaRecordPartitionType(v) => v.size(),
        }
    }

    pub fn serialize<W: std::io::Write>(&self, resp: &mut W) -> errors::Result<()> {
        match self {
            Self::Invalid => Ok(()),
            Self::KafkaRecordFeatureType(v) => v.serialize(resp),
            Self::KafkaRecordTopicRecordType(v) => v.serialize(resp),
            Self::KafkaRecordPartitionType(v) => v.serialize(resp),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct KafkaRecordFeature {
    pub frame_version: i8,
    pub frame_type: i8,
    pub version: i8,
    pub name_length: u8,
    pub name: Vec<u8>,
    pub feature_level: i16,
    pub tagged_field_count: i8,
}

impl std::fmt::Display for KafkaRecordFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut msg = String::new();
        write!(
            &mut msg,
            "Frame version: {}, Frame Type: {}, Version: {}\n",
            self.frame_version, self.frame_type, self.version
        )
        .expect("KafkaRecordFeature write failed 1");

        write!(
            &mut msg,
            "Name length: {}, Name: {:?}\n",
            self.name_length, self.name
        )
        .expect("KafkaRecordFeature write failed 1");

        write!(
            &mut msg,
            "feature level: {}, tagged field count: {}\n",
            self.feature_level, self.tagged_field_count
        )
        .expect("KafkaRecordFeature write failed 1");
        write!(f, "{}", msg)
    }
}

impl KafkaRecordFeature {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn deserialize<R: Read>(buffer: &mut R, mut len: usize) -> errors::Result<Self> {
        let mut rec = Self {
            ..Default::default()
        };

        if len > 0 {
            rec.name = parser::read_compact_string(buffer)?;
            rec.name_length = rec.name.len() as u8;
            len -= 1 + rec.name.len();
        }

        if len >= 2 {
            rec.feature_level = parser::read_short(buffer)?;
            len -= 2;
        }
        if len >= 1 {
            rec.tagged_field_count = parser::read_varint(buffer)?;
            len -= 1;
        }
        assert_eq!(
            len, 0,
            "Kafka record value deserialize should have exhaused entire length"
        );
        Ok(rec)
    }

    fn size(&self) -> usize {
        size_of(&self.frame_version)
            + size_of(&self.frame_type)
            + size_of(&self.version)
            + size_of(&self.name_length)
            + size_of(&self.name)
            + size_of(&self.feature_level)
            + size_of(&self.tagged_field_count)
    }

    pub fn serialize<W: std::io::Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.frame_version)?;
        writer::write_bytes(resp, &self.frame_type)?;
        writer::write_bytes(resp, &self.version)?;
        writer::write_varint(resp, self.name_length as usize)?;
        writer::write_compact_string(resp, &self.name)?;
        writer::write_bytes(resp, &self.feature_level)?;
        writer::write_varint(resp, self.tagged_field_count as usize)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct KafkaRecordTopicRecord {
    pub frame_version: i8,
    pub frame_type: i8,
    pub version: i8,
    pub name_length: u8,
    pub topic_name: Vec<u8>,
    pub topic_uuid: [u8; 16],
    pub tagged_field_count: i8,
}

impl std::fmt::Display for KafkaRecordTopicRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut msg = String::new();
        write!(
            &mut msg,
            "Frame version: {}, Frame Type: {}, Version: {}\n",
            self.frame_version, self.frame_type, self.version
        )
        .expect("KafkaRecordFeature write failed 1");

        write!(
            &mut msg,
            "Name length: {}, topic Name: {:?}\n",
            self.name_length, self.topic_name
        )
        .expect("KafkaRecordFeature write failed 1");

        write!(
            &mut msg,
            "topic UUID: {:?}, tagged field count: {}\n",
            self.topic_uuid, self.tagged_field_count
        )
        .expect("KafkaRecordFeature write failed 1");
        write!(f, "{}", msg)
    }
}

impl KafkaRecordTopicRecord {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn deserialize<R: Read>(buffer: &mut R, mut len: usize) -> errors::Result<Self> {
        let mut rec = Self {
            ..Default::default()
        };

        if len > 0 {
            rec.topic_name = parser::read_compact_string(buffer)?;
            rec.name_length = rec.topic_name.len() as u8;
            len -= 1 + rec.name_length as usize;
        }

        if len > 16 {
            buffer
                .read_exact(&mut rec.topic_uuid)
                .expect("Failed to read from buffer!");
            len -= 16;
        }

        if len >= 1 {
            rec.tagged_field_count = parser::read_varint(buffer)?;
            len -= 1;
        }
        assert_eq!(
            len, 0,
            "Kafka record value deserialize should have exhaused entire length"
        );

        Ok(rec)
    }

    fn size(&self) -> usize {
        size_of(&self.frame_version)
            + size_of(&self.frame_type)
            + size_of(&self.version)
            + size_of(&self.name_length)
            + size_of(&self.topic_name)
            + size_of(&self.topic_uuid)
            + size_of(&self.tagged_field_count)
    }

    pub fn serialize<W: std::io::Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.frame_version)?;
        writer::write_bytes(resp, &self.frame_type)?;
        writer::write_bytes(resp, &self.version)?;
        // TODO - check me - what's the best way to encode
        writer::write_varint(resp, self.name_length as usize)?;
        writer::write_compact_string(resp, &self.topic_name)?;

        writer::write_bytes(resp, &self.topic_uuid)?;
        writer::write_varint(resp, self.tagged_field_count as usize)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct KafkaRecordPartitionRecord {
    pub frame_version: i8,
    pub frame_type: i8,
    pub version: i8,
    pub partition_id: i32,
    pub topic_uuid: [u8; 16],
    pub replica_array_length: i8,
    pub replica_array: Vec<i32>,
    pub insync_replica_array_length: i8,
    pub insync_replica_array: Vec<i32>,
    pub removing_replica_array_length: i8,
    pub adding_replica_array_length: i8,
    pub leader: i32,
    pub leader_epoch: i32,
    pub partition_epoch: i32,
    pub dir_array_length: i8,
    pub dir_array: Vec<[u8; 16]>, // eachmemer is 16 byte - interpret as UUID
    pub tagged_field_count: i8,
}

impl std::fmt::Display for KafkaRecordPartitionRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut msg = String::new();
        write!(
            &mut msg,
            "Frame version: {}, Frame Type: {}, Version: {}\n",
            self.frame_version, self.frame_type, self.version
        )
        .expect("KafkaRecordPartitionRecord failed 1");

        write!(
            &mut msg,
            "Partition ID: {}, topic UUID: {:?}\n",
            self.partition_id, self.topic_uuid
        )
        .expect("KafkaRecordPartitionRecord write failed 2");

        write!(
            &mut msg,
            "replica array length: {}, replica array: {:?}\n",
            self.replica_array_length, self.replica_array
        )
        .expect("KafkaRecordPartitionRecord write failed 3");

        write!(
            &mut msg,
            "insync replica array length: {}, insync replica array: {:?}\n",
            self.insync_replica_array_length, self.insync_replica_array
        )
        .expect("KafkaRecordPartitionRecord write failed 4");

        write!(
            &mut msg,
            "removing replica array length: {}, adding replica array length: {}\n",
            self.removing_replica_array_length, self.adding_replica_array_length
        )
        .expect("KafkaRecordPartitionRecord write failed 5");

        write!(
            &mut msg,
            "leader: {}, leader epoch: {}, partition_epoch: {}, dir length: {}\n",
            self.leader, self.leader_epoch, self.partition_epoch, self.dir_array_length
        )
        .expect("KafkaRecordPartitionRecord write failed 6");

        for i in 0..self.dir_array_length {
            write!(&mut msg, "Dir {}: {:?}\n", i, self.dir_array[i as usize])
                .expect("KafkaRecordPartitionRecord write failed 7");
        }

        write!(
            &mut msg,
            "tagged field count: {}\n",
            self.tagged_field_count
        )
        .expect("KafkaRecordPartitionRecord write failed 8");

        write!(f, "{}", msg)
    }
}

#[allow(dead_code)]
impl KafkaRecordPartitionRecord {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn deserialize<R: Read>(buffer: &mut R, mut len: usize) -> errors::Result<Self> {
        let mut rec = Self {
            ..Default::default()
        };
        rec.partition_id = parser::read_int(buffer)?;
        len -= 4;
        buffer
            .read_exact(&mut rec.topic_uuid)
            .expect("Failed to deserialize topic uuid!");
        len -= 16;
        rec.replica_array_length = parser::read_varint(buffer)?;
        len -= 1;
        rec.replica_array = vec![0_i32; rec.replica_array_length as usize];
        for i in 0..rec.replica_array_length as usize {
            rec.replica_array[i] = parser::read_int(buffer)?;
            len -= 4;
        }

        rec.insync_replica_array_length = parser::read_varint(buffer)?;
        len -= 1;
        rec.insync_replica_array = vec![0_i32; rec.insync_replica_array_length as usize];
        for i in 0..rec.insync_replica_array_length as usize {
            rec.insync_replica_array[i] = parser::read_int(buffer)?;
            len -= 4;
        }

        rec.removing_replica_array_length = parser::read_varint(buffer)?;
        len -= 1;
        rec.adding_replica_array_length = parser::read_varint(buffer)?;
        len -= 1;
        rec.leader = parser::read_int(buffer)?;
        len -= 4;
        rec.leader_epoch = parser::read_int(buffer)?;
        len -= 4;
        rec.partition_epoch = parser::read_int(buffer)?;
        len -= 4;

        rec.dir_array_length = parser::read_varint(buffer)?;
        len -= 1;
        rec.dir_array = vec![[0_u8; 16]; rec.dir_array_length as usize];
        for i in 0..rec.dir_array_length as usize {
            buffer
                .read_exact(&mut rec.dir_array[i])
                .expect("Failed to read directory array!");
            len -= 16;
        }

        if len > 1 {
            rec.tagged_field_count = parser::read_varint(buffer)?;
            len -= 1;
        }
        assert_eq!(
            len, 0,
            "Kafka partition record value deserialize should have exhaused entire length"
        );

        Ok(rec)
    }

    fn size(&self) -> usize {
        size_of(&self.frame_version)
            + size_of(&self.frame_type)
            + size_of(&self.version)
            + size_of(&self.partition_id)
            + size_of(&self.topic_uuid)
            + size_of(&self.replica_array_length)
            + size_of(&self.insync_replica_array_length)
            + size_of(&self.insync_replica_array)
            + size_of(&self.removing_replica_array_length)
            + size_of(&self.adding_replica_array_length)
            + size_of(&self.leader)
            + size_of(&self.leader_epoch)
            + size_of(&self.partition_epoch)
            + size_of(&self.dir_array_length)
            + size_of(&self.dir_array)
            + size_of(&self.tagged_field_count)
    }

    pub fn serialize<W: std::io::Write>(&self, _resp: &mut W) -> errors::Result<()> {
        todo!()
    }
}
