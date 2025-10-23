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
        writeln!(
            &mut msg,
            "base_offset: {}, batch_length: {}",
            self.base_offset, self.batch_length
        )
        .expect("filed to write 1");
        writeln!(
            &mut msg,
            "leader epoch: {}, magic: {}, crc: {:0x}",
            self.partition_leader_epoch, self.magic, self.crc
        )
        .expect("failed to writeln! 2");
        writeln!(
            &mut msg,
            "Attributes: {:0x}, last offset delta: {}",
            self.attributes, self.last_offset_delta
        )
        .expect("filed to write 3!");
        writeln!(
            &mut msg,
            "Base timestamp: {:0x}, Max timestamp: {:0x}",
            self.base_timestamp, self.max_timestamp
        )
        .expect("filed to write 4!");
        writeln!(
            &mut msg,
            "produced ID: {:0x}, producer epoch: {:0x}, num records: {}",
            self.producer_id, self.producer_epoch, self.rec_length
        )
        .expect("filed to write 5!");
        self.records.iter().enumerate().for_each(|(i, record)| {
            writeln!(&mut msg, "record {}:\n {:?}", i, record).expect("failed to write record!");
        });
        writeln!(f, "{}", msg)
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

    pub fn deserialize<R: Read>(input_buffer: &mut R) -> errors::Result<Self> {
        let mut rec = RecordsBatch {
            ..Default::default()
        };

        rec.base_offset = parser::read_u64(input_buffer)?;
        rec.batch_length = parser::read_int(input_buffer)?;

        // lets create a cursor over batch length slice
        // TODO - should have to copy but create it over the existing data
        let mut data_buffer: Vec<u8> = vec![0_u8; rec.batch_length as usize];
        input_buffer.read_exact(&mut data_buffer)?;
        let mut buffer = std::io::Cursor::new(data_buffer);

        rec.partition_leader_epoch = parser::read_int(&mut buffer)?;
        rec.magic = parser::read_byte(&mut buffer)?;
        rec.crc = parser::read_int(&mut buffer)?;
        rec.attributes = parser::read_short(&mut buffer)?;
        rec.last_offset_delta = parser::read_int(&mut buffer)?;
        rec.base_timestamp = parser::read_u64(&mut buffer)?;
        rec.max_timestamp = parser::read_u64(&mut buffer)?;
        rec.producer_id = parser::read_u64(&mut buffer)?;
        rec.producer_epoch = parser::read_short(&mut buffer)?;
        rec.base_sequence = parser::read_int(&mut buffer)?;
        rec.rec_length = parser::read_int(&mut buffer)?;

        for _i in 0..rec.rec_length {
            match KafkaRecord::deserialize(&mut buffer) {
                Ok(r) => rec.records.push(r),
                Err(e) => {
                    println!("*********************** Unable to parse KafkaRecord!! Err: {e:?}");
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn serialize<W: std::io::Write>(&self, resp: &mut W) -> errors::Result<()> {
        //let (crc, batch_length) = self.calc_meta()?;
        //println!(
        //    "---------- crc: {crc:#x} vs {:#x}, batch length: {batch_length} vs {} ----------",
        //    self.crc, self.batch_length
        //);
        let length = self.size() as i32;
        println!("=================== calculated batch length: {length} =================");
        writer::write_bytes(resp, &self.base_offset)?;
        writer::write_bytes(resp, &length)?;
        writer::write_bytes(resp, &self.partition_leader_epoch)?;
        writer::write_bytes(resp, &self.magic)?;
        writer::write_bytes(resp, &self.crc)?;
        writer::write_bytes(resp, &self.attributes)?;
        writer::write_bytes(resp, &self.last_offset_delta)?;
        writer::write_bytes(resp, &self.base_timestamp)?;
        writer::write_bytes(resp, &self.max_timestamp)?;
        writer::write_bytes(resp, &self.producer_id)?;
        writer::write_bytes(resp, &self.producer_epoch)?;
        writer::write_bytes(resp, &self.base_sequence)?;
        // this length is 32-bit
        writer::write_bytes(resp, &(self.rec_length as u32 - 1))?;
        self.records
            .iter()
            .try_for_each(|record| record.serialize(resp))?;

        let _ = writer::write_bytes(resp, &0_i8);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        //size_of(&self.base_offset)
        //    + size_of(&self.batch_length)
        size_of(&self.partition_leader_epoch)
            + size_of(&self.magic)
            + size_of(&self.crc)
            + size_of(&self.attributes)
            + size_of(&self.last_offset_delta)
            + size_of(&self.base_timestamp)
            + size_of(&self.max_timestamp)
            + size_of(&self.producer_id)
            + size_of(&self.producer_epoch)
            + size_of(&self.base_sequence)
            + size_of(&self.rec_length)
            + self.records.iter().take(1).fold(0, |acc, r| acc + r.size())
            + 1
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
        writeln!(
            &mut msg,
            "length: {}, attributes: {}, timestamp_delta: {}, offset_delta: {}",
            self.length, self.attributes, self.timestamp_delta, self.offset_delta
        )
        .expect("KR write failed 1!");
        writeln!(
            &mut msg,
            "key length: {}, key: {:?}",
            self.key_length, self.key
        )
        .expect("KR write failed 2!");
        writeln!(
            &mut msg,
            "value length: {}, value: {:?}",
            self.value_length, self.value
        )
        .expect("KR write failed 3!");
        writeln!(&mut msg, "num headers: {}", self.header_count).expect("KR write failed 4!");
        for i in 0..self.header_count {
            writeln!(&mut msg, "Header {} - {:?}", i, self.headers[i as usize])
                .expect("KR Write failed 5!");
        }

        writeln!(f, "{}", msg)
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
        let length = parser::read_varint(buffer)?;
        if length <= 0 {
            return Err(
                errors::KafkaErrors::InvalidWriterArg("Invalid record length".to_string()).into(),
            );
        }

        let mut record_data = vec![0u8; length as usize];
        buffer.read_exact(&mut record_data)?;
        let mut record_cursor = std::io::Cursor::new(record_data);

        let attributes = parser::read_byte(&mut record_cursor)?;
        let timestamp_delta = parser::read_varint(&mut record_cursor)? as i8; // Assuming it fits
        let offset_delta = parser::read_varint(&mut record_cursor)? as i8; // Assuming it fits
        let key_length = parser::read_varint(&mut record_cursor)? as i8;
        let mut key = vec![];
        if key_length > 0 {
            key = vec![0_u8; key_length as usize];
            record_cursor.read_exact(&mut key)?;
        }

        let value_length = parser::read_varint(&mut record_cursor)?;
        let value = if value_length > 0 {
            KafkaRecordValue::deserialize(&mut record_cursor, value_length as usize)?
        } else {
            KafkaRecordValue::Invalid
        };

        let header_count = parser::read_varint(&mut record_cursor)? as i8;
        // Header parsing logic would go here if needed

        Ok(Self {
            length: length as i8,
            attributes,
            timestamp_delta,
            offset_delta,
            key_length,
            key,
            value_length: value_length as i8,
            value,
            header_count,
            headers: vec![],
        })
    }

    fn size(&self) -> usize {
        size_of(&self.attributes)
            + size_of(&self.timestamp_delta)
            + size_of(&self.offset_delta)
            + size_of(&self.key_length)
            + self.key.len()
            //+ size_of(&self.value_length)
            + self.value.size()
            + size_of(&self.header_count)
            + self.headers.iter().fold(0, |acc, h| acc + h.size())
    }

    pub fn serialize<W: std::io::Write>(&self, resp: &mut W) -> errors::Result<()> {
        // need to determine length dynamically
        writer::write_varint_main(resp, self.size() as i32)?;
        writer::write_bytes(resp, &self.attributes)?;
        writer::write_varint(resp, self.timestamp_delta as usize)?;
        writer::write_varint_main(resp, self.offset_delta as i32)?;
        writer::write_compact_string(resp, &self.key)?;
        // if valus is compact string, ensure that it gets
        // 1 byte accounted for
        self.value.serialize(resp)?;
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
        writeln!(&mut msg, "key: {:?}, value: {:?}", self.key, self.value)
            .expect("KRH write failed 1!");
        writeln!(f, "{}", msg)
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
            Self::Invalid => writeln!(f, "Invalid!"),
            Self::KafkaRecordFeatureType(v) => writeln!(f, "{}", v),
            Self::KafkaRecordTopicRecordType(v) => writeln!(f, "{}", v),
            Self::KafkaRecordPartitionType(v) => writeln!(f, "{}", v),
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
            "frame version: {}, frame type: {}, version: {}, data_len: {}",
            frame_version, frame_type, version, data_len
        );
        let mut value_reader = buffer.take(data_len as u64 - 3); // -3 for the fields we just read
        match frame_type {
            KAFKA_RECORDTYPE_FEATURE => {
                // feature record
                let mut rec = KafkaRecordFeature::deserialize(&mut value_reader)?;
                rec.frame_version = frame_version;
                rec.frame_type = frame_type;
                rec.version = version;
                Ok(Self::KafkaRecordFeatureType(rec))
            }
            KAFKA_RECORDTYPE_TOPIC => {
                // topic record
                let mut rec = KafkaRecordTopicRecord::deserialize(&mut value_reader)?;
                rec.frame_version = frame_version;
                rec.frame_type = frame_type;
                rec.version = version;
                Ok(Self::KafkaRecordTopicRecordType(rec))
            }
            KAFKA_RECORDTYPE_PARTITION => {
                // partition record
                let mut rec = KafkaRecordPartitionRecord::deserialize(&mut value_reader)?;
                rec.frame_version = frame_version;
                rec.frame_type = frame_type;
                rec.version = version;
                Ok(Self::KafkaRecordPartitionType(rec))
            }
            //112 => {}
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
        writeln!(
            &mut msg,
            "Frame version: {}, Frame Type: {}, Version: {}",
            self.frame_version, self.frame_type, self.version
        )
        .expect("KafkaRecordFeature write failed 1");

        writeln!(
            &mut msg,
            "Name length: {}, Name: {:?}",
            self.name_length, self.name
        )
        .expect("KafkaRecordFeature write failed 1");

        writeln!(
            &mut msg,
            "feature level: {}, tagged field count: {}",
            self.feature_level, self.tagged_field_count
        )
        .expect("KafkaRecordFeature write failed 1");
        writeln!(f, "{}", msg)
    }
}

impl KafkaRecordFeature {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn deserialize<R: Read>(buffer: &mut R) -> errors::Result<Self> {
        let mut rec = Self {
            ..Default::default()
        };

        rec.name = parser::read_compact_string(buffer)?;
        rec.name_length = rec.name.len() as u8;
        rec.feature_level = parser::read_short(buffer)?;
        rec.tagged_field_count = parser::read_varint(buffer)? as i8;

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
        writeln!(
            &mut msg,
            "Frame version: {}, Frame Type: {}, Version: {}",
            self.frame_version, self.frame_type, self.version
        )
        .expect("KafkaRecordFeature write failed 1");

        writeln!(
            &mut msg,
            "Name length: {}, topic Name: {:?}",
            self.name_length, self.topic_name
        )
        .expect("KafkaRecordFeature write failed 1");

        writeln!(
            &mut msg,
            "topic UUID: {:?}, tagged field count: {}",
            self.topic_uuid, self.tagged_field_count
        )
        .expect("KafkaRecordFeature write failed 1");
        writeln!(f, "{}", msg)
    }
}

impl KafkaRecordTopicRecord {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn deserialize<R: Read>(buffer: &mut R) -> errors::Result<Self> {
        let mut rec = Self {
            ..Default::default()
        };

        rec.topic_name = parser::read_compact_string(buffer)?;
        rec.name_length = rec.topic_name.len() as u8;

        buffer
            .read_exact(&mut rec.topic_uuid)
            .expect("Failed to read from buffer!");

        rec.tagged_field_count = parser::read_varint(buffer)? as i8;

        Ok(rec)
    }

    // Only value (name) is part of record response
    fn size(&self) -> usize {
        self.topic_name.len() + 1
        //size_of(&self.tagged_field_count)
    }

    pub fn serialize<W: std::io::Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_compact_string(resp, &self.topic_name)?;

        //writer::write_varint(resp, self.tagged_field_count as usize)?;
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
        writeln!(
            &mut msg,
            "Frame version: {}, Frame Type: {}, Version: {}",
            self.frame_version, self.frame_type, self.version
        )
        .expect("KafkaRecordPartitionRecord failed 1");

        writeln!(
            &mut msg,
            "Partition ID: {}, topic UUID: {:?}",
            self.partition_id, self.topic_uuid
        )
        .expect("KafkaRecordPartitionRecord write failed 2");

        writeln!(
            &mut msg,
            "replica array length: {}, replica array: {:?}",
            self.replica_array_length, self.replica_array
        )
        .expect("KafkaRecordPartitionRecord write failed 3");

        writeln!(
            &mut msg,
            "insync replica array length: {}, insync replica array: {:?}",
            self.insync_replica_array_length, self.insync_replica_array
        )
        .expect("KafkaRecordPartitionRecord write failed 4");

        writeln!(
            &mut msg,
            "removing replica array length: {}, adding replica array length: {}",
            self.removing_replica_array_length, self.adding_replica_array_length
        )
        .expect("KafkaRecordPartitionRecord write failed 5");

        writeln!(
            &mut msg,
            "leader: {}, leader epoch: {}, partition_epoch: {}, dir length: {}",
            self.leader, self.leader_epoch, self.partition_epoch, self.dir_array_length
        )
        .expect("KafkaRecordPartitionRecord write failed 6");

        for i in 0..self.dir_array_length {
            writeln!(&mut msg, "Dir {}: {:?}", i, self.dir_array[i as usize])
                .expect("KafkaRecordPartitionRecord write failed 7");
        }

        writeln!(&mut msg, "tagged field count: {}", self.tagged_field_count)
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

    pub fn deserialize<R: Read>(buffer: &mut R) -> errors::Result<Self> {
        let mut rec = Self {
            ..Default::default()
        };
        rec.partition_id = parser::read_int(buffer)?;
        buffer
            .read_exact(&mut rec.topic_uuid)
            .expect("Failed to deserialize topic uuid!");
        rec.replica_array_length = parser::read_varint(buffer)? as i8;
        rec.replica_array = vec![0_i32; rec.replica_array_length as usize];
        for i in 0..rec.replica_array_length as usize {
            rec.replica_array[i] = parser::read_int(buffer)?;
        }

        rec.insync_replica_array_length = parser::read_varint(buffer)? as i8;
        rec.insync_replica_array = vec![0_i32; rec.insync_replica_array_length as usize];
        for i in 0..rec.insync_replica_array_length as usize {
            rec.insync_replica_array[i] = parser::read_int(buffer)?;
        }

        rec.removing_replica_array_length = parser::read_varint(buffer)? as i8;
        rec.adding_replica_array_length = parser::read_varint(buffer)? as i8;
        rec.leader = parser::read_int(buffer)?;
        rec.leader_epoch = parser::read_int(buffer)?;
        rec.partition_epoch = parser::read_int(buffer)?;

        rec.dir_array_length = parser::read_varint(buffer)? as i8;
        rec.dir_array = vec![[0_u8; 16]; rec.dir_array_length as usize];
        for i in 0..rec.dir_array_length as usize {
            buffer
                .read_exact(&mut rec.dir_array[i])
                .expect("Failed to read directory array!");
        }

        rec.tagged_field_count = parser::read_varint(buffer)? as i8;

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
