use super::{errors, writer};
use std::io::Write;

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
    pub records: Vec<KafkaRecord>,
}

impl RecordsBatch {
    pub fn new() -> Self {
        Self {
            base_sequence: 0x33,
            records: vec![],
            ..Default::default()
        }
    }

    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.base_offset)?;
        writer::write_bytes(resp, &self.batch_length)?;
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
        println!("------ encoding records - count: {}", self.records.len());
        writer::write_bytes(resp, &(self.records.len() as u32 + 1))?;
        self.records
            .iter()
            .try_for_each(|record| record.serialize(resp))?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct KafkaRecord {
    pub length: i32,
    pub attributes: i8,
    pub timestamp_delta: i8,
    pub offset_delta: i8,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub headers: Vec<KafkaRecordHeader>,
}

impl KafkaRecord {
    pub fn new() -> Self {
        Self {
            length: 0,
            ..Default::default()
        }
    }

    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.length)?;
        writer::write_bytes(resp, &self.attributes)?;
        writer::write_bytes(resp, &self.timestamp_delta)?;
        writer::write_bytes(resp, &self.offset_delta)?;
        writer::write_compact_string(resp, &self.key)?;
        writer::write_compact_string(resp, &self.value)?;
        writer::write_bytes(resp, &(self.headers.len() as u8 + 1))?;
        self.headers.iter().try_for_each(|h| h.serialize(resp))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct KafkaRecordHeader {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KafkaRecordHeader {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_compact_string(resp, &self.key)?;
        writer::write_compact_string(resp, &self.value)?;
        Ok(())
    }
}
