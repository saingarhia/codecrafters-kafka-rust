use crate::kafka::{basics, errors, metadata, parser, records, writer};
use std::io::{Read, Write};
use std::ptr::write_bytes;
use std::sync::{Arc, Mutex};

const PRODUCE_RESPONSE_UNKNOWN_TOPIC_OR_PARTITION: u16 = 3;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub transaction_id: u8,
    pub required_acks: u16,
    pub timeout: u32,
    pub topics: Vec<ProduceRequestTopic>,
    pub tag_buffer: u8,
}

impl std::fmt::Display for ProduceRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        let _ = std::fmt::write(
            &mut output,
            format_args!(
                "Produce Request: \ntx id: {}, required acks: {}, timeout: {}, topics: \n",
                self.transaction_id, self.required_acks, self.timeout
            ),
        );
        self.topics.iter().for_each(|t| {
            let _ = std::fmt::write(&mut output, format_args!("{}\n", t));
        });
        write!(f, "{}", output)
    }
}

impl ProduceRequest {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let transaction_id = parser::read_byte(req)? as u8;
        let required_acks = parser::read_short(req)? as u16;
        let timeout = parser::read_int(req)? as u32;
        let num_topics = parser::read_byte(req)? as u8;
        let mut topics = vec![];
        for _i in 0..num_topics - 1 {
            let p = ProduceRequestTopic::new(req)?;
            topics.push(p);
        }
        let tag_buffer = parser::read_byte(req)? as u8;
        Ok(Self {
            transaction_id,
            required_acks,
            timeout,
            topics,
            tag_buffer,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProduceRequestTopic {
    pub topic_name: Vec<u8>,
    pub partitions: Vec<ProduceRequestTopicPartition>,
    pub tag_buffer: u8,
}

impl std::fmt::Display for ProduceRequestTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        let _ = std::fmt::write(
            &mut output,
            format_args!(
                "Produce Topic: \nname: {}, partitions: \n",
                String::from_utf8(self.topic_name.clone()).unwrap()
            ),
        );
        self.partitions.iter().for_each(|p| {
            let _ = std::fmt::write(&mut output, format_args!("{}\n", p));
        });
        write!(f, "{}", output)
    }
}

impl ProduceRequestTopic {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let topic_name = parser::read_compact_string(req)?;
        let num_parts = parser::read_byte(req)? as u8;
        let mut partitions = vec![];
        for _i in 0..num_parts - 1 {
            let p = ProduceRequestTopicPartition::new(req)?;
            partitions.push(p);
        }
        let tag_buffer = parser::read_byte(req)? as u8;
        Ok(Self {
            topic_name,
            partitions,
            tag_buffer,
        })
    }

    #[allow(dead_code)]
    pub fn serialize<W: Write>(_resp: &mut W) -> errors::Result<()> {
        todo!()
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ProduceRequestTopicPartition {
    pub partition_idx: u32,
    pub record_batches: Vec<records::RecordsBatch>,
    pub tag_buffer: u8,
}

impl std::fmt::Display for ProduceRequestTopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        let _ = std::fmt::write(
            &mut output,
            format_args!(
                "Produce Topic Partition: \nidx: {}, records: \n",
                self.partition_idx
            ),
        );
        self.record_batches.iter().for_each(|r| {
            let _ = std::fmt::write(&mut output, format_args!("{}\n", r));
        });
        write!(f, "{}", output)
    }
}

impl ProduceRequestTopicPartition {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let partition_idx = parser::read_int(req)? as u32;
        let num_record_batches = parser::read_byte(req)? as u8;
        let mut record_batches = vec![];
        for _i in 0..num_record_batches - 1 {
            //let p = records::RecordsBatch::deserialize(req)?;
            //record_batches.push(p);
            record_batches.push(records::RecordsBatch::new());
        }
        let tag_buffer = parser::read_byte(req)? as u8;
        Ok(Self {
            partition_idx,
            record_batches,
            tag_buffer,
        })
    }

    #[allow(dead_code)]
    pub fn serialize<W: Write>(_resp: &mut W) -> errors::Result<()> {
        todo!()
    }
}

///////////////////////// response section ////////////////////
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProduceResponse {
    topics: Vec<ProduceResponseTopic>,
    throttle_time: u32,
    tag_buffer: u8,
}

impl ProduceResponse {
    pub fn new(request: &ProduceRequest, metadata: &Arc<Mutex<metadata::Metadata>>) -> Self {
        let topics = request.topics.iter().fold(vec![], |mut acc, topic| {
            acc.push(ProduceResponseTopic::new(topic, metadata));
            acc
        });
        Self {
            topics,
            throttle_time: 0,
            tag_buffer: request.tag_buffer,
        }
    }

    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_varint(resp, 1 + self.topics.len())?;
        self.topics
            .iter()
            .try_for_each(|topic| topic.serialize(resp))?;
        writer::write_bytes(resp, &self.throttle_time)?;
        writer::write_bytes(resp, &self.tag_buffer)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProduceResponseTopic {
    topic_name: Vec<u8>,
    partitions: Vec<ProduceResponseTopicPartition>,
    tag_buffer: u8,
}

impl ProduceResponseTopic {
    pub fn new(request: &ProduceRequestTopic, metadata: &Arc<Mutex<metadata::Metadata>>) -> Self {
        Self {
            topic_name: request.topic_name.clone(),
            partitions: request.partitions.iter().fold(vec![], |mut acc, part| {
                acc.push(ProduceResponseTopicPartition::new(part, metadata));
                acc
            }),
            tag_buffer: request.tag_buffer,
        }
    }

    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_varint(resp, self.topic_name.len() + 1)?;
        resp.write_all(&self.topic_name)?;
        writer::write_varint(resp, 1 + self.partitions.len())?;
        self.partitions
            .iter()
            .try_for_each(|part| part.serialize(resp))?;
        writer::write_bytes(resp, &self.tag_buffer)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProduceResponseTopicPartition {
    partition_id: u32,
    error_code: u16,
    base_offset: u64,
    log_append_time: u64,
    log_start_offset: u64,
    errors: Vec<ProduceResponseTopicPartitionError>,
    error_message: basics::CompactNullableString,
    tag_buffer: u8,
}

impl ProduceResponseTopicPartition {
    pub fn new(
        request: &ProduceRequestTopicPartition,
        _metadata: &Arc<Mutex<metadata::Metadata>>,
    ) -> Self {
        Self {
            partition_id: request.partition_idx,
            error_code: PRODUCE_RESPONSE_UNKNOWN_TOPIC_OR_PARTITION,
            base_offset: 0xFFFFFFFFFFFFFFFF,
            log_append_time: 0xFFFFFFFFFFFFFFFF,
            log_start_offset: 0xFFFFFFFFFFFFFFFF,
            errors: vec![],
            error_message: basics::CompactNullableString::from(vec![]),
            tag_buffer: request.tag_buffer,
        }
    }

    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.partition_id)?;
        writer::write_bytes(resp, &self.error_code)?;
        writer::write_bytes(resp, &self.base_offset)?;
        writer::write_bytes(resp, &self.log_append_time)?;
        writer::write_bytes(resp, &self.log_start_offset)?;
        writer::write_varint(resp, 1 + self.errors.len())?;
        self.errors.iter().try_for_each(|e| e.serialize(resp))?;
        self.error_message.serialize(resp)?;
        writer::write_bytes(resp, &self.tag_buffer)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProduceResponseTopicPartitionError {
    length: u8,
}

impl ProduceResponseTopicPartitionError {
    #[allow(dead_code)]
    pub fn new(_request: &ProduceRequest, _metadata: &Arc<Mutex<metadata::Metadata>>) -> Self {
        todo!()
    }

    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.length)
    }
}
