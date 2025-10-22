use crate::kafka::{errors, metadata, parser, records, writer};
use std::io::{Read, Write};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub transaction_id: u8,
    pub required_acks: u16,
    pub timeout: u32,
    pub topics: Vec<ProduceTopic>,
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
            let p = ProduceTopic::new(req)?;
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
pub struct ProduceTopic {
    pub topic_name: Vec<u8>,
    pub partitions: Vec<ProduceTopicPartition>,
    pub tag_buffer: u8,
}

impl std::fmt::Display for ProduceTopic {
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

impl ProduceTopic {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let topic_name = parser::read_compact_string(req)?;
        let num_parts = parser::read_byte(req)? as u8;
        let mut partitions = vec![];
        for _i in 0..num_parts - 1 {
            let p = ProduceTopicPartition::new(req)?;
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
pub struct ProduceTopicPartition {
    pub partition_idx: u32,
    pub record_batches: Vec<records::RecordsBatch>,
    pub tag_buffer: u8,
}

impl std::fmt::Display for ProduceTopicPartition {
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

impl ProduceTopicPartition {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let partition_idx = parser::read_int(req)? as u32;
        let num_record_batches = parser::read_byte(req)? as u8;
        let mut record_batches = vec![];
        for _i in 0..num_record_batches - 1 {
            let p = records::RecordsBatch::deserialize(req)?;
            record_batches.push(p);
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
