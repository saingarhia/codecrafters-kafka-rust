use crate::kafka::{errors, parser, writer};
use std::io::{Read, Write};

const FETCH_RESPONSE_UNKNOWN_TOPIC: u16 = 100;

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct FetchRequestForgottenTopic {
    topic_id: u128,
    partitions: Vec<u32>,
    tag_buffer: u8,
}
impl FetchRequestForgottenTopic {
    fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let topic_id = parser::read_u128(req)?;
        let num_partitions = parser::read_byte(req)? as u8;
        let mut partitions = vec![];
        for _i in 0..num_partitions {
            let p = parser::read_int(req)? as u32;
            partitions.push(p);
        }
        let tag_buffer = parser::read_byte(req)? as u8;
        Ok(Self {
            topic_id,
            partitions,
            tag_buffer,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct FetchPartition {
    partition: u32,
    current_leader_epoch: u32,
    fetch_offset: u64,
    last_fetched_epoch: u32,
    log_start_offset: u64,
    partition_max_bytes: u32,
    tag_buffer: u8,
}

impl FetchPartition {
    fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let partition = parser::read_int(req)? as u32;
        let current_leader_epoch = parser::read_int(req)? as u32;
        let fetch_offset = parser::read_u64(req)?;
        let last_fetched_epoch = parser::read_int(req)? as u32;
        let log_start_offset = parser::read_u64(req)?;
        let partition_max_bytes = parser::read_int(req)? as u32;
        let tag_buffer = parser::read_byte(req)? as u8;

        Ok(Self {
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
            tag_buffer,
        })
    }
}

impl std::fmt::Display for FetchPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "partition: {}, fetch offset: {}, max: {}",
            self.partition, self.fetch_offset, self.partition_max_bytes
        )
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FetchTopic {
    topic_id: u128,
    partitions: Vec<FetchPartition>,
    tag_buffer: u8,
}

impl std::fmt::Display for FetchTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        let _ = std::fmt::write(
            &mut output,
            format_args!(
                "topic: {}, partitions: {}",
                self.topic_id,
                self.partitions.len()
            ),
        );
        self.partitions.iter().for_each(|p| {
            let _ = std::fmt::write(&mut output, format_args!("Partition info: {}", p));
        });
        write!(f, "{}", output)
    }
}

impl FetchTopic {
    fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let topic_id = parser::read_u128(req)?;
        println!("topic id: {topic_id}");
        let num_partitions = parser::read_byte(req)? as u8;
        let mut partitions = vec![];
        println!("num partitions: {num_partitions}");
        for _i in 0..num_partitions - 1 {
            let p = FetchPartition::new(req)?;
            println!("new partition: {p:?}");
            partitions.push(p);
        }
        // tag buffer
        let tag_buffer = parser::read_byte(req)? as u8;
        Ok(Self {
            topic_id,
            partitions,
            tag_buffer,
        })
    }
}

impl FetchTopic {}

// Version 16
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FetchRequest {
    max_wait_ms: u32,
    min_bytes: u32,
    max_bytes: u32,
    isolation_level: u8,
    session_id: u32,
    session_epoch: u32,
    topics: Vec<FetchTopic>,
    forgotten_topics_data: Vec<FetchRequestForgottenTopic>,
    rack_id: Vec<u8>,
    tag_buffer: u8,
}

impl std::fmt::Display for FetchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        let _ = std::fmt::write(
            &mut output,
            format_args!(
                "Fetch Request: \nsession id: {}, max_wait_ms: {}, min_bytes: {}, topics: \n",
                self.session_id, self.max_wait_ms, self.min_bytes
            ),
        );
        self.topics.iter().for_each(|t| {
            let _ = std::fmt::write(&mut output, format_args!("{}\n", t));
        });
        write!(f, "{}", output)
    }
}
impl FetchRequest {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let max_wait_ms = parser::read_int(req)? as u32;
        let min_bytes = parser::read_int(req)? as u32;
        let max_bytes = parser::read_int(req)? as u32;
        let isolation_level = parser::read_byte(req)? as u8;
        let session_id = parser::read_int(req)? as u32;
        let session_epoch = parser::read_int(req)? as u32;

        println!("session epoch: {session_epoch}");
        let num_topics = parser::read_byte(req)? as u8;
        println!("num_topics: {num_topics}");
        let mut topics = vec![];
        for _i in 0..num_topics - 1 {
            let p = FetchTopic::new(req)?;
            println!("new topic extracted: {p:?}");
            topics.push(p);
        }
        let num_forgotten_topics = parser::read_byte(req)? as u8;
        println!("num_forgotten_topics: {num_forgotten_topics}");
        let mut forgotten_topics_data = vec![];
        for _i in 0..num_forgotten_topics - 1 {
            let p = FetchRequestForgottenTopic::new(req)?;
            forgotten_topics_data.push(p);
        }
        println!("Done with all the forgotten_topics_data");
        let rack_id = parser::read_compact_string(req)?;
        println!("rack_id: {rack_id:?}");
        let tag_buffer = parser::read_byte(req)? as u8;
        println!("tag buffer: {tag_buffer}");

        Ok(Self {
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
            tag_buffer,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
struct FetchResponseAbortedTransaction {
    producer_id: u64,
    first_offset: u64,
    tag_buffer: u8,
}

impl FetchResponseAbortedTransaction {
    fn new(_req: &FetchRequest) -> Self {
        Self {
            ..Default::default()
        }
    }

    fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.producer_id)?;
        writer::write_bytes(resp, &self.first_offset)?;
        writer::write_bytes(resp, &self.tag_buffer)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
struct FetchResponsePartition {
    partiton_index: u32,
    error_code: u16,
    high_watermark: u64,
    last_stable_offset: u64,
    log_start_offset: u64,
    aborted_transactions: Vec<FetchResponseAbortedTransaction>,
    preferred_read_replica: u32,
    records: Vec<u8>,
    tag_buffer: u8,
}

impl FetchResponsePartition {
    fn new(_part: &FetchPartition) -> Self {
        let aborted_transactions = vec![];
        Self {
            aborted_transactions,
            error_code: FETCH_RESPONSE_UNKNOWN_TOPIC,
            ..Default::default()
        }
    }

    fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.partiton_index)?;
        writer::write_bytes(resp, &self.error_code)?;
        writer::write_bytes(resp, &self.high_watermark)?;
        writer::write_bytes(resp, &self.last_stable_offset)?;
        writer::write_bytes(resp, &self.log_start_offset)?;
        writer::write_bytes(resp, &(self.aborted_transactions.len() as u8 + 1))?;
        self.aborted_transactions
            .iter()
            .try_for_each(|t| t.serialize(resp))?;
        writer::write_bytes(resp, &self.preferred_read_replica)?;
        writer::write_compact_string(resp, &self.records)?;
        writer::write_bytes(resp, &self.tag_buffer)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct FetchResponseInternal {
    topic_id: u128,
    partitions: Vec<FetchResponsePartition>,
    tag_buffer: u8,
}

impl FetchResponseInternal {
    pub fn new(topic: &FetchTopic) -> Self {
        let partitions = topic.partitions.iter().fold(vec![], |mut acc, part| {
            acc.push(FetchResponsePartition::new(part));
            acc
        });
        Self {
            topic_id: topic.topic_id,
            partitions,
            tag_buffer: topic.tag_buffer,
        }
    }

    fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.topic_id)?;
        writer::write_bytes(resp, &(self.partitions.len() as u8))?;
        self.partitions.iter().try_for_each(|p| p.serialize(resp))?;
        writer::write_bytes(resp, &self.tag_buffer)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FetchResponse {
    throttle_time_ms: u32,
    error_code: u16,
    session_id: u32,
    responses: Vec<FetchResponseInternal>,
    tag_buffer: u8,
}

impl FetchResponse {
    pub fn new(req: &FetchRequest) -> Self {
        let responses = req.topics.iter().fold(vec![], |mut acc, t| {
            acc.push(FetchResponseInternal::new(t));
            acc
        });
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            session_id: req.session_id,
            responses,
            tag_buffer: req.tag_buffer,
        }
    }

    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.throttle_time_ms)?;
        writer::write_bytes(resp, &self.error_code)?;
        writer::write_bytes(resp, &self.session_id)?;
        writer::write_bytes(resp, &(self.responses.len() as u8))?;
        self.responses.iter().try_for_each(|r| r.serialize(resp))?;
        writer::write_bytes(resp, &self.tag_buffer)?;
        Ok(())
    }
}
