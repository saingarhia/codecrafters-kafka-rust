use crate::kafka::{errors, parser, writer};
use std::io::{Read, Write};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FetchPartition {
    partition: u32,
    fetch_offset: u64,
    partition_max_bytes: u32,
}

impl FetchPartition {
    fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let partition = parser::read_int(req)? as u32;
        let fetch_offset = parser::read_u64(req)?;
        let partition_max_bytes = parser::read_int(req)? as u32;
        Ok(Self {
            partition,
            fetch_offset,
            partition_max_bytes,
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
    topic: Vec<u8>,
    partitions: Vec<FetchPartition>,
}

impl std::fmt::Display for FetchTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        let topic_name =
            String::from_utf8(self.topic.clone()).expect("expected proper topic name!");
        let _ = std::fmt::write(&mut output, format_args!("topic: {}, ", topic_name));
        self.partitions.iter().for_each(|p| {
            let _ = std::fmt::write(&mut output, format_args!("Partition info: {}", p));
        });
        write!(f, "{}", output)
    }
}

impl FetchTopic {
    fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let topic = parser::read_compact_string(req)?;
        let num_partitions = parser::read_byte(req)? as u8;
        let mut partitions = vec![];
        for _i in 0..num_partitions {
            let p = FetchPartition::new(req)?;
            partitions.push(p);
        }
        Ok(Self { topic, partitions })
    }
}

impl FetchTopic {}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FetchRequest {
    replica_id: u32,
    max_wait_ms: u32,
    min_bytes: u32,
    topics: Vec<FetchTopic>,
}

impl std::fmt::Display for FetchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        let _ = std::fmt::write(
            &mut output,
            format_args!(
                "Fetch Request: \nreplica id: {}, max_wait_ms: {}, min_bytes: {}, topics: \n",
                self.replica_id, self.max_wait_ms, self.min_bytes
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
        let replica_id = parser::read_int(req)? as u32;
        let max_wait_ms = parser::read_int(req)? as u32;
        let min_bytes = parser::read_int(req)? as u32;
        let num_topics = parser::read_byte(req)? as u8;
        let mut topics = vec![];
        for _i in 0..num_topics {
            let p = FetchTopic::new(req)?;
            topics.push(p);
        }
        Ok(Self {
            replica_id,
            max_wait_ms,
            min_bytes,
            topics,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FetchResponse {}

impl FetchResponse {
    pub fn serialize<W: Write>(&self, _resp: &W) -> errors::Result<()> {
        Ok(())
    }
}
