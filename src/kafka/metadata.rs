use crate::kafka::errors;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

use crate::kafka::parser;

use super::records;

pub type LogBatchRecords = [records::RecordsBatch; 4];

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub uuid: u128,
    pub record_id1: usize,
    pub record_id2: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub partition_id: i32,
    pub record_id1: usize,
    pub record_id2: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Metadata {
    pub topic_map: HashMap<Vec<u8>, TopicMetadata>,
    pub partition_map: HashMap<u128, Vec<PartitionMetadata>>,
    pub records: LogBatchRecords,
}

impl Metadata {
    pub fn new(filename: &str) -> errors::Result<Self> {
        let f = File::open(filename)?;
        let mut reader = BufReader::new(f);
        Self::decode(&mut reader)
    }

    fn decode<R: Read>(buffer: &mut R) -> errors::Result<Self> {
        let mut topic_map = HashMap::new();
        let mut partition_map = HashMap::new();
        let mut records: [records::RecordsBatch; 4] = [
            records::RecordsBatch::new(),
            records::RecordsBatch::new(),
            records::RecordsBatch::new(),
            records::RecordsBatch::new(),
        ];

        for rec in 0..4 {
            let base_offset = parser::read_u64(buffer)?;
            records[rec].base_offset = base_offset;
            println!("base offset: {:?}", base_offset);
            let batch_length = parser::read_int(buffer)?;
            println!("batch length: {:?}", batch_length);
            records[rec].batch_length = batch_length;
            let partition_leader_epoch = parser::read_int(buffer)?;
            records[rec].partition_leader_epoch = partition_leader_epoch;
            let magic = parser::read_byte(buffer)?;
            records[rec].magic = magic;
            let crc = parser::read_int(buffer)?;
            records[rec].crc = crc;
            let attributes = parser::read_short(buffer)?;
            records[rec].attributes = attributes;
            let last_offset_delta = parser::read_int(buffer)?;
            records[rec].last_offset_delta = last_offset_delta;
            let base_timestamp = parser::read_u64(buffer)?;
            records[rec].base_timestamp = base_timestamp;
            let max_timestamp = parser::read_u64(buffer)?;
            records[rec].max_timestamp = max_timestamp;
            let producer_id = parser::read_u64(buffer)?;
            records[rec].producer_id = producer_id;
            let producer_epoch = parser::read_short(buffer)?;
            records[rec].producer_epoch = producer_epoch;
            let base_sequence = parser::read_int(buffer)?;
            records[rec].base_sequence = base_sequence;
            let record_size = parser::read_int(buffer)?;

            records[rec].records = vec![records::KafkaRecord::new(); record_size as usize];

            println!("Record size: {:?}", record_size);

            for i in 0..record_size as usize {
                let length = parser::read_varint(buffer)?;
                records[rec].records[i].length = length;
                println!("record {i} len: {:?}", length);
                let attributes = parser::read_byte(buffer)?;
                records[rec].records[i].attributes = attributes;
                let timestamp_delta = parser::read_byte(buffer)?;
                records[rec].records[i].timestamp_delta = timestamp_delta;
                let offset_delta = parser::read_byte(buffer)?;
                records[rec].records[i].offset_delta = offset_delta;
                let key_length = parser::read_varint(buffer)?;
                if key_length > 0 {
                    let mut key = vec![0_u8; key_length as usize];
                    buffer.read_exact(&mut key)?;
                    records[rec].records[i].key = key;
                }
                assert_eq!(key_length, -1);
                let value_length = parser::read_varint(buffer)?;
                if key_length > 0 {
                    let mut value = vec![0_u8; value_length as usize];
                    buffer.read_exact(&mut value)?;
                    records[rec].records[i].value = value;
                }
                println!("value {i} length: {:?}", value_length);
                let _frame_version = parser::read_byte(buffer)?;
                let value_type = parser::read_byte(buffer)?;
                println!("value {i} type: {:?}", value_type);
                let _value_version = parser::read_byte(buffer)?;

                match value_type {
                    12 => {
                        let _name = parser::read_compact_string(buffer)?;
                        println!("read name: {}", String::from_utf8(_name.clone()).unwrap());
                        let _feature_level = parser::read_short(buffer)?;
                        let tagged_field = parser::read_byte(buffer)?;
                        assert_eq!(tagged_field, 0);
                    }
                    2 => {
                        let topic_name = parser::read_compact_string(buffer)?;
                        let topic_uuid = parser::read_u128(buffer)?;
                        topic_map.entry(topic_name).or_insert(TopicMetadata {
                            uuid: topic_uuid,
                            record_id1: rec,
                            record_id2: i,
                        });

                        let tagged_field = parser::read_byte(buffer)?;
                        assert_eq!(tagged_field, 0);
                    }

                    3 => {
                        let partition_id = parser::read_int(buffer)?;
                        let topic_uuid = parser::read_u128(buffer)?;
                        let _replica_array = parser::read_int_array(buffer)?;
                        let _in_sync_replica_array = parser::read_int_array(buffer)?;
                        let _removing_replica_array = parser::read_int_array(buffer)?;
                        let _add_replica_array = parser::read_int_array(buffer)?;
                        let _leader = parser::read_int(buffer)?;
                        let _leader_epoch = parser::read_int(buffer)?;
                        let _partition_epoch = parser::read_int(buffer)?;
                        let _directory_array = parser::read_u128_array(buffer)?;
                        let tagged_field = parser::read_byte(buffer)?;
                        assert_eq!(tagged_field, 0);
                        partition_map
                            .entry(topic_uuid)
                            .and_modify(|pm: &mut Vec<PartitionMetadata>| {
                                pm.push(PartitionMetadata {
                                    partition_id,
                                    record_id1: rec,
                                    record_id2: i,
                                })
                            })
                            .or_insert(vec![PartitionMetadata {
                                partition_id,
                                record_id1: rec,
                                record_id2: i,
                            }]);
                    }
                    _ => unimplemented!(),
                }
                let headers_array_count = parser::read_byte(buffer)?;
                assert_eq!(headers_array_count, 0);
            }
        }
        println!("**************************");
        println!("topic map: {topic_map:?}");
        println!("topic map: {partition_map:?}");
        println!("records: {records:?}");
        println!("**************************");
        Ok(Metadata {
            topic_map,
            partition_map,
            records,
        })
    }
}
