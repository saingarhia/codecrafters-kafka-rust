use crate::kafka::errors;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

use crate::kafka::parser;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub uuid: u128,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub partition_id: i32,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Metadata {
    pub topic_map: HashMap<Vec<u8>, TopicMetadata>,
    pub partition_map: HashMap<u128, Vec<PartitionMetadata>>,
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

        for _record in 0..4 {
            let base_offset = parser::read_u64(buffer)?;
            println!("base offset: {:?}", base_offset);
            let batch_length = parser::read_int(buffer)?;
            println!("batch length: {:?}", batch_length);
            let _partition_leader_epoch = parser::read_int(buffer)?;
            let _magic = parser::read_byte(buffer)?;
            let _crc = parser::read_int(buffer)?;
            let _attributes = parser::read_short(buffer)?;
            let _last_offset_delta = parser::read_int(buffer)?;
            let _base_timestamp = parser::read_u64(buffer)?;
            let _max_timestamp = parser::read_u64(buffer)?;
            let _producer_id = parser::read_u64(buffer)?;
            let _producer_epoch = parser::read_short(buffer)?;
            let _base_sequence = parser::read_int(buffer)?;
            let record_size = parser::read_int(buffer)?;

            println!("Record size: {:?}", record_size);

            for i in 0..record_size {
                let length = parser::read_varint(buffer)?;
                println!("record {i} len: {:?}", length);
                let _attributes = parser::read_byte(buffer)?;
                let _timestamp_delta = parser::read_byte(buffer)?;
                let _offset_delta = parser::read_byte(buffer)?;
                let key_length = parser::read_varint(buffer)?;
                assert_eq!(key_length, -1);
                let value_length = parser::read_varint(buffer)?;
                println!("value {i} length: {:?}", value_length);
                let _frame_version = parser::read_byte(buffer)?;
                let value_type = parser::read_byte(buffer)?;
                println!("value {i} type: {:?}", value_type);
                let _value_version = parser::read_byte(buffer)?;

                match value_type {
                    12 => {
                        let _name = parser::read_compact_string(buffer)?;
                        let _feature_level = parser::read_short(buffer)?;
                        let tagged_field = parser::read_byte(buffer)?;
                        assert_eq!(tagged_field, 0);
                    }
                    2 => {
                        let topic_name = parser::read_compact_string(buffer)?;
                        let topic_uuid = parser::read_u128(buffer)?;
                        topic_map
                            .entry(topic_name)
                            .or_insert(TopicMetadata { uuid: topic_uuid });

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
                                pm.push(PartitionMetadata { partition_id })
                            })
                            .or_insert(vec![PartitionMetadata { partition_id }]);
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
        println!("**************************");
        Ok(Metadata {
            topic_map,
            partition_map,
        })
    }
}
