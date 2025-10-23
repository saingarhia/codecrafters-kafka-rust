use crate::kafka::{errors, parser, writer};
use std::convert::From;
use std::io::{Read, Write};

#[derive(Debug, Clone)]
pub struct CompactNullableString {
    val: Vec<u8>,
}

impl From<Vec<u8>> for CompactNullableString {
    fn from(val: Vec<u8>) -> Self {
        Self { val }
    }
}

#[allow(dead_code)]
impl CompactNullableString {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let val = parser::read_compact_string(req)?;
        Ok(Self { val })
    }

    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_compact_string(resp, &self.val)
    }
}
