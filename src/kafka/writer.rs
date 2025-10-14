use std::fmt::Debug;
use std::io::Write;

use super::errors;

/// A trait for types that can be written to a Kafka-protocol stream.
pub trait KafkaWrite {
    fn write<W: Write>(&self, writer: &mut W) -> errors::Result<()>;
}

macro_rules! kafka_write_impl {
    ($($t:ty),*) => {
        $(
            impl KafkaWrite for $t {
                fn write<W: Write>(&self, writer: &mut W) -> errors::Result<()> {
                    writer.write_all(&self.to_be_bytes())?;
                    Ok(())
                }
            }
        )*
    };
}

kafka_write_impl!(i8, u8, i16, u16, i32, u32, i64, u64, u128);

impl KafkaWrite for bool {
    fn write<W: Write>(&self, writer: &mut W) -> errors::Result<()> {
        (*self as u8).write(writer)
    }
}

pub fn write_bytes<W: Write, T: KafkaWrite>(resp: &mut W, data: &T) -> errors::Result<()> {
    data.write(resp)
}

pub fn write_bool<W: Write>(resp: &mut W, val: bool) -> errors::Result<()> {
    (val as u8).write(resp)
}

pub fn write_varint<W: Write>(resp: &mut W, val: usize) -> errors::Result<()> {
    (val as u8).write(resp)
}

#[allow(dead_code)]
pub fn write_varint_main<W: Write>(resp: &mut W, x: i32) -> errors::Result<()> {
    let mut ux = (x as u64) << 1;
    if x < 0 {
        ux = !ux;
    }
    (ux as u8).write(resp)
}

#[allow(dead_code)]
pub fn write_uvarint<W: Write>(resp: &mut W, x: i32) -> errors::Result<()> {
    let mut x = x;
    while x >= 0x80 {
        let val = x as u8 | 0x80;
        val.write(resp)?;
        x >>= 7;
    }
    let val = x as u8;
    val.write(resp)
}

#[allow(dead_code)]
pub fn write_null<W: Write>(resp: &mut W) -> errors::Result<()> {
    0xFF_u8.write(resp)
}

#[allow(dead_code)]
pub fn write_compact_string<W: Write>(resp: &mut W, s: &[u8]) -> errors::Result<()> {
    write_varint_main(resp, s.len() as i32)?;
    resp.write_all(s)?;
    Ok(())
}

#[allow(dead_code)]
pub fn write_nullable_compact_string<W: Write>(
    resp: &mut W,
    val: Option<&[u8]>,
) -> errors::Result<()> {
    if let Some(s) = val {
        write_compact_string(resp, s)
    } else {
        0_u8.write(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_write_bytes() {
        let mut buffer = Cursor::new(Vec::new());

        // Test with u8
        let u8_val: u8 = 123;
        u8_val.write(&mut buffer).unwrap();
        assert_eq!(buffer.get_ref(), &vec![123]);
        buffer.get_mut().clear();

        // Test with u16
        let u16_val: u16 = 0x1234;
        u16_val.write(&mut buffer).unwrap();
        assert_eq!(buffer.get_ref(), &vec![0x12, 0x34]);
        buffer.get_mut().clear();

        // Test with u32
        let u32_val: u32 = 0x12345678;
        u32_val.write(&mut buffer).unwrap();
        assert_eq!(buffer.get_ref(), &vec![0x12, 0x34, 0x56, 0x78]);
        buffer.get_mut().clear();

        // Test with u64
        let u64_val: u64 = 0x123456789ABCDEF0;
        u64_val.write(&mut buffer).unwrap();
        assert_eq!(
            buffer.get_ref(),
            &vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]
        );
        buffer.get_mut().clear();

        // Test with u128
        let u128_val: u128 = 0x123456789ABCDEF0123456789ABCDEF0;
        u128_val.write(&mut buffer).unwrap();
        assert_eq!(
            buffer.get_ref(),
            &vec![
                0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
                0xDE, 0xF0
            ]
        );
        buffer.get_mut().clear();

        // Test with a type that doesn't implement KafkaWrite - should not compile
        #[derive(Debug)]
        struct TestStruct {
            a: u32,
            b: u32,
            c: u32,
        }
        // The following line would fail to compile, which is the desired behavior for safety.
        // let struct_val = TestStruct { a: 1, b: 2, c: 3 };
        // let result = write_bytes(&mut buffer, &struct_val);
    }
}
