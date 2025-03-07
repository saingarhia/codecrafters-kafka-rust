use std::fmt::Debug;
use std::io::Write;

use super::errors;

// write_bytes
//
// writes the bytes into buffer/Buffer Writer
// this function assumes that data types sent are variables of either i8/u8, i16/u16, i32/u32,
// i64/u64, i128/u128
pub fn write_bytes<T: Sized + Debug, W: Write>(resp: &mut W, data: &T) -> errors::Result<()> {
    let data: Vec<u8> = match std::mem::size_of::<T>() {
        1 => {
            // byte (u8)
            let val = unsafe { std::ptr::read_unaligned(data as *const T as *const u8) };
            vec![val]
        }
        2 => {
            // short (u16)
            let val = unsafe { std::ptr::read_unaligned(data as *const T as *const u16) };
            u16::to_be_bytes(val).to_vec()
        }
        4 => {
            // int (u32)
            let val = unsafe { std::ptr::read_unaligned(data as *const T as *const u32) };
            u32::to_be_bytes(val).to_vec()
        }
        8 => {
            // long long (u64)
            let val = unsafe { std::ptr::read_unaligned(data as *const T as *const u64) };
            u64::to_be_bytes(val).to_vec()
        }
        16 => {
            // u128
            let val = unsafe { std::ptr::read_unaligned(data as *const T as *const u128) };
            u128::to_be_bytes(val).to_vec()
        }
        _ => {
            return Err(errors::KafkaErrors::InvalidWriterArg(format!(
                "Unsupported data size: {} bytes for type {:?}",
                std::mem::size_of::<T>(),
                std::any::type_name::<T>()
            ))
            .into());
        }
    };
    resp.write_all(&data)?;
    Ok(())
}

pub fn write_bool<W: Write>(resp: &mut W, val: bool) -> errors::Result<()> {
    write_bytes(resp, &(val as u8))
}

pub fn write_varint<W: Write>(resp: &mut W, val: usize) -> errors::Result<()> {
    write_bytes(resp, &(val as u8))
}

pub fn write_varint_main<W: Write>(resp: &mut W, x: i32) -> errors::Result<()> {
    let mut ux = (x as u64) << 1;
    if x < 0 {
        ux = !ux;
    }
    write_bytes(resp, &(ux as u8))
}

pub fn write_null<W: Write>(resp: &mut W) -> errors::Result<()> {
    write_bytes(resp, &(0xFF_u8))
}

pub fn write_compact_string<W: Write>(resp: &mut W, s: &[u8]) -> errors::Result<()> {
    write_varint_main(resp, s.len() as i32 + 1)?;
    resp.write_all(s)?;
    Ok(())
}

pub fn write_nullable_compact_string<W: Write>(
    resp: &mut W,
    val: Option<&[u8]>,
) -> errors::Result<()> {
    if let Some(s) = val {
        write_compact_string(resp, s)
    } else {
        write_bytes(resp, &0_u8)
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
        write_bytes(&mut buffer, &u8_val).unwrap();
        assert_eq!(buffer.get_ref(), &vec![123]);
        buffer.set_position(0); // Reset cursor for next test

        // Test with u16
        let u16_val: u16 = 0x1234;
        write_bytes(&mut buffer, &u16_val).unwrap();
        assert_eq!(buffer.get_ref(), &vec![0x12, 0x34]);
        buffer.set_position(0);

        // Test with u32
        let u32_val: u32 = 0x12345678;
        write_bytes(&mut buffer, &u32_val).unwrap();
        assert_eq!(buffer.get_ref(), &vec![0x12, 0x34, 0x56, 0x78]);
        buffer.set_position(0);

        // Test with u64
        let u64_val: u64 = 0x123456789ABCDEF0;
        write_bytes(&mut buffer, &u64_val).unwrap();
        assert_eq!(
            buffer.get_ref(),
            &vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]
        );
        buffer.set_position(0);

        // Test with u128
        let u128_val: u128 = 0x123456789ABCDEF0123456789ABCDEF0;
        write_bytes(&mut buffer, &u128_val).unwrap();
        assert_eq!(
            buffer.get_ref(),
            &vec![
                0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
                0xDE, 0xF0
            ]
        );
        buffer.set_position(0);

        // Test with unsupported size (e.g., struct) - should return error
        #[derive(Debug)]
        struct TestStruct {
            a: u32,
            b: u32,
            c: u32,
        }
        let struct_val = TestStruct { a: 1, b: 2, c: 3 };
        let result = write_bytes(&mut buffer, &struct_val);
        assert!(result.is_err());
        //match result.unwrap_err() {
        //    errors::KafkaErrors::InvalidWriterArg(_) => {} // Expected error type
        //    _ => panic!("Expected InvalidWriterArg error"),
        //}
    }
}
