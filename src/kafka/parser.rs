// parsing utilities
use crate::kafka::errors;
use std::io::Read;

pub fn read_uvarint<R: Read>(req: &mut R) -> errors::Result<u32> {
    let mut res: u32 = 0;
    let mut shift = 0;
    let mut i = 0;
    loop {
        let mut buf = [0; 1];
        req.read_exact(&mut buf)?;
        let b = buf[0];
        res |= ((b & 0x7f) as u32) << shift;
        shift += 7;
        i += 1;
        if (b & 0x80) == 0 {
            break;
        }
        if i >= 5 {
            return Err(errors::KafkaErrors::InvalidWriterArg("UVarInt is too long".to_string()).into());
        }
    }
    Ok(res)
}

pub fn read_compact_string<R: Read>(req: &mut R) -> errors::Result<Vec<u8>> {
    let len = read_uvarint(req)?;
    if len == 0 {
        return Ok(vec![]);
    }
    let str_len = (len - 1) as usize;
    let mut data = vec![0_u8; str_len];
    req.read_exact(&mut data)?;
    Ok(data)
}

pub fn array<R: Read>(req: &mut R) -> errors::Result<Vec<Vec<u8>>> {
    let len = read_uvarint(req)?;
    if len == 0 {
        return Ok(vec![]);
    }
    let array_len = (len - 1) as usize;
    let mut result = vec![];
    for _ in 0..array_len {
        result.push(read_compact_string(req)?);
    }
    Ok(result)
}

pub fn read_u128<R: Read>(req: &mut R) -> errors::Result<u128> {
    let mut buf = [0u8; 16];
    req.read_exact(&mut buf)?;
    Ok(u128::from_be_bytes(buf))
}

pub fn read_u64<R: Read>(req: &mut R) -> errors::Result<u64> {
    let mut buf = [0u8; 8];
    req.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

pub fn read_int<R: Read>(req: &mut R) -> errors::Result<i32> {
    let mut data = [0_u8; 4];
    req.read_exact(&mut data)?;
    Ok(i32::from_be_bytes(data))
}

pub fn read_short<R: Read>(req: &mut R) -> errors::Result<i16> {
    let mut data = [0_u8; 2];
    req.read_exact(&mut data)?;
    Ok(i16::from_be_bytes(data))
}

pub fn read_byte<R: Read>(req: &mut R) -> errors::Result<i8> {
    let mut data = [0_u8; 1];
    req.read_exact(&mut data)?;
    Ok(data[0] as i8)
}
pub fn tag_buffer<R: Read>(req: &mut R) -> errors::Result<()> {
    let _v = read_byte(req)?;
    Ok(())
}

pub fn read_varint<R: Read>(req: &mut R) -> errors::Result<i32> {
    let mut res: u32 = 0;
    let mut shift = 0;
    let mut i = 0;
    loop {
        let mut buf = [0; 1];
        req.read_exact(&mut buf)?;
        let b = buf[0];
        res |= ((b & 0x7f) as u32) << shift;
        shift += 7;
        i += 1;
        if (b & 0x80) == 0 {
            break;
        }
        if i >= 5 {
            return Err(errors::KafkaErrors::InvalidWriterArg("Varint is too long".to_string()).into());
        }
    }
    Ok(((res >> 1) as i32) ^ -((res & 1) as i32)) // zigzag decode
}
