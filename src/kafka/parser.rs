// parsing utilities
use crate::kafka::errors;
use std::convert::TryInto;
use std::io::Read;

fn convert<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into()
        .unwrap_or_else(|v: Vec<T>| panic!("Expected a Vec of length {} but it was {}", N, v.len()))
}

pub fn read_compact_string<R: Read>(req: &mut R) -> errors::Result<Vec<u8>> {
    let mut length = [0_u8; 1];
    req.read_exact(&mut length)?;
    println!("reading string of length: {length:?}");
    if length[0] == 0 {
        return Ok(vec![]);
    }

    // read these many bytes and convert into string
    let mut data = vec![0_u8; length[0] as usize - 1];
    req.read_exact(&mut data)?;
    Ok(data)
}

pub fn array<R: Read>(req: &mut R) -> errors::Result<Vec<Vec<u8>>> {
    let mut length = [0_u8; 1];
    req.read_exact(&mut length)?;
    let mut result = vec![];
    for _i in 0..length[0] - 1 {
        result.push(read_compact_string(req)?);
        //tag_buffer(req)?;
    }
    Ok(result)
}

fn read_bytes<T: Sized, R: Read>(req: &mut R) -> errors::Result<Vec<u8>> {
    let mut data: Vec<u8> = vec![0_u8; std::mem::size_of::<T>()];
    req.read_exact(&mut data[..])?;
    Ok(data)
}

pub fn read_u128<R: Read>(req: &mut R) -> errors::Result<u128> {
    let rv = convert::<u8, 16>(read_bytes::<u128, R>(req)?);
    Ok(u128::from_be_bytes(rv))
}

pub fn read_u64<R: Read>(req: &mut R) -> errors::Result<u64> {
    let rv = convert::<u8, 8>(read_bytes::<u64, R>(req)?);
    Ok(u64::from_be_bytes(rv))
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
    println!("tagged buffer read: {}", _v);
    Ok(())
}

pub fn read_varint<R: Read>(req: &mut R) -> errors::Result<i8> {
    let mut buffer = [0u8; 1];
    req.read_exact(&mut buffer)?;
    let x = i8::from_be_bytes(buffer);
    if x < 0 {
        let mut buffer = [0u8; 1];
        req.read_exact(&mut buffer)?;
        let y = (x as u8) as i16;
        let c = (y >> 1) ^ -(y & 1);
        return Ok(c as i8);
    }
    Ok((x >> 1) ^ -(x & 1)) //zigzag decode
}

#[allow(dead_code)]
fn decode_nullable_string<R: Read>(req: &mut R) -> errors::Result<Option<String>> {
    let n = read_byte(req)?;
    if n == -1 {
        return Ok(None);
    }
    let n2 = read_byte(req)? as u8;
    let size = i16::from_be_bytes([n as u8, n2]);
    let mut buffer = vec![0u8; size as usize];
    req.read_exact(&mut buffer[..])?;
    Ok(Some(String::from_utf8(buffer)?))
}

pub fn read_int_array<R: Read>(req: &mut R) -> errors::Result<Vec<i32>> {
    let num = read_byte(req)? as usize - 1; // number of elements in the array
    (0..num)
        .into_iter()
        .map(|_| read_int(req))
        .collect::<errors::Result<_>>()
}

pub fn read_u128_array<R: Read>(req: &mut R) -> errors::Result<Vec<u128>> {
    let num = read_byte(req)? as usize - 1; // number of elements in the array
    (0..num)
        .into_iter()
        .map(|_| read_u128(req))
        .collect::<errors::Result<_>>()
}
