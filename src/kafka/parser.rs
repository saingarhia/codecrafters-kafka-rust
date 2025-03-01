// parsing utilities
use std::io::{BufReader, Read};
use crate::kafka::errors;

pub fn compact_string(req: &mut BufReader<&[u8]>) -> errors::Result<Vec<u8>> {
    let mut length = [0_u8; 1];
    req.read_exact(&mut length)?;
    println!("reading string of length: {length:?}");
    if length[0] == 0 { return Ok(vec![]); }

    // read these many bytes and convert into string
    let mut data = vec![0_u8; length[0] as usize - 1];
    req.read_exact(&mut data)?;
    Ok(data)
}

pub fn array(req: &mut BufReader<&[u8]>) -> errors::Result<Vec<Vec<u8>>> {
    let mut length = [0_u8; 1];
    req.read_exact(&mut length)?;
    println!("number of topics: {}", length[0]-1);
    let mut result = vec![];
    for _i in 0..length[0]-1{
        println!("reading topic number: {_i}");
        result.push(compact_string(req)?);
        //tag_buffer(req)?;
    }
    Ok(result)
}

pub fn read_int(req: &mut BufReader<&[u8]>) -> errors::Result<i32> {
    let mut data = [0_u8; 4];
    req.read_exact(&mut data)?;
    Ok(i32::from_be_bytes(data))
}

pub fn read_short(req: &mut BufReader<&[u8]>) -> errors::Result<i16> {
    let mut data = [0_u8; 2];
    req.read_exact(&mut data)?;
    Ok(i16::from_be_bytes(data))
}

pub fn read_byte(req: &mut BufReader<&[u8]>) -> errors::Result<i8> {
    let mut data = [0_u8; 1];
    req.read_exact(&mut data)?;
    Ok(data[0] as i8)
}
pub fn tag_buffer(req: &mut BufReader<&[u8]>) -> errors::Result<()> {
    let _v = read_byte(req)?;
    println!("tagged buffer read: {}", _v);
    Ok(())
}
