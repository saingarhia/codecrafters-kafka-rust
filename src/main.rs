#![allow(unused_imports)]
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::prelude::*;
use std::thread;

mod kafka;

fn process_connection(mut stream: TcpStream) -> kafka::errors::Result<()> {
    let mut size = [0; 4];
    let mut response = [0_u8; 1500]; // reuse
    loop {
        // lets read size of this request
        let n = stream.read(&mut size)?;
        if n <= 0 {
            // close socket
            println!("received {n} bytes, closing this socket!!");
            break;
        }
        // else we received something, lets write back
        println!("received {n} bytes on this tcp socket");
        let req_size = i32::from_be_bytes(size) as usize;
        println!("Request size is: {req_size}");

        let mut req_data = vec![0; req_size];
        // lets read the reques
        let n = stream.read(&mut req_data)?;
        if n <= req_size {
            // close socket
            println!("received {n} bytes, closing socket!!, expected: {req_size}");
            // TODO - should implement cursor if request is not fully here
            break;
        }
        // process this request
        let mut req_reader = BufReader::new(&req_data[..]);
        let req_processor = kafka::incoming::Request::new(&mut req_reader)?;
        // jump over the size field - populate it before sending on wire 
        let mut writer = BufWriter::new(&mut response[4..]);
        let message_size = req_processor.process(&mut writer) as u32;
        // we do not writer any more
        drop(writer);

        message_size.to_be_bytes()
            .into_iter()
            .enumerate()
            .for_each(|(idx, v)| response[idx] = v);
        
        let _ = stream.write_all(&response[..message_size as usize+4])?; // 4 bytes for message size
    }
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

fn process_tcp() -> kafka::errors::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092")?;
    println!("Listening on {}", listener.local_addr()?);
    
    for stream in listener.incoming() {
       thread::spawn(move || process_connection(stream?));
    }
    Ok(())
}

fn main() {
    let _ = process_tcp();
}
