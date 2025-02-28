#![allow(unused_imports)]
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::prelude::*;
use std::task;
use bytes::{BytesMut, BufMut};
use std::thread;

fn process_connection(mut stream: TcpStream) -> io::Result<()> {
    let mut readbuf = [0; 1500];
    loop {
        let n = stream.read(&mut readbuf)?;
        if n <= 0 {
            // close socket
            println!("received {n} bytes, closing this socket!!");
            break;
        }
        // else we received something, lets write back
        println!("received {n} bytes on this tcp socket");
        println!("-> {}\n", String::from_utf8_lossy(&readbuf[..n]));

        let mut output_buf = BytesMut::with_capacity(1500);
        let message_size: u32 = 19;

        let api_key = u16::from_be_bytes([readbuf[4], readbuf[5]]);
        let api_ver = u16::from_be_bytes([readbuf[6], readbuf[7]]);
        println!("api key received: {api_key}, version: {api_ver}");

        if api_ver <= 4 {
            output_buf.extend_from_slice(&message_size.to_be_bytes());
            output_buf.extend_from_slice(&readbuf[8..12]);
            output_buf.extend_from_slice(&0_u16.to_be_bytes());
            output_buf.put_i8(2); // api key records number +1
            output_buf.extend_from_slice(&18_u16.to_be_bytes());
            output_buf.extend_from_slice(&0_u16.to_be_bytes());
            output_buf.extend_from_slice(&4_u16.to_be_bytes());
            output_buf.put_i8(0);
            output_buf.extend_from_slice(&420_u32.to_be_bytes());
            output_buf.put_i8(0);
        } else {
            let message_size: u32 = 6;
            output_buf.extend_from_slice(&message_size.to_be_bytes());
            output_buf.extend_from_slice(&readbuf[8..12]);
            output_buf.extend_from_slice(&35_u16.to_be_bytes());
        }
        let _ = stream.write_all(&output_buf[..])?;
    }
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())

}

fn process_tcp() -> std::io::Result<()> {
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
