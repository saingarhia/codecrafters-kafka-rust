#![allow(unused_imports)]
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::prelude::*;
use std::sync::{Arc, Mutex};
use std::thread;

mod kafka;

const METADATA_FILENAME: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

fn process_connection(
    mut stream: TcpStream,
    metadata: Arc<Mutex<kafka::metadata::Metadata>>,
) -> kafka::errors::Result<()> {
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
        if n < req_size {
            // close socket
            println!("received {n} bytes, closing socket!!, expected: {req_size}");
            // TODO - should implement cursor if request is not fully here
            break;
        }
        // process this request
        let mut req_reader = BufReader::new(&req_data[..]);
        let req_processor = kafka::incoming::Request::new(&mut req_reader)?;
        println!("Request processor: {:?}", req_processor);
        // jump over the size field - populate it before sending on wire
        let mut writer = BufWriter::new(&mut response[4..]);
        req_processor.process(&mut writer, &metadata)?;
        let message_size = writer.buffer().len() as u32;
        // we do not writer any more
        drop(writer);

        message_size
            .to_be_bytes()
            .into_iter()
            .enumerate()
            .for_each(|(idx, v)| response[idx] = v);

        stream.write_all(&response[..message_size as usize + 4])?; // 4 bytes for message size
    }
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

fn process_tcp() -> kafka::errors::Result<()> {
    let metadata: Arc<Mutex<kafka::metadata::Metadata>> = Arc::new(Mutex::new(
        kafka::metadata::Metadata::new(METADATA_FILENAME)?,
    ));
    println!("read metadata: {:?}", metadata);
    let listener = TcpListener::bind("127.0.0.1:9092")?;
    println!("Listening on {}", listener.local_addr()?);

    // The main loop for accepting connections should not die on a single error.
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Accepted new connection.");
                let mclone = Arc::clone(&metadata);
                // Handle errors within the thread to prevent panics from taking down the server.
                thread::spawn(move || {
                    if let Err(e) = process_connection(stream, mclone) {
                        println!("Error processing connection: {}", e);
                    }
                });
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
    Ok(())
}

fn main() {
    match process_tcp() {
        Ok(_) => (),
        Err(e) => println!("Error processing connection: {e:?}"),
    };
}
