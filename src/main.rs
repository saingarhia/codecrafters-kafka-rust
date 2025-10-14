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
    loop {
        // lets read size of this request
        // Use read_exact to ensure all 4 bytes of the size are read.
        if let Err(e) = stream.read_exact(&mut size) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                println!("Client disconnected.");
                break; // Cleanly exit loop on disconnect.
            }
            return Err(e.into()); // Propagate other I/O errors.
        }

        let req_size = i32::from_be_bytes(size) as usize;
        println!("Request size is: {req_size}");

        let mut req_data = vec![0; req_size];
        // Also use read_exact for the request body.
        stream.read_exact(&mut req_data)?;

        // process this request
        let mut req_reader = BufReader::new(&req_data[..]);
        let req_processor = kafka::incoming::Request::new(&mut req_reader)?;
        println!("Request processor: {:?}", req_processor);

        // Use a dynamically sized Vec for the response buffer to avoid overflows.
        let mut response_body_writer = BufWriter::new(Vec::new());
        req_processor.process(&mut response_body_writer, &metadata)?;
        let response_body = response_body_writer.into_inner()?;
        let message_size = response_body.len() as u32;

        // Write the size prefix followed by the response body.
        stream.write_all(&message_size.to_be_bytes())?;
        stream.write_all(&response_body)?;
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
