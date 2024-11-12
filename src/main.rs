#![allow(unused_imports)]
use async_std::io;
use async_std::net::{TcpListener, TcpStream, Shutdown};
use async_std::prelude::*;
use async_std::task;
use bytes::{BytesMut};

async fn process_connection(stream: TcpStream) -> io::Result<()> {
    let mut reader = stream.clone();
    let mut writer = stream;
    let mut readbuf = [0; 1500];
    loop {
        let n = reader.read(&mut readbuf).await?;
        if n <= 0 {
            // close socket
            println!("received {n} bytes, closing this socket!!");
            break;
        }
        // else we received something, lets write back
        println!("received {n} bytes on this tcp socket");
        println!("-> {}\n", String::from_utf8_lossy(&readbuf[..n]));

        let message_size: u32 = 0;
        let error_code: u16 = 35;
        let mut output_buf = BytesMut::with_capacity(1500);
        output_buf.extend_from_slice(&message_size.to_be_bytes());
        output_buf.extend_from_slice(&readbuf[8..12]);
        output_buf.extend_from_slice(&error_code.to_be_bytes());
        let _ = writer.write_all(&output_buf[..]).await?;
    }
    let _ = writer.shutdown(Shutdown::Both);
    Ok(())

}

async fn process_tcp() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").await?;
    println!("Listening on {}", listener.local_addr()?);
    let mut incoming = listener.incoming();
    
     while let Some(stream) = incoming.next().await {
        let stream = stream?;
        task::spawn(async {
            process_connection(stream).await.unwrap();
        });
     }
    Ok(())
}

fn main() {
    task::block_on(async {
            let _ = process_tcp().await;
        });
}
