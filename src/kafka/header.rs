

#[derive(Debug)]
struct Header {
    req_api_key: u16,
    req_api_ver: u16,
    correlation_id: i32,
    //client_id: // nullable string 
    //tag_buffer: compact array
}
