use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Response {
    key: String,
    value: u64,
}

impl Response {
    pub fn new(key: String, value: u64) -> Response {
        Response { key, value }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn value(&self) -> &u64 {
        &self.value
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Response> {
        let key = parse.next_string()?;
        let value = parse.next_int()?;
        Ok(Response { key, value })
    }
    pub fn to_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("response");
        frame.push_string(&self.key);
        frame.push_int(self.value);
        frame
    }
}
