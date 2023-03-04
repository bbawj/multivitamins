use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Response {
    key: String,
    value: String,
}

impl Response {
    pub fn new(key: String, value: String) -> Response {
        Response { key, value }
    }
    pub fn key(&self) -> &String {
        &self.key
    }
    pub fn value(&self) -> &String {
        &self.value
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Response> {
        let key = parse.next_string()?;
        let value = parse.next_string()?;
        Ok(Response::new(key.to_string(), value.to_string()))
    }
    pub fn to_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("response");
        frame.push_string(&self.key);
        frame.push_string(&self.value);
        frame
    }
}
