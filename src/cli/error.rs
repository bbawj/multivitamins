use super::{frame::Frame, parse::Parse, Result};

#[derive(Debug)]
pub struct Error {
    value: String,
}

impl Error {
    pub fn new(value: String) -> Error {
        Error { value }
    }
    pub fn value(&self) -> &str {
        &self.value
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Error> {
        Ok(Error {
            value: parse.next_string()?,
        })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("error");
        frame.push_string(&self.value());
        frame
    }
}
