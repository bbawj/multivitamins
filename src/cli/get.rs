use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: String) -> Get {
        Get { key: key }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Get> {
        let key = parse.next_string()?;
        Ok(Get { key })
    }
    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("get".to_owned());
        frame.push_string(self.key);
        frame
    }
}
