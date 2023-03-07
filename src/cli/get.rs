use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Get {
    key: String,
    node: String
}

impl Get {
    pub fn new(key: String, node: String) -> Get {
        Get { key, node }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn node(&self) -> &str{
        &self.node
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Get> {
        let key = parse.next_string()?;
        let node = parse.next_string()?;
        Ok(Get { key, node })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("get");
        frame.push_string(&self.key);
        frame.push_string(&self.node);
        frame
    }
}
