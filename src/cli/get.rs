use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Get {
    target_node: u64,
    key: String,
}

impl Get {
    pub fn new(target_node: u64, key: String) -> Get {
        Get { target_node, key }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn target_node(&self) -> u64 {
        self.target_node
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Get> {
        let target_node = parse.next_int()?;
        let key = parse.next_string()?;
        Ok(Get { target_node, key })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("get");
        frame.push_int(self.target_node());
        frame.push_string(self.key());
        frame
    }
}
