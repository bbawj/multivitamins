use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Get {
    key: String,
    target_node: String,
}

impl Get {
    pub fn new(target_node: String, key: String) -> Get {
        Get { key, target_node }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn target_node(&self) -> &str {
        &self.target_node
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Get> {
        let target_node = parse.next_string()?;
        let key = parse.next_string()?;
        Ok(Get { key, target_node })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("get");
        frame.push_string(&self.target_node());
        frame.push_string(&self.key());
        frame
    }
}
