use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Put {
    key: String,
    val: String,
    node: String,
}

impl Put {
    pub fn new(key: String, val: String, node: String) -> Put {
        Put { key, val, node }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn val(&self) -> &str {
        &self.val
    }
    pub fn node(&self) -> &str {
        &self.node
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Put> {
        let key = parse.next_string()?;
        let val = parse.next_string()?;
        let node = parse.next_string()?;
        Ok(Put { key, val, node})
    }
    pub fn to_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("put");
        frame.push_string(&self.key);
        frame.push_string(&self.val);
        frame.push_string(&self.node);
        frame
    }
}
