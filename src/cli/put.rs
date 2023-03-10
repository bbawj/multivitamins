use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Put {
    key: String,
    val: String,
    target_node: String,
}

impl Put {
    pub fn new(target_node: String, key: String, val: String) -> Put {
        Put {
            key,
            val,
            target_node,
        }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn val(&self) -> &str {
        &self.val
    }
    pub fn target_node(&self) -> &str {
        &self.target_node
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Put> {
        let target_node = parse.next_string()?;
        let key = parse.next_string()?;
        let val = parse.next_string()?;
        Ok(Put {
            key,
            val,
            target_node,
        })
    }
    pub fn to_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("put");
        frame.push_string(&self.target_node());
        frame.push_string(&self.key());
        frame.push_string(&self.val());
        frame
    }
}
