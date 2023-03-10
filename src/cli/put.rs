use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Put {
    target_node: u64,
    key: String,
    val: String,
}

impl Put {
    pub fn new(target_node: u64, key: String, val: String) -> Put {
        Put {
            target_node,
            key,
            val,
        }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn val(&self) -> &str {
        &self.val
    }
    pub fn target_node(&self) -> u64 {
        self.target_node
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Put> {
        let target_node = parse.next_int()?;
        let key = parse.next_string()?;
        let val = parse.next_string()?;
        Ok(Put {
            target_node,
            key,
            val,
        })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("put");
        frame.push_int(self.target_node());
        frame.push_string(self.key());
        frame.push_string(self.val());
        frame
    }
}
