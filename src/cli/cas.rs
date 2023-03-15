use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct CAS {
    target_node: u64,
    key: String,
    expected_value: String,
    new_value: String,
}

impl CAS {
    pub fn new(target_node: u64, key: String, expected_value: String, new_value: String) -> CAS {
        CAS { target_node, key, expected_value, new_value }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn expected_value(&self) -> &str {
        &self.expected_value
    }
    pub fn new_value(&self) -> &str {
        &self.new_value
    }
    pub fn target_node(&self) -> u64 {
        self.target_node
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<CAS> {
        let target_node = parse.next_int()?;
        let key = parse.next_string()?;
        let expected_value = parse.next_string()?;
        let new_value = parse.next_string()?;
        Ok(CAS { target_node, key, expected_value, new_value })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("cas");
        frame.push_int(self.target_node());
        frame.push_string(self.key());
        frame.push_string(self.expected_value());
        frame.push_string(self.new_value());
        frame
    }
}
