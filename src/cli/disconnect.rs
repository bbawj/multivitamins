use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Disconnect {
    target_node: u64,
}

impl Disconnect {
    pub fn new(target_node: u64) -> Disconnect {
        Disconnect { target_node }
    }
    pub fn target_node(&self) -> u64 {
        self.target_node
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Disconnect> {
        let target_node = parse.next_int()?;
        Ok(Disconnect { target_node })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("disconnect");
        frame.push_int(self.target_node());
        frame
    }
}
