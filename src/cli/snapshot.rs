use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Snapshot {
    pid: u64,
}

impl Snapshot {
    pub fn new(pid: u64) -> Snapshot {
        Snapshot { pid }
    }
    pub fn pid(&self) -> u64 {
        self.pid
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Snapshot> {
        let pid = parse.next_int()?;
        Ok(Snapshot { pid })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("snapshot");
        frame.push_int(self.pid());
        frame
    }
}
