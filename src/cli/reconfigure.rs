use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct Reconfigure {
    pid: u64,
}

impl Reconfigure {
    pub fn new(pid: u64) -> Reconfigure {
        Reconfigure { pid }
    }
    pub fn pid(&self) -> u64 {
        self.pid
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<Reconfigure> {
        let pid = parse.next_int()?;
        Ok(Reconfigure { pid })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("reconfigure");
        frame.push_int(self.pid);
        frame
    }
}
