use crate::cli::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub struct SaveSnapshot {
    pid: u64,
}

impl SaveSnapshot {
    pub fn new(pid: u64) -> SaveSnapshot {
        SaveSnapshot { pid }
    }
    pub fn pid(&self) -> u64 {
        self.pid
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<SaveSnapshot> {
        let pid = parse.next_int()?;
        Ok(SaveSnapshot { pid })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("savesnapshot");
        frame.push_int(self.pid());
        frame
    }
}

#[derive(Debug)]
pub struct ReadSnapshot {
    pid: u64,
    file_path: String,
}

impl ReadSnapshot {
    pub fn new(pid: u64, file_path: String) -> ReadSnapshot {
        ReadSnapshot { pid, file_path }
    }
    pub fn pid(&self) -> u64 {
        self.pid
    }
    pub fn file_path(&self) -> &str {
        &self.file_path
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<ReadSnapshot> {
        let pid = parse.next_int()?;
        let file_path = parse.next_string()?;
        Ok(ReadSnapshot { pid, file_path })
    }
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("readsnapshot");
        frame.push_int(self.pid());
        frame.push_string(self.file_path());
        frame
    }
}
