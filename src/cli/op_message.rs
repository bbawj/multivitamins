use crate::{
    cli::{frame::Frame, parse::Parse},
    op_server::KeyValue,
};

#[derive(Debug)]
pub struct OpMessage {
    key: String,
    value: u64,
}

impl OpMessage {
    pub fn new(kv: KeyValue) -> OpMessage {
        OpMessage {
            key: kv.key,
            value: kv.value,
        }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn value(&self) -> &u64 {
        &self.value
    }
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let key = parse.next_string()?;
        let value = parse.next_int()?;
        Ok(OpMessage { key, value })
    }
    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("opmessage");
        frame.push_string(&self.key);
        frame.push_int(self.value);
        frame
    }
}
