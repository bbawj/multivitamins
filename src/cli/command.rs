use omnipaxos_core::messages::Message;

use crate::op_server::{KeyValue, KeyValueSnapshot};

use super::{frame::Frame, get::Get, op_message::OpMessage, parse::Parse, response::Response};

pub enum Command {
    Get(Get),
    Response(Response),
    OpMessage(Message<KeyValue, KeyValueSnapshot>),
}

impl Command {
    pub fn from_frame(frame: Frame) -> super::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frame(&mut parse)?),
            "response" => Command::Response(Response::parse_frame(&mut parse)?),
            "opmessage" => Command::OpMessage(OpMessage::from_frame(&mut parse)?),
            _ => panic!("invalid command name provided"),
        };

        Ok(command)
    }
}
