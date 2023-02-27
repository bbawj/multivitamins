use omnipaxos_core::{
    ballot_leader_election::Ballot,
    messages::{
        ballot_leader_election::{BLEMessage, HeartbeatMsg},
        sequence_paxos::{AcceptSync, FirstAccept, PaxosMessage, PaxosMsg, Prepare, Promise},
        Message,
    },
    storage::StopSign,
};

use crate::op_server::KeyValue;

use super::{frame::Frame, parse::Parse};

pub enum OpMessage {
    SequencePaxos(PaxosMessage<KeyValue, ()>),
    PaxosMsg(PaxosMsg<KeyValue, ()>),
    BLEMessage(BLEMessage),
    HeartbeatMessage(HeartbeatMsg),
    KeyValue(KeyValue),
    Ballot(Ballot),
    StopSign(Option<StopSign>),
}

impl OpMessage {
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        match self {
            OpMessage::SequencePaxos(m) => {
                frame.push_int(m.from);
                frame.push_int(m.to);
                match m.msg {
                    PaxosMsg::PrepareReq => {
                        frame.push_string("prepare");
                        frame
                    }
                    PaxosMsg::Prepare(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::Promise(_) => todo!(),
                    PaxosMsg::AcceptSync(_) => todo!(),
                    PaxosMsg::FirstAccept(_) => todo!(),
                    PaxosMsg::AcceptDecide(_) => todo!(),
                    PaxosMsg::Accepted(_) => todo!(),
                    PaxosMsg::Decide(_) => todo!(),
                    PaxosMsg::ProposalForward(_) => todo!(),
                    PaxosMsg::Compaction(_) => todo!(),
                    PaxosMsg::AcceptStopSign(_) => todo!(),
                    PaxosMsg::AcceptedStopSign(_) => todo!(),
                    PaxosMsg::DecideStopSign(_) => todo!(),
                    PaxosMsg::ForwardStopSign(_) => todo!(),
                }
            }
            OpMessage::KeyValue(_) => todo!(),
            OpMessage::Ballot(_) => todo!(),
            OpMessage::BLEMessage(_) => todo!(),
            OpMessage::StopSign(_) => todo!(),
            OpMessage::PaxosMsg(_) => todo!(),
            OpMessage::HeartbeatMessage(_) => todo!(),
        }
    }
    pub fn from_frame(parse: &mut Parse) -> crate::cli::Result<Message<KeyValue, ()>> {
        let message_type = parse.next_string()?.to_lowercase();
        let from = parse.next_int()?;
        let to = parse.next_int()?;
        let op_message = match &message_type[..] {
            "preparereq" => todo!(),
            "prepare" => Prepare::parse_frame(parse)?,
            "promise" => Promise::parse_frame(parse)?,
            "acceptsync" => AcceptSync::parse_frame(parse)?,
            "firstaccept" => todo!(),
            "acceptdecide" => todo!(),
            "accepted" => todo!(),
            "decide" => todo!(),
            "proposalforward" => todo!(),
            "compaction" => todo!(),
            "acceptstopsign" => todo!(),
            "acceptedstopsign" => todo!(),
            "decidestopsign" => todo!(),
            "forwardstopsign" => todo!(),
            "heartbeat" => todo!(),
            _ => panic!(""),
        };
        match op_message {
            OpMessage::PaxosMsg(msg) => Ok(Message::SequencePaxos(PaxosMessage { from, to, msg })),
            OpMessage::HeartbeatMessage(msg) => Ok(Message::BLE(BLEMessage { from, to, msg })),
            _ => panic!()
            // OpMessage::SequencePaxos(_) => todo!(),
            // OpMessage::BLEMessage(_) => todo!(),
            // OpMessage::KeyValue(_) => todo!(),
            // OpMessage::Ballot(_) => todo!(),
            // OpMessage::StopSign(_) => todo!(),
        }
    }
}

trait ToFromFrame {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame;
    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage>;
}

impl ToFromFrame for KeyValue {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string(&self.key);
        frame.push_int(self.value);
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let key = parse.next_string()?;
        let value = parse.next_int()?;
        Ok(OpMessage::KeyValue(KeyValue { key, value }))
    }
}

impl ToFromFrame for Ballot {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_int(self.n.into());
        frame.push_int(self.priority);
        frame.push_int(self.pid);
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse.next_int()?.try_into().unwrap();
        let priority = parse.next_int()?;
        let pid = parse.next_int()?;
        Ok(OpMessage::Ballot(Ballot { n, priority, pid }))
    }
}

impl ToFromFrame for StopSign {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_int(1);
        frame.push_int(self.config_id.into());
        frame.push_int(self.nodes.len().try_into().unwrap());
        for node in &self.nodes[0..] {
            frame.push_int(*node);
        }
        if self.metadata.is_some() {
            frame.push_int(1);
            let metadata = self.metadata.as_ref().unwrap();
            frame.push_int(metadata.len().try_into().unwrap());
            for x in metadata {
                frame.push_int((*x).into());
            }
        } else {
            frame.push_int(0);
        }
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let has_stop_sign = parse.next_int()?;
        if has_stop_sign == 1 {
            let config_id = parse.next_int()?.try_into().unwrap();
            let len_nodes = parse.next_int()?;
            let mut nodes = Vec::new();
            for _ in 0..len_nodes {
                nodes.push(parse.next_int()?);
            }
            let has_metadata = parse.next_int()?;
            let mut metadata = Vec::new();
            if has_metadata == 1 {
                let len_metadata = parse.next_int()?;
                for _ in 0..len_metadata {
                    metadata.push(parse.next_int()?.try_into().unwrap());
                }
            }
            return Ok(OpMessage::StopSign(Some(StopSign {
                config_id,
                nodes,
                metadata: if has_metadata == 1 {
                    Some(metadata)
                } else {
                    None
                },
            })));
        }
        Ok(OpMessage::StopSign(None))
    }
}

impl ToFromFrame for Promise<KeyValue, ()> {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("promise");
        self.n.to_frame(frame);
        self.n_accepted.to_frame(frame);
        // encode additional length of suffix
        frame.push_int(self.suffix.len().try_into().unwrap());
        for x in &self.suffix[0..] {
            x.to_frame(frame);
        }
        frame.push_int(self.decided_idx);
        frame.push_int(self.accepted_idx);
        if self.stopsign.is_some() {
            let stopsign = self.stopsign.as_ref().unwrap();
            stopsign.to_frame(frame);
        } else {
            frame.push_int(0);
        }
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = match Ballot::parse_frame(parse)? {
            OpMessage::Ballot(m) => m,
            _ => panic!("message error; incorrect spmessage parsed"),
        };
        let n_accepted = match Ballot::parse_frame(parse)? {
            OpMessage::Ballot(m) => Ballot {
                n: m.n,
                priority: m.priority,
                pid: m.pid,
            },
            _ => panic!("message error; incorrect spmessage parsed"),
        };
        let len = parse.next_int()?;
        let mut suffix = Vec::new();
        for _ in 0..len {
            suffix.push(match KeyValue::parse_frame(parse)? {
                OpMessage::KeyValue(m) => KeyValue {
                    key: m.key,
                    value: m.value,
                },
                _ => panic!("message error; incorrect spmessage parsed"),
            });
        }
        let decided_idx = parse.next_int()?;
        let accepted_idx = parse.next_int()?;
        let has_stop_sign = parse.next_int()?;
        let stopsign = match StopSign::parse_frame(parse).unwrap() {
            OpMessage::StopSign(s) => s,
            _ => panic!("message error; incorrect message parsed"),
        };

        Ok(OpMessage::PaxosMsg(PaxosMsg::Promise(Promise {
            n,
            n_accepted,
            decided_snapshot: None,
            suffix,
            decided_idx,
            accepted_idx,
            stopsign,
        })))
    }
}

impl ToFromFrame for Prepare {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("prepare");
        self.n.to_frame(frame);
        frame.push_int(self.decided_idx);
        self.n_accepted.to_frame(frame);
        frame.push_int(self.accepted_idx);
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = match Ballot::parse_frame(parse)? {
            OpMessage::Ballot(m) => m,
            _ => panic!("message error; incorrect spmessage parsed"),
        };
        let decided_idx = parse.next_int()?;
        let n_accepted = match Ballot::parse_frame(parse)? {
            OpMessage::Ballot(m) => Ballot {
                n: m.n,
                priority: m.priority,
                pid: m.pid,
            },
            _ => panic!("message error; incorrect spmessage parsed"),
        };
        let accepted_idx = parse.next_int()?;
        Ok(OpMessage::PaxosMsg(PaxosMsg::Prepare(Prepare {
            n,
            decided_idx,
            n_accepted,
            accepted_idx,
        })))
    }
}

impl ToFromFrame for AcceptSync<KeyValue, ()> {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("promise");
        self.n.to_frame(frame);
        // encode additional length of suffix
        frame.push_int(self.suffix.len().try_into().unwrap());
        for x in &self.suffix[0..] {
            x.to_frame(frame);
        }
        frame.push_int(self.sync_idx);
        frame.push_int(self.decided_idx);
        if self.stopsign.is_some() {
            let stopsign = self.stopsign.as_ref().unwrap();
            stopsign.to_frame(frame);
        } else {
            frame.push_int(0);
        }
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = match Ballot::parse_frame(parse)? {
            OpMessage::Ballot(m) => m,
            _ => panic!("message error; incorrect spmessage parsed"),
        };
        let len = parse.next_int()?;
        let mut suffix = Vec::new();
        for _ in 0..len {
            suffix.push(match KeyValue::parse_frame(parse)? {
                OpMessage::KeyValue(m) => KeyValue {
                    key: m.key,
                    value: m.value,
                },
                _ => panic!("message error; incorrect spmessage parsed"),
            });
        }
        let sync_idx = parse.next_int()?;
        let decided_idx = parse.next_int()?;
        let stopsign = match StopSign::parse_frame(parse).unwrap() {
            OpMessage::StopSign(s) => s,
            _ => panic!("message error; incorrect message parsed"),
        };

        Ok(OpMessage::PaxosMsg(PaxosMsg::AcceptSync(AcceptSync {
            n,
            decided_snapshot: None,
            suffix,
            sync_idx,
            decided_idx,
            stopsign,
        })))
    }
}

impl ToFromFrame for FirstAccept {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        self.n.to_frame(frame)
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = match Ballot::parse_frame(parse)? {
            OpMessage::Ballot(m) => m,
            _ => panic!(),
        };
        Ok(OpMessage::PaxosMsg(PaxosMsg::FirstAccept(FirstAccept {
            n,
        })))
    }
}
