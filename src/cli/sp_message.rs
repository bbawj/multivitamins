use omnipaxos_core::{
    ballot_leader_election::Ballot,
    messages::sequence_paxos::{PaxosMessage, PaxosMsg, Prepare, Promise},
    storage::StopSign,
};

use crate::op_server::KeyValue;

use super::{frame::Frame, parse::Parse};

pub enum SpMessage {
    SpMessage(PaxosMessage<KeyValue, ()>),
    KeyValue(KeyValue),
    Ballot(Ballot),
}
impl SpMessage {
    pub(crate) fn parse(&self) -> Frame {
        let mut frame = Frame::array();
        match self {
            SpMessage::SpMessage(m) => {
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
            SpMessage::KeyValue(_) => todo!(),
            SpMessage::Ballot(_) => todo!(),
        }
    }
}

trait ToFromFrame {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame;
    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<SpMessage>;
}

impl ToFromFrame for KeyValue {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string(&self.key);
        frame.push_int(self.value);
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<SpMessage> {
        let key = parse.next_string()?;
        let value = parse.next_int()?;
        Ok(SpMessage::KeyValue(KeyValue { key, value }))
    }
}

impl ToFromFrame for Ballot {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_int(self.n.into());
        frame.push_int(self.priority);
        frame.push_int(self.pid);
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<SpMessage> {
        let n = parse.next_int()?.try_into().unwrap();
        let priority = parse.next_int()?;
        let pid = parse.next_int()?;
        Ok(SpMessage::Ballot(Ballot { n, priority, pid }))
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
            frame.push_int(1);
            let stopsign = self.stopsign.as_ref().unwrap();
            frame.push_int(stopsign.config_id.into());
            frame.push_int(stopsign.nodes.len().try_into().unwrap());
            for node in &stopsign.nodes[0..] {
                frame.push_int(*node);
            }
            if stopsign.metadata.is_some() {
                frame.push_int(1);
                let metadata = stopsign.metadata.as_ref().unwrap();
                frame.push_int(metadata.len().try_into().unwrap());
                for x in metadata {
                    frame.push_int((*x).into());
                }
            } else {
                frame.push_int(0);
            }
        } else {
            frame.push_int(0);
        }
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<SpMessage> {
        let n = match Ballot::parse_frame(parse)? {
            SpMessage::Ballot(m) => Ballot {
                n: m.n,
                priority: m.priority,
                pid: m.pid,
            },
            _ => panic!("message error; incorrect spmessage parsed"),
        };
        let n_accepted = match Ballot::parse_frame(parse)? {
            SpMessage::Ballot(m) => Ballot {
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
                SpMessage::KeyValue(m) => KeyValue {
                    key: m.key,
                    value: m.value,
                },
                _ => panic!("message error; incorrect spmessage parsed"),
            });
        }
        let decided_idx = parse.next_int()?;
        let accepted_idx = parse.next_int()?;
        let has_stop_sign = parse.next_int()?;
        let stopsign = if has_stop_sign == 1 {
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
            Some(StopSign {
                config_id,
                nodes,
                metadata: if has_metadata == 1 {
                    Some(metadata)
                } else {
                    None
                },
            })
        } else {
            None
        };

        Ok(SpMessage::SpMessage(PaxosMessage {
            from: parse.next_int()?,
            to: parse.next_int()?,
            msg: PaxosMsg::Promise(Promise {
                n,
                n_accepted,
                decided_snapshot: None,
                suffix,
                decided_idx,
                accepted_idx,
                stopsign,
            }),
        }))
    }
}

impl ToFromFrame for Prepare {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("prepare");
        frame.push_int(self.n.n.into());
        frame.push_int(self.n.priority);
        frame.push_int(self.n.pid);
        frame.push_int(self.decided_idx);
        frame.push_int(self.n_accepted.n.into());
        frame.push_int(self.n_accepted.priority);
        frame.push_int(self.n_accepted.pid);
        frame.push_int(self.accepted_idx);
        frame
    }

    fn parse_frame(parse: &mut Parse) -> crate::cli::Result<SpMessage> {
        todo!()
    }
}
