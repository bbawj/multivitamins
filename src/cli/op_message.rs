use std::collections::HashMap;

use omnipaxos_core::{
    ballot_leader_election::Ballot,
    messages::{
        ballot_leader_election::{BLEMessage, HeartbeatMsg, HeartbeatReply, HeartbeatRequest},
        sequence_paxos::{
            AcceptDecide, AcceptStopSign, AcceptSync, Accepted, AcceptedStopSign, Compaction,
            Decide, DecideStopSign, FirstAccept, PaxosMessage, PaxosMsg, Prepare, Promise,
        },
        Message,
    },
    storage::{SnapshotType, StopSign},
};

use crate::op_server::{KeyValue, KeyValueSnapshot};

use super::{frame::Frame, parse::Parse};

pub enum OpMessage {
    SequencePaxos(PaxosMessage<KeyValue, KeyValueSnapshot>),
    PaxosMsg(PaxosMsg<KeyValue, KeyValueSnapshot>),
    BLEMessage(BLEMessage),
    HeartbeatMessage(HeartbeatMsg),
    KeyValue(KeyValue),
    KeyValueSnapshot(KeyValueSnapshot),
    SnapshotType(SnapshotType<KeyValue, KeyValueSnapshot>),
    Ballot(Ballot),
    StopSign(Option<StopSign>),
}

impl OpMessage {
    pub fn to_frame(&self) -> Frame {
        let mut frame = Frame::array();
        match self {
            OpMessage::SequencePaxos(m) => {
                frame.push_string("opmessage");
                frame.push_int(m.from);
                frame.push_int(m.to);
                match &m.msg {
                    PaxosMsg::PrepareReq => {
                        frame.push_string("preparereq");
                        frame
                    }
                    PaxosMsg::Prepare(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::Promise(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::AcceptSync(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::FirstAccept(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::AcceptDecide(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::Accepted(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::Decide(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::ProposalForward(p) => {
                        frame.push_string("proposalforward");
                        frame.push_int(p.len().try_into().unwrap());
                        for kv in p {
                            kv.to_frame(&mut frame);
                        }
                        frame
                    }
                    PaxosMsg::Compaction(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::AcceptStopSign(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::AcceptedStopSign(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::DecideStopSign(p) => {
                        p.to_frame(&mut frame);
                        frame
                    }
                    PaxosMsg::ForwardStopSign(p) => {
                        frame.push_string("forwardstopsign");
                        p.to_frame(&mut frame);
                        frame
                    }
                }
            }
            OpMessage::BLEMessage(m) => {
                frame.push_string("opmessage");
                frame.push_int(m.from);
                frame.push_int(m.to);
                m.msg.to_frame(&mut frame);
                frame
            }
            _ => panic!("OpMessage to_frame should only ever take in Message struct"),
        }
    }
    pub(crate) fn from_frame(
        parse: &mut Parse,
    ) -> crate::cli::Result<Message<KeyValue, KeyValueSnapshot>> {
        let from = parse.next_int()?;
        let to = parse.next_int()?;
        let message_type = parse.next_string()?.to_lowercase();
        match &message_type[..] {
            "preparereq" => Ok(Message::SequencePaxos(PaxosMessage {
                from,
                to,
                msg: PaxosMsg::PrepareReq,
            })),
            "prepare" => {
                let msg = Prepare::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::Prepare(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::Prepare(msg),
                }))
            }
            "promise" => {
                let msg = Promise::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::Promise(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::Promise(msg),
                }))
            }
            "acceptsync" => {
                let msg = AcceptSync::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::AcceptSync(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::AcceptSync(msg),
                }))
            }
            "firstaccept" => {
                let msg = FirstAccept::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::FirstAccept(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::FirstAccept(msg),
                }))
            }
            "acceptdecide" => {
                let msg = AcceptDecide::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::AcceptDecide(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::AcceptDecide(msg),
                }))
            }
            "accepted" => {
                let msg = Accepted::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::Accepted(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::Accepted(msg),
                }))
            }
            "decide" => {
                let msg = Decide::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::Decide(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::Decide(msg),
                }))
            }
            "proposalforward" => {
                let len = parse.next_int()?;
                let mut vec = Vec::new();
                for _ in 0..len {
                    vec.push(parse_keyvalue(parse)?);
                }
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::ProposalForward(vec),
                }))
            }
            "compaction" => {
                let msg = Compaction::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::Compaction(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::Compaction(msg),
                }))
            }
            "acceptstopsign" => {
                let msg = AcceptStopSign::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::AcceptStopSign(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::AcceptStopSign(msg),
                }))
            }
            "acceptedstopsign" => {
                let msg = AcceptedStopSign::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::AcceptedStopSign(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::AcceptedStopSign(msg),
                }))
            }
            "decidestopsign" => {
                let msg = DecideStopSign::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::PaxosMsg(PaxosMsg::DecideStopSign(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::DecideStopSign(msg),
                }))
            }
            "forwardstopsign" => {
                let msg = match StopSign::from_frame(parse)? {
                    OpMessage::StopSign(p) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::SequencePaxos(PaxosMessage {
                    from,
                    to,
                    msg: PaxosMsg::ForwardStopSign(msg.unwrap()), // we can be sure that StopSign
                                                                  // is not none since we are
                                                                  // forwarding a StopSign
                }))
            }
            "heartbeatrequest" => {
                let msg = HeartbeatRequest::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::HeartbeatMessage(HeartbeatMsg::Request(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::BLE(BLEMessage {
                    from,
                    to,
                    msg: HeartbeatMsg::Request(msg),
                }))
            }
            "heartbeatreply" => {
                let msg = HeartbeatReply::from_frame(parse)?;
                let msg = match msg {
                    OpMessage::HeartbeatMessage(HeartbeatMsg::Reply(p)) => p,
                    _ => panic!("invalid message type"),
                };
                Ok(Message::BLE(BLEMessage {
                    from,
                    to,
                    msg: HeartbeatMsg::Reply(msg),
                }))
            }
            _ => panic!(""),
        }
    }
}

trait ToFromFrame {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame;
    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage>;
}

impl ToFromFrame for KeyValue {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string(&self.key);
        frame.push_string(&self.val);
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let key = parse.next_string()?;
        let val = parse.next_string()?;
        Ok(OpMessage::KeyValue(KeyValue { key, val }))
    }
}

impl ToFromFrame for SnapshotType<KeyValue, KeyValueSnapshot> {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        match self {
            SnapshotType::Complete(s) => {
                frame.push_string("complete");
                s.to_frame(frame);
            }
            SnapshotType::Delta(s) => {
                frame.push_string("delta");
                s.to_frame(frame);
            }
            // unsure what to do about this actually
            SnapshotType::_Phantom(_) => (),
        }
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let snapshot_type = parse.next_string()?;
        match &snapshot_type[..] {
            "complete" => {
                let kv_snapshot_message = KeyValueSnapshot::from_frame(parse)?;
                let kv_snapshot = match kv_snapshot_message {
                    OpMessage::KeyValueSnapshot(s) => s,
                    _ => panic!(),
                };
                Ok(OpMessage::SnapshotType(Self::Complete(kv_snapshot)))
            }
            "delta" => {
                let kv_snapshot_message = KeyValueSnapshot::from_frame(parse)?;
                let kv_snapshot = match kv_snapshot_message {
                    OpMessage::KeyValueSnapshot(s) => s,
                    _ => panic!(),
                };
                Ok(OpMessage::SnapshotType(Self::Delta(kv_snapshot)))
            }
            _ => panic!("invalid snapshot_type parsed"),
        }
    }
}

impl ToFromFrame for KeyValueSnapshot {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_int(self.db.len().try_into().unwrap());
        for (k, v) in &self.db {
            let kv = KeyValue {
                key: k.to_string(),
                val: v.to_string(),
            };
            kv.to_frame(frame);
        }
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let len = parse.next_int()?;
        let mut kv_snapshot: KeyValueSnapshot = KeyValueSnapshot { db: HashMap::new() };
        for _ in 0..len {
            let key = parse.next_string()?;
            let val = parse.next_string()?;
            kv_snapshot.db.insert(key, val);
        }
        Ok(OpMessage::KeyValueSnapshot(kv_snapshot))
    }
}

impl ToFromFrame for Ballot {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_int(self.n.into());
        frame.push_int(self.priority);
        frame.push_int(self.pid);
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
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

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
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

impl ToFromFrame for Promise<KeyValue, KeyValueSnapshot> {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("promise");
        self.n.to_frame(frame);
        self.n_accepted.to_frame(frame);
        if self.decided_snapshot.is_some() {
            frame.push_int(1);
            self.decided_snapshot.as_ref().unwrap().to_frame(frame);
        } else {
            frame.push_int(0);
        }
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

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse_ballot(parse)?;
        let n_accepted = parse_ballot(parse)?;
        let decided_snapshot = parse_snapshot(parse)?;
        let len = parse.next_int()?;
        let mut suffix = Vec::new();
        for _ in 0..len {
            suffix.push(parse_keyvalue(parse)?);
        }
        let decided_idx = parse.next_int()?;
        let accepted_idx = parse.next_int()?;
        let stopsign = match StopSign::from_frame(parse).unwrap() {
            OpMessage::StopSign(s) => s,
            _ => panic!("message error; incorrect message parsed"),
        };

        Ok(OpMessage::PaxosMsg(PaxosMsg::Promise(Promise {
            n,
            n_accepted,
            decided_snapshot,
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

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse_ballot(parse)?;
        let decided_idx = parse.next_int()?;
        let n_accepted = match Ballot::from_frame(parse)? {
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

impl ToFromFrame for AcceptSync<KeyValue, KeyValueSnapshot> {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("acceptsync");
        self.n.to_frame(frame);
        if self.decided_snapshot.is_some() {
            frame.push_int(1);
            self.decided_snapshot.as_ref().unwrap().to_frame(frame);
        } else {
            frame.push_int(0);
        }
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

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse_ballot(parse)?;
        let decided_snapshot = parse_snapshot(parse)?;
        let len = parse.next_int()?;
        let mut suffix = Vec::new();
        for _ in 0..len {
            suffix.push(parse_keyvalue(parse)?);
        }
        let sync_idx = parse.next_int()?;
        let decided_idx = parse.next_int()?;
        let stopsign = match StopSign::from_frame(parse).unwrap() {
            OpMessage::StopSign(s) => s,
            _ => panic!("message error; incorrect message parsed"),
        };

        Ok(OpMessage::PaxosMsg(PaxosMsg::AcceptSync(AcceptSync {
            n,
            decided_snapshot,
            suffix,
            sync_idx,
            decided_idx,
            stopsign,
        })))
    }
}

impl ToFromFrame for FirstAccept {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("firstaccept");
        self.n.to_frame(frame)
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = match Ballot::from_frame(parse)? {
            OpMessage::Ballot(m) => m,
            _ => panic!(),
        };
        Ok(OpMessage::PaxosMsg(PaxosMsg::FirstAccept(FirstAccept {
            n,
        })))
    }
}

impl ToFromFrame for AcceptDecide<KeyValue> {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("acceptdecide");
        self.n.to_frame(frame);
        frame.push_int(self.decided_idx);
        frame.push_int(self.entries.len().try_into().unwrap());
        for kv in &self.entries[..] {
            kv.to_frame(frame);
        }
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse_ballot(parse)?;
        let decided_idx = parse.next_int()?;
        let len = parse.next_int()?;
        let mut entries = Vec::new();
        for _ in 0..len {
            entries.push(parse_keyvalue(parse)?);
        }
        Ok(OpMessage::PaxosMsg(PaxosMsg::AcceptDecide(AcceptDecide {
            n,
            decided_idx,
            entries,
        })))
    }
}

impl ToFromFrame for Accepted {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("accepted");
        self.n.to_frame(frame);
        frame.push_int(self.accepted_idx);
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse_ballot(parse)?;
        let accepted_idx = parse.next_int()?;
        Ok(OpMessage::PaxosMsg(PaxosMsg::Accepted(Accepted {
            n,
            accepted_idx,
        })))
    }
}

impl ToFromFrame for Decide {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("decide");
        self.n.to_frame(frame);
        frame.push_int(self.decided_idx);
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse_ballot(parse)?;
        let decided_idx = parse.next_int()?;
        Ok(OpMessage::PaxosMsg(PaxosMsg::Decide(Decide {
            n,
            decided_idx,
        })))
    }
}

impl ToFromFrame for Compaction {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        match self {
            Compaction::Trim(m) => {
                frame.push_string("trim");
                frame.push_int(*m);
            }
            Compaction::Snapshot(m) => {
                frame.push_string("snapshot");
                if m.is_some() {
                    frame.push_int(1);
                    frame.push_int(m.unwrap());
                } else {
                    frame.push_int(0);
                }
            }
        }
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        match &(parse.next_string()?)[..] {
            "trim" => Ok(OpMessage::PaxosMsg(PaxosMsg::Compaction(Compaction::Trim(
                parse.next_int()?,
            )))),
            "snapshot" => Ok(OpMessage::PaxosMsg(PaxosMsg::Compaction(
                Compaction::Snapshot(Some(parse.next_int()?)),
            ))),
            _ => panic!("invalid compaction type"),
        }
    }
}

impl ToFromFrame for AcceptStopSign {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("acceptstopsign");
        self.n.to_frame(frame);
        self.ss.to_frame(frame);
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse_ballot(parse)?;
        let ss = parse_stopsign(parse)?;
        Ok(OpMessage::PaxosMsg(PaxosMsg::AcceptStopSign(
            AcceptStopSign { n, ss },
        )))
    }
}

impl ToFromFrame for AcceptedStopSign {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("acceptedstopsign");
        self.n.to_frame(frame);
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse_ballot(parse)?;
        Ok(OpMessage::PaxosMsg(PaxosMsg::AcceptedStopSign(
            AcceptedStopSign { n },
        )))
    }
}

impl ToFromFrame for DecideStopSign {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("decidestopsign");
        self.n.to_frame(frame);
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let n = parse_ballot(parse)?;
        Ok(OpMessage::PaxosMsg(PaxosMsg::DecideStopSign(
            DecideStopSign { n },
        )))
    }
}

impl ToFromFrame for HeartbeatMsg {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        match self {
            HeartbeatMsg::Request(m) => m.to_frame(frame),
            HeartbeatMsg::Reply(m) => m.to_frame(frame),
        };
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let msg_type = parse.next_string()?;
        match &msg_type[..] {
            "heartbeatrequest" => HeartbeatRequest::from_frame(parse),
            "heartbeatreply" => HeartbeatReply::from_frame(parse),
            _ => panic!("invalid heartbeatmsg"),
        }
    }
}

impl ToFromFrame for HeartbeatRequest {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("heartbeatrequest");
        frame.push_int(self.round.into());
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let round = parse.next_int()?.try_into().unwrap();
        Ok(OpMessage::HeartbeatMessage(HeartbeatMsg::Request(
            HeartbeatRequest { round },
        )))
    }
}

impl ToFromFrame for HeartbeatReply {
    fn to_frame<'a>(&'a self, frame: &'a mut Frame) -> &mut Frame {
        frame.push_string("heartbeatreply");
        frame.push_int(self.round.into());
        self.ballot.to_frame(frame);
        frame.push_int(self.quorum_connected as u64);
        frame
    }

    fn from_frame(parse: &mut Parse) -> crate::cli::Result<OpMessage> {
        let round = parse.next_int()?.try_into().unwrap();
        let ballot = parse_ballot(parse)?;
        let quorum_connected = if parse.next_int()? == 1 { true } else { false };
        Ok(OpMessage::HeartbeatMessage(HeartbeatMsg::Reply(
            HeartbeatReply {
                round,
                ballot,
                quorum_connected,
            },
        )))
    }
}

fn parse_keyvalue(parse: &mut Parse) -> crate::cli::Result<KeyValue> {
    match KeyValue::from_frame(parse)? {
        OpMessage::KeyValue(m) => Ok(m),
        _ => panic!(),
    }
}

fn parse_ballot(parse: &mut Parse) -> crate::cli::Result<Ballot> {
    match Ballot::from_frame(parse)? {
        OpMessage::Ballot(m) => Ok(m),
        _ => panic!(),
    }
}

fn parse_snapshot(
    parse: &mut Parse,
) -> crate::cli::Result<Option<SnapshotType<KeyValue, KeyValueSnapshot>>> {
    let mut decided_snapshot = None;
    let has_snapshot = parse.next_int()?;
    if has_snapshot == 1 {
        let snapshot = match SnapshotType::from_frame(parse)? {
            OpMessage::SnapshotType(s) => s,
            _ => panic!("expected SnapshotType enum while parsing message"),
        };
        decided_snapshot = Some(snapshot);
    }
    Ok(decided_snapshot)
}

fn parse_stopsign(parse: &mut Parse) -> crate::cli::Result<StopSign> {
    match StopSign::from_frame(parse)? {
        OpMessage::StopSign(m) => Ok(m.unwrap()),
        _ => panic!(),
    }
}
