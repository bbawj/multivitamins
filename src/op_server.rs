use std::sync::{Arc, Mutex};
use std::io::{BufRead, BufReader};
use tokio::net::{TcpStream, TcpListener};
use omnipaxos_core::util::NodeId;
use std::collections::HashMap;

use crate::OmniPaxosKV;

pub struct OmniPaxosServer {
    pub node_id : NodeId,
    pub db : HashMap<String, u64>,
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,
    pub incoming: mpsc::Receiver<Message<KeyValue, ()>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<KeyValue, ()>>>,
}

impl OmniPaxosServer {
    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();
            let channel = self
                .outgoing
                .get_mut(&receiver)
                .expect("No channel for receiver");
            let _ = channel.send(msg).await;
        }
    }

    pub async fn listen(&mut self) {
        // Bind the listener to the address
        let address: String = String::from("127.0.0.1");
        let port: u64 = 50000 + self.node_id;
        let listener = TcpListener::bind(format!("{}:{}", address, port)).await.unwrap();

        loop {
            // The second item contains the ip and port of the new connection.
            let (socket, _) = listener.accept().await.unwrap();

            // A new task is spawned for each inbound socket.  The socket is
            // moved to the new task and processed there.
            tokio::spawn(async move {
                process(socket).await;
            });
        }
    }
    async fn process(socket: TcpStream) {
        let buf_reader = BufReader::new(&mut socket);
        let msg: Vec<_> = buf_reader
            .lines()
            .map(|result| result.unwrap())
            .take_while(|line| !line.is_empty())
            .collect();

        omnipaxos.handle_incoming(msg);

        // Use `read_frame` to receive a command from the connection.
        while let Some(frame) = connection.read_frame().await.unwrap() {
            let response = match Command::from_frame(frame).unwrap() {
                Set(cmd) => {
                    // The value is stored as `Vec<u8>`
                    db.insert(cmd.key().to_string(), cmd.value().to_vec());
                    Frame::Str("OK".to_string())
                }
                Get(cmd) => {
                    if let Some(value) = db.get(cmd.key()) {
                        // `Frame::Bulk` expects data to be of type `Bytes`. This
                        // type will be covered later in the tutorial. For now,
                        // `&Vec<u8>` is converted to `Bytes` using `into()`.
                        Frame::Bulk(value.clone().into())
                    } else {
                        Frame::Null
                    }
                }
                cmd => panic!("unimplemented {:?}", cmd),
            };

            // Write the response to the client
            connection.write_frame(&response).await.unwrap();
        }
    }

    pub(crate) async fn run(&mut self) {

        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut election_interval = time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                biased;

                _ = election_interval.tick() => { self.omni_paxos.lock().unwrap().election_timeout(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                Some(in_msg) = self.incoming.recv() => { self.omni_paxos.lock().unwrap().handle_incoming(in_msg); },
                else => { }
            }
        }
    }
}
