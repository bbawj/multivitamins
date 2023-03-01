use omnipaxos_core::messages::Message::{BLE, SequencePaxos};
use omnipaxos_core::omni_paxos::{OmniPaxosConfig, OmniPaxos};
use omnipaxos_core::storage::Snapshot;
use omnipaxos_core::util::NodeId;
use omnipaxos_storage::memory_storage::MemoryStorage;
use crate::cli::Result;
use crate::cli::command::Command;
use crate::cli::connection::Connection;
use crate::cli::op_message::OpMessage;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

use crate::DEFAULT_ADDR;

// Represents a node in the cluster. Contains enough information for two nodes to communicate with each other.
// To spawn nodes locally, which you can use for testing with threads, use localhost as ip_address.
#[derive(Clone, Debug)] // Clone and Debug are required traits.
pub struct Node {
    pub ip_address: String,
    pub port: u64,
}


#[derive(Clone, Debug)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[derive(Clone, Debug)]
pub struct KeyValueSnapshot {
    pub db: HashMap<String, u64>,
}

// Our implementation of a snapshot for the key-value store.
impl Snapshot<KeyValue> for KeyValueSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut db = HashMap::new();
        for e in entries {
            let KeyValue { key, value } = e;
            db.insert(key.clone(), *value);
        }
        KeyValueSnapshot {
            db
        }
    }

    // Merge two snapshots together.
    fn merge(&mut self, other: KeyValueSnapshot) {
        for (key, value) in other.db {
            self.db.insert(key, value);
        }
    }

    fn use_snapshots() -> bool {
        true
    }

}

pub type OmniPaxosKV = OmniPaxos<KeyValue, KeyValueSnapshot, MemoryStorage<KeyValue, KeyValueSnapshot>>;

pub struct OmniPaxosServer {
    pub port: u64,  // The port that this server is listening on.
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,  // The OmniPaxos instance that this server is running.
    pub topology: HashMap<u64, Node>,  // The topology of the cluster, mapping node ids to ports.
    pub connections: HashMap<u64, TcpStream>,  // The connections to other nodes in the cluster.
}

// Our implementation of an OmniPaxos server, that listens for incoming messages from a TCP socket.
impl OmniPaxosServer {

    // Create a new OmniPaxos server.
    pub fn new(pid: u64, port: u64, config: OmniPaxosConfig, topology: HashMap<u64, Node>) -> Self {

        // Set up the OmniPaxos instance.
        let omni_paxos: Arc<Mutex<OmniPaxosKV>> =            
            Arc::new(Mutex::new(config.build(MemoryStorage::default())));
        
        // let mut new_topology = HashMap::new();
    
        // Set up the TCP connections.
        let connections = HashMap::new();
        // for (node_id, node) in topology {
            // Lower-numbered node initiates the connection, so we only need to connect to nodes with a higher id than pid.

            // For now, just create all connections.
            // println!("Connecting from node {pid} to node {node_id}");
            // let socket = TcpStream::connect(format!("{}:{}", node.ip_address, node.port)).await.unwrap();

            // Add the connection to the connections map.
            // connections.insert(node_id, socket);

            // TODO: not make a new copy...
            // new_topology.insert(node_id, node);
        // }

        // Return data with lifetime of 2
        
        Self { port, omni_paxos, topology, connections }
    }


    pub async fn run(&mut self) {

        // Bind the listener to the port.
        let address: String = String::from(DEFAULT_ADDR);
        let port: u64 = self.port;
        let listener = TcpListener::bind(format!("{}:{}", address, port)).await.unwrap();

        // Spawn a thread to handle incoming messages.
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let omni_paxos = Arc::clone(&self.omni_paxos);
            tokio::spawn(async move {
                process(&omni_paxos, socket).await.unwrap();
            });
        }
    }

    async fn send_outgoing_msgs(&self) {
        let messages = self.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();

            // TODO: get the socket for the receiver. Currently, Connection takes in a TcpStream,
            // but we want to pass it a reference to a TcpStream.
            // let socket = &self.connections[&receiver];
            let ip_address = &self.topology[&receiver].ip_address;
            let port = self.topology[&receiver].port;
            let socket = TcpStream::connect(format!("{ip_address}:{port}")).await.unwrap();

            // Send message through socket
            match msg {
                SequencePaxos(m) => {
                    let frame = OpMessage::SequencePaxos(m).to_frame();
                    let mut connection = Connection::new(socket);
                    connection.write_frame(&frame).await.unwrap();
                }
                BLE(m) => todo!()
            }

        }
    }
}
    
    // Method that handles incoming messages.
    async fn process(omni_paxos: &Arc<Mutex<OmniPaxosKV>>, socket: TcpStream) -> Result<()> {

        // Read the incoming message from the socket.
        let mut connection = Connection::new(socket);
        let maybe_frame = connection.read_frame().await.unwrap();
        let frame= match maybe_frame {
            Some(frame) => frame,
            None => return Ok(()),
        };

        // Handle the incoming message.
        println!("Received frame: {:?}", frame);
        // let cmd = Command::from_frame(frame)?;
        // match cmd {
        //     Command::OpMessage(m) => {
        //         omni_paxos.lock().unwrap().handle_incoming(m);
        //         Ok(())
        //     },
        //     Command::Get(_) => todo!(),
        //     Command::Response(_) => todo!(),
        // }
        Ok(())
    }

//
// impl OmniPaxosServer {
//     async fn send_outgoing_msgs(&mut self) {
//         let messages = self.omni_paxos.lock().unwrap().outgoing_messages();
//         for msg in messages {
//             let receiver = msg.get_receiver();
//             let channel = self
//                 .outgoing
//                 .get_mut(&receiver)
//                 .expect("No channel for receiver");
//             let _ = channel.send(msg).await;
//         }
//     }
//
//     pub async fn listen(&mut self) {
//         // Bind the listener to the address
//         let address: String = String::from("127.0.0.1");
//         let port: u64 = 50000 + self.node_id;
//         let listener = TcpListener::bind(format!("{}:{}", address, port)).await.unwrap();
//
//         loop {
//             // The second item contains the ip and port of the new connection.
//             let (socket, _) = listener.accept().await.unwrap();
//
//             // A new task is spawned for each inbound socket.  The socket is
//             // moved to the new task and processed there.
//             tokio::spawn(async move {
//                 process(socket).await;
//             });
//         }
//     }
//     async fn process(socket: TcpStream) {
//         let buf_reader = BufReader::new(&mut socket);
//         let msg: Vec<_> = buf_reader
//             .lines()
//             .map(|result| result.unwrap())
//             .take_while(|line| !line.is_empty())
//             .collect();
//
//         omnipaxos.handle_incoming(msg);
//
//         // Use `read_frame` to receive a command from the connection.
//         while let Some(frame) = connection.read_frame().await.unwrap() {
//             let response = match Command::from_frame(frame).unwrap() {
//                 Set(cmd) => {
//                     // The value is stored as `Vec<u8>`
//                     db.insert(cmd.key().to_string(), cmd.value().to_vec());
//                     Frame::Str("OK".to_string())
//                 }
//                 Get(cmd) => {
//                     if let Some(value) = db.get(cmd.key()) {
//                         // `Frame::Bulk` expects data to be of type `Bytes`. This
//                         // type will be covered later in the tutorial. For now,
//                         // `&Vec<u8>` is converted to `Bytes` using `into()`.
//                         Frame::Bulk(value.clone().into())
//                     } else {
//                         Frame::Null
//                     }
//                 }
//                 cmd => panic!("unimplemented {:?}", cmd),
//             };
//
//             // Write the response to the client
//             connection.write_frame(&response).await.unwrap();
//         }
//     }
//
//     pub(crate) async fn run(&mut self) {
//
//         let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
//         let mut election_interval = time::interval(ELECTION_TIMEOUT);
//         loop {
//             tokio::select! {
//                 biased;
//
//                 _ = election_interval.tick() => { self.omni_paxos.lock().unwrap().election_timeout(); },
//                 _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
//                 Some(in_msg) = self.incoming.recv() => { self.omni_paxos.lock().unwrap().handle_incoming(in_msg); },
//                 else => { }
//             }
//         }
//     }
// }
// send outgoing messages. This should be called periodically, e.g. every ms
// fn periodically_send_outgoing_msgs(
//     mut omnipaxos: OmniPaxos<KeyValue, (), MemoryStorage<KeyValue, ()>>,
// ) {
//     for out_msg in omnipaxos.outgoing_messages() {
//         let receiver = out_msg.get_receiver();
//         // send out_msg to receiver on network layer
//     }
// }
