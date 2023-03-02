use omnipaxos_core::messages::Message::{BLE, SequencePaxos};
use omnipaxos_core::omni_paxos::{OmniPaxosConfig, OmniPaxos};
use omnipaxos_core::storage::Snapshot;
use omnipaxos_core::util::NodeId;
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::runtime::Runtime;
use crate::cli::Result;
use crate::cli::command::Command;
use crate::cli::connection::Connection;
use crate::cli::op_message::OpMessage;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::{time, join, task};


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
    pub pid: u64,  // The id of this server.
    pub ip_addr: String,  // The ip address that this server is listening on (technically can be retrieved from topology using pid)
    pub port: u64,  // The port that this server is listening on (technically can be retrieved from topology using pid)
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,  // The OmniPaxos instance that this server is running.
    pub topology: HashMap<u64, Node>,  // The topology of the cluster, mapping node ids to ports.
    pub connections: HashMap<u64, TcpStream>,  // The TCP connections to other nodes in the cluster.
    pub listener: TcpListener,  // The listener for incoming messages.
}

// Our implementation of an OmniPaxos server, that listens for incoming messages from a TCP socket.
impl OmniPaxosServer {

    // Create a new OmniPaxos server.
    pub async fn new(config: OmniPaxosConfig, pid: u64, topology: HashMap<u64, Node>) -> Self {

        // Set up the OmniPaxos instance.
        let omni_paxos: Arc<Mutex<OmniPaxosKV>> =            
            Arc::new(Mutex::new(config.build(MemoryStorage::default())));

        // Create a listener for incoming messages.
        let ip_addr = topology.clone().get(&pid).unwrap().ip_address.clone();
        let port = topology.clone().get(&pid).unwrap().port.clone();
        let listener = TcpListener::bind(format!("{}:{}", ip_addr, port)).await.unwrap();
    
    
        // Set up the TCP connections in the run method.
        let mut connections = HashMap::new();
        
        
        Self { pid, ip_addr, port, omni_paxos, topology, connections, listener }
    }

    // Set up outgoing TCP connections.
    pub async fn setup_outgoing_connections(&mut self) {
        for (node_id, node) in &self.topology { 
            if *node_id == self.pid {
                continue;
            }
            println!("Connecting from node {} to node {}", self.pid, node_id);
            let socket = TcpStream::connect(format!("{}:{}", node.ip_address, node.port)).await.unwrap();

            // Add the connection to the connections map.
            self.connections.insert(*node_id, socket);
        }
    }



    // The main method that runs the server.
    // pub async fn run(&mut self) {
    //
    //     // Todo: call self.listen(), self.send_outgoing_msgs_periodically(), and self.setup_outgoing_connections()
    //     // Idea would be to have the first two be executed on separate threads
    //
    //     self.listen();
    //     println!("listening");
    //     self.send_outgoing_msgs_periodically();
    //     println!("will send");
    //     self.setup_outgoing_connections().await;
    //     println!("setup");
    //
    // }

}

// Method that handles incoming messages, that gets called by the listen method,
// in a separate thread, when a new message is received.
async fn process_incoming_messages(omni_paxos: &Arc<Mutex<OmniPaxosKV>>, socket: TcpStream) -> Result<()> {

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

// Private method that processes incoming messages.
async fn listen(op: &Arc<tokio::sync::Mutex<OmniPaxosServer>>) {
    loop {
        let server = op.lock().await;
        let (stream, addr) = server.listener.accept().await.unwrap();
        println!("Received connection from {}", addr);
        let omni_paxos = Arc::clone(&op.lock().await.omni_paxos);
        tokio::spawn(async move {
            process_incoming_messages(&omni_paxos, stream).await.unwrap();
        });
    }
}

// Method that sends outgoing messages - to be called periodically.
async fn send_outgoing_msgs_periodically(op: &Arc<tokio::sync::Mutex<OmniPaxosServer>>) {

    let mut outgoing_interval = time::interval(Duration::from_millis(1));
    loop {
        outgoing_interval.tick().await;  // Wait for this duration

        let messages = op.lock().await.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();

            // TODO: get the socket for the receiver. Currently, Connection takes in a TcpStream,
            // but we want to pass it a reference to a TcpStream.
            // let socket = &self.connections[&receiver];
            let ip_address = &op.lock().await.topology[&receiver].ip_address;
            let port = op.lock().await.topology[&receiver].port;
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

pub fn run(op: OmniPaxosServer) {
    let op_lock = Arc::new(tokio::sync::Mutex::new(op));

    // Start the listener.
    let op_lock_clone_1 = Arc::clone(&op_lock);
    tokio::spawn(async move {
        listen(&op_lock_clone_1).await;
    });

    // Start the periodic sender.
    let op_lock_clone_2 = Arc::clone(&op_lock);
    tokio::spawn(async move {
        send_outgoing_msgs_periodically(&op_lock_clone_2).await;
    });

    // Start the outgoing connections.
    let op_lock_clone_3 = Arc::clone(&op_lock);
    tokio::spawn(async move {
        op_lock_clone_3.lock().await.setup_outgoing_connections().await;
    });

}
