use omnipaxos_core::messages::Message::{BLE, SequencePaxos};
use omnipaxos_core::omni_paxos::{OmniPaxosConfig, OmniPaxos};
use omnipaxos_core::storage::Snapshot;
use omnipaxos_core::util::NodeId;
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::runtime::Runtime;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::{time, task};
use tokio::sync::{MutexGuard};


use crate::cli::Result;
use crate::cli::command::Command;
use crate::cli::connection::Connection;
use crate::cli::op_message::OpMessage;


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
    pub socket_addr: String,  // The socket address (ip_address:port) of this server (which is also topology[pid])
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,  // The OmniPaxos instance that this server is running.
    pub topology: HashMap<u64, String>,  // The topology of the cluster, mapping node ids to ports.
    pub connections: HashMap<u64, TcpStream>,  // The TCP connections to other nodes in the cluster.
    pub listener: TcpListener,  // The listener for incoming messages.
}

// Our implementation of an OmniPaxos server, that listens for incoming messages from a TCP socket.
impl OmniPaxosServer {

    // Create a new OmniPaxos server, by passing the configuration id, the topology of the cluster, and the id of this node.
    // topology[pid] should return the "socket" (ip_address:port) of this node.
    // pub async fn new(configuration_id: u32, topology: HashMap<u64, Node>, pid: u64) -> Self {

    pub async fn new(configuration_id: u32, topology: HashMap<u64, String>, pid: u64) -> Self {

        let num_nodes_total = topology.len() as u64;

        // Basically, if there are n nodes, and this node's PID is 3, then peers == [1, 2, 4, 5, ..., n]
        let mut peers: Vec<u64> = (1..(num_nodes_total+1) as u64).collect();
        peers.retain(|&x| x != pid);

        // Create a new OmniPaxosConfig instance with the given configuration
        let omnipaxos_config = OmniPaxosConfig {
            configuration_id,
            pid,
            peers,
            ..Default::default()
        };

        // Set up the OmniPaxos instance.
        let omni_paxos: Arc<Mutex<OmniPaxosKV>> =            
            Arc::new(Mutex::new(omnipaxos_config.build(MemoryStorage::default())));

        // =================== The OmniPaxos instance is now set up. ===================


        // =================== Now we set up the TCP connections. ===================

        // Create a listener for incoming messages.
        let socket_addr = topology.get(&pid).unwrap().clone();
        let std_listener = std::net::TcpListener::bind(&socket_addr).expect("Failed to bind");
        std_listener.set_nonblocking(true).expect("Failed to initialize non-blocking");
        let listener = TcpListener::from_std(std_listener).expect("Failed to convert to async");

        // Set up the TCP connections in the run method.
        let connections = HashMap::new();
        
        Self { pid, socket_addr, omni_paxos, topology, connections, listener }
    }

    // Set up outgoing TCP connections.
    pub async fn setup_outgoing_connections(&mut self) {
        for (node_id, socket_addr) in &self.topology { 
            if *node_id == self.pid {
                continue;
            }
            println!("[OPServer {}] Connecting from node {} to node {}", self.socket_addr, self.pid, node_id);
            let stream = TcpStream::connect(&socket_addr).await.unwrap();

            // Add the connection to the connections map.
            self.connections.insert(*node_id, stream);
        }
    }

}

// Method that handles incoming messages, that gets called by the listen method,
// in a separate thread, when a new message is received.
async fn process_incoming_messages(omni_paxos: &Arc<Mutex<OmniPaxosKV>>, stream: &mut TcpStream) -> Result<()> {
    
    // Read the incoming message from the socket.
    let mut connection = Connection::new(stream);
    let maybe_frame = connection.read_frame().await.unwrap();
    let frame= match maybe_frame {
        Some(frame) => frame,
        None => return Ok(()),
    };

    // Handle the incoming message.
    println!("[OPServer] Received frame: {:?}", frame);
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
    
    let mut outgoing_interval = time::interval(Duration::from_millis(1));
    // println!("[OPServer {}] Begin listening for incoming messages", server.socket_addr);

    loop {
        outgoing_interval.tick().await;
        println!("[OPServer] Trying to acquire lock to listen for incoming messages");
        let op_obj: MutexGuard<OmniPaxosServer> = op.lock().await;
        match op_obj.listener.accept().await {
            Ok((mut stream, addr)) => {
                println!("[OPServer {}] Received connection from {}", op_obj.socket_addr, addr);
                let omni_paxos = Arc::clone(&op_obj.omni_paxos);
                tokio::spawn(async move {
                    process_incoming_messages(
                        &omni_paxos, 
                        &mut stream
                    ).await.unwrap();
                });
            },
            Err(e) => {}
        }
    }
}

// Method that periodically takes outgoing messages from the OmniPaxos instance, and sends them to the appropriate node.
async fn send_outgoing_msgs_periodically(op: &Arc<tokio::sync::Mutex<OmniPaxosServer>>) {

    let mut outgoing_interval = time::interval(Duration::from_millis(1));
    loop {
        outgoing_interval.tick().await;
        println!("[OPServer] Trying to acquire lock to send outgoing messages");
        let mut op_obj: MutexGuard<OmniPaxosServer> = op.lock().await;
        println!("[OPServer {}] Trying to send outgoing messages", &(op_obj.socket_addr));

        let messages = op_obj.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {

            let receiver = msg.get_receiver();
            let stream = op_obj.connections.get_mut(&receiver).unwrap();

            // Send message through socket
            match msg {
                SequencePaxos(m) => {
                    let frame = OpMessage::SequencePaxos(m).to_frame();
                    println!("[OPServer] Sending frame: {:?}", frame);
                    let mut connection = Connection::new(stream);
                    connection.write_frame(&frame).await.unwrap();
                }
                BLE(m) => todo!()
            }

        }
    
    }
}

pub async fn run(op: OmniPaxosServer) {
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
    op_lock.lock().await.setup_outgoing_connections().await;

}
