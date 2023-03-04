use omnipaxos_core::messages::Message::{BLE, SequencePaxos};
use omnipaxos_core::omni_paxos::{OmniPaxosConfig, OmniPaxos};
use omnipaxos_core::storage::Snapshot;
use omnipaxos_core::util::LogEntry;
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::time;
use tokio::sync::Mutex;


use crate::cli::Result;
use crate::cli::command::Command;
use crate::cli::connection::Connection;
use crate::cli::error::Error;
use crate::cli::frame::Frame;
use crate::cli::op_message::OpMessage;
use crate::cli::response::Response;
use crate::{OUTGOING_MESSAGES_TIMEOUT, ELECTION_TIMEOUT};


#[derive(Clone, Debug)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub val: String,
}

#[derive(Clone, Debug)]
pub struct KeyValueSnapshot {
    pub db: HashMap<String, String>,
}

// Our implementation of a snapshot for the key-value store.
impl Snapshot<KeyValue> for KeyValueSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut db = HashMap::new();
        for e in entries {
            let KeyValue { key, val } = e;
            db.insert(key.clone(), val.clone());
        }
        KeyValueSnapshot {
            db
        }
    }

    // Merge two snapshots together.
    fn merge(&mut self, other: KeyValueSnapshot) {
        for (key, val) in other.db {
            self.db.insert(key, val);
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
    pub omni_paxos: OmniPaxosKV,  // The OmniPaxos instance that this server is running.
    pub topology: HashMap<u64, String>,  // The topology of the cluster, mapping node ids to ports.
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

        let omni_paxos = omnipaxos_config.build(MemoryStorage::default());

        // =================== The OmniPaxos instance is now set up. ===================


        // Create a listener for incoming messages.
        let socket_addr = topology.get(&pid).unwrap().clone();
        let std_listener = std::net::TcpListener::bind(&socket_addr).expect("Failed to bind");
        std_listener.set_nonblocking(true).expect("Failed to initialize non-blocking");
        let listener = TcpListener::from_std(std_listener).expect("Failed to convert to async");
        
        Self {
            pid,
            socket_addr,
            omni_paxos,
            topology,
            listener,
        }

    }

}


async fn listen(omni_paxos: &Arc<Mutex<OmniPaxosKV>>, listener: TcpListener, pid: u64) {
    
    println!("[OPServer {}] Begin listening for incoming messages", pid);

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {

                // Received a new connection from a client.

                println!("[OPServer {}] Received connection from {}", pid, addr);
                let omni_paxos = Arc::clone(omni_paxos);

                tokio::spawn(async move {
                    let res = process_incoming_connection(
                        &omni_paxos, 
                        &mut stream,
                        pid
                    ).await;

                    if let Err(e) = res {
                        println!("[OPServer {}] Error while processing incoming message: {:?}", pid, e);
                    }
                });
            },
            Err(e) => {
                println!("[OPServer {}] Error while listening to message: {:?}", pid, e);
            }
        }
    }

}


// Method that handles incoming messages, that gets called by the listen method,
// in a separate thread, when a new message is received.
async fn process_incoming_connection(omni_paxos: &Arc<Mutex<OmniPaxosKV>>, stream: &mut TcpStream, pid: u64) -> Result<()> {
    
    let mut connection = Connection::new(stream);

    // Constantly tries to read
    loop {

        // Handle the incoming message.
        // println!("[OPServer {}] Received frame: {:?}\n", pid, frame);
        let maybe_frame = connection.read_frame().await.unwrap();
        let frame= match maybe_frame {
            Some(frame) => frame,
            None => continue
        };
        let cmd = Command::from_frame(frame)?;
        match cmd {
            Command::OpMessage(m) => {
                // println!("[OPServer {}] Received OpMessage: {:?}\n", pid, m);
                omni_paxos.lock().await.handle_incoming(m);
                continue
            },
            Command::Get(get_message) => {
                println!("[OPServer {}] Received get message: {:?}", pid, get_message);
                let key = get_message.key();
                // Retrieve all the values from the key-value store.
                // Maybe we can optimise this by locally tracking the idx we have updated to so far
                // so that a get command does not involve creating a new hashmap

                let maybe_log_entries = 
                    omni_paxos.lock().await
                    .read_decided_suffix(0);
    
                match maybe_log_entries {
                    Some(log_entries) => {
                        let mut kv_store = HashMap::new();
                        for ent in log_entries {
                            match ent {
                                LogEntry::Decided(kv) => {
                                    kv_store.insert(kv.key, kv.val);
                                }
                                _ => {}
                            }
                        }

                        let option_val = kv_store.get(key);
                        let response_frame = match option_val {
                            Some(val) => Response::new(key.to_string(), val.to_string()).to_frame(),
                            None => Error::new(format!("Key {} is not found", key)).to_frame(),
                        };
                        connection.write_frame(&response_frame).await.unwrap();
                        continue;
                    },
                    None => {
                        let error = format!("Key {} is not found", key);
                        let error_frame = Error::new(error).to_frame();
                        connection.write_frame(&error_frame).await.unwrap();
                        continue;
                    },
                }
            },
            Command::Put(put_message) => {
                println!("[OPServer {}] Received put message: {:?}", pid, put_message);
                let (key, val) = (put_message.key(), put_message.val());
                let kv_to_store = KeyValue { key: key.to_string(), val: val.to_string() };
                omni_paxos.lock().await.append(kv_to_store).expect("Failed to append to OmniPaxos instance");
                let response_frame = Response::new(key.to_string(), val.to_string()).to_frame();
                connection.write_frame(&response_frame).await.unwrap();
                continue;
            },
            Command::Response(response_msg) => {
                println!("[OPServer {}] Received response: {:?}", pid, response_msg);
                continue;
            }
            Command::Error(error_message) => {
                println!("[OPServer {}] Received error: {:?}", pid, error_message);
                continue;
            }
        }
    }

}


// Method that periodically takes outgoing messages from the OmniPaxos instance, and sends them to the appropriate node.
async fn send_outgoing_msgs_periodically(
    omni_paxos: &Arc<Mutex<OmniPaxosKV>>,
    topology: HashMap<u64, String>,
    pid: u64
) {

    let mut outgoing_interval = time::interval(OUTGOING_MESSAGES_TIMEOUT);

    // Store the connections in a mutable reference, so that we can modify it.
    let mut connections: HashMap<u64, TcpStream> = HashMap::new();

    loop {
        outgoing_interval.tick().await;
        
        let messages = omni_paxos.lock().await.outgoing_messages();

        for msg in messages {

            let receiver = msg.get_receiver();

            // If a TCP Connection has not been established before, establish one
            if !connections.contains_key(&receiver) {
                
                let socket_addr = topology.get(&receiver).unwrap();
                let new_stream = TcpStream::connect(&socket_addr).await.unwrap();
                connections.insert(receiver, new_stream);

            } 
            
            let stream = connections.get_mut(&receiver).unwrap();

            // Send message through socket
            match msg {
                SequencePaxos(m) => {
                    let frame = OpMessage::SequencePaxos(m).to_frame();
                    // println!("[OPServer {}] Sending SequencePaxos frame: {:?}", pid, frame);
                    let mut connection = Connection::new(stream);
                    connection.write_frame(&frame).await.unwrap();
                }
                BLE(m) => {
                    let frame = OpMessage::BLEMessage(m).to_frame();
                    // println!("[OPServer {}] Sending BLE frame: {:?}", pid, frame);
                    let mut connection = Connection::new(stream);
                    let write_res = connection.write_frame(&frame).await;
                    match write_res {
                        Ok(_) => {}
                        Err(e) => {
                            // println!("[OPServer {}] Error writing BLE Message frame: {:?}", pid, e);
                        }
                    }
                }
            }

        }
    
    }
}


// Method that periodically takes outgoing messages from the OmniPaxos instance, and sends them to the appropriate node.
async fn call_leader_election_periodically(omni_paxos: &Arc<Mutex<OmniPaxosKV>>) {
    
    let mut election_interval = time::interval(ELECTION_TIMEOUT);
    loop {
        election_interval.tick().await;
        omni_paxos.lock().await.election_timeout();
    }
}

pub async fn run(pid: u64, op: OmniPaxosServer) {

    let op_lock = Arc::new(Mutex::new(op.omni_paxos));

    // Listen to incoming connections
    let listener = op.listener;
    let op_lock_listener_clone = Arc::clone(&op_lock);
    tokio::spawn(async move {
        listen(&op_lock_listener_clone, listener, pid).await;
    });

    // Periodically send outgoing messages if any
    let topology = op.topology;
    let op_lock_sender_clone = Arc::clone(&op_lock);
    tokio::spawn(async move {
        send_outgoing_msgs_periodically(&op_lock_sender_clone, topology, pid).await;
    });

    // Periodically send outgoing messages if any = op.topology;
    let op_lock_election_clone = Arc::clone(&op_lock);
    tokio::spawn(async move {
        call_leader_election_periodically(&op_lock_election_clone).await;
    });

}
