use futures::FutureExt;
use futures::future::BoxFuture;
use omnipaxos_core::messages::Message::{BLE, SequencePaxos};
use omnipaxos_core::omni_paxos::{OmniPaxosConfig, OmniPaxos, ReconfigurationRequest};
use omnipaxos_core::storage::Snapshot;
use omnipaxos_core::util::LogEntry;
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, TcpSocket};
use tokio::time;
use tokio::sync::Mutex;


use crate::cli::Result;
use crate::cli::command::Command;
use crate::cli::connection::Connection;
use crate::cli::error::Error;
use crate::cli::frame::Frame;
use crate::cli::op_message::OpMessage;
use crate::cli::response::Response;
use crate::{OUTGOING_MESSAGES_TIMEOUT, ELECTION_TIMEOUT, DEFAULT_ADDR, CHECK_STOPSIGN_TIMEOUT};


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
        // set reuseport with Tokio TcpSocket
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_reuseport(true).expect("Failed to set reuseport on TcpSocket");
        socket.bind(socket_addr.parse().unwrap()).expect("Failed to bind");
        let listener = socket.listen(1024).unwrap();
        // set non-blocking using std::net::TcpListener
        let std_listener = listener.into_std().unwrap();
        
        // let std_listener = std::net::TcpListener::bind(&socket_addr).expect("Failed to bind");
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


async fn listen(omni_paxos: &Arc<Mutex<OmniPaxosKV>>, listener: TcpListener, pid: u64, topology: HashMap<u64, String>) {
    
    println!("[OPServer {}] Begin listening for incoming messages", pid);

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {

                // Received a new connection from a client.

                println!("[OPServer {}] Received connection from {}", pid, addr);
                let omni_paxos = Arc::clone(omni_paxos);

                let processor_topology = topology.clone();
                tokio::spawn(async move {
                    let res = process_incoming_connection(
                        &omni_paxos, 
                        &mut stream,
                        pid,
                        processor_topology,
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
async fn process_incoming_connection(omni_paxos: &Arc<Mutex<OmniPaxosKV>>, stream: &mut TcpStream, pid: u64, topology: HashMap<u64, String>) -> Result<()> {
    
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
            Command::Reconfigure(reconfigure_message) => {
                println!("[OPServer {}] Received reconfigure message {:?}", pid, reconfigure_message);
                let new_pid = reconfigure_message.pid();
                let metadata = None;
                let mut new_topology_ids: Vec<u64> = topology.clone().into_keys().collect();
                new_topology_ids.push(new_pid);
                let rc = ReconfigurationRequest::with(new_topology_ids, metadata);
                match omni_paxos.lock().await.reconfigure(rc) {
                    Ok(_) => {
                        let response_frame = Response::new("reconfigure".to_string(), "OK".to_string()).to_frame();
                        connection.write_frame(&response_frame).await;
                    }
                    Err(_) => {
                        let error_frame = Error::new("Failed to propose reconfiguration".to_string()).to_frame();
                        connection.write_frame(&error_frame).await;
                    }
                }
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
                
                println!("[OPServer {}]: trying to send message to receiver {}", pid, receiver);
                match topology.get(&receiver) {
                    Some(socket_addr) => {
                        let new_stream = TcpStream::connect(&socket_addr).await.unwrap();
                        connections.insert(receiver, new_stream);
                    },
                    None => continue,
                };

            } 
            
            let stream = connections.get_mut(&receiver).unwrap();

            // Send message through socket
            match msg {
                SequencePaxos(m) => {
                    let frame = OpMessage::SequencePaxos(m).to_frame();
                    println!("[OPServer {}] Sending SequencePaxos frame: {:?}", pid, frame);
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

async fn check_stopsign_periodically(omni_paxos: Arc<Mutex<OmniPaxosKV>>, pid: u64, prev_config_handles:&Vec<tokio::task::JoinHandle<()>>) -> Option<OmniPaxosServer>{
    let handles_to_abort = prev_config_handles;
    let mut interval = time::interval(CHECK_STOPSIGN_TIMEOUT);
    loop {
        interval.tick().await;
        // println!("[OPServer {}]: checking stopsign", {pid});

        let decided_entries: Option<Vec<LogEntry<KeyValue, KeyValueSnapshot>>> = omni_paxos.lock().await.read_decided_suffix(0);
        if let Some(de) = decided_entries {
            for d in de {
                match d {
                    LogEntry::StopSign(stopsign) => {
                        println!("[OPServer {}]: StopSign found, beginning reconfiguration", pid);
                        let new_configuration = stopsign.nodes;
                        if new_configuration.contains(&pid) {
                            // we are in new configuration, start new instance
                            let mut new_topology = HashMap::new();
                            for node in new_configuration {
                                new_topology.insert(node, format!("{}:{}", DEFAULT_ADDR, node + 50000 - 1));
                            }
                            let new_op_server = OmniPaxosServer::new(stopsign.config_id, new_topology, pid).await;
                            // abort old process handles
                            for handle in &handles_to_abort[..] {
                                handle.abort();
                            }
                            return Some(new_op_server);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

pub fn run_recovery(pid: u64, op: OmniPaxosServer) -> BoxFuture<'static, Vec<tokio::task::JoinHandle<()>>>{
    async move {
        let op_lock = Arc::new(Mutex::new(op.omni_paxos));
        let op_lock_run_clone = Arc::clone(&op_lock);
        let op_lock_reconfig_clone = Arc::clone(&op_lock);
        let op_lock_check_clone = Arc::clone(&op_lock);
        let prev_config_handles = run(pid, op_lock_run_clone, op.listener, op.topology).await;
        let new_op_server = check_stopsign_periodically(op_lock_reconfig_clone, pid, &prev_config_handles).await;
        if new_op_server.is_some() {
            let new_op_server = new_op_server.unwrap();
            let new_config = &op_lock_check_clone.lock().await.is_reconfigured();
            if new_config.is_some() {
                println!("[OPServer {}]: running new configuration: {:?}", pid, new_config.as_ref().unwrap().nodes);
            }
            run_recovery(pid, new_op_server).await;
        }
        return prev_config_handles;
    }.boxed()
}

pub async fn run(pid: u64, op_lock: Arc<Mutex<OmniPaxosKV>>, listener: TcpListener, topology: HashMap<u64, String>) -> Vec<tokio::task::JoinHandle<()>>{
    // let op_lock = Arc::new(Mutex::new(op.omni_paxos));
    let mut handles = Vec::new();

    // Listen to incoming connections
    let listener = listener;
    let op_lock_listener_clone = Arc::clone(&op_lock);
    let listener_topology = topology.clone();
    handles.push(tokio::spawn(async move {
        listen(&op_lock_listener_clone, listener, pid, listener_topology).await;
    }));

    // Periodically send outgoing messages if any
    let op_lock_sender_clone = Arc::clone(&op_lock);
    let sender_topology = topology.clone();
    handles.push(tokio::spawn(async move {
        send_outgoing_msgs_periodically(&op_lock_sender_clone, sender_topology, pid).await;
    }));

    // Periodically send outgoing messages if any = op.topology;
    let op_lock_election_clone = Arc::clone(&op_lock);
    handles.push(tokio::spawn(async move {
        call_leader_election_periodically(&op_lock_election_clone).await;
    }));

    // let op_lock_reconfig_clone = Arc::clone(&op_lock);
    // let handle_arc = Arc::new(Mutex::new(handles));
    // let handle_clone = Arc::clone(&handle_arc);
    // handles.push(tokio::spawn(async move {
    //     check_stopsign_periodically(&op_lock_reconfig_clone, pid, handle_clone).await;
    // }));

    return handles;
}
