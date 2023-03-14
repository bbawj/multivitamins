use commitlog::LogOptions;
use futures::FutureExt;
use futures::future::BoxFuture;
use omnipaxos_core::messages::Message::{BLE, SequencePaxos};
use omnipaxos_core::omni_paxos::{OmniPaxosConfig, OmniPaxos, ReconfigurationRequest, CompactionErr};
use omnipaxos_core::storage::Snapshot;
use omnipaxos_core::util::LogEntry;
use omnipaxos_storage::persistent_storage::{PersistentStorageConfig, PersistentStorage};
use serde::{Serialize, Deserialize};
use sled::Config;
use tokio::sync::mpsc::Sender;
use std::collections::{HashMap, HashSet};
use std::str::Utf8Error;
use std::fmt;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, TcpSocket};
use tokio::time;
use tokio::sync::{Mutex, mpsc};


use crate::cli::Result;
use crate::cli::command::Command;
use crate::cli::connection::Connection;
use crate::cli::error::Error;
use crate::cli::op_message::OpMessage;
use crate::cli::response::Response;
use crate::{OUTGOING_MESSAGES_TIMEOUT, ELECTION_TIMEOUT, DEFAULT_ADDR, CHECK_STOPSIGN_TIMEOUT};


#[derive(Clone, Debug, Serialize, Deserialize)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub val: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

pub type OmniPaxosKV = OmniPaxos<KeyValue, KeyValueSnapshot, PersistentStorage<KeyValue, KeyValueSnapshot>>;

pub struct OmniPaxosServer {
    pub pid: u64,  // The id of this server.
    pub socket_addr: String,  // The socket address (ip_address:port) of this server (which is also topology[pid])
    pub omni_paxos: OmniPaxosKV,  // The OmniPaxos instance that this server is running.
    pub topology: HashMap<u64, String>,  // The topology of the cluster, mapping node ids to ports.
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

        // persistent storage
        let log_path = format!("logs/{}/server_{}_logs/", configuration_id, pid);
        let commitlog_options = LogOptions::new(log_path.clone());
        let sled_options = Config::new().path(log_path.clone());
        let storage_config = PersistentStorageConfig::with(log_path.to_string(), commitlog_options, sled_options);
        let storage = PersistentStorage::open(storage_config);
        let omni_paxos = omnipaxos_config.build(storage);
        // =================== The OmniPaxos instance is now set up. ===================

        let socket_addr = topology.get(&pid).unwrap().clone();
        
        Self {
            pid,
            socket_addr,
            omni_paxos,
            topology,
        }

    }

}

async fn process_until_disconnect(tx: Sender<bool>, listener: &TcpListener, omni_paxos: &Arc<Mutex<OmniPaxosKV>>, topology: HashMap<u64, String>, pid: u64) -> Option<tokio::task::JoinHandle<()>> {
    let connection_handle; 

    match listener.accept().await {
        Ok((stream, addr)) => {

            // Received a new connection from a client.

            println!("[OPServer {}] Received connection from {}", pid, addr);
            let omni_paxos = Arc::clone(omni_paxos);

            let processor_topology = topology.clone();

            connection_handle = tokio::spawn(async move {
                let res = process_incoming_connection(
                    &omni_paxos, 
                    stream,
                    pid,
                    processor_topology,
                    ).await;

                match res {
                    Ok(_) => todo!(),
                    Err(e) => {
                        match e {
                            ProcessConnectionError::Disconnect => {
                                // disconnect = true;
                                println!("[OPServer {}]: got disconnect error", pid);
                                tx.send(true).await.unwrap();
                            }
                            _ => {
                                println!("[OPServer {}] Error while processing incoming message: {:?}", pid, e);
                            }
                        }
                    }
                }
            });
        },
        Err(e) => {
            println!("[OPServer {}] Error while listening to message: {:?}", pid, e);
            return None;
        }
    };

    return Some(connection_handle);
}

async fn listen(omni_paxos: &Arc<Mutex<OmniPaxosKV>>, listener: TcpListener, pid: u64, topology: HashMap<u64, String>) {
    
    println!("[OPServer {}] Begin listening for incoming messages", pid);
    let mut disconnect = false;
    let mut connection_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    let (tx, mut rx) = mpsc::channel::<bool>(32);
    loop {
        let tx2 = tx.clone();
        if disconnect {
            println!("[OPServer {}]: disconnecting", pid);
            for handle in &connection_handles {
                    handle.abort();
            }
            break
        }

        tokio::select! {
            maybe_handle = process_until_disconnect(tx2, &listener, omni_paxos, topology.clone(), pid) => {
                if maybe_handle.is_some() {
                    connection_handles.push(maybe_handle.unwrap());
                }
            }
            Some(disconnect_request) = rx.recv() => {
                if disconnect_request {
                    println!("setting disconnect to true");
                    disconnect = true;
                }
            }
        };
    }

}

#[derive(Debug)]
pub enum ProcessConnectionError {
    Disconnect,
    SaveSnapshotError(sled::Error),
    ReadSnapshotError(Utf8Error),
    Other(crate::cli::Error)
}

impl From<crate::cli::Error> for ProcessConnectionError {
    fn from(src: crate::cli::Error) -> ProcessConnectionError {
        ProcessConnectionError::Other(src.into())
    }
}

impl From<std::io::Error> for ProcessConnectionError {
    fn from(src: std::io::Error) -> ProcessConnectionError {
        ProcessConnectionError::Other(src.into())
    }
}

impl From<sled::Error> for ProcessConnectionError {
    fn from(src: sled::Error) -> ProcessConnectionError {
        ProcessConnectionError::SaveSnapshotError(src.into())
    }
}

impl From<Utf8Error> for ProcessConnectionError {
    fn from(src: Utf8Error) -> ProcessConnectionError {
        ProcessConnectionError::ReadSnapshotError(src.into())
    }
}

impl fmt::Display for ProcessConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcessConnectionError::Disconnect => "process connection error; disconnect requested".fmt(f),
            ProcessConnectionError::SaveSnapshotError(e) => write!(f, "process connection error; save snapshot failed with {:?}", e),
            ProcessConnectionError::ReadSnapshotError(e) => write!(f, "process connection error; read snapshot failed with {:?}", e),
            ProcessConnectionError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ProcessConnectionError {}

// Method that handles incoming messages, that gets called by the listen method,
// in a separate thread, when a new message is received.
async fn process_incoming_connection(omni_paxos: &Arc<Mutex<OmniPaxosKV>>, stream: TcpStream, pid: u64, topology: HashMap<u64, String>) -> std::result::Result<(), ProcessConnectionError> {
    
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
                match m {
                    SequencePaxos(_) => {
                        println!("[OPServer {}] Received OpMessage: {:?}\n", pid, m);
                    },
                    BLE(_) => {},
                }
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
                        connection.write_frame(&response_frame).await?;
                    }
                    Err(_) => {
                        let error_frame = Error::new("Failed to propose reconfiguration".to_string()).to_frame();
                        connection.write_frame(&error_frame).await?;
                    }
                }
            }
            Command::SaveSnapshot(save_snapshot_message) => {
                println!("[OPServer {}] Received save snapshot message {:?}", pid, save_snapshot_message);
                // unsure if we want specific or just latest snapshot
                let snapshot_idx = None;
                let local_only = true;
                let mut op = omni_paxos.lock().await;
                match op.snapshot(snapshot_idx, local_only) {
                    Ok(_) => {
                        // save to persistent storage
                        println!("[OPServer {}] Snapshot OK", pid);
                        let compacted_idx = op.get_compacted_idx();
                        println!("[OPServer {}] Snapshot ID {}", pid, compacted_idx);

                        match op.read(compacted_idx-1) {
                            Some(LogEntry::Snapshotted(s)) => {
                                println!("[OPServer {}] Read Snapshot OK", pid);
                                let snapshot = s.snapshot;
                                let db_path = format!("snapshots/{}/{}", pid, compacted_idx);
                                let snapshot_db = sled::open(db_path.clone()).unwrap();
                                println!("[OPServer {}] Open DB OK", pid);
                                for (k, v) in snapshot.db {
                                    snapshot_db.insert(&k[..], &v[..])?;
                                }
                                snapshot_db.flush_async().await?;
                                let response_frame = Response::new("snapshot".to_string(), format!("saved on OPServer {} as {}", pid, db_path).to_string()).to_frame();
                                connection.write_frame(&response_frame).await?;
                            }
                            None => {
                                let error_frame = Error::new("Log entry does not exist".to_string()).to_frame();
                                connection.write_frame(&error_frame).await?;
                            }
                            _ => {
                                let error_frame = Error::new("Failed to create snapshot".to_string()).to_frame();
                                connection.write_frame(&error_frame).await?;
                            }
                        }
                    }
                    Err(e) => {
                        match e {
                            CompactionErr::UndecidedIndex(e) => {
                                // Our provided snapshot index is not decided yet. The currently decided index is `idx`.
                                println!("[OPServer {}] Error saving snapshot {:?}", pid, e);
                            }
                            // these 2 errors are related to Trimming instead
                            CompactionErr::NotAllDecided(_) => unreachable!(),
                            CompactionErr::NotCurrentLeader(_) => unreachable!()
                        }
                    }
                }
            },
            Command::ReadSnapshot(read_snapshot_message) => {
                println!("[OPServer {}] Received read snapshot message {:?}", pid, read_snapshot_message);
                let snapshot_db = match sled::open(read_snapshot_message.file_path()) {
                    Ok(db) => db,
                    Err(_) => {
                        let error_frame = Error::new("Failed to open path to snapshot".to_string()).to_frame();
                        connection.write_frame(&error_frame).await?;
                        continue
                    }
                };
                let res = snapshot_db.export();
                let mut pairs: String = Default::default();
                for i in res {
                    let entries = i.2;
                    for line in entries {
                        let k = &line[0];
                        let v = &line[1];
                        let key = std::str::from_utf8(&k)?.to_owned();
                        let value = std::str::from_utf8(&v)?.to_owned();
                        let pair = format!("{{key: {}, value: {}}}, ", key, value);
                        pairs.push_str(&pair);
                    }
                }
                let response_frame = Response::new("snapshot".to_string(), pairs).to_frame();
                connection.write_frame(&response_frame).await?;
            }
            Command::Response(response_msg) => {
                println!("[OPServer {}] Received response: {:?}", pid, response_msg);
                continue;
            }
            Command::Error(error_message) => {
                println!("[OPServer {}] Received error: {:?}", pid, error_message);
                continue;
            }
            Command::Disconnect(disconnect_message) => {
                println!("[OPServer {}] Received disconnect: {:?}", pid, disconnect_message);
                let response_frame = Response::new("disconnect".to_string(), "OK".to_string()).to_frame();
                connection.write_frame(&response_frame).await.unwrap();
                return Err(ProcessConnectionError::Disconnect);
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
    let mut connections: HashMap<u64, Connection> = HashMap::new();
    let mut failed_nodes: HashSet<u64> = HashSet::new();

    loop {
        outgoing_interval.tick().await;
        
        let mut op = omni_paxos.lock().await;
        let messages = op.outgoing_messages();

        for msg in messages {

            let receiver = msg.get_receiver();

            // If a TCP Connection has not been established before, establish one
            if !connections.contains_key(&receiver) {
                
                println!("[OPServer {}]: trying to send message to receiver {}", pid, receiver);
                let _ = match topology.get(&receiver) {
                    Some(socket_addr) => {
                        match TcpStream::connect(&socket_addr).await {
                            Ok(new_stream) => {
                                connections.insert(receiver, Connection::new(new_stream));
                                if failed_nodes.contains(&receiver) {
                                    failed_nodes.remove(&receiver);
                                    println!("[OPServer {}]: Reconnected with {}", pid, receiver);
                                }
                            }
                            Err(e) => { match e.kind() {
                                std::io::ErrorKind::ConnectionRefused => {
                                    // connections.remove(&receiver);
                                    continue;
                                }
                                _ => panic!("{}", e),
                            }
                            }
                        }
                    },
                    None => continue,
                };

            } 
            
            let connection = connections.get_mut(&receiver).unwrap();

            // Send message through socket
            match msg {
                SequencePaxos(m) => {
                    let frame = OpMessage::SequencePaxos(m).to_frame();
                    println!("[OPServer {}] Sending SequencePaxos frame: {:?}", pid, frame);
                    // let mut connection = Connection::new(stream);
                    let write_res = connection.write_frame(&frame).await;
                    match write_res {
                        Ok(_) => {}
                        Err(e) => {
                            println!("[OPServer {}] Error writing BLE Message frame: {:?}", pid, e);
                            connections.remove(&receiver);
                            failed_nodes.insert(receiver);
                            continue;
                        }
                    }
                }
                BLE(m) => {
                    let frame = OpMessage::BLEMessage(m).to_frame();
                    // println!("[OPServer {}] Sending BLE frame: {:?}", pid, frame);
                    // let mut connection = Connection::new(stream);
                    let write_res = connection.write_frame(&frame).await;
                    match write_res {
                        Ok(_) => {}
                        Err(e) => {
                            // println!("[OPServer {}] Error writing BLE Message frame: {:?}", pid, e);
                            connections.remove(&receiver);
                            failed_nodes.insert(receiver);
                            continue;
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

async fn check_stopsign_periodically(omni_paxos: Arc<Mutex<OmniPaxosKV>>, pid: u64, prev_config_handles:&Vec<tokio::task::JoinHandle<()>>) -> Result<()>{
    let handles_to_abort = prev_config_handles;
    let mut interval = time::interval(CHECK_STOPSIGN_TIMEOUT);
    loop {
        interval.tick().await;
        let op = omni_paxos.lock().await;
        let maybe_stopsign = op.is_reconfigured();
        if maybe_stopsign.is_none() {
            continue;
         }
         // println!("[OPServer {}]: checking stopsign", {pid});
 
        let decided_entries: Option<Vec<LogEntry<KeyValue, KeyValueSnapshot>>> = op.read_decided_suffix(0);
        // I wonder why we need to loop through all the decided entries rather than calling
        // is_reconfigured for the StopSign directly...
        if let Some(de) = decided_entries {
            for d in de {
                match d {
                    LogEntry::StopSign(stopsign) => {
                        println!("[OPServer {}]: StopSign found, beginning reconfiguration", pid);
                        let new_configuration = stopsign.nodes;
                        if new_configuration.contains(&pid) {
                            // we are in new configuration, start new instance
                            // abort old process handles
                            for handle in handles_to_abort {
                                handle.abort();
                            }

                            return Ok(());
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
        let prev_config_handles = run(pid, op_lock_run_clone, op.topology.clone()).await;
        match check_stopsign_periodically(op_lock_reconfig_clone, pid, &prev_config_handles).await {
            Ok(_) => {
                for handle in prev_config_handles {
                    assert!(handle.await.unwrap_err().is_cancelled());
                }

                // drop(op_lock);

                if let Some(new_config) = &op_lock_check_clone.lock().await.is_reconfigured() {
                    println!("[OPServer {}]: running new configuration: {:?}", pid, new_config.nodes);
                    let mut new_topology = HashMap::new();
                    for id in 0..(new_config.nodes.len() as u64) {
                        new_topology.insert(1+id, format!("{}:{}", DEFAULT_ADDR, id + 50000));
                    }
                    let new_op_server = OmniPaxosServer::new(new_config.config_id, new_topology, pid).await;
                    run_recovery(pid, new_op_server).await;
                }
            }
            Err(e) => {
                println!("[OPServer {}]: error in check_stopsign_periodically {:?}", pid, e);
            }
        }

        // return prev_config_handles;
        return Vec::new();
    }.boxed()
}

pub async fn listen_until_disconnect(pid: u64, op_lock: &Arc<Mutex<OmniPaxosKV>>, topology: HashMap<u64, String>) -> Vec<tokio::task::JoinHandle<()>> {
    loop {
        // Create a listener for incoming messages.
        let topology_clone = topology.clone();
        let socket_addr = topology_clone.get(&pid).unwrap().clone();
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
        listen(&op_lock, listener, pid, topology_clone).await;
        time::sleep(time::Duration::from_secs(20)).await;
        // call fail_recovery in order to sync any logs
        op_lock.lock().await.fail_recovery();
    }
}

pub async fn run(pid: u64, op_lock: Arc<Mutex<OmniPaxosKV>>, topology: HashMap<u64, String>) -> Vec<tokio::task::JoinHandle<()>>{
    // let op_lock = Arc::new(Mutex::new(op.omni_paxos));
    let mut handles = Vec::new();

    // Listen to incoming connections
    let op_lock_listener_clone = Arc::clone(&op_lock);
    let sender_topology = topology.clone();
    handles.push(tokio::spawn(async move {
        listen_until_disconnect(pid, &op_lock_listener_clone, topology).await;
    }));

    // Periodically send outgoing messages if any
    let op_lock_sender_clone = Arc::clone(&op_lock);
    handles.push(tokio::spawn(async move {
        send_outgoing_msgs_periodically(&op_lock_sender_clone, sender_topology, pid).await;
    }));

    // Periodically send outgoing messages if any = op.topology;
    let op_lock_election_clone = Arc::clone(&op_lock);
    handles.push(tokio::spawn(async move {
        call_leader_election_periodically(&op_lock_election_clone).await;
    }));

    return handles;
}
