use clap::Parser;
use multivitamins::{cli::COMMAND_LISTENER_PORT, SERVER_PORTS};
use omnipaxos_core::omni_paxos::{OmniPaxos, OmniPaxosConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::net::TcpListener;
use std::io::{BufRead, BufReader};

#[derive(Parser)]
struct GetCommand {
    pattern: String,
    key: String,
}

#[derive(Clone, Debug)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

type OmniPaxosKV = OmniPaxos<KeyValue, (), MemoryStorage<KeyValue, ()>>;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let args = GetCommand::parse();
    // configuration with id 1 and the following cluster
    let configuration_id = 1;

    // create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on the other nodes)
    let my_pid = 2;
    let my_peers = vec![1, 3];

    let omnipaxos_config = OmniPaxosConfig {
        configuration_id,
        pid: my_pid,
        peers: my_peers,
        ..Default::default()
    };

    let storage = MemoryStorage::<KeyValue, ()>::default();
    let mut omnipaxos = omnipaxos_config.build(storage);

    // spawn command listener
    tokio::spawn(async move {

    });

    // spawn op_servers
    for pid in SERVER_PORTS {
        let peers = SERVER_PORTS.iter().filter(|&&p| p != pid).copied().collect();
        let op_config = OmniPaxosConfig {
            pid,
            configuration_id,
            peers,
            ..Default::default()
        };
        let omni_paxos: Arc<Mutex<OmniPaxosKV>> =
            Arc::new(Mutex::new(op_config.build(MemoryStorage::default())));
        let mut op_server = OmniPaxosServer {
            omni_paxos: Arc::clone(&omni_paxos),
            incoming: receiver_channels.remove(&pid).unwrap(),
            outgoing: sender_channels.clone(),
        };
        let join_handle = runtime.spawn({
            async move {
                op_server.run().await;
            }
        });
    }
}

fn handle_connection(
    mut stream: TcpStream,
    mut omnipaxos: OmniPaxos<KeyValue, (), MemoryStorage<KeyValue, ()>>,
) {
    let buf_reader = BufReader::new(&mut stream);
    let msg: Vec<_> = buf_reader
        .lines()
        .map(|result| result.unwrap())
        .take_while(|line| !line.is_empty())
        .collect();

    omnipaxos.handle_incoming(msg);

    println!("Request: {:#?}", http_request);
}
//
// send outgoing messages. This should be called periodically, e.g. every ms
fn periodically_send_outgoing_msgs(
    mut omnipaxos: OmniPaxos<KeyValue, (), MemoryStorage<KeyValue, ()>>,
) {
    for out_msg in omnipaxos.outgoing_messages() {
        let receiver = out_msg.get_receiver();
        // send out_msg to receiver on network layer
    }
}
