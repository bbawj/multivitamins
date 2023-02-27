use multivitamins::{SERVER_PORTS, CliServer};
use omnipaxos_core::omni_paxos::{OmniPaxos, OmniPaxosConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::net::TcpListener;


#[tokio::main]
async fn main() {
    println!("Hello, world!");
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

    // spawn command listener
    // tokio::spawn(async move {
    //     let cli_server = CliServer::new(SERVER_PORTS.to_vec());
    //     cli_server.listen().await;
    // });
    let cli_server = CliServer::new(SERVER_PORTS.to_vec());
    cli_server.listen().await;

    // spawn op_servers
    // for pid in SERVER_PORTS {
    //     let peers = SERVER_PORTS.iter().filter(|&&p| p != pid).copied().collect();
    //     let op_config = OmniPaxosConfig {
    //         pid,
    //         configuration_id,
    //         peers,
    //         ..Default::default()
    //     };
    //     let omni_paxos: Arc<Mutex<OmniPaxosKV>> =
    //         Arc::new(Mutex::new(op_config.build(MemoryStorage::default())));
    //     let mut op_server = OmniPaxosServer {
    //         omni_paxos: Arc::clone(&omni_paxos),
    //         incoming: receiver_channels.remove(&pid).unwrap(),
    //         outgoing: sender_channels.clone(),
    //     };
    //     let join_handle = runtime.spawn({
    //         async move {
    //             op_server.run().await;
    //         }
    //     });
    // }
}

// fn handle_connection(
//     mut stream: TcpStream,
//     mut omnipaxos: OmniPaxos<KeyValue, (), MemoryStorage<KeyValue, ()>>,
// ) {
//     let buf_reader = BufReader::new(&mut stream);
//     let msg: Vec<_> = buf_reader
//         .lines()
//         .map(|result| result.unwrap())
//         .take_while(|line| !line.is_empty())
//         .collect();
//
//     omnipaxos.handle_incoming(msg);
//
//     println!("Request: {:#?}", http_request);
// }
//
