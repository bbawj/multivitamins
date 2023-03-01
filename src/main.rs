use multivitamins::{DEFAULT_ADDR, SERVER_PORTS, CliServer};
use multivitamins::op_server::{OmniPaxosServer, Node};
use omnipaxos_core::omni_paxos::{OmniPaxos, OmniPaxosConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::net::TcpListener;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};



#[tokio::main]
async fn main() {

    println!("Hello, world!");

    // configuration with id 1 and the following cluster
    // TODO: bring this out into a config file
    let configuration_id = 1;

    // create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on the other nodes)
    // TODO: bring this out into a config file
    let my_pid = 2;

    // Vector of all the other nodes in the cluster
    // TODO: dynamically create this from the config file
    let all_nodes: Vec<Node> = vec![
        Node {
            ip_address: String::from(DEFAULT_ADDR),
            port: 50000
        },
        Node {
            ip_address: String::from(DEFAULT_ADDR),
            port: 50001
        },
        Node {
            ip_address: String::from(DEFAULT_ADDR),
            port: 50002
        },
    ];

    let num_nodes_total = all_nodes.len();

    // Create a mapping of peer IDs (1, 2, ..., n) to Node
    let mut topology: HashMap<u64, &Node> = HashMap::new();
    for idx in 1..num_nodes_total + 1 {
        if (idx as u64) != my_pid {
            topology.insert(idx as u64, &all_nodes[idx]);
        }
        
    }

    // For each node in the cluster, if the ip_address is localhost,
    // then spawn a new thread and run the node on that thread.
    for (pid, node) in topology {

        if node.ip_address != String::from("localhost") {
            continue;
        }

        // Spawn thread to run the node: we need the port, the OmniPaxosConfig, and the topology

        // Basically, if there are n nodes, and this node's PID is 3, then peers == [1, 2, 4, 5, ..., n]
        let mut peers: Vec<u64> = (1..=(num_nodes_total+1) as u64).collect();
        peers.retain(|&x| x != pid);

        // Create a new OmniPaxosConfig instance with the given configuration
        let op_config = OmniPaxosConfig {
            configuration_id,
            pid,
            peers,
            ..Default::default()
        };

        // // Make a copy of the topology to pass into the new OmniPaxos instance
        // let topology_copy = topology;


        // Create a new OmniPaxos instance with the OmniPaxosConfig
        let mut op_server = OmniPaxosServer::new(pid, node.port, op_config, topology).await;
        // Spawn a new thread to run the node
        tokio::spawn(async move {
            op_server.run().await;
        });
    }

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
