use multivitamins::{DEFAULT_ADDR, CliServer};
use multivitamins::op_server::OmniPaxosServer;
use std::collections::HashMap;
use std::usize;
use std::cell::Cell;



#[tokio::main]

// This file does two things:
// 1. To simulate a cluster, it spawns a new thread for each node in the cluster, and runs the node on that thread.
// 2. It starts the CLI server, which is used to send commands to the cluster.
// In reality, the nodes would be running on different machines, and the CLI server would be running on a separate machine.
async fn main() {


    // configuration with id 1 and the following cluster
    // TODO: bring this out into a config file
    let configuration_id: u32 = 1;

    // Create a mapping of peer IDs (1, 2, ..., n) to Node
    // TODO: dynamically create this from a config file
    let mut topology: HashMap<u64, String> = HashMap::new();

    const num_nodes: usize = 10;

    for id in 0..(num_nodes as u64){
        topology.insert(1+id, format!("{}:{}", DEFAULT_ADDR, 50000+id));
    }
    //topology.insert(1, format!("{}:{}", DEFAULT_ADDR, 50000));
    //topology.insert(2, format!("{}:{}", DEFAULT_ADDR, 50001));
    // topology.insert(3, format!("{}:{}", DEFAULT_ADDR, 50002));

    // The main work that this function does.
    let node_handler: Vec<tokio::task::JoinHandle<Vec<tokio::task::JoinHandle<()>>>> = spawn_local_nodes(configuration_id, topology.clone()).await;
    spawn_cli_server(topology.clone()).await;
    multivitamins::test::run_tests(node_handler).await;
}

// Spawn a new thread for each node in the cluster, and run the node on that thread.
async fn spawn_local_nodes(configuration_id: u32, topology: HashMap<u64, String>) -> Vec<tokio::task::JoinHandle<Vec<tokio::task::JoinHandle<()>>>>{

    //let node_handler: [tokio::task::JoinHandle<()>; num_nodes] = [tokio::spawn(async{}); num_nodes];
    let mut node_handler: Vec<tokio::task::JoinHandle<Vec<tokio::task::JoinHandle<()>>>> = Vec::new();
    let mut index: usize = 0;

    for (pid, node) in topology.clone() {

        println!("[Main] Spawning node {} on {}", pid, node);

        // Spawn thread to run the node: we need the port, the OmniPaxosConfig, and the topology
        let op_server = OmniPaxosServer::new(configuration_id, topology.clone(), pid).await;

        // Spawn a new thread to run the node, assign to node handler
        node_handler.push(tokio::spawn(async move {
            return multivitamins::op_server::run(pid, op_server).await;
        }));

        index += 1;
    }

    return node_handler;
}

// Start the CLI server
async fn spawn_cli_server(topology: HashMap<u64, String>) {
    let mut cli_server = CliServer::new(topology.clone());
    tokio::spawn(async move {
        cli_server.listen().await;
    });
}
