use multivitamins::{DEFAULT_ADDR, SERVER_PORTS, CliServer};
use multivitamins::op_server::{OmniPaxosServer};
use omnipaxos_core::omni_paxos::{OmniPaxos, OmniPaxosConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::net::TcpListener;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};



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
    topology.insert(1, format!("{}:{}", DEFAULT_ADDR, 50000));
    topology.insert(2, format!("{}:{}", DEFAULT_ADDR, 50001));
    // topology.insert(3, format!("{}:{}", DEFAULT_ADDR, 50002));

    // The main work that this function does.
    spawn_local_nodes(configuration_id, topology.clone()).await;
    spawn_cli_server(topology.clone()).await;

}

// Spawn a new thread for each node in the cluster, and run the node on that thread.
async fn spawn_local_nodes(configuration_id: u32, topology: HashMap<u64, String>) {
    for (pid, node) in topology.clone() {

        println!("[Main] Spawning node {} on {}", pid, node);

        // Spawn thread to run the node: we need the port, the OmniPaxosConfig, and the topology
        let mut op_server = OmniPaxosServer::new(configuration_id, topology.clone(), pid).await;

        // Spawn a new thread to run the node
        tokio::spawn(async move {
            multivitamins::op_server::run(pid, op_server).await;
        });
    }
}

// Start the CLI server
async fn spawn_cli_server(topology: HashMap<u64, String>) {
    let cli_server = CliServer::new(topology.clone());
    cli_server.listen().await;
}
