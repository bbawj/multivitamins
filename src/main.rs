use multivitamins::{DEFAULT_ADDR, CliServer};
use multivitamins::op_server::OmniPaxosServer;
use serde::Deserialize;
use std::collections::HashMap;
use std::{usize, fs};
use toml;


// Top level struct to hold the TOML data.
#[derive(Deserialize)]
struct ConfigData {
    config: Config,
}

// Config struct holds to data from the `[config]` section.
#[derive(Deserialize)]
struct Config {
    config_id: u32,
    nodes: Vec<String>,
}


#[tokio::main]

// This file does two things:
// 1. To simulate a cluster, it spawns a new thread for each node in the cluster, and runs the node on that thread.
// 2. It starts the CLI server, which is used to send commands to the cluster.
// In reality, the nodes would be running on different machines, and the CLI server would be running on a separate machine.
async fn main() {

    let config_file_name = "config.toml";
    let config_toml_contents = match fs::read_to_string(config_file_name) {
        Ok(contents) => contents,
        Err(e) => {
            println!("Error reading config file: {}", e);
            return;
        }
    };
    let config_data: ConfigData = match toml::from_str(&config_toml_contents) {
        Ok(data) => data,
        Err(e) => {
            println!("Error parsing config file: {}", e);
            return;
        }
    };

    
    let configuration_id = config_data.config.config_id;

    // Create a mapping of peer IDs (1, 2, ..., n) to Node
    let mut topology: HashMap<u64, String> = HashMap::new();
    let topology_arr = config_data.config.nodes;
    for id in 0..(topology_arr.len() as u64){
        topology.insert(1+id, topology_arr[id as usize].clone());
    }

    // The main work that this function does.
    let node_handler: Vec<tokio::task::JoinHandle<Vec<tokio::task::JoinHandle<()>>>> = spawn_local_nodes(configuration_id, topology.clone()).await;
    spawn_cli_server(topology.clone()).await;
    // multivitamins::test::run_tests(node_handler).await;
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
            return multivitamins::op_server::run_recovery(pid, op_server).await;
        }));

        index += 1;
    }

    return node_handler;
}

// Start the CLI server
async fn spawn_cli_server(topology: HashMap<u64, String>) {
    let mut cli_server = CliServer::new(topology.clone());
    // tokio::spawn(async move {
        cli_server.listen().await;
    // });
}
