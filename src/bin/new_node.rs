use std::collections::HashMap;

use multivitamins::{DEFAULT_ADDR, op_server::OmniPaxosServer};

#[tokio::main]
async fn main() {
    let pid = 11;
    let node = format!("{}:{}", DEFAULT_ADDR, 50000+pid-1);
    // TODO: dynamically create this from a config file
    let mut topology: HashMap<u64, String> = HashMap::new();
    const num_nodes: usize = 11;

    for id in 0..(num_nodes as u64){
        topology.insert(1+id, format!("{}:{}", DEFAULT_ADDR, 50000+id));
    }
    // topology.insert(pid, format!("{}:{}", DEFAULT_ADDR, 50000+pid));

    println!("[New node] Spawning node {} on {}", pid, node);

    let op_server = OmniPaxosServer::new(2, topology.clone(), pid).await;

    multivitamins::op_server::run_recovery(pid, op_server).await;
}
