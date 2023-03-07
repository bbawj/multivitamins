use std::collections::HashMap;

use multivitamins::{DEFAULT_ADDR, op_server::OmniPaxosServer};

#[tokio::main]
async fn main() {
    let pid = 4;
    let node = format!("{}:{}", DEFAULT_ADDR, 60000);
    // TODO: dynamically create this from a config file
    let mut topology: HashMap<u64, String> = HashMap::new();
    topology.insert(1, format!("{}:{}", DEFAULT_ADDR, 50000));
    topology.insert(2, format!("{}:{}", DEFAULT_ADDR, 50001));
    topology.insert(pid, format!("{}:{}", DEFAULT_ADDR, 60000));

    println!("[New node] Spawning node {} on {}", pid, node);

    let op_server = OmniPaxosServer::new(2, topology.clone(), pid).await;

    multivitamins::op_server::run_recovery(pid, op_server).await;
}
