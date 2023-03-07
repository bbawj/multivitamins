use std::time::Duration;

pub mod cli;
pub mod cli_server;
pub use cli_server::*;
pub mod op_server;
pub mod test;

pub const DEFAULT_ADDR: &str = "127.0.0.1";
pub const SERVER_PORTS: [u64; 3] = [50000, 50001, 50002];

pub const OUTGOING_MESSAGES_TIMEOUT: Duration = Duration::from_millis(1);
pub const ELECTION_TIMEOUT: Duration = Duration::from_millis(100);
pub const CHECK_STOPSIGN_TIMEOUT: Duration = Duration::from_millis(1000);
