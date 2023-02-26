pub mod cli;
pub mod cli_server;
pub use cli_server::*;
pub mod op_server;

pub const DEFAULT_ADDR: &str = "127.0.0.1";
pub const SERVER_PORTS: [u64; 3] = [50000, 50001, 50002];
