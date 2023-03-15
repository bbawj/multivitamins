pub mod client;
pub mod command;
pub mod connection;
pub mod delete;
pub mod disconnect;
pub mod error;
pub mod frame;
pub mod get;
pub mod op_message;
pub mod parse;
pub mod put;
pub mod reconfigure;
pub mod response;
pub mod snapshot;

pub const COMMAND_LISTENER_ADDR: &str = "127.0.0.1";
pub const COMMAND_LISTENER_PORT: &str = "60000";
pub const DEFAULT_ADDR: &str = "127.0.0.1";

/// Error returned by most functions.
///
/// When writing a real application, one might want to consider a specialized
/// error handling crate or defining an error type as an `enum` of causes.
/// However, for our example, using a boxed `std::error::Error` is sufficient.
///
/// For performance reasons, boxing is avoided in any hot path. For example, in
/// `parse`, a custom error `enum` is defined. This is because the error is hit
/// and handled during normal execution when a partial frame is received on a
/// socket. `std::error::Error` is implemented for `parse::Error` which allows
/// it to be converted to `Box<dyn std::error::Error>`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for mini-redis operations.
///
/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;
