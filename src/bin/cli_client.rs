use clap::{arg, command, Command};
use multivitamins::cli::{get::Get, connection::Connection, COMMAND_LISTENER_PORT};
use tokio::net::TcpStream;


#[tokio::main]
async fn main() {
    // accepts user commands and parses it before sending a "Frame"
    let matches = command!() // requires `cargo` feature
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(Command::new("get").about("Get a key").arg(arg!([KEY])))
        .subcommand(Command::new("put").about("Set a key").arg(arg!([KEY])).arg(arg!([VALUE])))
        .get_matches();

    let key;
    let value;
    let frame;

    match matches.subcommand() {
        Some(("get", sub_matches)) => {
            key = sub_matches.get_one::<String>("KEY").expect("get command; key was not a string");
            let get_cmd = Get::new(key.to_string());
            frame = get_cmd.into_frame();
        },
       Some(("put", sub_matches)) => {
            key = sub_matches.get_one::<String>("KEY").expect("get command; key was not a string");
            value = sub_matches.get_one::<u64>("VALUE");
       }, 
        _ => unreachable!("Exhausted list of subcommands and subcommand_required prevents `None`"),
    }
    // create a Frame

    // sets up tcp connection to the command listener in main.rs
    let address = String::from("127.0.0.1");
    let port = COMMAND_LISTENER_PORT;
    let socket = TcpStream::connect(format!("{}:{}", address, port)).await.unwrap();
    let connection = Connection::new(socket);
    connection.write_frame(&frame).await.unwrap()
}
