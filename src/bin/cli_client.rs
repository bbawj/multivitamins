
use clap::{arg, command, Command};
use multivitamins::cli::{
    get::Get, put::Put, client};

#[tokio::main]
async fn main() {
    // accepts user commands and parses it before sending a "Frame"
    let matches = command!() // requires `cargo` feature
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(Command::new("get").about("Get a key").arg(arg!([KEY])).arg(arg!([NODE])))
        .subcommand(Command::new("put").about("Set a key").arg(arg!([KEY])).arg(arg!([VALUE])).arg(arg!([NODE])))
        .get_matches();

    let key: &str;
    let value: &str;
    let maybe_node: Option<&String>;
    let node: &str;
    let frame;

    match matches.subcommand() {
        Some(("get", sub_matches)) => {
            key = sub_matches.get_one::<String>("KEY").expect("[CliClient] get command; key was not a string");
            maybe_node = sub_matches.get_one::<String>("NODE");
            node = if maybe_node.is_none(){
                &"0"
            } else {
                maybe_node.unwrap()
            };
            let get_cmd = Get::new(key.to_string(), node.to_string());
            frame = get_cmd.to_frame();
        },
       Some(("put", sub_matches)) => {
            key = sub_matches.get_one::<String>("KEY").expect("[CliClient] put command; key was not a string");
            value = sub_matches.get_one::<String>("VALUE").expect("[CliClient] must have a value");
            maybe_node = sub_matches.get_one::<String>("NODE");
            node = if maybe_node.is_none(){
                &"0"
            } else {
                maybe_node.unwrap()
            };
            println!("[CliClient] Key to put is {}", key);
            println!("[CliClient] Value to put is {}", value);
            println!("[CliClient] Connect to node {}", node);
            let put_cmd = Put::new(key.to_string(), value.to_string(), node.to_string());
            frame = put_cmd.to_frame();
       }, 
        _ => unreachable!("[CliClient] Exhausted list of subcommands and subcommand_required prevents `None`"),
    }
    // connect to CLI server
    client::send_frame(&frame).await;
}
