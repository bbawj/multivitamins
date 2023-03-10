
use clap::{arg, command, Command, value_parser};
use multivitamins::cli::{
    get::Get, put::Put, client, reconfigure::Reconfigure};

#[tokio::main]
async fn main() {
    // accepts user commands and parses it before sending a "Frame"
    let matches = command!() // requires `cargo` feature
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(Command::new("get").about("Get a key").arg(arg!(-t <TARGET_NODE>).required(false).default_value("0")).arg(arg!([KEY])))
        .subcommand(Command::new("put").about("Set a key").arg(arg!(-t <TARGET_NODE>).required(false).default_value("0")).arg(arg!([KEY])).arg(arg!([VALUE])))
        .subcommand(Command::new("reconfigure").about("Add a node with specified PID").arg(arg!([PID]).value_parser(value_parser!(u64))))
        .get_matches();

    let key: &str;
    let value: &str;
    let frame;

    match matches.subcommand() {
        Some(("get", sub_matches)) => {
            let target_node = sub_matches.get_one::<String>("TARGET_NODE").expect("[CliClient] get command; key was not a string");
            key = sub_matches.get_one::<String>("KEY").expect("[CliClient] get command; key was not a string");
            let get_cmd = Get::new(target_node.to_string(), key.to_string());
            frame = get_cmd.to_frame();
        },
       Some(("put", sub_matches)) => {
            let target_node = sub_matches.get_one::<String>("TARGET_NODE").expect("[CliClient] get command; key was not a string");
            key = sub_matches.get_one::<String>("KEY").expect("[CliClient] put command; key was not a string");
            value = sub_matches.get_one::<String>("VALUE").expect("[CliClient] must have a value");
            println!("[CliClient] Key to put is {}", key);
            println!("[CliClient] Value to put is {}", value);
            let put_cmd = Put::new(target_node.to_string(), key.to_string(), value.to_string());
            frame = put_cmd.to_frame();
       }, 
       Some(("reconfigure", sub_matches)) => {
            let pid = sub_matches.get_one::<u64>("PID").expect("[CliClient] reconfigure command; pid was not a u64");
            println!("[CliClient] Node pid to add is {}", *pid);
            let reconfig_cmd = Reconfigure::new(*pid);
            frame = reconfig_cmd.to_frame();
       }, 
        _ => unreachable!("[CliClient] Exhausted list of subcommands and subcommand_required prevents `None`"),
    }
    // connect to CLI server
    client::send_frame(&frame).await;
}
