use tokio::net::{TcpListener, TcpStream};
use rand::seq::IteratorRandom;


use std::{collections::HashMap};

use crate::DEFAULT_ADDR;
use crate::cli::parse::Parse;
use crate::{
    cli::{connection::Connection, Result,
        COMMAND_LISTENER_PORT, COMMAND_LISTENER_ADDR},
};


pub struct CliServer {

    // Mapping of node id to Node
    topology: HashMap<u64, String>
}

impl CliServer {

    pub fn new(topology: HashMap<u64, String>) -> CliServer {
        CliServer { topology }
    }

    pub async fn listen(& mut self) -> Result<()> {

        // Set up listener.
        let address = String::from(COMMAND_LISTENER_ADDR);
        let port = String::from(COMMAND_LISTENER_PORT);
        let listener = TcpListener::bind(format!("{}:{}", address, port)).await.unwrap();

        println!("[CliServer] Listening for connections on port {}", port);
        

        loop {

            // Accept a new incoming connection from a client.
            let (cli_client_incoming_stream, _) = listener.accept().await.unwrap();
            println!("[CliServer] Accepted connection from {}", cli_client_incoming_stream.peer_addr().unwrap());
            let mut cli_client_connection = Connection::new(cli_client_incoming_stream);

            // Read the incoming message from the socket.
            let maybe_frame = cli_client_connection.read_frame().await.unwrap();
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };
            let inbound_frame = frame.clone();
            let mut parse = Parse::new(frame)?;
            let command_name = parse.next_string()?;
            let mut target_node = 0;

            match &command_name[..] {
                "put" | "get" | "savesnapshot" | "readsnapshot" | "disconnect" => {
                    target_node = parse.next_int()?;
                }   
                "reconfigure" => {
                    let new_node: u64 = parse.next_int()?;
                    self.topology.insert(new_node, format!("{}:{}", DEFAULT_ADDR, new_node + 50000 - 1));
                }
                _ => {}
            } 

            // Connect to random server
            let random = true;
            let keys = self.topology.keys();
            let server_num: u64 = if target_node == 0 && random {
                keys.copied().choose(& mut rand::thread_rng()).unwrap()
            } else {
                target_node
            };

            println!("[CliServer] Connecting to OPServer node {}", server_num);
            let rand_server_socket_addr = self.topology.get(&server_num).unwrap().to_string();
            let socket_result = TcpStream::connect(&rand_server_socket_addr).await;


            // If node is ok (TCP connection established)
            if socket_result.is_ok(){
                let socket = socket_result.unwrap();

                let mut outbound_connection = Connection::new(socket);
                println!("[CliServer] Connected to OPServer node {} at {}", server_num, rand_server_socket_addr);

                // Send request to op_server
                outbound_connection.write_frame(&inbound_frame).await?;
                println!("[CliServer] Forwarded frame to OPServer node {}: {:?}", server_num, inbound_frame);

                // Wait for response
                let response_frame_result = outbound_connection.read_frame().await;
                if response_frame_result.is_ok(){
                    let maybe_response_frame = response_frame_result.unwrap();
                    let response_frame = match maybe_response_frame {
                        Some(response_frame) => response_frame,
                        None => return Ok(()),
                    };
                    println!("[CliServer] Received response frame from node {}: {:?}", server_num, response_frame);

                    // Forward response to cli_client
                    let response_frame_copy = response_frame.clone();
                    cli_client_connection.write_frame(&response_frame).await.unwrap();
                    println!("[CliServer] Forwarded response frame to cli_client: {:?}", response_frame_copy);
                }
                else{
                    println!("[CliServer] Response error from node {}", server_num);
                }
                       
            }
            else {
                println!("[CliServer] TCP connection error to OPServer node {}", server_num);

                // TCP connection to node died, update local topology
                // self.topology.remove(&server_num);

                println!("[CliServer] OPServer node {} removed from local topology" , server_num);
            }
            
        }
    }

}
