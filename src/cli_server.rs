use clap::Error;
use rand::seq::SliceRandom;
use tokio::net::{TcpListener, TcpStream};
use rand::{seq::IteratorRandom, thread_rng};

use rand::Rng;
use std::{collections::HashMap, result};

use crate::{
    cli::{command::Command, connection::Connection, Result, parse::Parse, frame::Frame,
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
            let (mut cli_client_incoming_stream, _) = listener.accept().await.unwrap();
            println!("[CliServer] Accepted connection from {}", cli_client_incoming_stream.peer_addr().unwrap());
            let mut cli_client_connection = Connection::new(&mut cli_client_incoming_stream);

            // Read the incoming message from the socket.
            let maybe_frame = cli_client_connection.read_frame().await.unwrap();
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };
            let frame_received = frame.clone();

            let mut parse = Parse::new(frame_received)?;
            let command_name = parse.next_string()?.to_lowercase();


            let mut key = parse.next_string()?;
            let mut value: String;
            let mut inbound_frame = Frame::array();

            inbound_frame.push_string(&command_name);
            inbound_frame.push_string(&key);
            
            if command_name == "put"{
                value = parse.next_string()?;
                inbound_frame.push_string(&value);

                println!("Frame parsed: {} {} {}", command_name, key, value);
            }
            let node = parse.next_string()?;

            let temp_frame = inbound_frame.clone();
        

            // let command_name = parse.next_string()?.to_lowercase();
            // println!("Parsed {}", command_name);



            // Connect to random server
            let mut random = true;
            let mut keys = self.topology.keys();
            let server_num = if random {
                keys.copied().choose(& mut rand::thread_rng()).unwrap()
            } else {
                1 as u64
            };

            println!("[CliServer] Connecting to OPServer node {}", server_num);
            let rand_server_socket_addr = self.topology.get(&server_num).unwrap().to_string();
            let mut socket_result = TcpStream::connect(&rand_server_socket_addr).await;


            // If node is ok (TCP connection established)
            if socket_result.is_ok(){
                let mut socket = socket_result.unwrap();

                let mut outbound_connection = Connection::new(&mut socket);
                println!("[CliServer] Connected to OPServer node {} at {}", server_num, rand_server_socket_addr);

                // Send request to op_server
                outbound_connection.write_frame(&temp_frame).await;
                println!("[CliServer] Forwarded frame to OPServer node {}: {:?}", server_num, frame);

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
                self.topology.remove(&server_num);

                println!("[CliServer] OPServer node {} removed from local topology" , server_num);
            }
            
        }
    }

}
