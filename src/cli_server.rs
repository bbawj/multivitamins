use tokio::net::{TcpListener, TcpStream};

use rand::Rng;
use std::collections::HashMap;

use crate::{
    cli::{command::Command, connection::Connection, get::Get, put::Put, Result, response::Response,
        COMMAND_LISTENER_PORT, COMMAND_LISTENER_ADDR},
};


pub struct CliServer {

    // Mapping of node id to Node
    topology: HashMap<u64, String>,
}

impl CliServer {

    pub fn new(topology: HashMap<u64, String>) -> CliServer {
        CliServer { topology }
    }

    pub async fn listen(&self) -> Result<()> {

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
            let inbound_frame = frame.clone();
            println!("[CliServer] Received frame: {:?}", frame);

            // Connect to random server
            let random_server_num = rand::thread_rng().gen_range(1..=self.topology.len()) as u64;
            let rand_server_socket_addr = self.topology.get(&random_server_num).expect("").to_string();
            let mut socket = TcpStream::connect(rand_server_socket_addr).await.unwrap();
            let mut outbound_connection = Connection::new(&mut socket);

            // Send request to op_server
            outbound_connection.write_frame(&inbound_frame).await.unwrap();
            println!("[CliServer] Forwarded frame to op_server node {}: {:?}", random_server_num, frame);

            // Wait for response
            let maybe_response_frame = outbound_connection.read_frame().await.unwrap();
            let response_frame = match maybe_response_frame {
                Some(response_frame) => response_frame,
                None => return Ok(()),
            };
            println!("[CliServer] Received response frame from node {}: {:?}", random_server_num, response_frame);

            // let key = v.key();
            // println!("Get command received with key: {}", v.key());
            // let response = Response::new("hello".to_string(), 1);
            // let response_frame = response.to_frame();

            // Forward response to cli_client
            let response_frame_copy = response_frame.clone();
            let response_cmd = Command::from_frame(response_frame).expect("");
            match response_cmd {
                Command::Response(r) => {
                    cli_client_connection.write_frame(&response_frame_copy).await.unwrap();
                }
                _ => panic!("[CliServer] Should be response"),
            }
            println!("[CliServer] Forwarded response frame to cli_client: {:?}", response_frame_copy);
        }
    }
}
