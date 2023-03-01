use tokio::net::{TcpListener, TcpStream};

use crate::{
    cli::{command::Command, connection::Connection, get::Get, COMMAND_LISTENER_PORT, Result, response::Response},
};

use crate::DEFAULT_ADDR;


pub struct CliServer {
    // list of ports of our op servers
    topology: Vec<u64>,
}

impl CliServer {
    pub fn new(topology: Vec<u64>) -> CliServer {
        CliServer { topology }
    }

    pub async fn listen(&self) -> Result<()> {
        let address = String::from(DEFAULT_ADDR);
        let port = COMMAND_LISTENER_PORT;
        let listener = TcpListener::bind(format!("{}:{}", address, port)).await.unwrap();
        loop {
            let (incoming_socket, _) = listener.accept().await.unwrap();
            println!("Accepted connection from {}", incoming_socket.peer_addr().unwrap());
            let mut connection = Connection::new(incoming_socket);
            let maybe_frame = connection.read_frame().await.unwrap();
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };
            let inbound_frame = frame.clone();

            let cmd = Command::from_frame(frame).expect("");
            match cmd {
                Command::Get(v) => {
                    // let key = v.key();
                    // // connect to random op_server and request a read
                    // // just connect to first one for now
                    // let port = self.topology.get(0).expect("").to_string();
                    // let socket = TcpStream::connect(format!("{}:{}", DEFAULT_ADDR, port)).await.unwrap();
                    // let mut outbound_connection = Connection::new(socket);
                    // outbound_connection.write_frame(&inbound_frame).await.unwrap();
                    // // wait for response
                    // let maybe_response_frame = outbound_connection.read_frame().await.unwrap();
                    // let response_frame = match maybe_response_frame {
                    //     Some(response_frame) => response_frame,
                    //     None => return Ok(()),
                    // };

                    println!("Get command received with key: {}", v.key());
                    let response = Response::new("hello".to_string(), 1);
                    let response_frame = response.to_frame();
                    let response_frame_copy = response_frame.clone();
                    let response_cmd = Command::from_frame(response_frame).expect("");
                    // forward response to cli_client
                    match response_cmd {
                        Command::Response(r) => {
                            connection.write_frame(&response_frame_copy).await.unwrap();
                        }
                        _ => panic!("should be response"),
                    }
                }
                _ => panic!("not supposed to get response yet"), // connect to designated op_server and request a write
            }
        }
        Ok(())
    }
}
