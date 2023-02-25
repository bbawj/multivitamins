use tokio::net::{TcpListener, TcpStream};

use crate::{
    cli::{command::Command, connection::Connection, get::Get, COMMAND_LISTENER_PORT, Result},
    DEFAULT_ADDR,
};

pub struct CliServer {
    // list of ports of our op servers
    topology: Vec<u64>,
}

impl CliServer {
    pub fn new(topology: Vec<u64>) -> CliServer {
        CliServer { topology }
    }

    pub async fn listen(&self) -> Result<() >{
        let address = String::from("127.0.0.1");
        let port = COMMAND_LISTENER_PORT;
        let listener = TcpListener::bind(format!("{}:{}", address, port)).await.unwrap();
        loop {
            let (incoming_socket, _) = listener.accept().await.unwrap();
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
                    let key = v.key();
                    // connect to random op_server and request a read
                    // just connect to first one for now
                    let port = self.topology.get(0).expect("").to_string();
                    let socket = TcpStream::connect(format!("{}:{}", DEFAULT_ADDR, port)).await.unwrap();
                    let mut outbound_connection = Connection::new(socket);
                    outbound_connection.write_frame(&inbound_frame).await.unwrap();
                    // wait for response
                    let maybe_response_frame = outbound_connection.read_frame().await.unwrap();
                    let response_frame = match maybe_response_frame {
                        Some(response_frame) => response_frame,
                        None => return Ok(()),
                    };

                    let response_frame_copy = response_frame.clone();
                    let response = Command::from_frame(response_frame).expect("");
                    // forward response to cli_client
                    match response {
                        Command::Response(r) => {
                            connection.write_frame(&response_frame_copy).await.unwrap()
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
