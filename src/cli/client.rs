use tokio::net::TcpStream;

use crate::DEFAULT_ADDR;

use super::{frame::Frame, COMMAND_LISTENER_PORT, connection::Connection, command::Command};


pub async fn send_frame(frame: &Frame) {
    // Sets up tcp connection
    let address = String::from(DEFAULT_ADDR);
    let port = COMMAND_LISTENER_PORT;
    let mut socket = TcpStream::connect(format!("{}:{}", address, port)).await.unwrap();
    let mut connection = Connection::new(&mut socket);

    // send the frame to the server
    connection.write_frame(&frame).await.unwrap();


    let response_frame = connection.read_frame().await.unwrap();
    match response_frame {
        Some(response) => {
            let cmd = Command::from_frame(response).expect("[CliClient] Failed to read response");
            match cmd {
                Command::Response(r) => println!("[CliClient] Key: {}, Value: {}", r.key(), r.value()),
                Command::Error(e) => println!("[CliClient] Error - {:?}", e.value()),
                _ => panic!("[CliClient] Incorrect command received")
            }
        }
        None => {
            println!("Sadge");
        }
    }
}

