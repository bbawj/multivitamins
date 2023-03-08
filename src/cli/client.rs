use tokio::net::TcpStream;
use std::time::Duration;
use tokio::time;

use crate::DEFAULT_ADDR;

use super::{frame::Frame, COMMAND_LISTENER_PORT, connection::Connection, command::Command};


pub async fn send_frame(frame: &Frame) -> i32 { //to remove "-> i32"
    // Sets up tcp connection
    let address = String::from(DEFAULT_ADDR);
    let port = COMMAND_LISTENER_PORT;

    let mut socket_result = TcpStream::connect(format!("{}:{}", address, port)).await;

    // If connection to server gives error (refused)
    while socket_result.is_err() {
        
        println!{"[CliClient] TCP connection to server refused"};
        time::interval(Duration::from_millis(1000)).tick().await;
        socket_result = TcpStream::connect(format!("{}:{}", address, port)).await;

    }
    let socket = socket_result.unwrap();
    //let mut socket = TcpStream::connect(format!("{}:{}", address, port)).await.unwrap();
    let mut connection = Connection::new(socket);

    let mut to_return = 0; //to remove

    // send the frame to the server
    connection.write_frame(&frame).await.unwrap();


    let response_frame = connection.read_frame().await.unwrap();
    match response_frame {
        Some(response) => {
            let cmd = Command::from_frame(response).expect("[CliClient] Failed to read response");
            match cmd {
                Command::Response(r) => {
                    println!("[CliClient] Key: {}, Value: {}", r.key(), r.value());
                    to_return = 1;
                },
                Command::Error(e) => {
                    println!("[CliClient] Error - {:?}", e.value());
                    to_return = 0;
                },
                _ => panic!("[CliClient] Incorrect command received")
            }
        }
        None => {
            println!("[CliClient] Sadge");
        }
    }
    return to_return;
}

