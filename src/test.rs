use std::time::Duration;

use tokio::time;

use crate::{cli::{client, get::Get, put::Put}, OUTGOING_MESSAGES_TIMEOUT};

pub async fn run_tests() {
    let mut outgoing_interval = time::interval(Duration::from_millis(100));
    outgoing_interval.tick().await;
    println!("Running put test");
    put_test("ya", "yeet").await;
    println!("Running get test");
    get_test("ya").await;
}

async fn put_test(key: &str, value: &str) {
    let put_cmd = Put::new(key.to_string(), value.to_string());
    let frame = put_cmd.to_frame();
    client::send_frame(&frame).await;
}

async fn get_test(key: &str) {
    let get_cmd = Get::new(key.to_string());
    let frame = get_cmd.to_frame();
    client::send_frame(&frame).await;
}
