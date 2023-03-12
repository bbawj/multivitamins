// use std::time::{Duration, Instant};
//
// use tokio::time;
//
// use crate::{cli::{client, get::Get, put::Put}, OUTGOING_MESSAGES_TIMEOUT};
//
// pub async fn run_tests(node_handler: Vec<tokio::task::JoinHandle<Vec<tokio::task::JoinHandle<()>>>>) {
//
//     time::interval(Duration::from_millis(1000)).tick().await;
//     let dur = 100;
//     let mut key = 1;
//     let value = "yeet";
//     let mut count = 1;
//     let mut timing: u128;
//     let mut total_times = 0;
//     let num_iters = 1;
//
//
//     loop{
//         let mut outgoing_interval = time::interval(Duration::from_millis(dur));
//         outgoing_interval.tick().await;
//         println!("Running put test");
//         put_test(&(key.to_string()), value).await;
//
//         println!("Running get test");
//         timing = get_test(&(key.to_string()), true).await.unwrap();
//         total_times += timing;
//
//         println!("Avg duration: {}", total_times/count);
//         //dur -= 10;
//         if count == num_iters{
//             break;
//         }
//         key += 1;
//         count += 1;
//
//     }
//
//
//     //test for illegal input
//     let illegal_get = "illegal_input";
//     get_test(illegal_get, false).await;
//
//     //test for node failure
//     let mut num_fail = 5; //node_handler.len()-1;
//     for handle in node_handler{
//         if num_fail > 0{
//             let sub_handles = handle.await.unwrap();
//             for sub_handle in &sub_handles{
//                 sub_handle.abort();
//             }
//             println!("OPServer node aborted");
//         }
//         num_fail -= 1;
//
//     }
//     // for i in 0..(node_handler.len()-1){
//     //     node_handler[i].abort();
//     //     while !node_handler[i].is_finished(){
//     //         println!("Aborting node {}", i);
//     //         time::interval(Duration::from_millis(500)).tick().await;
//     //     }
//     //     println!("Node {} aborted", i);
//     // }
//     for i in 0..10{
//         get_test(&(key.to_string()), false).await;
//     }
//     //get_test(&(key.to_string()), false).await;
//     // time::interval(Duration::from_millis(100000)).tick().await;
//     // println!("Ass");
//
//
//
// }
//
// async fn put_test(key: &str, value: &str){
//     let put_cmd = Put::new(key.to_string(), value.to_string());
//     let frame = put_cmd.to_frame();
//     client::send_frame(&frame).await;
// }
//
// async fn get_test(key: &str, timed: bool) -> Option<u128>{
//     let get_cmd = Get::new(key.to_string());
//     let frame = get_cmd.to_frame();
//     if timed {
//         let now = Instant::now();
//         while client::send_frame(&frame).await == 0{
//             //time::interval(Duration::from_millis(1)).tick().await;
//         }
//         return Some(now.elapsed().as_millis());
//     }
//     else{
//         client::send_frame(&frame).await;
//         return None;
//     }
// }
//
//
