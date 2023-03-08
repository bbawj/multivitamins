# Multivitamins
A distributed key value store using OmniPaxos. Simulated using threads and TCP based network communication.

## Getting started
Launching the cluster: `cargo run --bin multivitamins`

Interacting with the cluster: `cargo run --bin cli_client <COMMAND>`
- A list of commands and their functions can be found by passing the `-h` flag

Spawning a new node (for reconfiguration): `cargo run --bin new_node`

