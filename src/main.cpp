#include "PerfectLink.h"
#include <iostream>
#include <ostream>
#include <thread>
#include <unistd.h>
#include <vector>

int main(int argc, char *argv[]) {
  // create 3 clients server socket
  int client_ports[3] = {8080, 8081, 8082};
  int n = sizeof(client_ports) / sizeof(int);
  PerfectLink *client_array[3];
  std::thread threads[3];
  // each client opens its own server socket
  for (int i = 0; i < n; i++) {
    std::cout << "Init " << client_ports[i] << std::endl;
    client_array[i] = new PerfectLink(client_ports[i]);
    threads[i] = std::thread(client_array[i]->Listen, client_ports[i]);
  }

  sleep(5);

  client_array[0]->Send("127.0.0.1", 8081, "Message from 8080");
  client_array[1]->Send("127.0.0.1", 8082, "Message from 8081");

  for (int i = 0; i < n; i++) {
    threads[i].join();
  }

  // save each client in a list to rmb which client is on which port
  // connect clients which each other (send each client a list of ports)

  return 0;
}
