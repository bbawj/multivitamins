#include "PerfectLink.h"
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <ostream>
#include <thread>
#include <unistd.h>
#include <vector>

int n = 0;
int listeners = 0;
std::mutex cv_m;
std::condition_variable cv;

int main(int argc, char *argv[]) {
  // create 3 clients server socket
  int client_ports[3] = {8080, 8081, 8082};
  n = sizeof(client_ports) / sizeof(int);
  PerfectLink *client_array[3];
  std::thread threads[3];
  for (int i = 0; i < n; i++) {
    std::cout << "Init " << client_ports[i] << std::endl;
    client_array[i] = new PerfectLink(client_ports[i]);
    threads[i] = std::thread(client_array[i]->Run, client_ports[i], n,
                             std::ref(listeners), std::ref(cv_m), std::ref(cv));
  }

  std::cout << "Main waiting for init" << std::endl;

  std::unique_lock<std::mutex> lk(cv_m);
  cv.wait(lk, [=] { return listeners == n; });
  lk.unlock();

  std::cout << "Begin building connections" << std::endl;
  PerfectLink::BuildConnections();

  client_array[0]->Send("127.0.0.1", 8081, "Message 1 from 8080");
  client_array[0]->Send("127.0.0.1", 8081, "Message 2 from 8080");
  client_array[1]->Send("127.0.0.1", 8082, "Message 1 from 8081");

  for (int i = 0; i < n; i++) {
    threads[i].join();
  }

  // save each client in a list to rmb which client is on which port
  // connect clients which each other (send each client a list of ports)

  return 0;
}
