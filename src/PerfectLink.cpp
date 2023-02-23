#include "PerfectLink.h"
#include <arpa/inet.h>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

PerfectLink::PerfectLink(int port) : port(port) {}

void *PerfectLink::Listen(int port) {
  struct sockaddr_in server, client;
  int socket_fd;
  socket_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (socket_fd == -1) {
    std::cerr << "Cant create socket";
    exit(EXIT_FAILURE);
  }
  server.sin_family = AF_INET;
  server.sin_port = htons(port);

  inet_pton(AF_INET, "127.0.0.1", &server.sin_addr);

  std::cout << "Binding socket to sockaddr " << inet_ntoa(server.sin_addr)
            << ":" << ntohs(server.sin_port) << std::endl;
  if (bind(socket_fd, (struct sockaddr *)&server, sizeof(server)) == -1) {
    std::cerr << "Can't bind to IP/port";
    exit(EXIT_FAILURE);
  }
  std::cout << "Binded to " << inet_ntoa(server.sin_addr) << ":"
            << ntohs(server.sin_port) << std::endl;

  std::cout << "Mark the socket for listening..." << std::endl;
  if (listen(socket_fd, SOMAXCONN) == -1) {
    std::cerr << "Can't listen !";
    exit(EXIT_FAILURE);
  }

  int conn_id;
  socklen_t addrlen = sizeof(struct sockaddr_in);
  while (true) {
    std::cout << "waiting for connection" << std::endl;
    // accept new connections
    conn_id =
        accept(socket_fd, (struct sockaddr *)&client, (socklen_t *)&addrlen);

    // if connection acception failed
    if (conn_id == -1) {
      std::cout << "[WARNING] CAN'T ACCEPT NEW CONNECTION\n";
    } else {
      std::cout << "[INFO] NEW CONNECTION ACCEPTED FROM "
                << inet_ntoa(client.sin_addr) << ":" << ntohs(client.sin_port)
                << "\n";
      // create new thread for new connection
      std::thread t(ConnectionHandler, new int(conn_id), new int(port));
      t.detach();
    }
  }
}

void *PerfectLink::ConnectionHandler(void *socket_fd, void *server_port) {
  // byte size
  int read_byte = 0;

  // Get the socket descriptor
  int conn_id = *(int *)socket_fd;

  // request data
  const int BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE] = {0};

  // response data
  // char response[] = "Hello";

  // read response continue
  while ((read_byte = recv(conn_id, buffer, BUFFER_SIZE, 0)) > 0) {
    std::cout << server_port << "[RECEIVED] " << buffer << "\n";
    // clear buffer data
    memset(buffer, 0, BUFFER_SIZE);

    // send response
    // if (send(conn_id, response, strlen(response), 0) > 0) {
    //   std::cout << "[SEND] " << response << "\n";
    // } else {
    //   std::cout << "[WARNING][SEND] " << strerror(errno) << "\n";
    // }
  }

  // terminate connection
  close(conn_id);
  std::cout << "[INFO] CONNECTION CLOSED\n";

  // thread automatically terminate after exit connection handler
  std::cout << "[INFO] THREAD TERMINATED" << std::endl;

  delete (int *)socket_fd;

  pthread_exit(NULL);
}

void PerfectLink::Send(std::string host, int port, std::string data) {
  int client_fd = 0;
  if ((client_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    std::cout << "[ERROR] CAN'T CREATE SOCKET"
              << "\n";
    exit(EXIT_FAILURE);
  }
  struct sockaddr_in server;
  server.sin_family = AF_INET;
  server.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &server.sin_addr) == -1) {
    std::cout << "Address not supported" << std::endl;
  }

  while (connect(client_fd, (struct sockaddr *)&server, sizeof(server)) == -1) {
    std::cout << "[ERROR] CANNOT CONNECT TO HOST " << inet_ntoa(server.sin_addr)
              << ":" << ntohs(server.sin_port) << std::endl;
    sleep(5);
  }

  while (send(client_fd, data.c_str(), data.size(), 0) <= 0) {
    std::cout << "ERROR failed to send data to " << host << ":" << port
              << std::endl;
    sleep(5);
  }
  close(client_fd);
}
