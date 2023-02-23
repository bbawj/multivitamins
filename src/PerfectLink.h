#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <vector>

class PerfectLink {
public:
  static std::vector<int> topology;
  PerfectLink(int port);
  static void *Listen(int port);
  void Send(std::string host, int port, std::string data);

private:
  int port;
  static void *ConnectionHandler(void *socket_fd, void *port);
};
