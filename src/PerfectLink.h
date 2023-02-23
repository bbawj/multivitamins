#include <atomic>
#include <condition_variable>
#include <map>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>

class PerfectLink {
public:
  static std::map<int, std::pair<int, sockaddr_in>> port_to_socket_info;
  static std::map<int, std::map<int, int>> topology;
  PerfectLink(int port);
  static void *Run(int port, int n, int &listeners, std::mutex &cv_m,
                   std::condition_variable &cv);
  static void BuildConnections();
  void Send(std::string host, int port, std::string data);

private:
  int port;
  static void Listen(int port, struct sockaddr_in server);
  static void *ConnectionHandler(void *socket_fd, void *port);
};
