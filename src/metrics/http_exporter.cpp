#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <thread>

#include "me/metrics.h"

namespace me::metrics {

HttpExporter::HttpExporter(const Registry& reg, uint16_t port, std::function<bool()> ready)
    : reg_(reg), port_(port), ready_(std::move(ready)) {}

HttpExporter::~HttpExporter() { stop(); }

bool HttpExporter::start() {
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (listen_fd_ < 0) return false;
  int one = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port_);
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0 || ::listen(listen_fd_, 64) < 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }
  if (port_ == 0) {
    socklen_t len = sizeof addr;
    getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
  }
  thread_ = std::make_unique<std::thread>([this] { serve(); });
  return true;
}

void HttpExporter::stop() {
  if (!thread_) return;
  stop_.store(true);
  ::shutdown(listen_fd_, SHUT_RDWR);
  ::close(listen_fd_);
  thread_->join();
  thread_.reset();
  listen_fd_ = -1;
}

void HttpExporter::serve() {
  while (!stop_.load()) {
    pollfd pfd{listen_fd_, POLLIN, 0};
    if (::poll(&pfd, 1, 200) <= 0) continue;
    int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
    if (fd < 0) continue;
    timeval tv{2, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    char buf[2048];
    ssize_t n = ::recv(fd, buf, sizeof buf - 1, 0);
    std::string status = "200 OK", body, ctype = "text/plain; version=0.0.4; charset=utf-8";
    if (n > 0) {
      buf[n] = 0;
      std::string req(buf);
      std::string path;
      size_t sp = req.find(' ');
      if (sp != std::string::npos) {
        size_t sp2 = req.find(' ', sp + 1);
        path = req.substr(sp + 1, sp2 == std::string::npos ? std::string::npos : sp2 - sp - 1);
      }
      if (path == "/metrics") {
        body = reg_.render();
      } else if (path == "/healthz" || path == "/livez") {
        body = "ok\n";
      } else if (path == "/readyz") {
        bool ready = ready_ ? ready_() : true;
        status = ready ? "200 OK" : "503 Service Unavailable";
        body = ready ? "ready\n" : "not ready\n";
      } else {
        status = "404 Not Found";
        body = "not found\n";
      }
    } else {
      status = "400 Bad Request";
      body = "bad request\n";
    }
    std::string resp = "HTTP/1.1 " + status + "\r\nContent-Type: " + ctype +
                       "\r\nContent-Length: " + std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
    size_t off = 0;
    while (off < resp.size()) {
      ssize_t w = ::send(fd, resp.data() + off, resp.size() - off, MSG_NOSIGNAL);
      if (w <= 0) break;
      off += static_cast<size_t>(w);
    }
    ::close(fd);
  }
}

}  // namespace me::metrics
