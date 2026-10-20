#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "me/metrics.h"

using namespace me::metrics;

static std::string http_get(uint16_t port, const std::string& path) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(port);
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) return "";
  std::string req = "GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n";
  ::send(fd, req.data(), req.size(), 0);
  std::string out;
  char buf[4096];
  ssize_t n;
  while ((n = ::recv(fd, buf, sizeof buf, 0)) > 0) out.append(buf, static_cast<size_t>(n));
  ::close(fd);
  return out;
}

TEST(Histogram, BucketsAndSum) {
  Histogram h;
  h.observe(50);     // <= 100
  h.observe(150);    // <= 200
  h.observe(1e12);   // +Inf
  EXPECT_EQ(h.count_at(0), 1u);
  EXPECT_EQ(h.count_at(1), 1u);
  EXPECT_EQ(h.count_at(Histogram::kBuckets), 1u);
  EXPECT_EQ(h.total(), 3u);
  EXPECT_DOUBLE_EQ(h.sum(), 50 + 150 + 1e12);
}

TEST(Registry, RendersPrometheusText) {
  ShardMetrics s0, s1;
  ProcessMetrics pm;
  s0.orders_received.inc(3);
  s1.orders_received.inc(4);
  s0.matching_latency_ns.observe(250);
  pm.symbols.set(7);
  Registry reg({&s0, &s1}, &pm);
  reg.set_extra_provider([] { return std::string("me_extra 1\n"); });
  std::string out = reg.render();
  EXPECT_NE(out.find("me_orders_received_total{shard=\"0\"} 3"), std::string::npos);
  EXPECT_NE(out.find("me_orders_received_total{shard=\"all\"} 7"), std::string::npos);
  EXPECT_NE(out.find("me_matching_latency_ns_bucket{shard=\"0\",le=\"500\"} 1"), std::string::npos);
  EXPECT_NE(out.find("me_symbols 7"), std::string::npos);
  EXPECT_NE(out.find("process_resident_memory_bytes"), std::string::npos);
  EXPECT_NE(out.find("me_extra 1"), std::string::npos);
  EXPECT_NE(out.find("# TYPE me_grpc_latency_ns histogram"), std::string::npos);
}

TEST(HttpExporter, ServesMetricsAndProbes) {
  ShardMetrics s0;
  ProcessMetrics pm;
  Registry reg({&s0}, &pm);
  bool ready = false;
  HttpExporter ex(reg, 0, [&] { return ready; });
  ASSERT_TRUE(ex.start());
  ASSERT_NE(ex.port(), 0);
  std::string m = http_get(ex.port(), "/metrics");
  EXPECT_NE(m.find("HTTP/1.1 200 OK"), std::string::npos);
  EXPECT_NE(m.find("me_operations_total"), std::string::npos);
  EXPECT_NE(http_get(ex.port(), "/healthz").find("200 OK"), std::string::npos);
  EXPECT_NE(http_get(ex.port(), "/readyz").find("503"), std::string::npos);
  ready = true;
  EXPECT_NE(http_get(ex.port(), "/readyz").find("200 OK"), std::string::npos);
  EXPECT_NE(http_get(ex.port(), "/nope").find("404"), std::string::npos);
  ex.stop();
}
