// kong-takehome-rate-limiter - Take-home option 1: distributed cluster
// rate-limiting middleware.
//
// A gateway-side rate-limit middleware: N replicas share one quota pool
// backed by Redis, with per-request decisions executed atomically in Lua.
#include "config.h"
#include "limiter.h"
#include "server.h"

#include <atomic>
#include <csignal>
#include <cstring>
#include <iostream>
#include <string>

namespace {

rl::Gateway* g_gateway = nullptr;

void on_signal(int) {
  if (g_gateway) g_gateway->stop();
}

struct Options {
  std::string listen = "0.0.0.0:8080";
  std::string redis = "127.0.0.1:6379";
  std::string redis_password;
  int redis_db = 0;
  std::string config_file;
  std::string upstream;
  bool fail_open = true;
  int poll_ms = 1000;
  bool trust_xff = true;
};

void usage(const char* argv0) {
  std::cerr <<
      "usage: " << argv0 << " [options]\n"
      "  --listen HOST:PORT        gateway listen address   (default 0.0.0.0:8080)\n"
      "  --redis HOST:PORT         Redis address            (default 127.0.0.1:6379)\n"
      "  --redis-password STR      AUTH password            (default none)\n"
      "  --redis-db N              SELECT database index    (default 0)\n"
      "  --config FILE             JSON rule file bootstrapped into Redis at start\n"
      "  --upstream URL            http://host:port to reverse-proxy (default: local echo)\n"
      "  --fail-open               allow traffic when Redis is down (default)\n"
      "  --fail-closed             reject (503) when Redis is down\n"
      "  --poll-ms N               config sync poll interval (default 1000)\n"
      "  --no-xff                  ignore X-Forwarded-For, use socket peer IP\n";
}

bool split_host_port(const std::string& in, std::string& host, int& port) {
  const auto colon = in.rfind(':');
  if (colon == std::string::npos) return false;
  host = in.substr(0, colon);
  try {
    port = std::stoi(in.substr(colon + 1));
  } catch (...) {
    return false;
  }
  return !host.empty() && port > 0 && port < 65536;
}

int parse_args(int argc, char** argv, Options& o) {
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto need = [&](const char* flag) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << flag << "\n";
        exit(2);
      }
      return argv[++i];
    };
    if (a == "--listen") o.listen = need("--listen");
    else if (a == "--redis") o.redis = need("--redis");
    else if (a == "--redis-password") o.redis_password = need("--redis-password");
    else if (a == "--redis-db") o.redis_db = std::stoi(need("--redis-db"));
    else if (a == "--config") o.config_file = need("--config");
    else if (a == "--upstream") o.upstream = need("--upstream");
    else if (a == "--fail-open") o.fail_open = true;
    else if (a == "--fail-closed") o.fail_open = false;
    else if (a == "--poll-ms") o.poll_ms = std::stoi(need("--poll-ms"));
    else if (a == "--no-xff") o.trust_xff = false;
    else if (a == "--help" || a == "-h") { usage(argv[0]); return 1; }
    else { std::cerr << "unknown argument: " << a << "\n"; usage(argv[0]); return 1; }
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  Options opt;
  const int pr = parse_args(argc, argv, opt);
  if (pr != 0) return pr == 1 ? 0 : 2;

  std::string host;
  int port = 0;
  if (!split_host_port(opt.listen, host, port)) {
    std::cerr << "invalid --listen (want HOST:PORT): " << opt.listen << "\n";
    return 2;
  }
  std::string redis_host;
  int redis_port = 0;
  if (!split_host_port(opt.redis, redis_host, redis_port)) {
    std::cerr << "invalid --redis (want HOST:PORT): " << opt.redis << "\n";
    return 2;
  }

  redis::Config rc;
  rc.host = redis_host;
  rc.port = redis_port;
  rc.password = opt.redis_password;
  rc.db = opt.redis_db;

  rl::RuleStore store(rc, opt.config_file, opt.poll_ms);
  {
    std::string err;
    if (!store.start(err)) {
      std::cerr << "failed to start rule store: " << err << "\n";
      return 1;
    }
  }

  rl::Limiter limiter(rc, opt.fail_open, store);

  rl::ServerConfig sc;
  sc.listen_host = host;
  sc.listen_port = port;
  sc.upstream = opt.upstream;
  sc.trust_xff = opt.trust_xff;
  rl::Gateway gateway(sc, store, limiter);
  g_gateway = &gateway;

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  std::string err;
  const bool ok = gateway.run(err);  // blocks
  g_gateway = nullptr;

  store.stop();
  if (!ok && !err.empty()) {
    std::cerr << "gateway error: " << err << "\n";
    return 1;
  }
  std::cerr << "[gateway] shut down\n";
  return 0;
}
