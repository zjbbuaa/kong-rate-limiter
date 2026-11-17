// HTTP front end: rate-limit middleware on the protected pipeline plus the
// admin API. Runs on cpp-httplib's worker thread pool; the hot path is
//   match rules -> Lua decision(s) on Redis -> proxy or local echo,
// then enriches responses with the X-RateLimit-* / Retry-After header
// semantics from RFC 6585 / draft-ietf-httpapi-ratelimit-headers.
#pragma once

#include "config.h"
#include "limiter.h"

#include <httplib.h>

#include <atomic>
#include <string>

namespace rl {

struct ServerConfig {
  std::string listen_host = "0.0.0.0";
  int listen_port = 8080;
  std::string upstream;  // "http://host:port" to reverse-proxy; empty = local echo
  int upstream_timeout_ms = 5000;
  bool trust_xff = true;  // honor first hop of X-Forwarded-For
};

class Gateway {
 public:
  Gateway(ServerConfig cfg, RuleStore& store, Limiter& limiter);
  bool run(std::string& err);  // blocks until stop()
  void stop();

 private:
  void handle_protected(const httplib::Request& req, httplib::Response& res);
  void serve_local(const httplib::Request& req, httplib::Response& res) const;
  bool proxy_upstream(const httplib::Request& req, httplib::Response& res, std::string& err) const;
  Dims extract_dims(const httplib::Request& req) const;
  void setup_admin();
  void set_rate_limit_headers(httplib::Response& res, const Decision& dec) const;

  ServerConfig cfg_;
  RuleStore& store_;
  Limiter& limiter_;
  httplib::Server svr_;
};

}  // namespace rl
