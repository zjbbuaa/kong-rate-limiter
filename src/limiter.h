// The limiter: maps (rule, request) -> atomic Lua evaluation on Redis.
//
// Key layout:  rl:v1:<algorithm>:<rule_id>:<dimension>:<encoded-value>:<window_ms>
// The dimension value is hex-encoded (first 96 bytes) with an FNV-1a/64
// digest appended, keeping keys binary-safe against arbitrary path/header
// content while staying deterministic per value. Every key carries a PEXPIRE
// (set inside the Lua scripts) so idle quota state is reaped automatically.
//
// Threading: one redis::Client per worker thread (thread_local). Each
// httplib worker handles one request at a time, so no interleaving on a
// connection; a lost connection is re-established on the next command.
#pragma once

#include "config.h"
#include "redis.h"

#include <memory>
#include <string>
#include <vector>

namespace rl {

struct Dims {
  std::string ip;        // client ip; first X-Forwarded-For hop when trusted
  std::string client_id; // X-Client-ID / X-Api-Key, "anonymous" when absent
  std::string path;      // request path without query string
};

struct Decision {
  bool allowed = true;
  bool degraded = false;  // Redis outage: true (allowed == fail-open mode)
  long long remaining = -1;
  long long limit = -1;
  long long reset_after_ms = 0;  // >0 on deny: when a retry may succeed
  std::string rule_id;           // denying rule (empty when allowed)
};

class Limiter {
 public:
  Limiter(redis::Config cfg, bool fail_open, RuleStore& stats_sink)
      : cfg_(std::move(cfg)), fail_open_(fail_open), stats_(stats_sink) {}

  Decision check(const std::vector<std::shared_ptr<const Rule>>& rules, const Dims& dims);

 private:
  redis::Config cfg_;
  bool fail_open_;
  RuleStore& stats_;
};

}  // namespace rl
