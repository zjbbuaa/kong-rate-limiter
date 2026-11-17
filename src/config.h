// Rule model + rule store with dynamic, cluster-wide distribution.
//
// Redis is the single source of truth for configuration so that a rule
// pushed to any replica's admin API converges to all replicas:
//   rl:config:version  INCR'd on every change
//   rl:config:rules    HASH rule_id -> canonical JSON
// Every replica polls the version key and reloads on change; the writer
// also applies its own write immediately (no poll latency on the writer).
//
// Chosen over Redis Pub/Sub push for the MVP: pull/poll is stateless across
// reconnects (a replica that missed a message converges on the next tick)
// and needs no extra connection handling for subscribe-mode pushes. The
// trade-off is convergence latency ~= poll interval; see docs/DESIGN.md.
#pragma once

#include "redis.h"

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace rl {

enum class Algorithm { kFixedWindow, kSlidingWindow, kTokenBucket };
enum class Dimension { kIp, kClientId, kPath };

const char* to_string(Algorithm a);
const char* to_string(Dimension d);
bool parse_algorithm(const std::string& s, Algorithm& out);
bool parse_dimension(const std::string& s, Dimension& out);

struct Rule {
  std::string id;
  Algorithm algorithm = Algorithm::kFixedWindow;
  Dimension dimension = Dimension::kIp;
  std::string path_prefix = "/";  // rule applies to paths starting with this
  std::string method = "*";       // "*" or an uppercase verb (GET, POST, ...)
  long long limit = 0;            // max requests per window / bucket capacity
  long long window_ms = 0;        // window length; bucket refill period
};

bool rule_from_json(const nlohmann::json& j, Rule& out, std::string& errmsg);
nlohmann::json rule_to_json(const Rule& r);

enum class Outcome { kAllowed, kDenied, kDegraded };

struct RuleStats {
  std::atomic<long long> allowed{0};
  std::atomic<long long> denied{0};
  std::atomic<long long> degraded{0};
};

class RuleStore {
 public:
  RuleStore(redis::Config redis_cfg, std::string bootstrap_file, int poll_ms = 1000);
  ~RuleStore();
  RuleStore(const RuleStore&) = delete;

  // Bootstrap (optional JSON file) -> push to Redis -> sync local -> start poller.
  bool start(std::string& err);
  void stop();

  // Snapshot of rules matching method+path, ordered by id (deterministic).
  std::vector<std::shared_ptr<const Rule>> match(const std::string& method,
                                                 const std::string& path) const;
  nlohmann::json snapshot_json() const;
  bool get(const std::string& id, Rule& out) const;

  // Cluster-visible mutations: write Redis first, then apply locally.
  bool upsert(const Rule& r, std::string& errmsg);
  bool remove(const std::string& id, std::string& errmsg);

  void record(const std::string& rule_id, Outcome o);
  nlohmann::json stats_json() const;

  bool redis_ping() const;

  static const char* kVersionKey;
  static const char* kRulesKey;

 private:
  void poll_loop();
  bool load_from_redis(std::map<std::string, std::shared_ptr<const Rule>>& out,
                       std::string& err) const;

  mutable redis::Client redis_;
  std::string bootstrap_file_;
  int poll_ms_;

  mutable std::shared_mutex mu_;
  std::map<std::string, std::shared_ptr<const Rule>> rules_;
  std::string version_;  // opaque string from Redis INCR (compared as string)

  std::atomic<bool> stop_{false};
  std::thread poll_thread_;

  mutable std::mutex stats_mu_;
  std::map<std::string, std::shared_ptr<RuleStats>> stats_;
};

}  // namespace rl
