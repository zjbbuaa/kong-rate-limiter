#include "config.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>

namespace rl {

const char* RuleStore::kVersionKey = "rl:config:version";
const char* RuleStore::kRulesKey = "rl:config:rules";

const char* to_string(Algorithm a) {
  switch (a) {
    case Algorithm::kFixedWindow: return "fixed_window";
    case Algorithm::kSlidingWindow: return "sliding_window";
    case Algorithm::kTokenBucket: return "token_bucket";
  }
  return "unknown";
}

const char* to_string(Dimension d) {
  switch (d) {
    case Dimension::kIp: return "ip";
    case Dimension::kClientId: return "client_id";
    case Dimension::kPath: return "path";
  }
  return "unknown";
}

bool parse_algorithm(const std::string& s, Algorithm& out) {
  if (s == "fixed_window") { out = Algorithm::kFixedWindow; return true; }
  if (s == "sliding_window") { out = Algorithm::kSlidingWindow; return true; }
  if (s == "token_bucket") { out = Algorithm::kTokenBucket; return true; }
  return false;
}

bool parse_dimension(const std::string& s, Dimension& out) {
  if (s == "ip") { out = Dimension::kIp; return true; }
  if (s == "client_id") { out = Dimension::kClientId; return true; }
  if (s == "path") { out = Dimension::kPath; return true; }
  return false;
}

namespace {

bool valid_id(const std::string& id) {
  if (id.empty() || id.size() > 64) return false;
  return std::all_of(id.begin(), id.end(), [](unsigned char c) {
    return std::isalnum(c) || c == '-' || c == '_';
  });
}

bool valid_method(std::string m) {
  if (m == "*") return true;
  return std::all_of(m.begin(), m.end(), [](unsigned char c) { return std::isupper(c); });
}

std::string require_string(const nlohmann::json& j, const char* key, bool& ok) {
  const auto it = j.find(key);
  if (it == j.end() || !it->is_string()) { ok = false; return {}; }
  return it->get<std::string>();
}

long long require_int(const nlohmann::json& j, const char* key, bool& ok) {
  const auto it = j.find(key);
  if (it == j.end() || !it->is_number_integer()) { ok = false; return 0; }
  return it->get<long long>();
}

}  // namespace

bool rule_from_json(const nlohmann::json& j, Rule& out, std::string& errmsg) {
  if (!j.is_object()) { errmsg = "rule must be a JSON object"; return false; }
  Rule r;
  bool ok = true;
  r.id = require_string(j, "id", ok);
  const std::string algo = require_string(j, "algorithm", ok);
  const std::string dim = require_string(j, "dimension", ok);
  r.path_prefix = require_string(j, "path_prefix", ok);
  r.method = require_string(j, "method", ok);
  r.limit = require_int(j, "limit", ok);
  r.window_ms = require_int(j, "window_ms", ok);
  if (!ok) { errmsg = "missing/invalid fields (need string id, algorithm, dimension, "
                      "path_prefix, method and integer limit, window_ms)"; return false; }
  if (!valid_id(r.id)) { errmsg = "invalid id (1-64 chars of [A-Za-z0-9_-])"; return false; }
  if (!parse_algorithm(algo, r.algorithm)) {
    errmsg = std::string("unknown algorithm '") + algo +
             "' (fixed_window | sliding_window | token_bucket)";
    return false;
  }
  if (!parse_dimension(dim, r.dimension)) {
    errmsg = std::string("unknown dimension '") + dim + "' (ip | client_id | path)";
    return false;
  }
  if (r.path_prefix.empty() || r.path_prefix.front() != '/') {
    errmsg = "path_prefix must start with '/'";
    return false;
  }
  if (!valid_method(r.method)) { errmsg = "method must be '*' or an UPPERCASE verb"; return false; }
  if (r.limit < 1 || r.limit > 1000000000LL) { errmsg = "limit out of range [1, 1e9]"; return false; }
  if (r.window_ms < 100 || r.window_ms > 86400000LL) {
    errmsg = "window_ms out of range [100, 86400000]";
    return false;
  }
  out = std::move(r);
  return true;
}

nlohmann::json rule_to_json(const Rule& r) {
  return {{"id", r.id},
          {"algorithm", to_string(r.algorithm)},
          {"dimension", to_string(r.dimension)},
          {"path_prefix", r.path_prefix},
          {"method", r.method},
          {"limit", r.limit},
          {"window_ms", r.window_ms}};
}

RuleStore::RuleStore(redis::Config redis_cfg, std::string bootstrap_file, int poll_ms)
    : redis_(std::move(redis_cfg)),
      bootstrap_file_(std::move(bootstrap_file)),
      poll_ms_(poll_ms) {}

RuleStore::~RuleStore() { stop(); }

bool RuleStore::start(std::string& err) {
  // 1. Bootstrap: push file rules into Redis (authoritative seed).
  if (!bootstrap_file_.empty()) {
    std::ifstream in(bootstrap_file_);
    if (!in) {
      err = "cannot open config file: " + bootstrap_file_;
      return false;
    }
    const nlohmann::json arr = nlohmann::json::parse(in, nullptr, false);
    if (arr.is_discarded() || !arr.is_array()) {
      err = "config file must be a JSON array of rules";
      return false;
    }
    for (const auto& item : arr) {
      Rule r;
      std::string errmsg;
      if (!rule_from_json(item, r, errmsg)) {
        err = "invalid rule in " + bootstrap_file_ + ": " + errmsg;
        return false;
      }
      if (!upsert(r, errmsg)) {
        err = "failed to bootstrap rule " + r.id + ": " + errmsg;
        return false;
      }
    }
    std::cerr << "[rules] bootstrapped " << arr.size() << " rule(s) from " << bootstrap_file_
              << "\n";
  }

  // 2. Initial sync (also tolerates Redis being briefly unavailable: we keep
  //    an empty set and the poller will converge once it is back).
  {
    std::map<std::string, std::shared_ptr<const Rule>> fresh;
    std::string sync_err;
    if (load_from_redis(fresh, sync_err)) {
      std::unique_lock<std::shared_mutex> lk(mu_);
      for (auto& [id, rule] : fresh) rules_[id] = std::move(rule);
    } else {
      std::cerr << "[rules] initial sync from Redis failed (" << sync_err
                << ") - starting empty, poller will retry\n";
    }
  }

  poll_thread_ = std::thread([this] { poll_loop(); });
  return true;
}

void RuleStore::stop() {
  stop_.store(true);
  if (poll_thread_.joinable()) poll_thread_.join();
}

void RuleStore::poll_loop() {
  while (!stop_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms_));
    std::string err;
    resp::Value v = redis_.command({"GET", kVersionKey}, err);
    if (!err.empty() || v.type != resp::Value::Type::kBulkString) continue;  // Redis down: keep last known
    const std::string remote_ver = v.str;
    {
      std::shared_lock<std::shared_mutex> lk(mu_);
      if (remote_ver == version_) continue;
    }
    std::map<std::string, std::shared_ptr<const Rule>> fresh;
    if (!load_from_redis(fresh, err)) continue;
    {
      std::unique_lock<std::shared_mutex> lk(mu_);
      rules_ = std::move(fresh);
      version_ = remote_ver;
    }
    std::cerr << "[rules] config synced, version=" << remote_ver << " rules=" << rules_.size()
              << "\n";
  }
}

bool RuleStore::load_from_redis(
    std::map<std::string, std::shared_ptr<const Rule>>& out, std::string& err) const {
  out.clear();
  resp::Value v = redis_.command({"HGETALL", kRulesKey}, err);
  if (!err.empty()) return false;
  if (v.error()) { err = v.str; return false; }
  if (v.type != resp::Value::Type::kArray) { err = "unexpected HGETALL reply"; return false; }
  const auto& a = v.array;
  for (std::size_t i = 0; i + 1 < a.size(); i += 2) {
    const std::string& id = a[i].str;
    const nlohmann::json j = nlohmann::json::parse(a[i + 1].str, nullptr, false);
    Rule r;
    std::string errmsg;
    if (j.is_discarded() || !rule_from_json(j, r, errmsg)) {
      std::cerr << "[rules] skipping invalid stored rule " << id << ": " << errmsg << "\n";
      continue;
    }
    out[id] = std::make_shared<const Rule>(std::move(r));
  }
  return true;
}

std::vector<std::shared_ptr<const Rule>> RuleStore::match(const std::string& method,
                                                          const std::string& path) const {
  std::vector<std::shared_ptr<const Rule>> out;
  std::shared_lock<std::shared_mutex> lk(mu_);
  out.reserve(rules_.size());
  for (const auto& [id, rule] : rules_) {
    if (rule->method != "*" && rule->method != method) continue;
    if (path.compare(0, rule->path_prefix.size(), rule->path_prefix) != 0) continue;
    out.push_back(rule);
  }
  return out;  // map iteration order == sorted by id
}

bool RuleStore::get(const std::string& id, Rule& out) const {
  std::shared_lock<std::shared_mutex> lk(mu_);
  const auto it = rules_.find(id);
  if (it == rules_.end()) return false;
  out = *it->second;
  return true;
}

nlohmann::json RuleStore::snapshot_json() const {
  nlohmann::json rules = nlohmann::json::array();
  std::string ver;
  {
    std::shared_lock<std::shared_mutex> lk(mu_);
    ver = version_;
    for (const auto& [id, rule] : rules_) rules.push_back(rule_to_json(*rule));
  }
  return {{"version", ver}, {"rules", rules}};
}

bool RuleStore::upsert(const Rule& r, std::string& errmsg) {
  // Redis first: if the write fails we do NOT apply locally, so local state
  // never diverges from the shared source of truth.
  std::string err;
  resp::Value v = redis_.command({"HSET", kRulesKey, r.id, rule_to_json(r).dump()}, err);
  if (!err.empty() || v.error()) {
    errmsg = !err.empty() ? err : ("HSET rejected: " + v.str);
    return false;
  }
  redis_.command({"INCR", kVersionKey}, err);  // best effort; GET-miss also converges

  std::unique_lock<std::shared_mutex> lk(mu_);
  rules_[r.id] = std::make_shared<const Rule>(r);
  return true;
}

bool RuleStore::remove(const std::string& id, std::string& errmsg) {
  std::string err;
  resp::Value v = redis_.command({"HDEL", kRulesKey, id}, err);
  if (!err.empty() || v.error()) {
    errmsg = !err.empty() ? err : ("HDEL rejected: " + v.str);
    return false;
  }
  redis_.command({"INCR", kVersionKey}, err);

  std::unique_lock<std::shared_mutex> lk(mu_);
  const auto it = rules_.find(id);
  if (it == rules_.end()) {
    errmsg = "no such rule: " + id;
    return false;
  }
  rules_.erase(it);
  return true;
}

void RuleStore::record(const std::string& rule_id, Outcome o) {
  std::shared_ptr<RuleStats> s;
  {
    std::lock_guard<std::mutex> lk(stats_mu_);
    const auto it = stats_.find(rule_id);
    if (it != stats_.end()) {
      s = it->second;
    } else {
      s = std::make_shared<RuleStats>();
      stats_[rule_id] = s;
    }
  }
  switch (o) {
    case Outcome::kAllowed: s->allowed.fetch_add(1); break;
    case Outcome::kDenied: s->denied.fetch_add(1); break;
    case Outcome::kDegraded: s->degraded.fetch_add(1); break;
  }
}

nlohmann::json RuleStore::stats_json() const {
  nlohmann::json out = nlohmann::json::object();
  std::lock_guard<std::mutex> lk(stats_mu_);
  for (const auto& [id, s] : stats_) {
    out[id] = {{"allowed", s->allowed.load()}, {"denied", s->denied.load()},
               {"degraded", s->degraded.load()}};
  }
  return out;
}

bool RuleStore::redis_ping() const {
  std::string err;
  return redis_.ping(err);
}

}  // namespace rl
