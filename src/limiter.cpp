#include "limiter.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>
#include <unistd.h>

namespace rl {

namespace {

std::string hex_encode(const std::string& in, std::size_t max_raw) {
  static const char* kHex = "0123456789abcdef";
  const std::size_t n = std::min(in.size(), max_raw);
  std::string out;
  out.reserve(n * 2);
  for (std::size_t i = 0; i < n; ++i) {
    const unsigned char c = static_cast<unsigned char>(in[i]);
    out.push_back(kHex[c >> 4]);
    out.push_back(kHex[c & 0x0f]);
  }
  return out;
}

// FNV-1a/64 over the full value: guarantees distinct keys even when two
// dimension values share their first 96 bytes.
std::uint64_t fnv1a64(const std::string& s) {
  std::uint64_t h = 14695981039346656037ull;
  for (unsigned char c : s) {
    h ^= c;
    h *= 1099511628211ull;
  }
  return h;
}

std::string rate_string(long long limit, long long window_ms) {
  const double rate = static_cast<double>(limit) / static_cast<double>(window_ms);
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%.12g", rate);
  return buf;
}

std::string member_id() {
  // Globally unique ZSET member: epoch-micros + pid + thread + 64-bit random.
  // Uniqueness must hold across replicas, so wall-clock alone is not enough.
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now).count();
  static thread_local std::mt19937_64 rng(std::random_device{}());
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%lld-%d-%llu", static_cast<long long>(micros),
                static_cast<int>(::getpid()),
                static_cast<unsigned long long>(rng()));
  return buf;
}

}  // namespace

Decision Limiter::check(const std::vector<std::shared_ptr<const Rule>>& rules, const Dims& dims) {
  Decision dec;
  if (rules.empty()) return dec;

  // One connection per worker thread; lazily established.
  static thread_local std::unique_ptr<redis::Client> cli;
  if (!cli) cli = std::make_unique<redis::Client>(cfg_);

  long long min_remaining = -1;
  long long min_limit = -1;

  for (const auto& rule_ptr : rules) {
    const Rule& rule = *rule_ptr;

    const std::string* value = nullptr;
    switch (rule.dimension) {
      case Dimension::kIp: value = &dims.ip; break;
      case Dimension::kClientId: value = &dims.client_id; break;
      case Dimension::kPath: value = &dims.path; break;
    }
    const std::string key = "rl:v1:" + std::string(to_string(rule.algorithm)) + ":" + rule.id +
                            ":" + to_string(rule.dimension) + ":" + hex_encode(*value, 96) +
                            ":" + std::to_string(fnv1a64(*value)) + ":" +
                            std::to_string(rule.window_ms);

    std::vector<std::string> keys = {key};
    std::vector<std::string> args;
    std::string script;
    switch (rule.algorithm) {
      case Algorithm::kFixedWindow:
        script = "fixed_window";
        args = {std::to_string(rule.limit), std::to_string(rule.window_ms)};
        break;
      case Algorithm::kSlidingWindow:
        script = "sliding_window";
        args = {std::to_string(rule.limit), std::to_string(rule.window_ms), member_id()};
        break;
      case Algorithm::kTokenBucket:
        script = "token_bucket";
        args = {std::to_string(rule.limit), rate_string(rule.limit, rule.window_ms), "1",
                std::to_string(rule.window_ms * 2)};
        break;
    }

    resp::Value reply;
    std::string err;
    if (!cli->eval(script, keys, args, reply, err)) {
      // Redis unreachable / script failure: apply outage policy.
      stats_.record(rule.id, Outcome::kDegraded);
      dec.degraded = true;
      if (!fail_open_) {
        dec.allowed = false;
        dec.rule_id = rule.id;
        return dec;
      }
      continue;
    }

    const long long allowed_n = reply.at_int(0);
    const long long remaining = reply.at_int(1);
    const long long reset_ms = reply.at_int(2);

    min_remaining = (min_remaining < 0) ? remaining : std::min(min_remaining, remaining);
    min_limit = (min_limit < 0) ? rule.limit : std::min(min_limit, rule.limit);

    if (allowed_n == 1) {
      stats_.record(rule.id, Outcome::kAllowed);
      dec.reset_after_ms = std::max(dec.reset_after_ms, reset_ms);
    } else {
      stats_.record(rule.id, Outcome::kDenied);
      if (!dec.allowed) {
        // Already denied by another rule: keep the *longest* wait.
        dec.reset_after_ms = std::max(dec.reset_after_ms, reset_ms);
      } else {
        dec.allowed = false;
        dec.rule_id = rule.id;
        dec.reset_after_ms = reset_ms;
      }
    }
  }

  dec.remaining = min_remaining;
  dec.limit = min_limit;
  if (!dec.allowed) dec.remaining = 0;
  return dec;
}

}  // namespace rl
