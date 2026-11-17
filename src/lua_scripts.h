// Rate-limit algorithms as atomic Lua evaluations.
//
// Why Lua: every decision is a read-modify-write on shared counters
// (INCR+PEXPIRE, ZSET pruning+ZADD, bucket refill+consume). Executed as
// separate commands from N gateway replicas these would race; inside a Lua
// script they are atomic with respect to all other clients of the Redis
// server, which is what gives the cluster its "same quota pool" semantics.
//
// All scripts are loaded once per connection via SCRIPT LOAD and invoked by
// SHA (EVALSHA); a NOSCRIPT reply (server restart / SCRIPT FLUSH) triggers a
// reload-and-retry. See redis::Client::eval.
//
// Every script returns a 3-element integer array:
//   {allowed(0|1), remaining, reset_after_ms}
//   - remaining:      tokens/requests left in the current window
//   - reset_after_ms: denied  -> wait before retrying;
//                     allowed -> time until the current window/bucket fully
//                                resets (0 when not tracked cheaply)
//
// Time base: sliding-window and token-bucket scripts take `TIME` from the
// Redis server itself, so quota decisions are made on a single clock no
// matter which replica issued the request (removes cross-host clock skew
// from the consistency equation; fixed windows rely on key TTL instead).
#pragma once

namespace lua_scripts {

inline constexpr char kFixedWindow[] = R"LUA(
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then ttl = tonumber(ARGV[2]) end
local allowed = 0
if current <= tonumber(ARGV[1]) then allowed = 1 end
return {allowed, math.max(0, tonumber(ARGV[1]) - current), ttl}
)LUA";

inline constexpr char kSlidingWindow[] = R"LUA(
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - tonumber(ARGV[2]))
local count = redis.call('ZCARD', KEYS[1])
local allowed = 0
local retry_after = 0
if count < tonumber(ARGV[1]) then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  count = count + 1
  allowed = 1
else
  local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  if oldest[2] then
    retry_after = tonumber(oldest[2]) + tonumber(ARGV[2]) - now
    if retry_after < 0 then retry_after = 0 end
  end
end
return {allowed, math.max(0, tonumber(ARGV[1]) - count), retry_after}
)LUA";

inline constexpr char kTokenBucket[] = R"LUA(
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local burst = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])     -- tokens per millisecond (float string)
local amount = tonumber(ARGV[3])   -- requested tokens (always 1 today)
local idle_ttl = tonumber(ARGV[4]) -- reap idle buckets after this long
local data = redis.call('HMGET', KEYS[1], 't', 'tok')
local last = tonumber(data[1])
local tokens = tonumber(data[2])
if not last then
  last = now
  tokens = burst
end
tokens = math.min(burst, tokens + (now - last) * rate)
local allowed = 0
if tokens >= amount then
  tokens = tokens - amount
  allowed = 1
end
redis.call('HMSET', KEYS[1], 't', now, 'tok', tokens)
redis.call('PEXPIRE', KEYS[1], idle_ttl)
local retry_after = 0
if allowed == 0 then retry_after = math.ceil((amount - tokens) / rate) end
return {allowed, math.floor(tokens), retry_after}
)LUA";

}  // namespace lua_scripts
