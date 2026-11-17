// Thin Redis client over the RESP codec.
//
// Retry policy (documented in docs/DESIGN.md): a command that fails BEFORE
// being written to the socket is safe to re-send after reconnecting; a
// failure while reading the reply is NOT retried because the command may
// already have executed — that ambiguity is surfaced to the caller, which
// applies the configured fail-open/fail-closed policy.
#pragma once

#include "resp.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace redis {

struct Config {
  std::string host = "127.0.0.1";
  int port = 6379;
  std::string password;
  int db = 0;
  int io_timeout_ms = 1500;
};

class Client {
 public:
  explicit Client(Config cfg);
  ~Client();
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  // Generic command. Transport failure -> Null value + non-empty err.
  // Redis error replies are returned as Value{type=kError} (caller decides).
  resp::Value command(const std::vector<std::string>& args, std::string& err);

  bool ping(std::string& err);

  // Evaluate a script registered under `script_name` (see lua_scripts.h).
  // Reloads the script and retries once on NOSCRIPT.
  bool eval(const std::string& script_name, const std::vector<std::string>& keys,
            const std::vector<std::string>& args, resp::Value& out, std::string& err);

  bool connected() const { return fd_ >= 0; }

 private:
  bool connect_locked(std::string& err);  // tcp + auth/select + script load
  void disconnect();
  bool send_locked(const std::vector<std::string>& args, std::string& err);
  resp::Value read_reply_locked(std::string& err);

  Config cfg_;
  int fd_ = -1;
  // script name -> sha1, populated at connect time via SCRIPT LOAD.
  std::unordered_map<std::string, std::string> shas_;
};

}  // namespace redis
