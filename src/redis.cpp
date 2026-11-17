#include "redis.h"

#include "lua_scripts.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

namespace redis {

namespace {

struct SocketReader final : resp::Reader {
  explicit SocketReader(int fd) : fd_(fd) {}

  std::string read_bytes(std::size_t n) override {
    while (buf_.size() < n) recv_more();
    std::string out = buf_.substr(0, n);
    buf_.erase(0, n);
    return out;
  }

  std::string read_line() override {
    std::size_t pos;
    while ((pos = buf_.find("\r\n")) == std::string::npos) recv_more();
    std::string out = buf_.substr(0, pos + 2);
    buf_.erase(0, pos + 2);
    return out;
  }

 private:
  void recv_more() {
    char tmp[8192];
    const ssize_t n = ::recv(fd_, tmp, sizeof(tmp), 0);
    if (n == 0) throw std::runtime_error("redis: connection closed by peer");
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        throw std::runtime_error("redis: read timeout");
      throw std::runtime_error(std::string("redis: recv: ") + std::strerror(errno));
    }
    buf_.append(tmp, static_cast<std::size_t>(n));
  }

  int fd_;
  std::string buf_;
};

// Connect with deadline: non-blocking connect + select, then back to blocking
// with SO_RCVTIMEO/SO_SNDTIMEO so idle replies cannot stall a worker.
bool connect_with_timeout(const std::string& host, int port, int timeout_ms, int& out_fd,
                          std::string& err) {
  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* res = nullptr;
  const std::string port_str = std::to_string(port);
  const int gai = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
  if (gai != 0) {
    err = std::string("getaddrinfo: ") + gai_strerror(gai);
    return false;
  }

  int fd = -1;
  for (struct addrinfo* p = res; p; p = p->ai_next) {
    fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd < 0) continue;
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
    const int rc = ::connect(fd, p->ai_addr, p->ai_addrlen);
    if (rc == 0 || errno == EINPROGRESS) {
      fd_set wfds;
      FD_ZERO(&wfds);
      FD_SET(fd, &wfds);
      struct timeval tv;
      tv.tv_sec = timeout_ms / 1000;
      tv.tv_usec = (timeout_ms % 1000) * 1000;
      if (::select(fd + 1, nullptr, &wfds, nullptr, &tv) > 0) {
        int so_err = 0;
        socklen_t len = sizeof(so_err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &len);
        if (so_err == 0) break;  // connected
        errno = so_err;
      } else {
        errno = ETIMEDOUT;
      }
    }
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(res);

  if (fd < 0) {
    err = std::string("connect ") + host + ":" + std::to_string(port) + ": " +
          std::strerror(errno);
    return false;
  }

  ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) & ~O_NONBLOCK);
  struct timeval tv;
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef SO_NOSIGPIPE
  ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

  out_fd = fd;
  return true;
}

}  // namespace

Client::Client(Config cfg) : cfg_(std::move(cfg)) {}

Client::~Client() { disconnect(); }

void Client::disconnect() {
  if (fd_ >= 0) ::close(fd_);
  fd_ = -1;
  shas_.clear();
}

bool Client::connect_locked(std::string& err) {
  disconnect();
  if (!connect_with_timeout(cfg_.host, cfg_.port, cfg_.io_timeout_ms, fd_, err)) return false;

  if (!cfg_.password.empty()) {
    std::string e;
    resp::Value v = command({"AUTH", cfg_.password}, e);
    if (!e.empty() || v.error()) {
      err = !e.empty() ? e : ("AUTH rejected: " + v.str);
      disconnect();
      return false;
    }
  }
  if (cfg_.db != 0) {
    std::string e;
    resp::Value v = command({"SELECT", std::to_string(cfg_.db)}, e);
    if (!e.empty() || v.error()) {
      err = !e.empty() ? e : ("SELECT failed: " + v.str);
      disconnect();
      return false;
    }
  }

  // Load all rate-limit scripts once; later invocations go by SHA.
  const std::pair<const char*, const char*> scripts[] = {
      {"fixed_window", lua_scripts::kFixedWindow},
      {"sliding_window", lua_scripts::kSlidingWindow},
      {"token_bucket", lua_scripts::kTokenBucket},
  };
  for (const auto& [name, source] : scripts) {
    std::string e;
    resp::Value v = command({"SCRIPT", "LOAD", source}, e);
    if (!e.empty() || v.error() || v.type != resp::Value::Type::kBulkString) {
      err = !e.empty() ? e : ("SCRIPT LOAD failed: " + v.str);
      disconnect();
      return false;
    }
    shas_[name] = v.str;
  }
  return true;
}

bool Client::send_locked(const std::vector<std::string>& args, std::string& err) {
  const std::string wire = resp::encode(args);
  std::size_t sent = 0;
  while (sent < wire.size()) {
#ifdef MSG_NOSIGNAL
    const ssize_t n = ::send(fd_, wire.data() + sent, wire.size() - sent, MSG_NOSIGNAL);
#else
    const ssize_t n = ::send(fd_, wire.data() + sent, wire.size() - sent, 0);
#endif
    if (n < 0) {
      if (errno == EINTR) continue;
      err = std::string("redis: send: ") + std::strerror(errno);
      return false;
    }
    sent += static_cast<std::size_t>(n);
  }
  return true;
}

resp::Value Client::read_reply_locked(std::string& err) {
  try {
    SocketReader reader(fd_);
    return resp::parse(reader);
  } catch (const std::exception& e) {
    err = e.what();
    return {};
  }
}

resp::Value Client::command(const std::vector<std::string>& args, std::string& err) {
  err.clear();
  if (fd_ < 0 && !connect_locked(err)) return {};

  if (!send_locked(args, err)) {
    // Never reached the server -> safe to reconnect and try once more.
    std::string connect_err;
    if (!connect_locked(connect_err)) return {};
    if (!send_locked(args, err)) {
      disconnect();
      return {};
    }
  }

  resp::Value v = read_reply_locked(err);
  if (!err.empty()) {
    // Reply unreadable: the command may have executed. Do not retry.
    disconnect();
    return {};
  }
  return v;
}

bool Client::ping(std::string& err) {
  err.clear();
  resp::Value v = command({"PING"}, err);
  if (!err.empty()) return false;
  if (v.error()) {
    err = v.str;
    return false;
  }
  return true;
}

bool Client::eval(const std::string& script_name, const std::vector<std::string>& keys,
                  const std::vector<std::string>& args, resp::Value& out, std::string& err) {
  err.clear();
  const std::string* sha = nullptr;
  {
    const auto it = shas_.find(script_name);
    if (it != shas_.end()) sha = &it->second;
  }
  if (!sha) {
    // Not loaded (fresh connection race / unknown name): (re)connect loads.
    if (!connect_locked(err)) return false;
    const auto it = shas_.find(script_name);
    if (it == shas_.end()) {
      err = "unknown script: " + script_name;
      return false;
    }
    sha = &it->second;
  }

  auto invoke = [&]() {
    std::vector<std::string> cmd = {"EVALSHA", *sha, std::to_string(keys.size())};
    cmd.insert(cmd.end(), keys.begin(), keys.end());
    cmd.insert(cmd.end(), args.begin(), args.end());
    std::string e;
    resp::Value v = command(cmd, e);
    if (!e.empty()) {
      err = e;
      return resp::Value{};
    }
    return v;
  };

  resp::Value v = invoke();
  if (!err.empty()) return false;

  if (v.error() && v.str.rfind("NOSCRIPT", 0) == 0) {
    // Script cache evicted (server restart / SCRIPT FLUSH): reload and retry.
    // NOSCRIPT is returned before execution, so re-invoking is safe.
    std::string e;
    const char* source = nullptr;
    if (script_name == "fixed_window") source = lua_scripts::kFixedWindow;
    if (script_name == "sliding_window") source = lua_scripts::kSlidingWindow;
    if (script_name == "token_bucket") source = lua_scripts::kTokenBucket;
    resp::Value loaded = command({"SCRIPT", "LOAD", source}, e);
    if (!e.empty() || loaded.error() || loaded.type != resp::Value::Type::kBulkString) {
      err = !e.empty() ? e : ("SCRIPT LOAD failed: " + loaded.str);
      return false;
    }
    shas_[script_name] = loaded.str;
    sha = &shas_[script_name];
    v = invoke();
    if (!err.empty()) return false;
  }

  if (v.error()) {
    err = v.str;
    return false;
  }
  out = std::move(v);
  return true;
}

}  // namespace redis
