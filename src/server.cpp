#include "server.h"

#include <algorithm>
#include <iostream>

namespace rl {

namespace {

std::string first_hop(const std::string& xff) {
  const auto comma = xff.find(',');
  std::string ip = (comma == std::string::npos) ? xff : xff.substr(0, comma);
  const auto sp = ip.find_last_not_of(" \t");
  if (sp == std::string::npos) return {};
  ip.erase(sp + 1);
  const auto sp0 = ip.find_first_not_of(" \t");
  if (sp0 != std::string::npos) ip.erase(0, sp0);
  return ip;
}

void json_error(httplib::Response& res, int status, const std::string& code,
                const nlohmann::json& extra = {}) {
  nlohmann::json body = {{"error", code}};
  body.update(extra);
  res.status = status;
  res.set_content(body.dump(), "application/json");
}

}  // namespace

Gateway::Gateway(ServerConfig cfg, RuleStore& store, Limiter& limiter)
    : cfg_(std::move(cfg)), store_(store), limiter_(limiter) {}

bool Gateway::run(std::string& err) {
  setup_admin();

  // Protected pipeline: a pre-routing handler intercepts everything that is
  // not /admin or /healthz (those return Unhandled and hit the normal routes
  // registered in setup_admin).
  svr_.set_pre_routing_handler([this](const httplib::Request& req, httplib::Response& res) {
    if (req.path == "/healthz" || req.path.rfind("/admin/", 0) == 0) {
      return httplib::Server::HandlerResponse::Unhandled;
    }
    handle_protected(req, res);
    return httplib::Server::HandlerResponse::Handled;
  });

  if (!svr_.bind_to_port(cfg_.listen_host, cfg_.listen_port)) {
    err = "bind " + cfg_.listen_host + ":" + std::to_string(cfg_.listen_port) + " failed";
    return false;
  }
  std::cerr << "[gateway] listening on http://" << cfg_.listen_host << ":"
            << cfg_.listen_port;
  if (!cfg_.upstream.empty()) std::cerr << " -> upstream " << cfg_.upstream;
  std::cerr << (cfg_.upstream.empty() ? " (local echo mode)" : "") << "\n";
  return svr_.listen_after_bind();
}

void Gateway::stop() { svr_.stop(); }

Dims Gateway::extract_dims(const httplib::Request& req) const {
  Dims d;
  d.path = req.path;
  const std::string xff = cfg_.trust_xff ? req.get_header_value("X-Forwarded-For") : "";
  d.ip = !xff.empty() ? first_hop(xff) : req.remote_addr;
  if (d.ip.empty()) d.ip = "unknown";
  d.client_id = req.get_header_value("X-Client-ID");
  if (d.client_id.empty()) d.client_id = req.get_header_value("X-Api-Key");
  if (d.client_id.empty()) d.client_id = "anonymous";
  return d;
}

void Gateway::set_rate_limit_headers(httplib::Response& res, const Decision& dec) const {
  if (dec.limit >= 0) res.set_header("X-RateLimit-Limit", std::to_string(dec.limit));
  if (dec.remaining >= 0) res.set_header("X-RateLimit-Remaining", std::to_string(dec.remaining));
  if (dec.reset_after_ms > 0) {
    res.set_header("X-RateLimit-Reset",
                   std::to_string((dec.reset_after_ms + 999) / 1000));
  }
  if (dec.degraded) res.set_header("X-RateLimit-Degraded", "1");
}

void Gateway::handle_protected(const httplib::Request& req, httplib::Response& res) {
  try {
    const auto rules = store_.match(req.method, req.path);
    if (!rules.empty()) {
      const Decision dec = limiter_.check(rules, extract_dims(req));

      if (!dec.allowed && dec.degraded) {  // fail-closed outage
        set_rate_limit_headers(res, dec);
        json_error(res, 503, "rate_limiter_unavailable", {{"retry_after_seconds", 1}});
        res.set_header("Retry-After", "1");
        return;
      }
      if (!dec.allowed) {  // 429 per RFC 6585
        const long long retry_after = std::max(1LL, (dec.reset_after_ms + 999) / 1000);
        set_rate_limit_headers(res, dec);
        json_error(res, 429, "rate_limit_exceeded",
                   {{"rule_id", dec.rule_id}, {"retry_after_seconds", retry_after}});
        res.set_header("Retry-After", std::to_string(retry_after));
        return;
      }
      set_rate_limit_headers(res, dec);
    }

    if (!cfg_.upstream.empty()) {
      std::string err;
      if (!proxy_upstream(req, res, err)) {
        json_error(res, 502, "upstream_unavailable", {{"detail", err}});
      }
    } else {
      serve_local(req, res);
    }
  } catch (const std::exception& e) {
    json_error(res, 500, "internal_error", {{"detail", e.what()}});
  }
}

void Gateway::serve_local(const httplib::Request& req, httplib::Response& res) const {
  const nlohmann::json body = {{"status", "ok"},
                               {"service", "kong-takehome-rate-limiter"},
                               {"mode", "local-echo"},
                               {"method", req.method},
                               {"path", req.path}};
  res.status = 200;
  res.set_content(body.dump(), "application/json");
}

bool Gateway::proxy_upstream(const httplib::Request& req, httplib::Response& res,
                             std::string& err) const {
  // cfg_.upstream validated at startup: "http://host:port[/prefix]"
  std::string rest = cfg_.upstream;
  const auto scheme = rest.find("://");
  if (scheme == std::string::npos || rest.substr(0, scheme) != "http") {
    err = "only http:// upstream is supported";
    return false;
  }
  rest = rest.substr(scheme + 3);
  std::string prefix;
  const auto slash = rest.find('/');
  if (slash != std::string::npos) {
    prefix = rest.substr(slash);
    rest = rest.substr(0, slash);
  }
  const auto colon = rest.rfind(':');
  if (colon == std::string::npos) {
    err = "upstream must include an explicit port";
    return false;
  }
  const std::string host = rest.substr(0, colon);
  const int port = std::stoi(rest.substr(colon + 1));

  httplib::Client cli(host, port);
  cli.set_connection_timeout(0, cfg_.upstream_timeout_ms * 1000);
  cli.set_read_timeout(0, cfg_.upstream_timeout_ms * 1000);

  httplib::Request ureq;
  ureq.method = req.method;
  ureq.path = prefix + (req.target.empty() ? req.path : req.target);
  static const char* kHopByHop[] = {"Host",       "Content-Length", "Connection",
                                    "Keep-Alive", "Transfer-Encoding", "Accept-Encoding",
                                    "TE",         "Trailer",        "Upgrade"};
  for (const auto& h : req.headers) {
    bool skip = false;
    for (const char* hbh : kHopByHop)
      if (httplib::detail::case_ignore::equal(h.first, hbh)) skip = true;
    if (!skip) ureq.headers.emplace(h.first, h.second);
  }
  ureq.body = req.body;

  auto upstream = cli.send(ureq);
  if (!upstream) {
    err = "upstream request failed";
    return false;
  }
  res.status = upstream->status;
  res.body = upstream->body;
  for (const auto& h : upstream->headers) {
    bool skip = false;
    for (const char* hbh : kHopByHop)
      if (httplib::detail::case_ignore::equal(h.first, hbh)) skip = true;
    if (!skip) res.set_header(h.first, h.second);
  }
  if (res.body.empty() && upstream->status >= 200 && upstream->status < 300 &&
      res.get_header_value("Content-Type").empty()) {
    res.set_header("Content-Type", "application/json");
  }
  return true;
}

void Gateway::setup_admin() {
  svr_.Get("/healthz", [this](const httplib::Request&, httplib::Response& res) {
    const bool redis_ok = store_.redis_ping();
    res.status = 200;  // gateway is alive; Redis state is reported in the body
    res.set_content(nlohmann::json({{"status", "ok"}, {"redis", redis_ok ? "up" : "down"}}).dump(),
                    "application/json");
  });

  svr_.Get("/admin/v1/rules", [this](const httplib::Request&, httplib::Response& res) {
    res.set_content(store_.snapshot_json().dump(), "application/json");
  });

  auto get_one = [this](const httplib::Request& req, httplib::Response& res) {
    Rule r;
    if (!store_.get(req.matches[1], r)) {
      json_error(res, 404, "rule_not_found", {{"id", req.matches[1].str()}});
      return;
    }
    res.set_content(rule_to_json(r).dump(), "application/json");
  };
  svr_.Get(R"(/admin/v1/rules/([A-Za-z0-9_-]+))", get_one);

  auto put_one = [this](const httplib::Request& req, httplib::Response& res) {
    const nlohmann::json j = nlohmann::json::parse(req.body, nullptr, false);
    if (j.is_discarded()) {
      json_error(res, 400, "invalid_json");
      return;
    }
    Rule r;
    std::string errmsg;
    if (!rule_from_json(j, r, errmsg) || r.id != req.matches[1].str()) {
      json_error(res, 400, "invalid_rule",
                 {{"detail", r.id.empty() ? errmsg : "path id does not match body id"}});
      return;
    }
    if (!store_.upsert(r, errmsg)) {
      json_error(res, 502, "config_write_failed", {{"detail", errmsg}});
      return;
    }
    res.set_content(nlohmann::json({{"ok", true}, {"rule", rule_to_json(r)}}).dump(),
                    "application/json");
  };
  svr_.Put(R"(/admin/v1/rules/([A-Za-z0-9_-]+))", put_one);

  auto del_one = [this](const httplib::Request& req, httplib::Response& res) {
    std::string errmsg;
    if (!store_.remove(req.matches[1], errmsg)) {
      json_error(res, 404, "rule_not_found", {{"detail", errmsg}});
      return;
    }
    res.set_content(nlohmann::json({{"ok", true}, {"id", req.matches[1].str()}}).dump(),
                    "application/json");
  };
  svr_.Delete(R"(/admin/v1/rules/([A-Za-z0-9_-]+))", del_one);

  svr_.Get("/admin/v1/stats", [this](const httplib::Request&, httplib::Response& res) {
    res.set_content(nlohmann::json({{"rules", store_.stats_json()}}).dump(), "application/json");
  });
}

}  // namespace rl
