#include "resp.h"

namespace resp {

std::string encode(const std::vector<std::string>& args) {
  std::string out;
  out.reserve(64 * (args.size() + 1));
  out.push_back('*');
  out += std::to_string(args.size());
  out += "\r\n";
  for (const auto& arg : args) {
    out.push_back('$');
    out += std::to_string(arg.size());
    out += "\r\n";
    out += arg;
    out += "\r\n";
  }
  return out;
}

namespace {

std::string strip_crlf(std::string line) {
  while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) line.pop_back();
  return line;
}

}  // namespace

Value parse(Reader& r) {
  const std::string head = r.read_bytes(1);
  const char prefix = head[0];
  switch (prefix) {
    case '+': {
      Value v;
      v.type = Value::Type::kSimpleString;
      v.str = strip_crlf(r.read_line());
      return v;
    }
    case '-': {
      Value v;
      v.type = Value::Type::kError;
      v.str = strip_crlf(r.read_line());
      return v;
    }
    case ':': {
      Value v;
      v.type = Value::Type::kInteger;
      v.integer = std::stoll(strip_crlf(r.read_line()));
      return v;
    }
    case '$': {
      const long long len = std::stoll(strip_crlf(r.read_line()));
      if (len == -1) return {};  // null bulk string
      if (len < 0) throw std::runtime_error("resp: negative bulk length");
      Value v;
      v.type = Value::Type::kBulkString;
      v.str = r.read_bytes(static_cast<std::size_t>(len));
      r.read_bytes(2);  // trailing CRLF
      return v;
    }
    case '*': {
      const long long n = std::stoll(strip_crlf(r.read_line()));
      if (n == -1) return {};  // null array
      if (n < 0) throw std::runtime_error("resp: negative array length");
      Value v;
      v.type = Value::Type::kArray;
      v.array.reserve(static_cast<std::size_t>(n));
      for (long long i = 0; i < n; ++i) v.array.push_back(parse(r));
      return v;
    }
    default:
      throw std::runtime_error(std::string("resp: unknown type byte '") + prefix + "'");
  }
}

}  // namespace resp
