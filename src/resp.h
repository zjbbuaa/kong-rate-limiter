// Minimal RESP2 (Redis serialization protocol) codec.
//
// The gateway implements the wire protocol directly instead of linking
// hiredis: the command surface we need (AUTH/SELECT/GET/HSET/HDEL/EVALSHA/
// SCRIPT LOAD/INCR/VERSION keys) is small, and owning the codec keeps the
// dependency footprint at zero beyond the HTTP front end. See docs/DESIGN.md.
#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace resp {

struct Value {
  enum class Type {
    kSimpleString,
    kError,
    kInteger,
    kBulkString,
    kArray,
    kNull,
  };

  Type type = Type::kNull;
  long long integer = 0;  // valid when type == kInteger
  std::string str;        // payload for kSimpleString / kError / kBulkString
  std::vector<Value> array;  // elements when type == kArray

  bool error() const { return type == Type::kError; }
  bool is_null() const { return type == Type::kNull; }

  // First array element coerced to integer (scripts return integer arrays).
  long long at_int(size_t i) const {
    if (i < array.size() && array[i].type == Type::kInteger) return array[i].integer;
    throw std::runtime_error("resp: array element is not an integer");
  }
};

// Byte source abstraction so the parser stays transport-agnostic and unit
// testable without a socket. Implementations must return exactly n bytes or
// throw std::runtime_error.
class Reader {
 public:
  virtual ~Reader() = default;
  virtual std::string read_bytes(std::size_t n) = 0;
  // Up to and including the terminating CRLF.
  virtual std::string read_line() = 0;
};

// Encode a command as a RESP array of bulk strings.
std::string encode(const std::vector<std::string>& args);

// Parse exactly one value (recursive for arrays).
Value parse(Reader& r);

}  // namespace resp
