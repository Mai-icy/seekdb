// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cstdint>
#include <string>
#include <stdexcept>

namespace oceanbase { namespace sql {
// Length-delimited UTF-8 cells preserve decimal values and embedded NULs.
// This experimental whole-query protocol prioritizes correctness over packing.
struct SiriusWireWriter {
  std::string data;
  void integer(uint32_t n) {
    for (int i = 24; i >= 0; i -= 8) data.push_back(static_cast<char>(n >> i));
  }
  void string(const char *p, size_t n) {
    if (n > 16 * 1024 * 1024) throw std::length_error("Sirius cell exceeds frame limit");
    integer(static_cast<uint32_t>(n));
    if (n) data.append(p, n);
  }
  void string(const std::string &s) { string(s.data(), s.size()); }
};
struct SiriusWireReader {
  const std::string &data;
  size_t position = 0;
  uint32_t integer() {
    if (data.size() - position < 4) throw std::runtime_error("Truncated Sirius integer");
    uint32_t n = 0;
    for (int i = 0; i < 4; ++i) n = (n << 8) | static_cast<unsigned char>(data[position++]);
    return n;
  }
  bool cell(std::string &s) {
    const uint32_t n = integer();
    if (n == UINT32_MAX) { s.clear(); return false; }
    if (n > data.size() - position) throw std::runtime_error("Truncated Sirius cell");
    s.assign(data.data() + position, n); position += n; return true;
  }
  void end() const {
    if (position != data.size()) throw std::runtime_error("Trailing Sirius frame bytes");
  }
};

class SiriusQueryTransport {
public:
  ~SiriusQueryTransport() { close(); }
  void open(int64_t timeout_ms, bool (*cancel)(void *), void *context);
  void send(char kind, const std::string &payload = {});
  char receive(std::string &payload);
  void expect(char kind);
  void close();
private:
  void transfer(void *data, size_t bytes, bool write);
  int fd_ = -1;
  int64_t deadline_ = 0;
  bool (*cancel_)(void *) = nullptr;
  void *context_ = nullptr;
};
} }
