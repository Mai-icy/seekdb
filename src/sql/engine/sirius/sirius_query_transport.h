/*
 * Copyright (c) 2026 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <cstdint>
#include <string>
#include <stdexcept>
#include <memory>
struct ssl_st;
struct ssl_ctx_st;

namespace oceanbase { namespace sql {
// Length-delimited UTF-8 cells preserve decimal values and embedded NULs.
// This experimental whole-query protocol prioritizes correctness over packing.
struct WireWriter {
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
struct WireReader {
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
  bool cell_view(const char *&p, uint32_t &n) {
    n = integer();
    if (n == UINT32_MAX) { p = nullptr; n = 0; return false; }
    if (n > data.size() - position) throw std::runtime_error("Truncated Sirius cell");
    p = data.data() + position; position += n; return true;
  }
  void bytes_view(const char *&p, uint32_t &n) {
    n = integer();
    if (n > data.size() - position) throw std::runtime_error("Truncated Sirius buffer");
    p = data.data() + position; position += n;
  }
  void end() const {
    if (position != data.size()) throw std::runtime_error("Trailing Sirius frame bytes");
  }
};

class TransportTimeout final : public std::runtime_error {
public:
  TransportTimeout() : std::runtime_error("GPU query transport timed out") {}
};

class QueryTransport {
public:
  QueryTransport() = default;
  QueryTransport(const QueryTransport &) = delete;
  QueryTransport &operator=(const QueryTransport &) = delete;
  ~QueryTransport() { close(); }
  void open(int64_t timeout_ms, bool (*cancel)(void *), void *context);
  void send(char kind, const std::string &payload = {});
  char receive(std::string &payload);
  void expect(char kind);
  void close();
private:
  friend class PartUploader;
  void wait_io(short events);
  void transfer(void *data, size_t bytes, bool write);
  int fd_ = -1;
  ssl_ctx_st *tls_context_ = nullptr;
  ssl_st *tls_ = nullptr;
  int64_t deadline_ = 0;
  bool (*cancel_)(void *) = nullptr;
  void *context_ = nullptr;
};

// Borrows the transport exclusively until destruction. Only owned file paths
// cross threads; the uploader never accesses a SQL session or its worker state.
// Files must stay alive until destruction, which cancels and joins the worker.
class PartUploader final {
public:
  struct Stats {
    uint64_t files = 0;
    uint64_t bytes = 0;
    int64_t upload_us = 0;
  };
  explicit PartUploader(QueryTransport &transport, size_t capacity = 4);
  ~PartUploader();
  PartUploader(const PartUploader &) = delete;
  PartUploader &operator=(const PartUploader &) = delete;
  // Nonblocking: false means backpressure. Both calls propagate worker errors.
  bool submit(const std::string &path);
  bool idle();
  Stats stats();
private:
  struct State;
  std::unique_ptr<State> state_;
};
} }
