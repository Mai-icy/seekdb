// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "sirius_aggregate_client.h"
#include <cstdlib>
#include <cstring>
namespace oceanbase { namespace sql {
size_t SiriusAggregateClient::input_frame_bytes() {
  const char *value = std::getenv("SEEKDB_SIRIUS_FRAME_MIB");
  const size_t mib = value && !std::strcmp(value, "64") ? 64
                   : value && !std::strcmp(value, "128") ? 128 : 16;
  return mib * 1024 * 1024;
}
size_t SiriusAggregateClient::batch_row_limit() {
  return input_frame_bytes() == 16 * 1024 * 1024 ? BATCH_ROWS : MAX_BATCH_ROWS;
}
} }
#if defined(__linux__)
#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace oceanbase { namespace sql {
namespace {
int64_t now_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
}
bool SiriusAggregateClient::fail(const char *message) {
  std::snprintf(error_, sizeof(error_), "%s", message);
  return false;
}
void SiriusAggregateClient::close() {
  if (fd_ >= 0) ::close(fd_);
  fd_ = -1;
}
bool SiriusAggregateClient::transfer(void *data, size_t size, bool write) {
  auto *p = static_cast<char *>(data);
  while (size) {
    if (cancel_ && cancel_(context_)) return fail("Sirius aggregate query cancelled");
    const int64_t left = deadline_ms_ - now_ms();
    if (left <= 0) return fail("Sirius aggregate request timed out");
    pollfd item{fd_, static_cast<short>(write ? POLLOUT : POLLIN), 0};
    const int ready = poll(&item, 1, static_cast<int>(left < 100 ? left : 100));
    if (ready == 0 || (ready < 0 && errno == EINTR)) continue;
    if (ready < 0) return fail("Sirius aggregate poll failed");
    const ssize_t n = write ? send(fd_, p, size, MSG_NOSIGNAL) : recv(fd_, p, size, 0);
    if (n > 0) { p += n; size -= n; }
    else if (n == 0) return fail("Sirius aggregate peer disconnected");
    else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
      return fail("Sirius aggregate socket I/O failed");
    }
  }
  return true;
}
bool SiriusAggregateClient::frame(char kind, uint32_t size) {
  char header[5];
  header[0] = kind;
  size = htonl(size);
  std::memcpy(header + 1, &size, 4);
  return transfer(header, 5, true);
}
bool SiriusAggregateClient::response(char expected, char *data, size_t capacity, size_t &size) {
  char header[5];
  uint32_t length;
  size = 0;
  if (!transfer(header, 5, false)) return false;
  std::memcpy(&length, header + 1, 4);
  length = ntohl(length);
  if (header[0] == 'X') {
    if (length > 4096) return fail("Invalid Sirius aggregate error frame");
    char message[4097] = {};
    if (!transfer(message, length, false)) return false;
    return fail(message);
  }
  if (header[0] != expected || length > capacity) return fail("Invalid Sirius aggregate response");
  if (!transfer(data, length, false)) return false;
  size = length;
  return true;
}
bool SiriusAggregateClient::open(uint8_t groups, uint8_t sums, bool count,
                                 bool (*cancel)(void *), void *context) {
  return open_impl(groups, sums, count, false, cancel, context);
}
bool SiriusAggregateClient::open_integer_sums(uint8_t sums, bool (*cancel)(void *), void *context) {
  return open_impl(0, sums, false, 3, cancel, context);
}
bool SiriusAggregateClient::open_grouped_integer_sums(uint8_t groups, uint8_t sums, bool (*cancel)(void *), void *context) {
  return open_impl(groups, sums, false, 4, cancel, context);
}
bool SiriusAggregateClient::open_grouped_integer_min(uint8_t groups, uint8_t values, bool (*cancel)(void *), void *context) {
  return open_impl(groups, values, false, 7, cancel, context);
}
bool SiriusAggregateClient::open_impl(uint8_t groups, uint8_t sums, bool count, uint8_t exact_version,
                                      bool (*cancel)(void *), void *context) {
  close();
  cancel_ = cancel;
  context_ = context;
  error_[0] = 0;
  const unsigned columns = static_cast<unsigned>(groups) + sums;
  const bool exact = exact_version != 0;
  const uint16_t endian = 1;
  if (*reinterpret_cast<const uint8_t *>(&endian) != 1 || columns > 64
      || (exact ? (sums == 0 || count || (exact_version == 3 ? groups != 0 : groups < 1 || groups > 8))
                : (groups < 1 || groups > 8))) {
    return fail("Unsupported native Sirius schema or byte order");
  }
  columns_ = columns;
  frame_limit_ = input_frame_bytes();
  const char *path = std::getenv("SEEKDB_SIRIUS_SOCKET");
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  if (!path || path[0] != '/' || std::strlen(path) >= sizeof(address.sun_path)) {
    return fail("Set SEEKDB_SIRIUS_SOCKET to a valid absolute Unix socket path");
  }
  std::strcpy(address.sun_path, path);
  // Respect cancellation throughout large native scans, without a 60s dataset
  // size ceiling. The SQL execution context still owns the statement deadline.
  deadline_ms_ = now_ms() + (exact ? 3600000 : 60000);
  fd_ = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd_ < 0) return fail("Cannot create Sirius aggregate socket");
  // AF_UNIX connects synchronously; EAGAIN (full backlog) fails without consuming input.
  if (connect(fd_, reinterpret_cast<sockaddr *>(&address), sizeof(address))) {
    return fail("Cannot connect to Sirius aggregate sidecar");
  }
  ucred peer{};
  socklen_t peer_size = sizeof(peer);
  if (getsockopt(fd_, SOL_SOCKET, SO_PEERCRED, &peer, &peer_size) || peer.uid != geteuid()) {
    return fail("Sirius aggregate sidecar must have the same OS UID");
  }
  // Version 2 advertises the larger frame bound before consuming scan input.
  // Old sidecars reject it at open; default version 1 remains byte-compatible.
  const bool large = frame_limit_ > 16 * 1024 * 1024;
  uint8_t descriptor[5] = {static_cast<uint8_t>(exact ? exact_version : large ? 2 : 1), groups, sums,
                         static_cast<uint8_t>(count), static_cast<uint8_t>(frame_limit_ / (1024 * 1024))};
  const size_t descriptor_size = exact || large ? 5 : 4;
  size_t ignored = 0;
  return frame('N', descriptor_size) && transfer(descriptor, descriptor_size, true) && response('A', nullptr, 0, ignored);
}
bool SiriusAggregateClient::batch(const char *columns, uint32_t rows, uint32_t capacity_rows) {
  const size_t row_limit = frame_limit_ == 16 * 1024 * 1024 ? BATCH_ROWS : MAX_BATCH_ROWS;
  if (!rows || rows > capacity_rows || capacity_rows > row_limit) return fail("Invalid native Sirius input batch size");
  const uint32_t masks = (rows + 7) / 8;
  const size_t mask_capacity = (capacity_rows + 7) / 8;
  const size_t stride = mask_capacity + capacity_rows * 8;
  const uint64_t length = 4 + static_cast<uint64_t>(columns_) * (masks + static_cast<uint64_t>(rows) * 8);
  if (length > frame_limit_) return fail("Native Sirius input frame exceeds negotiated bound");
  if (!frame('B', static_cast<uint32_t>(length)) || !transfer(&rows, 4, true)) return false;
  for (uint8_t i = 0; i < columns_; ++i) {
    const char *column = columns + i * stride;
    if (!transfer(const_cast<char *>(column), masks, true)
        || !transfer(const_cast<char *>(column + mask_capacity), rows * 8, true)) return false;
  }
  size_t ignored = 0;
  return response('A', nullptr, 0, ignored);
}
bool SiriusAggregateClient::finish(char *result, size_t capacity, size_t &size) {
  return frame('E', 0) && response('R', result, capacity, size);
}
} }
#else
namespace oceanbase { namespace sql {
bool SiriusAggregateClient::open(uint8_t, uint8_t, bool, bool (*)(void *), void *) { return false; }
bool SiriusAggregateClient::open_integer_sums(uint8_t, bool (*)(void *), void *) { return false; }
bool SiriusAggregateClient::open_grouped_integer_sums(uint8_t, uint8_t, bool (*)(void *), void *) { return false; }
bool SiriusAggregateClient::open_grouped_integer_min(uint8_t, uint8_t, bool (*)(void *), void *) { return false; }
bool SiriusAggregateClient::batch(const char *, uint32_t, uint32_t) { return false; }
bool SiriusAggregateClient::finish(char *, size_t, size_t &) { return false; }
void SiriusAggregateClient::close() {}
} }
#endif
