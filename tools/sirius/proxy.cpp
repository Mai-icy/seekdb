// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
// CPU-only transport for the existing seekdb bridge ABI (Linux).
#include "sirius_bridge_api.h"
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
#include <stdexcept>
#include <string>

namespace {
using Clock = std::chrono::steady_clock;
struct Socket {
  int fd = -1;
  ~Socket() { if (fd >= 0) close(fd); }
};
void ready(int fd, short events, Clock::time_point deadline) {
  for (;;) {
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
    if (ms <= 0) throw std::runtime_error("Sirius sidecar request timed out");
    pollfd item{fd, events, 0};
    const int result = poll(&item, 1, static_cast<int>(ms));
    if (result > 0) return; // read/write reports EOF and socket errors.
    if (result == 0) throw std::runtime_error("Sirius sidecar request timed out");
    if (errno != EINTR) throw std::runtime_error("Cannot poll Sirius sidecar socket");
  }
}
void transfer(int fd, void *buffer, size_t size, bool write, Clock::time_point deadline) {
  auto *data = static_cast<char *>(buffer);
  while (size) {
    ready(fd, write ? POLLOUT : POLLIN, deadline);
    const ssize_t n = write ? send(fd, data, size, MSG_NOSIGNAL) : recv(fd, data, size, 0);
    if (n > 0) { data += n; size -= n; }
    else if (n == 0) throw std::runtime_error("Sirius sidecar disconnected");
    else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
      throw std::runtime_error("Sirius sidecar socket I/O failed");
    }
  }
}
int execute(const uint8_t *plan, size_t plan_size, char *output, size_t capacity,
            size_t *output_size, char *error, size_t error_capacity) noexcept {
  if (output_size) *output_size = 0;
  if (error && error_capacity) error[0] = '\0';
  if (!plan || !plan_size || plan_size > 1024 * 1024 || !output || !capacity
      || !output_size || !error || !error_capacity) return SEEKDB_SIRIUS_INVALID_ARGUMENT;
  try {
    const char *path = std::getenv("SEEKDB_SIRIUS_SOCKET");
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (!path || *path != '/' || std::strlen(path) >= sizeof(address.sun_path)) {
      throw std::runtime_error("SEEKDB_SIRIUS_SOCKET must be an absolute Unix socket path shorter than 108 bytes");
    }
    std::strcpy(address.sun_path, path);
    long timeout = 60000;
    if (const char *setting = std::getenv("SEEKDB_SIRIUS_TIMEOUT_MS")) {
      char *end = nullptr;
      errno = 0;
      timeout = std::strtol(setting, &end, 10);
      if (errno || !*setting || *end || timeout < 1 || timeout > 600000) {
        throw std::runtime_error("SEEKDB_SIRIUS_TIMEOUT_MS must be in [1,600000]");
      }
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout);
    Socket socket;
    socket.fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (socket.fd < 0) throw std::runtime_error("Cannot create Sirius sidecar socket");
    if (connect(socket.fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
      if (errno != EINPROGRESS) throw std::runtime_error("Cannot connect to Sirius sidecar");
      ready(socket.fd, POLLOUT, deadline);
      int result = 0;
      socklen_t size = sizeof(result);
      if (getsockopt(socket.fd, SOL_SOCKET, SO_ERROR, &result, &size) || result) {
        throw std::runtime_error("Cannot connect to Sirius sidecar");
      }
    }
    ucred peer{};
    socklen_t peer_size = sizeof(peer);
    if (getsockopt(socket.fd, SOL_SOCKET, SO_PEERCRED, &peer, &peer_size) || peer.uid != geteuid()) {
      throw std::runtime_error("Sirius sidecar must run as the same OS user");
    }
    unsigned char header[5]{'P', 0, 0, 0, 0};
    uint32_t length = htonl(static_cast<uint32_t>(plan_size));
    std::memcpy(header + 1, &length, sizeof(length));
    transfer(socket.fd, header, sizeof(header), true, deadline);
    transfer(socket.fd, const_cast<uint8_t *>(plan), plan_size, true, deadline);
    transfer(socket.fd, header, sizeof(header), false, deadline);
    std::memcpy(&length, header + 1, sizeof(length));
    length = ntohl(length);
    if (header[0] == 'X' && length <= 4096) {
      std::string message(length, '\0');
      transfer(socket.fd, message.data(), length, false, deadline);
      std::snprintf(error, error_capacity, "%s", message.c_str());
      return SEEKDB_SIRIUS_EXECUTION_ERROR;
    }
    if (header[0] != 'J' || !length || length > 1024 * 1024) {
      throw std::runtime_error("Invalid Sirius sidecar response frame");
    }
    if (length > capacity) {
      std::snprintf(error, error_capacity, "Sirius result exceeds output capacity");
      return SEEKDB_SIRIUS_RESULT_TOO_LARGE;
    }
    transfer(socket.fd, output, length, false, deadline);
    *output_size = length;
    return SEEKDB_SIRIUS_OK;
  } catch (const std::exception &e) {
    std::snprintf(error, error_capacity, "%s", e.what());
  } catch (...) {
    std::snprintf(error, error_capacity, "Unknown Sirius sidecar transport error");
  }
  return SEEKDB_SIRIUS_UNAVAILABLE;
}
const SeekdbSiriusApiV1 api{1, sizeof(SeekdbSiriusApiV1), execute};
}
extern "C" __attribute__((visibility("default")))
const SeekdbSiriusApiV1 *seekdb_sirius_get_api_v1() noexcept { return &api; }
