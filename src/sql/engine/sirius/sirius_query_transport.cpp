// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "sirius_query_transport.h"
#include <chrono>
#include <cstring>
#include <cstdlib>
#if defined(__linux__)
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <poll.h>
#include <cerrno>
#endif

namespace oceanbase { namespace sql {
namespace {
int64_t query_now_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
}
void SiriusQueryTransport::close() {
#if defined(__linux__)
  if (fd_ >= 0) ::close(fd_);
#endif
  fd_ = -1;
}
void SiriusQueryTransport::open(int64_t timeout_ms, bool (*cancel)(void *), void *context) {
  close(); cancel_ = cancel; context_ = context;
  deadline_ = query_now_ms() + timeout_ms;
#if defined(__linux__)
  const char *path = std::getenv("SEEKDB_SIRIUS_QUERY_SOCKET");
  sockaddr_un address{}; address.sun_family = AF_UNIX;
  if (!path || path[0] != '/' || std::strlen(path) >= sizeof(address.sun_path))
    throw std::runtime_error("Set SEEKDB_SIRIUS_QUERY_SOCKET to an absolute query-service socket path");
  std::strcpy(address.sun_path, path);
  fd_ = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd_ < 0 || connect(fd_, reinterpret_cast<sockaddr *>(&address), sizeof(address)))
    throw std::runtime_error("Cannot connect to Sirius whole-query service");
  ucred peer{}; socklen_t size = sizeof(peer);
  if (getsockopt(fd_, SOL_SOCKET, SO_PEERCRED, &peer, &size) || peer.uid != geteuid())
    throw std::runtime_error("Sirius query service must have the same OS UID");
#else
  throw std::runtime_error("Sirius whole-query transport requires Linux");
#endif
}
void SiriusQueryTransport::transfer(void *data, size_t bytes, bool write) {
#if defined(__linux__)
  auto *p = static_cast<char *>(data);
  while (bytes) {
    if (cancel_ && cancel_(context_)) throw std::runtime_error("Sirius query cancelled");
    const auto left = deadline_ - query_now_ms();
    if (left <= 0) throw std::runtime_error("Sirius query transport timed out");
    pollfd item{fd_, static_cast<short>(write ? POLLOUT : POLLIN), 0};
    const int ready = poll(&item, 1, static_cast<int>(left < 100 ? left : 100));
    if (!ready || (ready < 0 && errno == EINTR)) continue;
    if (ready < 0) throw std::runtime_error("Sirius query socket poll failed");
    const ssize_t n = write ? ::send(fd_, p, bytes, MSG_NOSIGNAL) : recv(fd_, p, bytes, 0);
    if (n > 0) { p += n; bytes -= n; }
    else if (!n) throw std::runtime_error("Sirius query service disconnected");
    else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
      throw std::runtime_error("Sirius query socket I/O failed");
  }
#else
  (void)data; (void)bytes; (void)write;
  throw std::runtime_error("Sirius whole-query transport requires Linux");
#endif
}
void SiriusQueryTransport::send(char kind, const std::string &payload) {
  if (payload.size() > 16 * 1024 * 1024) throw std::runtime_error("Sirius frame exceeds 16 MiB");
  SiriusWireWriter header; header.data.push_back(kind); header.integer(payload.size());
  transfer(&header.data[0], header.data.size(), true);
  if (!payload.empty()) transfer(const_cast<char *>(payload.data()), payload.size(), true);
}
char SiriusQueryTransport::receive(std::string &payload) {
  std::string header(5, '\0'); transfer(&header[0], 5, false);
  SiriusWireReader reader{header, 1}; const auto size = reader.integer();
  if (size > 16 * 1024 * 1024) throw std::runtime_error("Sirius response exceeds 16 MiB");
  payload.resize(size); if (size) transfer(&payload[0], size, false);
  if (header[0] == 'X') throw std::runtime_error(payload);
  return header[0];
}
void SiriusQueryTransport::expect(char kind) {
  std::string payload;
  if (receive(payload) != kind || !payload.empty()) throw std::runtime_error("Invalid Sirius acknowledgment");
}
} }
