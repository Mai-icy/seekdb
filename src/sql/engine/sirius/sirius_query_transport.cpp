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
#include "sirius_query_transport.h"
#include <chrono>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <fstream>
#include <mutex>
#include <thread>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#if defined(__linux__) || defined(__APPLE__)
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <poll.h>
#include <cerrno>
#include <fcntl.h>
#endif

namespace oceanbase { namespace sql {
namespace {
#if defined(__linux__) || defined(__APPLE__)
int create_query_socket(int domain) {
#if defined(__linux__)
  const int fd = ::socket(domain, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) throw std::runtime_error("Cannot create Sirius socket");
#else
  const int fd = ::socket(domain, SOCK_STREAM, 0);
  if (fd < 0) throw std::runtime_error("Cannot create Sirius socket");
  const int flags = ::fcntl(fd, F_GETFL, 0);
  const int no_sigpipe = 1;
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0
      || ::fcntl(fd, F_SETFD, FD_CLOEXEC) < 0
      || ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe)) < 0) {
    ::close(fd);
    throw std::runtime_error("Cannot configure Sirius socket");
  }
#endif
  return fd;
}
#endif
int64_t query_now_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
}
void QueryTransport::close() {
  if (tls_) SSL_free(tls_);
  if (tls_context_) SSL_CTX_free(tls_context_);
  tls_ = nullptr; tls_context_ = nullptr;
#if defined(__linux__) || defined(__APPLE__)
  if (fd_ >= 0) ::close(fd_);
#endif
  fd_ = -1;
}
void QueryTransport::open(int64_t timeout_ms, bool (*cancel)(void *), void *context) {
  close(); cancel_ = cancel; context_ = context;
  deadline_ = query_now_ms() + timeout_ms;
#if defined(__linux__) || defined(__APPLE__)
  const char *endpoint = std::getenv("SEEKDB_SIRIUS_ENDPOINT");
  if (endpoint && *endpoint) {
    const std::string address_text(endpoint);
    const auto colon = address_text.rfind(':');
    if (colon == std::string::npos) throw std::runtime_error("Use an IPv4:port Sirius endpoint");
    sockaddr_in address{}; address.sin_family = AF_INET;
    const auto host = address_text.substr(0, colon);
    const auto port_text = address_text.substr(colon + 1);
    char *end = nullptr;
    const long port = std::strtol(port_text.c_str(), &end, 10);
    if (port_text.empty() || *end || port < 1 || port > 65535
        || inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1)
      throw std::runtime_error("Invalid Sirius IPv4:port endpoint");
    address.sin_port = htons(port);
    fd_ = create_query_socket(AF_INET);
    if (fd_ < 0) throw std::runtime_error("Cannot create Sirius TCP socket");
    if (connect(fd_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
      if (errno != EINPROGRESS) throw std::runtime_error("Cannot connect to Sirius TCP service");
      wait_io(POLLOUT);
      int error = 0; socklen_t size = sizeof(error);
      if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &size) || error)
        throw std::runtime_error("Cannot connect to Sirius TCP service");
    }
    auto config = [](const char *name) {
      const char *value = std::getenv(name);
      if (!value || !*value) throw std::runtime_error("Configure Sirius TLS CA, client certificate and key");
      return value;
    };
    tls_context_ = SSL_CTX_new(TLS_client_method());
    if (!tls_context_ || !SSL_CTX_set_min_proto_version(tls_context_, TLS1_2_VERSION)
        || SSL_CTX_load_verify_locations(tls_context_, config("SEEKDB_SIRIUS_TLS_CA"), nullptr) != 1
        || SSL_CTX_use_certificate_chain_file(tls_context_, config("SEEKDB_SIRIUS_TLS_CERT")) != 1
        || SSL_CTX_use_PrivateKey_file(tls_context_, config("SEEKDB_SIRIUS_TLS_KEY"), SSL_FILETYPE_PEM) != 1
        || SSL_CTX_check_private_key(tls_context_) != 1)
      throw std::runtime_error("Cannot initialize Sirius TLS credentials");
    SSL_CTX_set_verify(tls_context_, SSL_VERIFY_PEER, nullptr);
    tls_ = SSL_new(tls_context_);
    if (!tls_ || SSL_set_fd(tls_, fd_) != 1) throw std::runtime_error("Cannot initialize Sirius TLS connection");
    const char *server_name = std::getenv("SEEKDB_SIRIUS_TLS_SERVER_NAME");
    if (server_name && *server_name) {
      if (SSL_set1_host(tls_, server_name) != 1 || SSL_set_tlsext_host_name(tls_, server_name) != 1)
        throw std::runtime_error("Invalid Sirius TLS server name");
    } else if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(tls_), host.c_str()) != 1) {
      throw std::runtime_error("Invalid Sirius TLS server IP");
    }
    for (;;) {
      const int result = SSL_connect(tls_);
      if (result == 1) break;
      const int error = SSL_get_error(tls_, result);
      if (error == SSL_ERROR_WANT_READ) wait_io(POLLIN);
      else if (error == SSL_ERROR_WANT_WRITE) wait_io(POLLOUT);
      else throw std::runtime_error("Sirius TLS handshake or certificate verification failed");
    }
  } else {
    const char *path = std::getenv("SEEKDB_SIRIUS_QUERY_SOCKET");
    sockaddr_un address{}; address.sun_family = AF_UNIX;
    if (!path || path[0] != '/' || std::strlen(path) >= sizeof(address.sun_path))
      throw std::runtime_error("Configure SEEKDB_SIRIUS_ENDPOINT or SEEKDB_SIRIUS_QUERY_SOCKET");
    std::strcpy(address.sun_path, path);
    fd_ = create_query_socket(AF_UNIX);
    if (fd_ < 0 || connect(fd_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0)
      throw std::runtime_error("Cannot connect to Sirius query service (unavailable or busy)");
#if defined(__linux__)
    ucred peer{}; socklen_t size = sizeof(peer);
    if (getsockopt(fd_, SOL_SOCKET, SO_PEERCRED, &peer, &size) || peer.uid != geteuid())
      throw std::runtime_error("Sirius query service must have the same OS UID");
#else
    uid_t uid; gid_t gid;
    if (::getpeereid(fd_, &uid, &gid) || uid != geteuid())
      throw std::runtime_error("Sirius query service must have the same OS UID");
#endif
  }
#else
  throw std::runtime_error("Sirius whole-query transport requires Linux or macOS");
#endif
}
void QueryTransport::wait_io(short events) {
#if defined(__linux__) || defined(__APPLE__)
  for (;;) {
    if (cancel_ && cancel_(context_)) throw std::runtime_error("Sirius query cancelled");
    const auto left = deadline_ - query_now_ms();
    if (left <= 0) throw TransportTimeout();
    pollfd item{fd_, events, 0};
    const int ready = poll(&item, 1, static_cast<int>(left < 100 ? left : 100));
    if (!ready || (ready < 0 && errno == EINTR)) continue;
    if (ready < 0) throw std::runtime_error("Sirius query socket poll failed");
    return;
  }
#else
  (void)events;
#endif
}
void QueryTransport::transfer(void *data, size_t bytes, bool write) {
#if defined(__linux__) || defined(__APPLE__)
  auto *p = static_cast<char *>(data);
  while (bytes) {
    if (cancel_ && cancel_(context_)) throw std::runtime_error("Sirius query cancelled");
    if (query_now_ms() >= deadline_) throw TransportTimeout();
    if (tls_) {
      const int amount = static_cast<int>(std::min<size_t>(bytes, 1024 * 1024));
      const int n = write ? SSL_write(tls_, p, amount) : SSL_read(tls_, p, amount);
      if (n > 0) { p += n; bytes -= n; }
      else {
        const int error = SSL_get_error(tls_, n);
        if (error == SSL_ERROR_WANT_READ) wait_io(POLLIN);
        else if (error == SSL_ERROR_WANT_WRITE) wait_io(POLLOUT);
        else throw std::runtime_error("Sirius TLS connection closed or I/O failed");
      }
    } else {
      wait_io(write ? POLLOUT : POLLIN);
#if defined(__linux__)
      constexpr int send_flags = MSG_NOSIGNAL;
#else
      constexpr int send_flags = 0; // SO_NOSIGPIPE also covers OpenSSL's socket writes.
#endif
      const ssize_t n = write ? ::send(fd_, p, bytes, send_flags) : recv(fd_, p, bytes, 0);
      if (n > 0) { p += n; bytes -= n; }
      else if (!n) throw std::runtime_error("Sirius query service disconnected");
      else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
        throw std::runtime_error("Sirius query socket I/O failed");
    }
  }
#else
  (void)data; (void)bytes; (void)write;
  throw std::runtime_error("Sirius whole-query transport requires Linux or macOS");
#endif
}
void QueryTransport::send(char kind, const std::string &payload) {
  if (payload.size() > 16 * 1024 * 1024) throw std::runtime_error("Sirius frame exceeds 16 MiB");
  WireWriter header; header.data.push_back(kind); header.integer(payload.size());
  transfer(&header.data[0], header.data.size(), true);
  if (!payload.empty()) transfer(const_cast<char *>(payload.data()), payload.size(), true);
}
char QueryTransport::receive(std::string &payload) {
  std::string header(5, '\0'); transfer(&header[0], 5, false);
  WireReader reader{header, 1}; const auto size = reader.integer();
  if (size > 16 * 1024 * 1024) throw std::runtime_error("Sirius response exceeds 16 MiB");
  payload.resize(size); if (size) transfer(&payload[0], size, false);
  if (header[0] == 'X') throw std::runtime_error(payload);
  return header[0];
}
void QueryTransport::expect(char kind) {
  std::string payload;
  if (receive(payload) != kind || !payload.empty()) throw std::runtime_error("Invalid Sirius acknowledgment");
}

// The owner checks SQL cancellation on the query thread. During the exclusive
// borrow, I/O checks only this atomic flag and the transport's original deadline.
struct PartUploader::State {
  QueryTransport &transport;
  const size_t capacity;
  bool (*prior_cancel)(void *);
  void *prior_context;
  std::atomic<bool> cancelled{false};
  std::mutex mutex;
  std::condition_variable changed;
  std::deque<std::string> pending;
  bool active = false;
  std::exception_ptr error;
  Stats totals;
  std::thread worker;

  State(QueryTransport &value, size_t bound)
      : transport(value), capacity(bound), prior_cancel(value.cancel_), prior_context(value.context_) {
    if (capacity == 0 || capacity > 16) throw std::invalid_argument("Invalid Parquet upload capacity");
    transport.cancel_ = [](void *p) { return static_cast<State *>(p)->cancelled.load(std::memory_order_acquire); };
    transport.context_ = this;
    try {
      worker = std::thread([this] { run(); });
    } catch (...) {
      transport.cancel_ = prior_cancel;
      transport.context_ = prior_context;
      throw;
    }
  }
  ~State() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      cancelled.store(true, std::memory_order_release);
    }
    changed.notify_one();
    if (worker.joinable()) worker.join();
    transport.cancel_ = prior_cancel;
    transport.context_ = prior_context;
  }
  uint64_t upload(const std::string &path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) throw std::runtime_error("Cannot open GPU Parquet upload");
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!digest || EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr) != 1)
      throw std::runtime_error("Cannot initialize GPU file checksum");
    transport.send('B');
    std::string chunk(1024 * 1024, '\0');
    uint64_t total = 0;
    while (file) {
      file.read(chunk.data(), chunk.size());
      const auto count = file.gcount();
      if (count) {
        if (EVP_DigestUpdate(digest.get(), chunk.data(), count) != 1) throw std::runtime_error("GPU checksum failed");
        // Reuse the full-sized buffer; only the last short chunk changes its size.
        if (static_cast<size_t>(count) < chunk.size()) chunk.resize(count);
        transport.send('D', chunk);
        total += count;
      }
    }
    if (!file.eof()) throw std::runtime_error("Cannot read GPU Parquet upload");
    unsigned char bytes[EVP_MAX_MD_SIZE]; unsigned int size = 0;
    if (EVP_DigestFinal_ex(digest.get(), bytes, &size) != 1) throw std::runtime_error("GPU checksum failed");
    static const char hex[] = "0123456789abcdef";
    std::string checksum;
    for (unsigned int i = 0; i < size; ++i) { checksum += hex[bytes[i] >> 4]; checksum += hex[bytes[i] & 15]; }
    transport.send('E', checksum);
    transport.expect('A'); // Delete only after remote checksum validation and fsync.
#if defined(__linux__) || defined(__APPLE__)
    if (::unlink(path.c_str()) != 0) throw std::runtime_error("Cannot remove uploaded GPU input");
#endif
    return total;
  }
  void run() noexcept {
    try {
      for (;;) {
        std::string path;
        {
          std::unique_lock<std::mutex> lock(mutex);
          changed.wait(lock, [this] { return cancelled.load(std::memory_order_acquire) || !pending.empty(); });
          if (cancelled.load(std::memory_order_acquire)) return;
          path = std::move(pending.front());
          pending.pop_front();
          active = true;
        }
        const auto start = std::chrono::steady_clock::now();
        const auto bytes = upload(path);
        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start).count();
        {
          std::lock_guard<std::mutex> lock(mutex);
          ++totals.files;
          totals.bytes += bytes;
          totals.upload_us += elapsed;
          active = false;
        }
      }
    } catch (...) {
      std::lock_guard<std::mutex> lock(mutex);
      error = std::current_exception();
      active = false;
    }
  }
};
PartUploader::PartUploader(QueryTransport &transport, size_t capacity)
    : state_(std::make_unique<State>(transport, capacity)) {}
PartUploader::~PartUploader() = default;
bool PartUploader::submit(const std::string &path) {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->error) std::rethrow_exception(state_->error);
  if (state_->pending.size() + (state_->active ? 1 : 0) >= state_->capacity) return false;
  state_->pending.push_back(path);
  state_->changed.notify_one();
  return true;
}
bool PartUploader::idle() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->error) std::rethrow_exception(state_->error);
  return !state_->active && state_->pending.empty();
}
PartUploader::Stats PartUploader::stats() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->totals;
}
} }
