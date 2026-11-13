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
// Standalone native regression for the production upload component.
// Compile with sirius_query_transport.cpp, -Isrc, -pthread, -lssl and -lcrypto.
#include "sql/engine/sirius/sirius_query_transport.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <thread>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
using namespace oceanbase::sql;
using namespace std::chrono_literals;

namespace {
void transfer(int fd, void *buffer, size_t bytes, bool write) {
  auto *p = static_cast<char *>(buffer);
  while (bytes) {
    const auto n = write ? ::send(fd, p, bytes, 0) : ::recv(fd, p, bytes, 0);
    assert(n > 0);
    p += n; bytes -= n;
  }
}
std::pair<char, std::string> receive(int fd) {
  std::string header(5, '\0'); transfer(fd, header.data(), header.size(), false);
  WireReader reader{header, 1};
  std::string payload(reader.integer(), '\0');
  if (!payload.empty()) transfer(fd, payload.data(), payload.size(), false);
  return {header[0], payload};
}
void send(int fd, char kind, const std::string &payload = {}) {
  WireWriter frame; frame.data += kind; frame.integer(payload.size()); frame.data += payload;
  transfer(fd, frame.data.data(), frame.data.size(), true);
}
void read_file(int fd, const std::string &expected) {
  assert(receive(fd).first == 'B');
  std::string bytes;
  for (;;) {
    const auto frame = receive(fd);
    if (frame.first == 'E') { assert(frame.second.size() == 64); break; }
    assert(frame.first == 'D'); bytes += frame.second;
  }
  assert(bytes == expected);
}
struct Fixture {
  std::string directory;
  int listener;
  QueryTransport transport;
  std::thread::id query_thread = std::this_thread::get_id();
  std::atomic<bool> wrong_thread{false};
  Fixture(int timeout_ms = 5000) {
    char path[] = "/tmp/parquet-upload-XXXXXX";
    const char *created = mkdtemp(path); assert(created); directory = created;
    sockaddr_un address{}; address.sun_family = AF_UNIX;
    const auto socket = directory + "/socket";
    std::strcpy(address.sun_path, socket.c_str());
    listener = ::socket(AF_UNIX, SOCK_STREAM, 0); assert(listener >= 0);
    assert(bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0);
    assert(listen(listener, 1) == 0);
    unsetenv("SEEKDB_SIRIUS_ENDPOINT"); setenv("SEEKDB_SIRIUS_QUERY_SOCKET", socket.c_str(), 1);
    transport.open(timeout_ms, [](void *p) {
      auto &f = *static_cast<Fixture *>(p);
      if (std::this_thread::get_id() != f.query_thread) f.wrong_thread = true;
      return false;
    }, this);
  }
  ~Fixture() {
    transport.close(); close(listener);
    unlink((directory + "/socket").c_str());
    for (int i = 0; i < 3; ++i) unlink((directory + "/" + std::to_string(i)).c_str());
    assert(rmdir(directory.c_str()) == 0);
    assert(!wrong_thread);
  }
  std::string file(int index, const std::string &data) {
    const auto path = directory + "/" + std::to_string(index);
    std::ofstream out(path, std::ios::binary); out << data; out.close(); assert(out);
    return path;
  }
};
void wait_idle(PartUploader &upload) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!upload.idle()) {
    assert(std::chrono::steady_clock::now() < deadline);
    std::this_thread::sleep_for(1ms);
  }
}
void pipeline() {
  Fixture f;
  const std::string data(1024 * 1024 + 17, 'x'); // Full and partial upload chunks.
  const auto first = f.file(0, data), second = f.file(1, data), third = f.file(2, data);
  std::promise<void> arrived, release;
  auto gate = release.get_future();
  auto server = std::async(std::launch::async, [&] {
    const int fd = accept(f.listener, nullptr, nullptr); assert(fd >= 0);
    read_file(fd, data); arrived.set_value(); gate.get(); send(fd, 'A');
    read_file(fd, data); send(fd, 'A');
    read_file(fd, data); send(fd, 'A');
    assert(receive(fd).first == 'F'); close(fd);
  });
  {
    PartUploader upload(f.transport, 2);
    assert(upload.submit(first));
    assert(arrived.get_future().wait_for(5s) == std::future_status::ready);
    // Producer advances while the first file is waiting for acknowledgment.
    assert(upload.submit(second)); assert(!upload.submit(third));
    assert(access(first.c_str(), F_OK) == 0); // Not deleted before acknowledgment.
    release.set_value(); wait_idle(upload);
    assert(upload.submit(third)); wait_idle(upload);
    const auto stats = upload.stats(); assert(stats.files == 3 && stats.bytes == data.size() * 3);
    assert(access(first.c_str(), F_OK) != 0 && access(second.c_str(), F_OK) != 0 && access(third.c_str(), F_OK) != 0);
  }
  f.transport.send('F'); server.get();
}
void cancellation() {
  Fixture f;
  const auto path = f.file(0, "held");
  std::promise<void> arrived;
  auto server = std::async(std::launch::async, [&] {
    const int fd = accept(f.listener, nullptr, nullptr); assert(fd >= 0);
    read_file(fd, "held"); arrived.set_value(); char c;
    assert(recv(fd, &c, 1, 0) == 0); close(fd);
  });
  auto upload = std::make_unique<PartUploader>(f.transport);
  assert(upload->submit(path));
  assert(arrived.get_future().wait_for(5s) == std::future_status::ready);
  const auto started = std::chrono::steady_clock::now(); upload.reset();
  assert(std::chrono::steady_clock::now() - started < 1s);
  assert(access(path.c_str(), F_OK) == 0); // Owner removes an unacknowledged file.
  f.transport.close(); server.get();
}
void failure(bool timeout) {
  Fixture f(timeout ? 300 : 5000);
  const auto path = f.file(0, "failed");
  auto server = std::async(std::launch::async, [&] {
    const int fd = accept(f.listener, nullptr, nullptr); assert(fd >= 0);
    read_file(fd, "failed");
    if (timeout) { char c; assert(recv(fd, &c, 1, 0) == 0); }
    else send(fd, 'X', "checksum rejected");
    close(fd);
  });
  bool failed = false;
  {
    PartUploader upload(f.transport); assert(upload.submit(path));
    try { wait_idle(upload); }
    catch (const TransportTimeout &) { assert(timeout); failed = true; }
    catch (const std::runtime_error &e) { assert(!timeout && std::string(e.what()) == "checksum rejected"); failed = true; }
  }
  assert(failed && access(path.c_str(), F_OK) == 0);
  f.transport.close(); server.get();
}
}
int main() {
  pipeline(); cancellation(); failure(false); failure(true);
  std::cout << "PASS: pipeline/backpressure, acknowledged deletion, cancellation, remote error, deadline, callback thread isolation\n";
}
