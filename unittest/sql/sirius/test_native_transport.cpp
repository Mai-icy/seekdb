// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "sirius_aggregate_client.h"
#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

using oceanbase::sql::SiriusAggregateClient;
namespace {
bool read_all(int fd, void *data, size_t n) {
  auto *p = static_cast<char *>(data);
  while (n) {
    const ssize_t count = recv(fd, p, n, 0);
    if (count <= 0) return false;
    p += count;
    n -= count;
  }
  return true;
}
std::string read_frame(int fd, char kind) {
  char header[5];
  if (!read_all(fd, header, 5)) return {};
  EXPECT_EQ(kind, header[0]);
  uint32_t n;
  std::memcpy(&n, header + 1, 4);
  n = ntohl(n);
  if (n > 1024) return {};
  std::string data(n, 0);
  EXPECT_TRUE(read_all(fd, data.data(), n));
  return data;
}
void reply(int fd, char kind, const std::string &data = {}) {
  char header[5]{kind, 0, 0, 0, 0};
  const uint32_t n = htonl(data.size());
  std::memcpy(header + 1, &n, 4);
  ASSERT_EQ(5, send(fd, header, 5, MSG_NOSIGNAL));
  if (!data.empty()) ASSERT_EQ(data.size(), send(fd, data.data(), data.size(), MSG_NOSIGNAL));
}
struct Server {
  char directory[32] = "/tmp/seekdb-native-XXXXXX";
  int fd = -1;
  std::string path;
  std::thread worker;
  explicit Server(std::function<void(int)> serve) {
    EXPECT_NE(nullptr, mkdtemp(directory));
    path = std::string(directory) + "/s";
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strcpy(addr.sun_path, path.c_str());
    EXPECT_EQ(0, bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)));
    EXPECT_EQ(0, listen(fd, 1));
    setenv("SEEKDB_SIRIUS_SOCKET", path.c_str(), 1);
    worker = std::thread([this, serve] {
      const int peer = accept(fd, nullptr, nullptr);
      if (peer >= 0) { serve(peer); ::close(peer); }
    });
  }
  ~Server() {
    worker.join();
    ::close(fd);
    unlink(path.c_str());
    rmdir(directory);
  }
};
}

TEST(SiriusNativeTransport, TypedBatchAndResult) {
  Server server([](int fd) {
    EXPECT_EQ(std::string("\1\1\0\1", 4), read_frame(fd, 'N'));
    reply(fd, 'A');
    auto batch = read_frame(fd, 'B');
    ASSERT_EQ(13, batch.size());
    EXPECT_EQ(1, static_cast<unsigned char>(batch[0]));
    EXPECT_EQ(255, static_cast<unsigned char>(batch[4]));
    int64_t key;
    std::memcpy(&key, batch.data() + 5, 8);
    EXPECT_EQ(42, key);
    reply(fd, 'A');
    EXPECT_TRUE(read_frame(fd, 'E').empty());
    std::string result(22, 0);
    result[0] = result[4] = result[13] = result[14] = 1;
    result[5] = 42;
    reply(fd, 'R', result);
  });
  SiriusAggregateClient client;
  ASSERT_TRUE(client.open(1, 0, true, nullptr, nullptr));
  std::vector<char> input(SiriusAggregateClient::COLUMN_BYTES, 0);
  input[0] = static_cast<char>(255);
  int64_t value = 42;
  std::memcpy(input.data() + SiriusAggregateClient::MASK_BYTES, &value, 8);
  ASSERT_TRUE(client.batch(input.data(), 1));
  char result[64];
  size_t size = 0;
  ASSERT_TRUE(client.finish(result, sizeof(result), size));
  EXPECT_EQ(22, size);
}

TEST(SiriusNativeTransport, ExactGroupedDescriptorAndEmptyResult) {
  Server server([](int fd) {
    EXPECT_EQ(std::string("\4\1\2\0\20", 5), read_frame(fd, 'N'));
    reply(fd, 'A');
    EXPECT_TRUE(read_frame(fd, 'E').empty());
    reply(fd, 'R', std::string(4, 0));
  });
  SiriusAggregateClient client;
  EXPECT_FALSE(client.open_grouped_integer_sums(0, 1, nullptr, nullptr));
  EXPECT_FALSE(client.open_grouped_integer_sums(1, 64, nullptr, nullptr));
  ASSERT_TRUE(client.open_grouped_integer_sums(1, 2, nullptr, nullptr));
  char output[4]; size_t size = 0;
  ASSERT_TRUE(client.finish(output, sizeof(output), size));
  EXPECT_EQ(4, size);
}
TEST(SiriusNativeTransport, ExactMinimumDescriptor) {
  Server server([](int fd) {
    EXPECT_EQ(std::string("\7\1\1\0\20", 5), read_frame(fd, 'N'));
    reply(fd, 'A');
    EXPECT_TRUE(read_frame(fd, 'E').empty());
    reply(fd, 'R', std::string(4, 0));
  });
  SiriusAggregateClient client;
  EXPECT_FALSE(client.open_grouped_integer_min(0, 1, nullptr, nullptr));
  ASSERT_TRUE(client.open_grouped_integer_min(1, 1, nullptr, nullptr));
  char output[4]; size_t size = 0;
  ASSERT_TRUE(client.finish(output, sizeof(output), size));
  EXPECT_EQ(4, size);
}
TEST(SiriusNativeTransport, ExactScalarDescriptorAndWideResult) {
  Server server([](int fd) {
    EXPECT_EQ(std::string("\3\0\1\0\20", 5), read_frame(fd, 'N'));
    reply(fd, 'A');
    auto batch = read_frame(fd, 'B');
    EXPECT_EQ(13, batch.size());
    reply(fd, 'A');
    EXPECT_TRUE(read_frame(fd, 'E').empty());
    std::string result(21, 0);
    result[0] = result[4] = 1;
    result[13] = 1; // 2^64 cannot be carried by the legacy 8-byte value.
    reply(fd, 'R', result);
  });
  SiriusAggregateClient client;
  ASSERT_TRUE(client.open_integer_sums(1, nullptr, nullptr));
  char input[9] = {1};
  ASSERT_TRUE(client.batch(input, 1, 1));
  char result[32]; size_t size = 0;
  ASSERT_TRUE(client.finish(result, sizeof(result), size));
  EXPECT_EQ(21, size);
  EXPECT_EQ(1, result[13]);
}
TEST(SiriusNativeTransport, RejectsUntrustedResultLength) {
  Server server([](int fd) {
    read_frame(fd, 'N'); reply(fd, 'A'); read_frame(fd, 'E');
    const unsigned char header[5] = {'R', 255, 255, 255, 255};
    send(fd, header, 5, MSG_NOSIGNAL);
  });
  SiriusAggregateClient client;
  ASSERT_TRUE(client.open(1, 0, true, nullptr, nullptr));
  char result[64]; size_t size = 123;
  EXPECT_FALSE(client.finish(result, sizeof(result), size));
  EXPECT_EQ(0, size);
}
TEST(SiriusNativeTransport, QueryCancellationClosesRequest) {
  Server server([](int fd) { char byte; EXPECT_EQ(0, recv(fd, &byte, 1, 0)); });
  SiriusAggregateClient client;
  EXPECT_FALSE(client.open(1, 0, true, [](void *) { return true; }, nullptr));
  EXPECT_NE(nullptr, std::strstr(client.error(), "cancelled"));
  client.close();
}
TEST(SiriusNativeTransport, BrokenPeerDoesNotRaiseSigpipe) {
  Server server([](int fd) { read_frame(fd, 'N'); reply(fd, 'A'); });
  SiriusAggregateClient client;
  ASSERT_TRUE(client.open(1, 0, true, nullptr, nullptr));
  std::vector<char> input(SiriusAggregateClient::COLUMN_BYTES, 0);
  EXPECT_FALSE(client.batch(input.data(), 1));
}
TEST(SiriusNativeTransport, RejectsSchemaOverflowAndOversizedBatch) {
  SiriusAggregateClient invalid;
  EXPECT_FALSE(invalid.open(1, 255, false, nullptr, nullptr));
  EXPECT_NE(nullptr, std::strstr(invalid.error(), "schema"));
  EXPECT_FALSE(invalid.open(9, 0, false, nullptr, nullptr));
  Server server([](int fd) {
    read_frame(fd, 'N'); reply(fd, 'A');
    char byte;
    EXPECT_EQ(0, recv(fd, &byte, 1, 0));
  });
  SiriusAggregateClient client;
  ASSERT_TRUE(client.open(8, 56, false, nullptr, nullptr));
  EXPECT_FALSE(client.batch(nullptr, SiriusAggregateClient::BATCH_ROWS));
  EXPECT_NE(nullptr, std::strstr(client.error(), "negotiated bound"));
}

TEST(SiriusNativeTransport, NegotiatesLargeFrameAndRejectsLengthOverflow) {
  struct RestoreEnv {
    std::string previous;
    bool present;
    RestoreEnv() : present(std::getenv("SEEKDB_SIRIUS_FRAME_MIB") != nullptr) {
      if (present) previous = std::getenv("SEEKDB_SIRIUS_FRAME_MIB");
      setenv("SEEKDB_SIRIUS_FRAME_MIB", "128", 1);
    }
    ~RestoreEnv() {
      if (present) setenv("SEEKDB_SIRIUS_FRAME_MIB", previous.c_str(), 1);
      else unsetenv("SEEKDB_SIRIUS_FRAME_MIB");
    }
  } restore;
  Server server([](int fd) {
    const auto descriptor = read_frame(fd, 'N');
    ASSERT_EQ(5, descriptor.size());
    EXPECT_EQ(2, descriptor[0]);
    EXPECT_EQ(128, static_cast<unsigned char>(descriptor[4]));
    reply(fd, 'A');
    char byte;
    EXPECT_EQ(0, recv(fd, &byte, 1, 0));
  });
  SiriusAggregateClient client;
  ASSERT_TRUE(client.open(8, 56, false, nullptr, nullptr));
  // 64 columns * 8M rows wraps uint32 length to a value below 128 MiB.
  // Reject using wide arithmetic before dereferencing the intentionally null buffer.
  EXPECT_FALSE(client.batch(nullptr, SiriusAggregateClient::MAX_BATCH_ROWS,
                            SiriusAggregateClient::MAX_BATCH_ROWS));
  EXPECT_NE(nullptr, std::strstr(client.error(), "negotiated bound"));
}
