// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "arrow_json.h"
#include "sirius_client.h"
#include <gtest/gtest.h>
#include <cstdlib>
#include <future>
#include <limits>
#include <string>

namespace {
struct StreamFixture {
  int schema_releases = 0, batch_releases = 0, stream_releases = 0;
  int next = 0;
  bool fail_read = false;
  int64_t integers[4] = {10, -20, 30, 40};
  const void *buffers[3] = {nullptr, integers, nullptr};
  ArrowSchema column{};
  ArrowSchema *columns[1] = {&column};
  ArrowSchema schema{};
  ArrowArray array{};
  ArrowArray *arrays[1] = {&array};
  ArrowArray batch{};
  ArrowArrayStream stream{};
  StreamFixture() {
    column.format = "l"; column.name = "value";
    schema.format = "+s"; schema.n_children = 1; schema.children = columns;
    schema.private_data = this;
    schema.release = [](ArrowSchema *s) {
      ++static_cast<StreamFixture *>(s->private_data)->schema_releases; s->release = nullptr;
    };
    array.length = 4; array.n_buffers = 2; array.buffers = buffers;
    batch.length = 3; batch.n_children = 1; batch.children = arrays; batch.private_data = this;
    batch.release = [](ArrowArray *a) {
      ++static_cast<StreamFixture *>(a->private_data)->batch_releases; a->release = nullptr;
    };
    stream.private_data = this;
    stream.get_schema = [](ArrowArrayStream *s, ArrowSchema *out) {
      *out = static_cast<StreamFixture *>(s->private_data)->schema; return 0;
    };
    stream.get_next = [](ArrowArrayStream *s, ArrowArray *out) {
      auto &fixture = *static_cast<StreamFixture *>(s->private_data);
      if (fixture.fail_read) { return 5; }
      if (fixture.next++ == 0) { *out = fixture.batch; }
      else { *out = {}; }
      return 0;
    };
    stream.get_last_error = [](ArrowArrayStream *) { return "producer failure"; };
    stream.release = [](ArrowArrayStream *s) {
      ++static_cast<StreamFixture *>(s->private_data)->stream_releases; s->release = nullptr;
    };
  }
  std::string json() {
    char output[4096];
    const auto n = seekdb_sirius::arrow_to_json(stream, output, sizeof(output));
    return {output, n};
  }
};

TEST(SiriusArrow, IntegersNullsAndOwnership) {
  StreamFixture f;
  const uint8_t validity[] = {0b1101};
  f.buffers[0] = validity; f.array.null_count = 1;
  EXPECT_EQ(f.json(), "{\"columns\":[{\"name\":\"value\",\"format\":\"l\"}],\"rows\":[[10],[null],[30]]}");
  EXPECT_EQ(f.schema_releases, 1); EXPECT_EQ(f.batch_releases, 1); EXPECT_EQ(f.stream_releases, 1);
}
TEST(SiriusArrow, ParentAndChildOffsets) {
  StreamFixture f;
  f.batch.offset = 1; f.batch.length = 1;
  f.array.offset = 1; f.array.length = 3;
  EXPECT_NE(f.json().find("\"rows\":[[30]]"), std::string::npos);
}
TEST(SiriusArrow, StringEscaping) {
  StreamFixture f;
  int32_t offsets[] = {0, 5, 5, 6};
  const char data[] = {'a', '"', '\\', '\n', '\0', 'z'};
  f.column.format = "u"; f.array.n_buffers = 3;
  f.buffers[1] = offsets; f.buffers[2] = data;
  EXPECT_NE(f.json().find("\"rows\":[[\"a\\\"\\\\\\u000a\\u0000\"],[\"\"],[\"z\"]]"), std::string::npos);
}
TEST(SiriusArrow, DecimalPrecision) {
  StreamFixture f;
  __int128 values[] = {-123, 0, 1};
  f.column.format = "d:38,2"; f.buffers[1] = values;
  EXPECT_NE(f.json().find("\"rows\":[[\"-1.23\"],[\"0.00\"],[\"0.01\"]]"), std::string::npos);
}
TEST(SiriusArrow, UnsignedIntegerRetainsAllDigits) {
  StreamFixture f;
  const uint64_t values[] = {UINT64_MAX};
  f.column.format = "L"; f.buffers[1] = values; f.batch.length = 1;
  EXPECT_NE(f.json().find("18446744073709551615"), std::string::npos);
}
TEST(SiriusArrow, EmptyResultPreservesSchema) {
  StreamFixture f; f.next = 1;
  EXPECT_EQ(f.json(), "{\"columns\":[{\"name\":\"value\",\"format\":\"l\"}],\"rows\":[]}");
  EXPECT_EQ(f.batch_releases, 0); EXPECT_EQ(f.stream_releases, 1);
}
TEST(SiriusArrow, RejectsUnsupportedEmptySchema) {
  StreamFixture f; f.next = 1; f.column.format = "tsu:";
  EXPECT_THROW(f.json(), std::runtime_error);
  EXPECT_EQ(f.schema_releases, 1); EXPECT_EQ(f.stream_releases, 1);
}
TEST(SiriusArrow, ResultLimitReleasesBatch) {
  StreamFixture f; char buffer[65];
  EXPECT_THROW(seekdb_sirius::arrow_to_json(f.stream, buffer, sizeof(buffer)), seekdb_sirius::ResultTooLarge);
  EXPECT_EQ(f.batch_releases, 1); EXPECT_EQ(f.schema_releases, 1); EXPECT_EQ(f.stream_releases, 1);
}
TEST(SiriusArrow, StreamFailureReleasesResources) {
  StreamFixture f; f.fail_read = true;
  try { f.json(); FAIL() << "expected producer failure"; }
  catch (const std::runtime_error &e) { EXPECT_STREQ(e.what(), "producer failure"); }
  EXPECT_EQ(f.schema_releases, 1); EXPECT_EQ(f.stream_releases, 1);
}
TEST(SiriusArrow, RejectsNonFiniteFloat) {
  StreamFixture f; const double values[] = {std::numeric_limits<double>::infinity()};
  f.column.format = "g"; f.buffers[1] = values; f.batch.length = 1;
  EXPECT_THROW(f.json(), std::runtime_error); EXPECT_EQ(f.batch_releases, 1);
}

int invoke(oceanbase::sql::SiriusClient &client, std::string *message = nullptr, bool valid = true) {
  const uint8_t plan[] = {1, 0, static_cast<uint8_t>(valid ? 2 : 3)};
  char output[128], error[512]; size_t size = 999;
  const int status = client.execute(plan, sizeof(plan), output, sizeof(output), &size, error, sizeof(error));
  if (message) { *message = error; }
  if (status == 0) { EXPECT_EQ(std::string(output, size), "{\"columns\":[],\"rows\":[[]]}"); }
  else { EXPECT_EQ(size, 0u); }
  return status;
}
TEST(SiriusLoader, DisabledByDefault) {
  unsetenv("SEEKDB_SIRIUS_LIBRARY"); oceanbase::sql::SiriusClient client; std::string error;
  EXPECT_EQ(invoke(client, &error), SEEKDB_SIRIUS_UNAVAILABLE);
  EXPECT_NE(error.find("disabled"), std::string::npos);
}
TEST(SiriusLoader, RejectsRelativeLibraryPath) {
  setenv("SEEKDB_SIRIUS_LIBRARY", "./untrusted.so", 1);
  oceanbase::sql::SiriusClient client; std::string error;
  EXPECT_EQ(invoke(client, &error), SEEKDB_SIRIUS_UNAVAILABLE);
  EXPECT_NE(error.find("absolute"), std::string::npos);
}
TEST(SiriusLoader, RejectsWrongAbi) {
  setenv("SEEKDB_SIRIUS_LIBRARY", WRONG_BRIDGE_PATH, 1);
  oceanbase::sql::SiriusClient client; std::string error;
  EXPECT_EQ(invoke(client, &error), SEEKDB_SIRIUS_UNAVAILABLE);
  EXPECT_NE(error.find("ABI"), std::string::npos);
}
TEST(SiriusLoader, BinaryPlanAndConcurrentCalls) {
  setenv("SEEKDB_SIRIUS_LIBRARY", FAKE_BRIDGE_PATH, 1);
  oceanbase::sql::SiriusClient client;
  auto a = std::async(std::launch::async, [&] { return invoke(client); });
  auto b = std::async(std::launch::async, [&] { return invoke(client); });
  EXPECT_EQ(a.get(), SEEKDB_SIRIUS_OK); EXPECT_EQ(b.get(), SEEKDB_SIRIUS_OK);
}
TEST(SiriusLoader, ForwardsBackendFailure) {
  setenv("SEEKDB_SIRIUS_LIBRARY", FAKE_BRIDGE_PATH, 1);
  oceanbase::sql::SiriusClient client; std::string error;
  EXPECT_EQ(invoke(client, &error, false), SEEKDB_SIRIUS_EXECUTION_ERROR);
  EXPECT_EQ(error, "invalid test plan");
}
}
