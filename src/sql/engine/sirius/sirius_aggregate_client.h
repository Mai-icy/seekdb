// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cstddef>
#include <cstdint>

namespace oceanbase { namespace sql {
// Bounded, CPU-only transport. Buffers belong to the query allocator.
class SiriusAggregateClient {
public:
  static constexpr size_t BATCH_ROWS = 262144;
  static constexpr size_t MAX_BATCH_ROWS = 8 * 1024 * 1024;
  static constexpr size_t MASK_BYTES = BATCH_ROWS / 8;
  static constexpr size_t COLUMN_BYTES = MASK_BYTES + BATCH_ROWS * 8;
  static constexpr size_t RESULT_BYTES = 1024 * 1024;
  static size_t input_frame_bytes();
  static size_t batch_row_limit();
  ~SiriusAggregateClient() { close(); }
  bool open(uint8_t groups, uint8_t sums, bool count, bool (*cancel)(void *), void *context);
  // V3: scalar SUM over signed int64 columns, exact signed-128 result cells.
  bool open_integer_sums(uint8_t sums, bool (*cancel)(void *), void *context);
  bool batch(const char *columns, uint32_t rows, uint32_t capacity_rows = BATCH_ROWS);
  bool finish(char *result, size_t capacity, size_t &size);
  void close();
  const char *error() const { return error_; }
private:
  bool open_impl(uint8_t groups, uint8_t sums, bool count, bool exact,
                 bool (*cancel)(void *), void *context);
  bool transfer(void *data, size_t size, bool write);
  bool frame(char kind, uint32_t size);
  bool response(char expected, char *data, size_t capacity, size_t &size);
  bool fail(const char *message);
  int fd_ = -1;
  uint8_t columns_ = 0;
  size_t frame_limit_ = 16 * 1024 * 1024;
  int64_t deadline_ms_ = 0;
  bool (*cancel_)(void *) = nullptr;
  void *context_ = nullptr;
  char error_[256] = {};
};
} }
