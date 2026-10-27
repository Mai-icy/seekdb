// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "arrow_c.h"
#include <stddef.h>

// Experimental in-process ABI. Inputs are consumed after argument validation,
// on success and failure. The callback must drain/release the result before it
// returns; it runs on the engine thread and must not reenter the bridge.
struct SeekdbSiriusTableV1 {
  const char *name;
  ArrowArrayStream *stream;
  uint64_t estimated_rows;
};
using SeekdbSiriusResultV1 = int (*)(ArrowArrayStream *, void *);
extern "C" int seekdb_sirius_execute_query_v1(
    const char *sql, size_t sql_size, SeekdbSiriusTableV1 *tables, size_t count,
    SeekdbSiriusResultV1 consume, void *user, char *error, size_t error_capacity) noexcept;
