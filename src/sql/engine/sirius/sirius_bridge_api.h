// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <stddef.h>
#include <stdint.h>

// No C++/CUDA/Arrow objects cross this boundary. The caller owns every buffer.
#ifdef __cplusplus
extern "C" {
#endif

enum SeekdbSiriusStatus {
  SEEKDB_SIRIUS_OK = 0,
  SEEKDB_SIRIUS_INVALID_ARGUMENT = 1,
  SEEKDB_SIRIUS_UNAVAILABLE = 2,
  SEEKDB_SIRIUS_EXECUTION_ERROR = 3,
  SEEKDB_SIRIUS_RESULT_TOO_LARGE = 4
};

struct SeekdbSiriusApiV1 {
  uint32_t abi_version;
  uint32_t struct_size;
  // plan is binary protobuf, including embedded NUL bytes. On success output
  // contains output_size bytes of UTF-8 JSON (no trailing NUL is required).
  // On failure output_size is zero; error is a NUL-terminated diagnostic.
  // Implementations must catch all exceptions and must be safe for concurrent calls.
  int (*execute)(const uint8_t *plan, size_t plan_size,
                 char *output, size_t output_capacity, size_t *output_size,
                 char *error, size_t error_capacity);
};

typedef const struct SeekdbSiriusApiV1 *(*SeekdbSiriusGetApiV1)(void);

#ifdef __cplusplus
}
#endif
