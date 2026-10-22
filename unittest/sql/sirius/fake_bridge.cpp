// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "sirius_bridge_api.h"
#include <cstdio>
#include <cstring>
namespace {
int execute(const uint8_t *plan, size_t size, char *output, size_t capacity, size_t *written,
            char *error, size_t error_capacity) {
  *written = 0;
  if (size != 3 || plan[0] != 1 || plan[1] != 0 || plan[2] != 2) {
    std::snprintf(error, error_capacity, "invalid test plan"); return SEEKDB_SIRIUS_EXECUTION_ERROR;
  }
  const char result[] = "{\"columns\":[],\"rows\":[[]]}";
  if (capacity < sizeof(result) - 1) { return SEEKDB_SIRIUS_RESULT_TOO_LARGE; }
  *written = sizeof(result) - 1;
  std::memcpy(output, result, *written);
  return SEEKDB_SIRIUS_OK;
}
#ifdef WRONG_ABI
const SeekdbSiriusApiV1 api{2, sizeof(SeekdbSiriusApiV1), execute};
#else
const SeekdbSiriusApiV1 api{1, sizeof(SeekdbSiriusApiV1), execute};
#endif
}
extern "C" const SeekdbSiriusApiV1 *seekdb_sirius_get_api_v1() { return &api; }
