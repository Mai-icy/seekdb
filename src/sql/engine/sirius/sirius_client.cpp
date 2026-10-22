// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "sirius_client.h"
#include <cstdio>
#include <cstdlib>

#if defined(__linux__) || defined(__APPLE__)
#include <dlfcn.h>
#endif

namespace oceanbase { namespace sql {

void SiriusClient::load()
{
  const char *path = std::getenv("SEEKDB_SIRIUS_LIBRARY");
  if (path == nullptr || path[0] == '\0') {
    std::snprintf(load_error_, sizeof(load_error_), "Sirius is disabled; set SEEKDB_SIRIUS_LIBRARY before starting seekdb");
  } else if (path[0] != '/') {
    std::snprintf(load_error_, sizeof(load_error_), "SEEKDB_SIRIUS_LIBRARY must be an absolute path");
  } else {
#if defined(__linux__) || defined(__APPLE__)
    handle_ = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (handle_ == nullptr) {
      std::snprintf(load_error_, sizeof(load_error_), "Cannot load Sirius bridge: %s", dlerror());
    } else {
      auto get_api = reinterpret_cast<SeekdbSiriusGetApiV1>(dlsym(handle_, "seekdb_sirius_get_api_v1"));
      if (get_api == nullptr) {
        std::snprintf(load_error_, sizeof(load_error_), "Sirius bridge has no seekdb_sirius_get_api_v1 symbol");
      } else {
        const SeekdbSiriusApiV1 *api = get_api();
        if (api == nullptr || api->abi_version != 1 || api->struct_size < sizeof(SeekdbSiriusApiV1)
            || api->execute == nullptr) {
          std::snprintf(load_error_, sizeof(load_error_), "Incompatible Sirius bridge ABI (expected version 1)");
        } else {
          api_ = api;
        }
      }
    }
#else
    std::snprintf(load_error_, sizeof(load_error_), "Sirius runtime loading is unavailable on this platform");
#endif
  }
}

int SiriusClient::execute(const uint8_t *plan, size_t plan_size,
                         char *output, size_t output_capacity, size_t *output_size,
                         char *error, size_t error_capacity)
{
  if (output_size != nullptr) { *output_size = 0; }
  if (error != nullptr && error_capacity > 0) { error[0] = '\0'; }
  if (plan == nullptr || plan_size == 0 || plan_size > 1024 * 1024
      || output == nullptr || output_capacity == 0 || output_size == nullptr
      || error == nullptr || error_capacity == 0) {
    if (error != nullptr && error_capacity > 0) {
      std::snprintf(error, error_capacity, "Expected a nonempty Substrait plan of at most 1 MiB and valid output buffers");
    }
    return SEEKDB_SIRIUS_INVALID_ARGUMENT;
  }
  std::call_once(once_, [this] { load(); });
  if (api_ == nullptr) {
    std::snprintf(error, error_capacity, "%s", load_error_);
    return SEEKDB_SIRIUS_UNAVAILABLE;
  }
  int status = api_->execute(plan, plan_size, output, output_capacity, output_size, error, error_capacity);
  error[error_capacity - 1] = '\0';
  if (status == SEEKDB_SIRIUS_OK && (*output_size == 0 || *output_size > output_capacity)) {
    std::snprintf(error, error_capacity, "Sirius bridge returned an invalid output length");
    status = SEEKDB_SIRIUS_EXECUTION_ERROR;
  }
  if (status != SEEKDB_SIRIUS_OK) { *output_size = 0; }
  return status;
}

} }
