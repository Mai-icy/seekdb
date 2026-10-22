// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "sirius_bridge_api.h"
#include <mutex>

namespace oceanbase { namespace sql {

// Optional runtime backend. Loading is deferred until the first invocation.
// The module stays loaded for process lifetime: its CUDA context and callbacks
// must not be unloaded while another query is using them.
class SiriusClient {
public:
  SiriusClient() = default;
  SiriusClient(const SiriusClient &) = delete;
  SiriusClient &operator=(const SiriusClient &) = delete;
  int execute(const uint8_t *plan, size_t plan_size,
              char *output, size_t output_capacity, size_t *output_size,
              char *error, size_t error_capacity);
private:
  void load();
  std::once_flag once_;
  void *handle_ = nullptr;
  const SeekdbSiriusApiV1 *api_ = nullptr;
  char load_error_[512] = {};
};

} }
