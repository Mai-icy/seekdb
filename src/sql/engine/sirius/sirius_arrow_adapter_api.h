// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Per-scan host input for a serialized Substrait plan. A named-table read uses
// sirius_stream_<id>. The host owns the transaction and must expose only visible
// rows. Independent scans/self joins need separate streams and distinct ids.
// All sources in one invocation must belong to the same host snapshot.
struct SeekdbSiriusArrowInputV1 {
  uint32_t struct_size;
  uint64_t id;
  uintptr_t stream; // address of ArrowArrayStream, moved after argument validation
  const char *snapshot_identity; // nonempty, opaque host transaction identity
  uint64_t estimated_rows; // UINT64_MAX when unknown
  uint64_t max_batch_rows;
  // Bounds COMPLETE retained backing, including a sliced batch's parents and
  // schema/control allocations. Host must enforce this before publishing.
  uint64_t max_batch_bytes;
  void *cancel_state;
  // Nonthrowing/thread-neutral; unblock get_next without freeing its backing.
  void (*cancel)(void *);
};

// Zero on success. Result is an ArrowArrayStream released by the caller.
// No C++ objects or database-specific types cross this ABI. Source callbacks
// run on dedicated threads; all releases must be thread-neutral. On failure no
// result is published; a partially consumed source is never retried on CPU.
int seekdb_sirius_execute_inputs_v1(
    const uint8_t *plan, size_t plan_size,
    struct SeekdbSiriusArrowInputV1 *inputs, size_t input_count,
    uintptr_t output_stream, char *error, size_t error_capacity);
int seekdb_sirius_adapter_shutdown_v1(char *error, size_t error_capacity);

#ifdef __cplusplus
}
#endif
