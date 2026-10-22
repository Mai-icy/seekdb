// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <stdint.h>

// Standard Arrow C Data/Stream ABI, with no dependency on an Arrow C++ runtime.
// https://arrow.apache.org/docs/format/CDataInterface.html
// https://arrow.apache.org/docs/format/CStreamInterface.html
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
struct ArrowSchema {
  const char *format;
  const char *name;
  const char *metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema **children;
  struct ArrowSchema *dictionary;
  void (*release)(struct ArrowSchema *);
  void *private_data;
};
struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void **buffers;
  struct ArrowArray **children;
  struct ArrowArray *dictionary;
  void (*release)(struct ArrowArray *);
  void *private_data;
};
#endif
#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE
struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *);
  int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *);
  const char *(*get_last_error)(struct ArrowArrayStream *);
  void (*release)(struct ArrowArrayStream *);
  void *private_data;
};
#endif
