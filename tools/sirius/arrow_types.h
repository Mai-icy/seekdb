// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "arrow_c.h"
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

namespace seekdb_sirius {
struct ArrowType {
  std::string sql;
  int width;
  int precision = 0, scale = 0;
};
inline ArrowType arrow_type(const ArrowSchema &column) {
  if (!column.format || column.dictionary || column.n_children) {
    throw std::invalid_argument("Expected a flat, non-dictionary Arrow column");
  }
  const std::string f(column.format);
  if (f == "b") return {"BOOLEAN", 0};
  if (f == "c") return {"TINYINT", 1};
  if (f == "s") return {"SMALLINT", 2};
  if (f == "i") return {"INTEGER", 4};
  if (f == "l") return {"BIGINT", 8};
  if (f == "f") return {"FLOAT", 4};
  if (f == "g") return {"DOUBLE", 8};
  if (f == "tdD") return {"DATE", 4};
  if (f == "u") return {"VARCHAR", -1};
  int precision = 0, scale = 0, end = 0;
  if (std::sscanf(f.c_str(), "d:%d,%d%n", &precision, &scale, &end) == 2) {
    if (f.substr(end) == ",128" || f.substr(end).empty()) {
      if (precision >= 1 && precision <= 38 && scale >= 0 && scale <= precision) {
        return {"DECIMAL(" + std::to_string(precision) + "," + std::to_string(scale) + ")",
                16, precision, scale};
      }
    }
  }
  throw std::invalid_argument("Unsupported Arrow input format: " + f);
}
inline uint64_t arrow_batch_bytes(const ArrowSchema &schema, const ArrowArray &array) {
  if (!schema.format || std::string(schema.format) != "+s" || schema.dictionary
      || schema.n_children < 1 || schema.n_children > 64 || !schema.children
      || array.n_children != schema.n_children || !array.children || array.offset != 0
      || array.null_count != 0 || array.length < 0 || array.length > 8 * 1024 * 1024) {
    throw std::invalid_argument("Invalid flat Arrow record batch");
  }
  uint64_t bytes = 0;
  for (int64_t i = 0; i < schema.n_children; ++i) {
    const auto *s = schema.children[i];
    const auto *a = array.children[i];
    if (!s || !a || !s->name || a->dictionary || a->n_children || a->offset < 0
        || a->offset > 8 * 1024 * 1024 || a->length != array.length || !a->buffers) {
      throw std::invalid_argument("Invalid Arrow input column");
    }
    const auto type = arrow_type(*s);
    if (a->n_buffers != (type.width == -1 ? 3 : 2)
        || (a->null_count > 0 && !a->buffers[0])
        || (a->length && !a->buffers[1])) {
      throw std::invalid_argument("Invalid Arrow column buffers");
    }
    bytes += (array.length + 7) / 8;
    if (type.width == -1) {
      if (a->length) {
        const auto *offsets = static_cast<const int32_t *>(a->buffers[1]);
        const int64_t begin = offsets[a->offset], end = offsets[a->offset + a->length];
        if (begin < 0 || end < begin || (end > begin && !a->buffers[2])) {
          throw std::invalid_argument("Invalid Arrow UTF-8 offsets");
        }
        bytes += end - begin;
      }
      bytes += (array.length + 1) * sizeof(int32_t);
    } else {
      bytes += type.width ? array.length * type.width : (array.length + 7) / 8;
    }
    if (bytes > 128 * 1024 * 1024) throw std::invalid_argument("Arrow batch exceeds 128 MiB");
  }
  return bytes;
}
} // namespace seekdb_sirius
