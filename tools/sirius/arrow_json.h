// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "arrow_c.h"
#include <cstddef>
#include <stdexcept>

namespace seekdb_sirius {
class ResultTooLarge : public std::runtime_error {
public:
  ResultTooLarge() : std::runtime_error("Sirius JSON result exceeds the output limit; reduce the result with aggregation or LIMIT") {}
};
// Consumes/releases the stream, schema and batches on both success and failure.
// Output: {"columns":[{"name":...,"format":...}],"rows":[[...],...]}
// Decimal values are strings, retaining precision and scale without JSON rounding.
size_t arrow_to_json(ArrowArrayStream &stream, char *output, size_t capacity);
}
