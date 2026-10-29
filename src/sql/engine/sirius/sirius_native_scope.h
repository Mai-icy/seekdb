// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "sql/engine/ob_operator.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase { namespace sql {
// The sidecar owns one stream at a time. While an outer native aggregate pulls
// serial child batches, nested aggregates must execute on CPU to avoid opening
// a second socket whose ACK waits on the outer stream. No PX/exchange or
// subplan-filter child is admitted by the whitelist below.
inline thread_local unsigned sirius_native_depth = 0;
struct SiriusNativeScope {
  SiriusNativeScope() { ++sirius_native_depth; }
  ~SiriusNativeScope() { --sirius_native_depth; }
  SiriusNativeScope(const SiriusNativeScope &) = delete;
  SiriusNativeScope &operator=(const SiriusNativeScope &) = delete;
};

inline bool sirius_serial_input(const ObOpSpec &spec, unsigned depth = 0)
{
  if (depth > 64 || !spec.is_vectorized()) return false;
  switch (spec.type_) {
    case PHY_TABLE_SCAN: {
      const auto &scan = static_cast<const ObTableScanSpec &>(spec);
      const auto &pushdown = scan.tsc_ctdef_.scan_ctdef_.pd_expr_spec_.pd_storage_flag_;
      return !pushdown.is_aggregate_pushdown() && !pushdown.is_group_by_pushdown();
    }
    case PHY_HASH_JOIN: case PHY_MERGE_JOIN: case PHY_NESTED_LOOP_JOIN:
    case PHY_SORT: case PHY_LIMIT: case PHY_MATERIAL: case PHY_SUBPLAN_SCAN:
    case PHY_WINDOW_FUNCTION: case PHY_HASH_DISTINCT: case PHY_MERGE_DISTINCT:
    case PHY_HASH_GROUP_BY: case PHY_MERGE_GROUP_BY: case PHY_SCALAR_AGGREGATE:
      break;
    default: return false;
  }
  for (uint32_t i = 0; i < spec.get_child_cnt(); ++i) {
    if (!spec.get_child(i) || !sirius_serial_input(*spec.get_child(i), depth + 1)) return false;
  }
  return spec.get_child_cnt() > 0;
}
} }
