// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "sql/executor/ob_execute_result.h"
#include "common/row/ob_row_store.h"

namespace oceanbase {
namespace transaction { class ObTxReadSnapshot; }
namespace sql {
class ObResultSet;
// Only set while this SQL worker reads inputs through nested SQL. This preserves
// the outer statement snapshot even at READ COMMITTED isolation.
const transaction::ObTxReadSnapshot *sirius_input_snapshot();

class SiriusQueryResult : public ObIExecuteResult {
public:
  static int create_if_requested(ObResultSet &result, SiriusQueryResult *&out);
  explicit SiriusQueryResult(ObResultSet &result, common::ObIAllocator &allocator)
      : result_(result), rows_(allocator), iterator_() {}
  int open(ObExecContext &ctx) override;
  int get_next_row(ObExecContext &ctx, const common::ObNewRow *&row) override;
  int close(ObExecContext &ctx) override;
private:
  ObResultSet &result_;
  common::ObRowStore rows_;
  common::ObRowStore::Iterator iterator_;
  common::ObNewRow row_;
};
} }
