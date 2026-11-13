/*
 * Copyright (c) 2026 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include "sql/executor/ob_execute_result.h"
#include "sql/engine/sirius/sirius_query_transport.h"
#include "lib/allocator/page_arena.h"
#include <vector>

namespace oceanbase { namespace sql {
class ObResultSet;
class GpuQueryResult final : public ObIExecuteResult {
public:
  static int create_if_requested(ObResultSet &result, GpuQueryResult *&out);
  explicit GpuQueryResult(ObResultSet &result, common::ObIAllocator &allocator)
      : result_(result), cells_allocator_(allocator) {}
  ~GpuQueryResult() override;
  int open(ObExecContext &ctx) override;
  int get_next_row(ObExecContext &ctx, const common::ObNewRow *&row) override;
  int close(ObExecContext &ctx) override;
private:
  int export_input(ObExecContext &ctx, const std::string &request);
  int describe_snapshot(ObExecContext &ctx, WireWriter &request);
  void cleanup();
  ObResultSet &result_;
  QueryTransport transport_;
  common::ObArenaAllocator cells_allocator_;
  common::ObNewRow row_;
  std::string directory_;
  std::vector<std::string> files_;
  std::vector<uint64_t> snapshot_tables_;
  std::string response_;
  size_t response_position_ = 0;
  uint32_t rows_remaining_ = 0;
  int cancel_ret_ = common::OB_SUCCESS;
  bool opened_ = false;
  bool done_ = false;
};
} }
