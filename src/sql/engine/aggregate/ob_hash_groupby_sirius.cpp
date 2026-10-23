// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/sirius/sirius_aggregate_client.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <cstdio>

namespace oceanbase { namespace sql {
using namespace common;
struct SiriusAggregateState {
  SiriusAggregateClient client;
  ObExpr *inputs[64] = {};
  ObExpr *outputs[128] = {};
  int output_map[128] = {};
  int groups = 0, sums = 0, output_count = 0;
  bool count = false, loaded = false;
  char *input = nullptr, *result = nullptr;
  size_t input_capacity = 0;
  uint32_t buffered = 0, result_rows = 0, position = 0;
  uint32_t batch_limit = SiriusAggregateClient::BATCH_ROWS;
  size_t mask_bytes = 0, column_bytes = 0;
  uint64_t input_rows = 0;
  int cancel_ret = OB_SUCCESS;
  void clear_masks() {
    for (int i = 0; i < groups + sums; ++i) {
      std::memset(input + i * column_bytes, 0xff, mask_bytes);
    }
    buffered = 0;
  }
};

void ObHashGroupByOp::reset_sirius() {
  sirius_checked_ = sirius_active_ = false;
  if (sirius_) {
    sirius_->client.close();
    sirius_->loaded = false;
    sirius_->buffered = sirius_->result_rows = sirius_->position = 0;
    sirius_->input_rows = 0;
    sirius_->cancel_ret = OB_SUCCESS;
  }
}
void ObHashGroupByOp::destroy_sirius() {
  if (sirius_) {
    ctx_.get_allocator().free(sirius_->input);
    ctx_.get_allocator().free(sirius_->result);
    sirius_->~SiriusAggregateState();
    ctx_.get_allocator().free(sirius_);
    sirius_ = nullptr;
  }
}
int ObHashGroupByOp::init_sirius() {
  int ret = OB_SUCCESS;
  const auto &spec = static_cast<const ObHashGroupBySpec &>(spec_);
  const char *enabled = std::getenv("SEEKDB_SIRIUS_NATIVE");
  const auto *session = ctx_.get_my_session();
  const ObObj *setting = session ? session->get_user_variable_value(ObString("sirius_gpu")) : nullptr;
  // Restrict to a complete, serial, read-only aggregation directly over a scan.
  // In particular, never recursively open two streams for nested aggregates.
  if (!enabled || std::strcmp(enabled, "1") || !setting || !setting->is_int()
      || setting->get_int() != 1 || !spec.is_vectorized() || !spec.get_phy_plan()->is_select_plan()
      || spec.get_phy_plan()->get_px_dop() > 1 || !child_
      || child_->get_spec().type_ != PHY_TABLE_SCAN
      || spec.aggr_stage_ != ObThreeStageAggrStage::NONE_STAGE
      || spec.group_exprs_.count() < 1 || spec.group_exprs_.count() > 8
      || spec.aggr_infos_.count() > 64 || !spec.org_dup_cols_.empty()
      || !spec.new_dup_cols_.empty() || !spec.distinct_exprs_.empty()) return ret;
  SiriusAggregateState shape;
  shape.groups = spec.group_exprs_.count();
  for (int i = 0; i < shape.groups; ++i) {
    auto *expr = spec.group_exprs_.at(i);
    if (expr->datum_meta_.type_ != ObIntType || expr->type_ != T_REF_COLUMN) return ret;
    shape.inputs[i] = shape.outputs[i] = expr;
    shape.output_map[i] = i;
  }
  shape.output_count = shape.groups;
  for (int64_t i = 0; i < spec.aggr_infos_.count(); ++i) {
    const auto &info = spec.aggr_infos_.at(i);
    if (!info.expr_ || info.has_distinct_ || info.has_order_by_ || info.is_implicit_first_aggr_) return ret;
    if (info.get_expr_type() == T_FUN_SUM && info.param_exprs_.count() == 1
        && info.param_exprs_.at(0)->datum_meta_.type_ == ObDoubleType
        && info.param_exprs_.at(0)->type_ == T_REF_COLUMN
        && info.expr_->datum_meta_.type_ == ObDoubleType) {
      if (shape.groups + shape.sums >= 64) return ret;
      shape.inputs[shape.groups + shape.sums] = info.param_exprs_.at(0);
      shape.output_map[shape.output_count] = shape.groups + shape.sums++;
    } else if (info.get_expr_type() == T_FUN_COUNT && info.param_exprs_.empty()
               && info.expr_->datum_meta_.type_ == ObIntType) {
      shape.count = true;
      shape.output_map[shape.output_count] = -1;
    } else return ret;
    shape.outputs[shape.output_count++] = info.expr_;
  }
  if (!sirius_) {
    void *mem = ctx_.get_allocator().alloc(sizeof(SiriusAggregateState));
    if (!mem) return OB_ALLOCATE_MEMORY_FAILED;
    sirius_ = new(mem) SiriusAggregateState();
  }
  auto &s = *sirius_;
  s.groups = shape.groups;
  s.sums = shape.sums;
  s.count = shape.count;
  s.output_count = shape.output_count;
  s.batch_limit = std::min<size_t>(SiriusAggregateClient::BATCH_ROWS,
                                  (16 * 1024 * 1024 - 4) / ((s.groups + s.sums) * 9));
  s.mask_bytes = (s.batch_limit + 7) / 8;
  s.column_bytes = s.mask_bytes + s.batch_limit * 8;
  std::memcpy(s.inputs, shape.inputs, sizeof(s.inputs));
  std::memcpy(s.outputs, shape.outputs, sizeof(s.outputs));
  std::memcpy(s.output_map, shape.output_map, sizeof(s.output_map));
  const size_t bytes = (s.groups + s.sums) * s.column_bytes;
  if (s.input_capacity < bytes) {
    ctx_.get_allocator().free(s.input);
    s.input = static_cast<char *>(ctx_.get_allocator().alloc(bytes));
    s.input_capacity = s.input ? bytes : 0;
  }
  if (!s.result) s.result = static_cast<char *>(ctx_.get_allocator().alloc(SiriusAggregateClient::RESULT_BYTES));
  if (!s.input || !s.result) return OB_ALLOCATE_MEMORY_FAILED;
  s.clear_masks();
  sirius_active_ = true;
  return ret;
}

int ObHashGroupByOp::sirius_get_next_batch(int64_t max_row_cnt) {
  int ret = OB_SUCCESS;
  auto &s = *sirius_;
  const auto &spec = static_cast<const ObHashGroupBySpec &>(spec_);
  const int columns = s.groups + s.sums;
  const int result_columns = columns + (s.count ? 1 : 0);
  if (!s.loaded) {
    const char *profile_env = std::getenv("SEEKDB_SIRIUS_PROFILE");
    const bool profile = profile_env && std::strcmp(profile_env, "1") == 0;
    auto clock_us = [profile]() -> int64_t {
      return profile ? std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count() : 0;
    };
    const int64_t started_us = clock_us();
    int64_t scan_us = 0, pack_us = 0, send_us = 0, batches = 0;
    auto cancelled = [](void *context) {
      auto *state = static_cast<SiriusAggregateState *>(context);
      state->cancel_ret = THIS_WORKER.check_status();
      return state->cancel_ret != OB_SUCCESS;
    };
    if (!s.client.open(s.groups, s.sums, s.count, cancelled, &s)) {
      ret = OB_ERR_UNEXPECTED;
    }
    const int64_t open_us = clock_us() - started_us;
    bool ended = false;
    while (OB_SUCC(ret) && !ended) {
      const ObBatchRows *batch = nullptr;
      clear_evaluated_flag();
      const int64_t scan_start = clock_us();
      if (OB_FAIL(THIS_WORKER.check_status())) {
      } else if (OB_FAIL(child_->get_next_batch(spec.max_batch_size_, batch))) {
      } else {
        scan_us += clock_us() - scan_start;
        const int64_t pack_start = clock_us(), send_before = send_us;
        ended = batch->end_;
        const bool all_active = batch->skip_->is_all_false(batch->size_);
        for (int c = 0; OB_SUCC(ret) && c < columns; ++c) {
          if (OB_FAIL(s.inputs[c]->eval_batch(eval_ctx_, *batch->skip_, batch->size_))) {}
        }
        for (int64_t start = 0; OB_SUCC(ret) && start < batch->size_;) {
          const int64_t stop = std::min<int64_t>(batch->size_, start + s.batch_limit - s.buffered);
          uint32_t position = s.buffered;
          for (int c = 0; c < columns; ++c) {
            position = s.buffered;
            char *mask = s.input + c * s.column_bytes;
            char *values = mask + s.mask_bytes;
            const auto datums = s.inputs[c]->locate_expr_datumvector(eval_ctx_);
            for (int64_t i = start; i < stop; ++i) {
              if (!all_active && batch->skip_->at(i)) continue;
              const auto *datum = datums.at(i);
              uint64_t value = 0;
              if (datum->is_null()) mask[position / 8] &= ~(1u << (position % 8));
              // Eligibility guarantees an 8-byte BIGINT or DOUBLE payload.
              // Preserve its bits without a per-row type branch/conversion.
              else std::memcpy(&value, datum->ptr_, 8);
              std::memcpy(values + position * 8, &value, 8);
              ++position;
            }
          }
          s.input_rows += position - s.buffered;
          s.buffered = position;
          if (s.buffered == s.batch_limit) {
            const int64_t send_start = clock_us();
            if (!s.client.batch(s.input, s.buffered, s.batch_limit)) ret = OB_ERR_UNEXPECTED;
            send_us += clock_us() - send_start;
            ++batches;
            s.clear_masks();
          }
          start = stop;
        }
        pack_us += clock_us() - pack_start - (send_us - send_before);
      }
    }
    size_t bytes = 0;
    if (OB_SUCC(ret) && s.buffered) {
      const int64_t send_start = clock_us();
      if (!s.client.batch(s.input, s.buffered, s.batch_limit)) ret = OB_ERR_UNEXPECTED;
      send_us += clock_us() - send_start;
      ++batches;
    }
    const int64_t finish_start = clock_us();
    if (OB_SUCC(ret) && !s.client.finish(s.result, SiriusAggregateClient::RESULT_BYTES, bytes)) ret = OB_ERR_UNEXPECTED;
    const int64_t finish_us = clock_us() - finish_start;
    s.client.close();
    if (profile) {
      const int64_t total_us = clock_us() - started_us;
      std::fprintf(stderr, "seekdb Sirius profile: ret=%d rows=%llu columns=%d batches=%lld open_us=%lld scan_us=%lld pack_us=%lld send_us=%lld finish_us=%lld total_us=%lld\n",
          ret, static_cast<unsigned long long>(s.input_rows), columns, static_cast<long long>(batches),
          static_cast<long long>(open_us), static_cast<long long>(scan_us), static_cast<long long>(pack_us),
          static_cast<long long>(send_us), static_cast<long long>(finish_us), static_cast<long long>(total_us));
    }
    if (OB_SUCC(ret)) {
      if (bytes < 4) ret = OB_ERR_UNEXPECTED;
      else {
        std::memcpy(&s.result_rows, s.result, 4);
        if (bytes != 4 + static_cast<uint64_t>(s.result_rows) * result_columns * 9) ret = OB_ERR_UNEXPECTED;
        for (size_t i = 4; OB_SUCC(ret) && i < bytes; i += 9) {
          if (s.result[i] != 0 && s.result[i] != 1) ret = OB_ERR_UNEXPECTED;
        }
      }
      s.loaded = OB_SUCC(ret);
    }
    if (OB_FAIL(ret)) {
      if (s.cancel_ret != OB_SUCCESS) ret = s.cancel_ret;
      LOG_WARN("native Sirius aggregate failed", K(ret), "reason", s.client.error(), K(s.input_rows));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "native Sirius aggregate failed; inspect server log");
      return ret;
    }
    LOG_INFO("native Sirius aggregate completed", K(s.input_rows), K(s.result_rows));
  }
  clear_evaluated_flag();
  brs_.size_ = std::min<int64_t>(std::min(max_row_cnt, spec.max_batch_size_), s.result_rows - s.position);
  brs_.skip_->reset(brs_.size_);
  brs_.end_ = s.position + brs_.size_ == s.result_rows;
  for (int c = 0; c < s.output_count; ++c) {
    auto *expr = s.outputs[c];
    const int column = s.output_map[c] < 0 ? columns : s.output_map[c];
    auto *datums = expr->locate_datums_for_update(eval_ctx_, expr->is_batch_result() ? brs_.size_ : 1);
    for (int64_t i = 0; i < brs_.size_; ++i) {
      const char *value = s.result + 4 + ((s.position + i) * result_columns + column) * 9;
      auto *datum = datums + (expr->is_batch_result() ? i : 0);
      if (!value[0]) datum->set_null();
      else if (column >= s.groups && column < columns) {
        double number;
        std::memcpy(&number, value + 1, 8);
        datum->set_double(number);
      } else {
        int64_t number;
        std::memcpy(&number, value + 1, 8);
        datum->set_int(number);
      }
    }
    expr->set_evaluated_projected(eval_ctx_);
  }
  s.position += brs_.size_;
  return ret;
}
} }
