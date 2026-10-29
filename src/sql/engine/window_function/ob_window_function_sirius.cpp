// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/window_function/ob_window_function_op.h"
#include "sql/engine/sirius/sirius_aggregate_client.h"
#include "sql/engine/sirius/sirius_native_scope.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <vector>

namespace oceanbase { namespace sql {
using namespace common;

// Bounded prototype for whole-partition decimal MIN. seekdb produces and owns
// the rows; GPU reduces exact unscaled integers. Replaying the saved rows keeps
// child order and transaction visibility, without a separate data snapshot.
struct SiriusWindowMinState {
  static constexpr size_t ROW_LIMIT = 262144;
  static constexpr size_t RESULT_LIMIT = 16 * 1024 * 1024;
  using Key = std::pair<bool, int64_t>;
  struct SavedDatum { ObDatum metadata; std::string payload; };
  struct Row { Key key; std::vector<SavedDatum> values; };
  ObExpr *key = nullptr, *value = nullptr, *output = nullptr;
  std::vector<ObExpr *> expressions;
  std::vector<Row> rows;
  std::map<Key, Key> minima;
  SiriusAggregateClient client;
  char *input = nullptr, *result = nullptr;
  size_t saved_bytes = 0, position = 0;
  uint32_t buffered = 0;
  bool loaded = false;
  int cancel_ret = OB_SUCCESS;
  void clear_masks() {
    for (int c = 0; c < 2; ++c) std::memset(input + c * SiriusAggregateClient::COLUMN_BYTES,
        0xff, SiriusAggregateClient::MASK_BYTES);
    buffered = 0;
  }
  static Key partition(const ObDatum &datum) {
    return { !datum.is_null(), datum.is_null() ? 0 : datum.get_int() };
  }
};

void ObWindowFunctionOp::destroy_sirius_min()
{
  sirius_min_checked_ = false;
  if (sirius_min_) {
    ctx_.get_allocator().free(sirius_min_->input);
    ctx_.get_allocator().free(sirius_min_->result);
    sirius_min_->~SiriusWindowMinState();
    ctx_.get_allocator().free(sirius_min_); sirius_min_ = nullptr;
  }
}

int ObWindowFunctionOp::init_sirius_min()
{
  const auto &spec = static_cast<const ObWindowFunctionSpec &>(spec_);
  const char *enabled = std::getenv("SEEKDB_SIRIUS_NATIVE");
  const auto *session = ctx_.get_my_session();
  const ObObj *setting = session ? session->get_user_variable_value(ObString("sirius_gpu")) : nullptr;
  if (!enabled || std::strcmp(enabled, "1") || !setting || !setting->is_int() || setting->get_int() != 1
      || sirius_native_depth || !spec.is_vectorized() || !spec.get_phy_plan()->is_select_plan()
      || spec.get_phy_plan()->get_px_dop() > 1 || !child_ || !sirius_serial_input(child_->get_spec())
      || spec.wf_infos_.count() != 1 || spec.all_expr_.count() > 64
      || spec.single_part_parallel_ || spec.range_dist_parallel_ || spec.is_push_down()
      || spec.get_rows() > SiriusWindowMinState::ROW_LIMIT) return OB_SUCCESS;
  const auto &wf = spec.wf_infos_.at(0);
  const char *profile = std::getenv("SEEKDB_SIRIUS_PROFILE");
  if (profile && !std::strcmp(profile, "1")) {
    std::fprintf(stderr, "Sirius window shape: function=%d partitions=%lld arguments=%lld bounds=%d/%d/%d/%d\n",
        wf.func_type_, static_cast<long long>(wf.partition_exprs_.count()),
        static_cast<long long>(wf.aggr_info_.param_exprs_.count()),
        wf.upper_.is_unbounded_, wf.upper_.is_preceding_, wf.lower_.is_unbounded_, wf.lower_.is_preceding_);
  }
  if (wf.func_type_ != T_FUN_MIN || wf.partition_exprs_.count() != 1
      || wf.aggr_info_.param_exprs_.count() != 1 || wf.aggr_info_.has_distinct_
      || wf.aggr_info_.has_order_by_ || !wf.expr_ || !wf.upper_.is_unbounded_
      || !wf.upper_.is_preceding_ || !wf.lower_.is_unbounded_ || wf.lower_.is_preceding_) return OB_SUCCESS;
  auto *key = wf.partition_exprs_.at(0), *value = wf.aggr_info_.param_exprs_.at(0);
  const auto &in = value->datum_meta_, &out = wf.expr_->datum_meta_;
  if (key->datum_meta_.type_ != ObIntType || in.type_ != ObDecimalIntType || in.precision_ < 1
      || in.precision_ > 18 || out.type_ != in.type_ || out.precision_ != in.precision_
      || out.scale_ != in.scale_) return OB_SUCCESS;
  for (int64_t c = 0; c < spec.all_expr_.count(); ++c) {
    const auto type = spec.all_expr_.at(c)->datum_meta_.type_;
    if (!ob_is_integer_type(type) && type != ObDecimalIntType && type != ObNumberType
        && type != ObVarcharType && type != ObCharType && type != ObDateType
        && type != ObDateTimeType && type != ObTimestampType && type != ObDoubleType
        && type != ObMySQLDateType && type != ObMySQLDateTimeType) return OB_SUCCESS;
  }
  void *memory = ctx_.get_allocator().alloc(sizeof(SiriusWindowMinState));
  if (!memory) return OB_ALLOCATE_MEMORY_FAILED;
  auto &s = *(sirius_min_ = new(memory) SiriusWindowMinState());
  s.key = key; s.value = value; s.output = wf.expr_;
  try {
    for (int64_t c = 0; c < spec.all_expr_.count(); ++c) s.expressions.push_back(spec.all_expr_.at(c));
  } catch (const std::bad_alloc &) { return OB_ALLOCATE_MEMORY_FAILED; }
  s.input = static_cast<char *>(ctx_.get_allocator().alloc(2 * SiriusAggregateClient::COLUMN_BYTES));
  s.result = static_cast<char *>(ctx_.get_allocator().alloc(SiriusWindowMinState::RESULT_LIMIT));
  if (!s.input || !s.result) return OB_ALLOCATE_MEMORY_FAILED;
  s.clear_masks();
  return OB_SUCCESS;
}

int ObWindowFunctionOp::sirius_min_get_next_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  auto &s = *sirius_min_;
  if (!s.loaded) {
    SiriusNativeScope scope;
    auto cancelled = [](void *context) {
      auto *state = static_cast<SiriusWindowMinState *>(context);
      state->cancel_ret = THIS_WORKER.check_status(); return state->cancel_ret != OB_SUCCESS;
    };
    if (!s.client.open_grouped_integer_min(1, 1, cancelled, &s)) ret = OB_ERR_UNEXPECTED;
    bool ended = false;
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    try {
      while (OB_SUCC(ret) && !ended) {
        clear_evaluated_flag();
        const ObBatchRows *batch = nullptr;
        if (OB_FAIL(THIS_WORKER.check_status())) {
        } else if (OB_FAIL(child_->get_next_batch(spec_.max_batch_size_, batch))) {
        } else {
          ended = batch->end_; guard.set_batch_size(batch->size_);
          for (auto *expr : s.expressions) {
            if (OB_FAIL(expr->eval_batch(eval_ctx_, *batch->skip_, batch->size_))) break;
          }
          if (OB_SUCC(ret) && OB_FAIL(s.key->eval_batch(eval_ctx_, *batch->skip_, batch->size_))) {}
          if (OB_SUCC(ret) && OB_FAIL(s.value->eval_batch(eval_ctx_, *batch->skip_, batch->size_))) {}
          for (int64_t row = 0; OB_SUCC(ret) && row < batch->size_; ++row) {
            if (batch->skip_->at(row)) continue;
            if (s.rows.size() >= SiriusWindowMinState::ROW_LIMIT) { ret = OB_SIZE_OVERFLOW; break; }
            SiriusWindowMinState::Row saved;
            saved.key = SiriusWindowMinState::partition(*s.key->locate_expr_datumvector(eval_ctx_).at(row));
            saved.values.resize(s.expressions.size());
            s.saved_bytes += sizeof(saved) + saved.values.size() * sizeof(SiriusWindowMinState::SavedDatum);
            for (size_t c = 0; c < s.expressions.size(); ++c) {
              const auto *datum = s.expressions[c]->locate_expr_datumvector(eval_ctx_).at(row);
              if (datum->len_ > 4096) { ret = OB_SIZE_OVERFLOW; break; }
              saved.values[c].metadata = *datum;
              if (!datum->is_null()) saved.values[c].payload.assign(datum->ptr_, datum->len_);
              s.saved_bytes += saved.values[c].payload.size();
            }
            if (s.saved_bytes > 256 * 1024 * 1024) ret = OB_SIZE_OVERFLOW;
            if (OB_FAIL(ret)) break;
            s.rows.push_back(std::move(saved));
            const auto *datum = s.value->locate_expr_datumvector(eval_ctx_).at(row);
            int64_t value = 0;
            if (!datum->is_null()) {
              const int len = datum->get_int_bytes();
              if (len != 4 && len != 8) { ret = OB_ERR_UNEXPECTED; break; }
              std::memset(&value, static_cast<unsigned char>(datum->ptr_[len - 1]) & 0x80 ? 0xff : 0, 8);
              std::memcpy(&value, datum->ptr_, len);
            }
            for (int c = 0; c < 2; ++c) {
              const bool valid = c == 0 ? s.rows.back().key.first : !datum->is_null();
              const int64_t integer = c == 0 ? s.rows.back().key.second : value;
              char *column = s.input + c * SiriusAggregateClient::COLUMN_BYTES;
              if (!valid) column[s.buffered / 8] &= ~(1u << (s.buffered % 8));
              std::memcpy(column + SiriusAggregateClient::MASK_BYTES + s.buffered * 8, &integer, 8);
            }
            if (++s.buffered == SiriusAggregateClient::BATCH_ROWS) {
              if (!s.client.batch(s.input, s.buffered)) ret = OB_ERR_UNEXPECTED;
              s.clear_masks();
            }
          }
        }
      }
      if (OB_SUCC(ret) && s.buffered && !s.client.batch(s.input, s.buffered)) ret = OB_ERR_UNEXPECTED;
      size_t bytes = 0; uint32_t count = 0;
      if (OB_SUCC(ret) && !s.client.finish(s.result, SiriusWindowMinState::RESULT_LIMIT, bytes)) ret = OB_ERR_UNEXPECTED;
      if (OB_SUCC(ret)) {
        if (bytes < 4) ret = OB_ERR_UNEXPECTED;
        else {
          std::memcpy(&count, s.result, 4);
          if (count > s.rows.size() || bytes != 4 + static_cast<uint64_t>(count) * 34) ret = OB_ERR_UNEXPECTED;
          for (uint32_t row = 0; OB_SUCC(ret) && row < count; ++row) {
            SiriusWindowMinState::Key cells[2];
            for (int c = 0; c < 2; ++c) {
              const char *cell = s.result + 4 + row * 34 + c * 17;
              int128_t value = 0; std::memcpy(&value, cell + 1, 16);
              if ((cell[0] != 0 && cell[0] != 1) || (cell[0] && (value < INT64_MIN || value > INT64_MAX))) {
                ret = OB_ERR_UNEXPECTED; break;
              }
              cells[c] = {cell[0] != 0, cell[0] ? static_cast<int64_t>(value) : 0};
            }
            if (OB_SUCC(ret) && !s.minima.emplace(cells[0], cells[1]).second) ret = OB_ERR_UNEXPECTED;
          }
        }
      }
    } catch (const std::bad_alloc &) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    s.client.close();
    if (OB_FAIL(ret)) {
      if (s.cancel_ret != OB_SUCCESS) ret = s.cancel_ret;
      LOG_WARN("native Sirius window minimum failed", K(ret), "reason", s.client.error()); return ret;
    }
    s.loaded = true;
    std::fprintf(stderr, "seekdb Sirius native window minimum: rows=%zu partitions=%zu\n", s.rows.size(), s.minima.size());
  }
  clear_evaluated_flag();
  brs_.size_ = std::min<int64_t>(std::min(max_row_cnt, spec_.max_batch_size_), s.rows.size() - s.position);
  brs_.skip_->reset(brs_.size_); brs_.end_ = s.position + brs_.size_ == s.rows.size();
  for (size_t c = 0; c <= s.expressions.size(); ++c) {
    auto *expr = c == s.expressions.size() ? s.output : s.expressions[c];
    auto *datums = expr->locate_datums_for_update(eval_ctx_, expr->is_batch_result() ? brs_.size_ : 1);
    for (int64_t row = 0; OB_SUCC(ret) && row < brs_.size_; ++row) {
      auto &datum = datums[expr->is_batch_result() ? row : 0];
      const auto &saved = s.rows[s.position + row];
      if (c < s.expressions.size()) {
        datum = saved.values[c].metadata;
        if (!datum.is_null()) datum.ptr_ = saved.values[c].payload.data();
      } else {
        const auto minimum = s.minima.find(saved.key);
        if (minimum == s.minima.end()) ret = OB_ERR_UNEXPECTED;
        else if (!minimum->second.first) datum.set_null();
        else if (expr->datum_meta_.precision_ <= 9) datum.set_decimal_int(static_cast<int32_t>(minimum->second.second));
        else datum.set_decimal_int(minimum->second.second);
      }
    }
    expr->set_evaluated_projected(eval_ctx_);
  }
  s.position += brs_.size_;
  return ret;
}
} }
