// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/aggregate/ob_scalar_aggregate_op.h"
#include "sql/engine/sirius/sirius_aggregate_client.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/session/ob_sql_session_info.h"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace oceanbase { namespace sql {
using namespace common;

struct SiriusScalarState {
  SiriusAggregateClient client;
  ObExpr *inputs[16] = {}, *outputs[16] = {};
  int sums = 0, columns = 0;
  int limbs[16] = {}, offsets[16] = {};
  char *input = nullptr;
  size_t capacity = 0, mask_bytes = 0, column_bytes = 0;
  uint32_t limit = 0, buffered = 0;
  uint64_t rows = 0;
  int cancel_ret = OB_SUCCESS;
  bool emitted = false;
  void clear() {
    for (int i = 0; i < columns; ++i) std::memset(input + i * column_bytes, 0xff, mask_bytes);
    buffered = 0;
  }
};

void ObScalarAggregateOp::reset_sirius()
{
  sirius_checked_ = sirius_active_ = false;
  if (sirius_) {
    sirius_->client.close();
    sirius_->buffered = 0;
    sirius_->rows = 0;
    sirius_->emitted = false;
    sirius_->cancel_ret = OB_SUCCESS;
  }
}

void ObScalarAggregateOp::destroy_sirius()
{
  if (sirius_) {
    ctx_.get_allocator().free(sirius_->input);
    sirius_->~SiriusScalarState();
    ctx_.get_allocator().free(sirius_);
    sirius_ = nullptr;
  }
}

int ObScalarAggregateOp::init_sirius()
{
  const auto &spec = static_cast<const ObScalarAggregateSpec &>(spec_);
  const char *enabled = std::getenv("SEEKDB_SIRIUS_NATIVE");
  const auto *session = ctx_.get_my_session();
  const ObObj *setting = session ? session->get_user_variable_value(ObString("sirius_gpu")) : nullptr;
  if (!enabled || std::strcmp(enabled, "1") || !setting || !setting->is_int() || setting->get_int() != 1
      || !spec.is_vectorized() || !spec.get_phy_plan()->is_select_plan()
      || spec.get_phy_plan()->get_px_dop() > 1 || !child_ || child_->get_spec().type_ != PHY_TABLE_SCAN
      || spec.aggr_stage_ != ObThreeStageAggrStage::NONE_STAGE
      || spec.aggr_infos_.empty() || spec.aggr_infos_.count() > 16) return OB_SUCCESS;
  const auto &scan = static_cast<const ObTableScanSpec &>(child_->get_spec());
  const auto &pushdown = scan.tsc_ctdef_.scan_ctdef_.pd_expr_spec_.pd_storage_flag_;
  if (pushdown.is_aggregate_pushdown() || pushdown.is_group_by_pushdown()) return OB_SUCCESS;
  // Eligibility is decided before opening transport or consuming the child.
  // seekdb evaluates the original SUM argument (including Q6 multiplication),
  // retains its scale and owns the scan's filters and transaction snapshot.
  for (int64_t i = 0; i < spec.aggr_infos_.count(); ++i) {
    const auto &info = spec.aggr_infos_.at(i);
    if (!info.expr_ || info.get_expr_type() != T_FUN_SUM || info.param_exprs_.count() != 1
        || info.has_distinct_ || info.has_order_by_ || info.is_implicit_first_aggr_) return OB_SUCCESS;
    const auto &input = info.param_exprs_.at(0)->datum_meta_;
    const auto &output = info.expr_->datum_meta_;
    if (input.type_ != ObDecimalIntType || input.precision_ < 1 || input.precision_ > 38
        || output.type_ != ObDecimalIntType || output.precision_ < input.precision_ || output.precision_ > 76
        || output.scale_ != input.scale_) return OB_SUCCESS;
  }
  if (!sirius_) {
    void *mem = ctx_.get_allocator().alloc(sizeof(SiriusScalarState));
    if (!mem) return OB_ALLOCATE_MEMORY_FAILED;
    sirius_ = new(mem) SiriusScalarState();
  }
  auto &s = *sirius_;
  s.sums = spec.aggr_infos_.count();
  s.columns = 0;
  for (int i = 0; i < s.sums; ++i) {
    s.inputs[i] = spec.aggr_infos_.at(i).param_exprs_.at(0);
    s.outputs[i] = spec.aggr_infos_.at(i).expr_;
    const int precision = s.inputs[i]->datum_meta_.precision_;
    s.limbs[i] = precision <= 9 ? 1 : precision <= 18 ? 2 : 4;
    s.offsets[i] = s.columns;
    s.columns += s.limbs[i];
  }
  s.limit = std::min<size_t>(SiriusAggregateClient::batch_row_limit(),
      (SiriusAggregateClient::input_frame_bytes() - 4) / (s.columns * 9));
  s.mask_bytes = (s.limit + 7) / 8;
  s.column_bytes = s.mask_bytes + s.limit * 8;
  const size_t bytes = s.columns * s.column_bytes;
  if (s.capacity < bytes) {
    ctx_.get_allocator().free(s.input);
    s.input = static_cast<char *>(ctx_.get_allocator().alloc(bytes));
    s.capacity = s.input ? bytes : 0;
  }
  if (!s.input) return OB_ALLOCATE_MEMORY_FAILED;
  s.clear();
  sirius_active_ = true;
  return OB_SUCCESS;
}

int ObScalarAggregateOp::sirius_get_next_batch()
{
  int ret = OB_SUCCESS;
  auto &s = *sirius_;
  if (s.emitted) { brs_.size_ = 0; brs_.end_ = true; return ret; }
  const auto started = std::chrono::steady_clock::now();
  const char *profile_setting = std::getenv("SEEKDB_SIRIUS_PROFILE");
  const bool profile = profile_setting && std::strcmp(profile_setting, "1") == 0;
  auto clock_us = [profile]() -> int64_t {
    return profile ? std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count() : 0;
  };
  int64_t scan_us = 0, evaluate_us = 0, pack_us = 0, transfer_us = 0, finish_us = 0;
  auto cancelled = [](void *context) {
    auto *state = static_cast<SiriusScalarState *>(context);
    state->cancel_ret = THIS_WORKER.check_status();
    return state->cancel_ret != OB_SUCCESS;
  };
  if (!s.client.open_integer_sums(s.columns, cancelled, &s)) ret = OB_ERR_UNEXPECTED;
  bool ended = false;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  while (OB_SUCC(ret) && !ended) {
    const ObBatchRows *batch = nullptr;
    clear_evaluated_flag();
    const int64_t scan_start = clock_us();
    if (OB_FAIL(THIS_WORKER.check_status())) {
    } else if (OB_FAIL(child_->get_next_batch(spec_.max_batch_size_, batch))) {
    } else {
      scan_us += clock_us() - scan_start;
      ended = batch->end_;
      guard.set_batch_size(batch->size_);
      const int64_t evaluate_start = clock_us();
      for (int c = 0; OB_SUCC(ret) && c < s.sums; ++c) {
        if (OB_FAIL(s.inputs[c]->eval_batch(eval_ctx_, *batch->skip_, batch->size_))) {}
      }
      evaluate_us += clock_us() - evaluate_start;
      const int64_t pack_start = clock_us();
      const int64_t transfer_before = transfer_us;
      const bool all_active = batch->skip_->is_all_false(batch->size_);
      for (int64_t start = 0; OB_SUCC(ret) && start < batch->size_;) {
        const int64_t stop = std::min<int64_t>(batch->size_, start + s.limit - s.buffered);
        uint32_t position = s.buffered;
        for (int c = 0; OB_SUCC(ret) && c < s.sums; ++c) {
          position = s.buffered;
          const auto datums = s.inputs[c]->locate_expr_datumvector(eval_ctx_);
          char *masks[4] = {}, *values[4] = {};
          for (int limb = 0; limb < s.limbs[c]; ++limb) {
            masks[limb] = s.input + (s.offsets[c] + limb) * s.column_bytes;
            values[limb] = masks[limb] + s.mask_bytes;
          }
          for (int64_t row = start; OB_SUCC(ret) && row < stop; ++row) {
            if (!all_active && batch->skip_->at(row)) continue;
            const auto *datum = datums.at(row);
            unsigned char value[16] = {};
            if (!datum->is_null()) {
              const auto len = datum->get_int_bytes();
              if (len != 4 && len != 8 && len != 16) { ret = OB_ERR_UNEXPECTED; break; }
              std::memset(value, static_cast<unsigned char>(datum->ptr_[len-1]) & 0x80 ? 0xff : 0, sizeof(value));
              std::memcpy(value, datum->ptr_, len);
            }
            // Exact base-2^32 decomposition: unsigned low limbs and a signed
            // high limb. Schema precision bounds this to 1, 2 or 4 limbs;
            // each is an int64 input promoted before GPU SUM. No rounding or
            // floating-point conversion occurs. Pack one column at a time
            // to avoid repeated expression-vector lookup for every row.
            for (int limb = 0; limb < s.limbs[c]; ++limb) {
              if (datum->is_null()) masks[limb][position / 8] &= ~(1u << (position % 8));
              uint32_t bits = 0;
              std::memcpy(&bits, value + limb * 4, 4);
              int32_t signed_bits = 0;
              std::memcpy(&signed_bits, &bits, 4);
              const int64_t part = limb == s.limbs[c] - 1 ? static_cast<int64_t>(signed_bits) : static_cast<int64_t>(bits);
              std::memcpy(values[limb] + position * 8, &part, 8);
            }
            ++position;
          }
        }
        if (OB_SUCC(ret)) {
          s.rows += position - s.buffered;
          s.buffered = position;
          if (s.buffered == s.limit) {
            const int64_t transfer_start = clock_us();
            if (!s.client.batch(s.input, s.buffered, s.limit)) ret = OB_ERR_UNEXPECTED;
            transfer_us += clock_us() - transfer_start;
            s.clear();
          }
        }
        start = stop;
      }
      pack_us += clock_us() - pack_start - (transfer_us - transfer_before);
    }
  }
  const int64_t transfer_start = clock_us();
  if (OB_SUCC(ret) && s.buffered && !s.client.batch(s.input, s.buffered, s.limit)) ret = OB_ERR_UNEXPECTED;
  transfer_us += clock_us() - transfer_start;
  char result[4 + 64 * 17] = {};
  size_t bytes = 0;
  const int64_t finish_start = clock_us();
  if (OB_SUCC(ret) && !s.client.finish(result, sizeof(result), bytes)) ret = OB_ERR_UNEXPECTED;
  finish_us += clock_us() - finish_start;
  s.client.close();
  uint32_t rows = 0;
  std::memcpy(&rows, result, 4);
  if (OB_SUCC(ret) && (bytes != 4 + s.columns * 17 || rows != 1)) ret = OB_ERR_UNEXPECTED;
  clear_evaluated_flag();
  guard.set_batch_size(1); guard.set_batch_idx(0);
  for (int c = 0; OB_SUCC(ret) && c < s.sums; ++c) {
    auto *expr = s.outputs[c];
    auto *datum = expr->locate_datums_for_update(eval_ctx_, 1);
    const char valid = result[4 + s.offsets[c] * 17];
    int256_t total = 0;
    for (int limb = s.limbs[c] - 1; OB_SUCC(ret) && limb >= 0; --limb) {
      const char *cell = result + 4 + (s.offsets[c] + limb) * 17;
      if ((valid != 0 && valid != 1) || cell[0] != valid) { ret = OB_ERR_UNEXPECTED; break; }
      int128_t part;
      std::memcpy(&part, cell + 1, 16);
      total = total * (int64_t{1} << 32) + int256_t(part);
    }
    if (OB_SUCC(ret)) {
      if (!valid) datum->set_null();
      else {
        int256_t bound = 1;
        for (int i = 0; i < expr->datum_meta_.precision_; ++i) bound = bound * 10;
        if (total >= bound || total <= -bound) ret = OB_OPERATE_OVERFLOW;
        else if (expr->datum_meta_.precision_ <= 38) datum->set_decimal_int(int128_t(total));
        else datum->set_decimal_int(total);
      }
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  if (OB_FAIL(ret)) {
    if (s.cancel_ret != OB_SUCCESS) ret = s.cancel_ret;
    LOG_WARN("native Sirius scalar aggregate failed", K(ret), "reason", s.client.error(), K(s.rows));
    return ret;
  }
  brs_.size_ = 1; brs_.skip_->reset(1); brs_.end_ = true;
  s.emitted = true;
  const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-started).count();
  std::fprintf(stderr, "seekdb Sirius native scalar: rows=%llu sums=%d elapsed_us=%lld exact_decimal=1\n",
      static_cast<unsigned long long>(s.rows), s.sums, static_cast<long long>(elapsed));
  if (profile) {
    std::fprintf(stderr, "seekdb Sirius scalar profile: scan_us=%lld evaluate_us=%lld pack_us=%lld transfer_us=%lld finish_us=%lld\n",
        static_cast<long long>(scan_us), static_cast<long long>(evaluate_us), static_cast<long long>(pack_us),
        static_cast<long long>(transfer_us), static_cast<long long>(finish_us));
  }
  return ret;
}
} }
