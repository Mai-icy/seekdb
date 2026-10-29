// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/sirius/sirius_aggregate_client.h"
#include "sql/engine/sirius/sirius_native_scope.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "common/wide_integer/ob_wide_integer_str_funcs.h"
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

namespace oceanbase { namespace sql {
using namespace common;

static int64_t sirius_signed_integer(const ObDatum &value, ObObjType type)
{
  switch (type) {
    case ObTinyIntType: return value.get_tinyint();
    case ObSmallIntType: return value.get_smallint();
    case ObMediumIntType: return value.get_mediumint();
    case ObInt32Type: return value.get_int32();
    default: return value.get_int();
  }
}

struct SiriusGroupedExactState {
  struct Measure {
    ObExpr *input = nullptr, *output = nullptr;
    int offset = 0, limbs = 0;
    bool count = false, integer = false, first = false;
  } measures[16];
  struct Dictionary {
    std::unordered_map<std::string, int64_t> ids;
    std::vector<std::string> values;
    void clear() { ids.clear(); values.clear(); }
  } dictionaries[8];
  struct SavedDatum { ObDatum metadata; std::string payload; };
  std::unordered_map<std::string, std::vector<SavedDatum>> first_rows;
  bool has_first = false;
  size_t first_bytes = 0;
  ObExpr *keys[8] = {};
  bool strings[8] = {};
  int groups = 0, aggregates = 0, columns = 0;
  SiriusAggregateClient client;
  char *input = nullptr, *result = nullptr;
  size_t capacity = 0, mask_bytes = 0, column_bytes = 0, dictionary_bytes = 0;
  uint32_t limit = 0, buffered = 0, result_rows = 0, position = 0;
  uint64_t rows = 0;
  int cancel_ret = OB_SUCCESS;
  bool loaded = false;
  std::string input_key() const {
    std::string key(groups * 9, '\0');
    for (int c = 0; c < groups; ++c) {
      const char *column = input + c * column_bytes;
      key[c * 9] = (static_cast<unsigned char>(column[buffered / 8]) >> (buffered % 8)) & 1;
      if (key[c * 9]) std::memcpy(&key[c * 9 + 1], column + mask_bytes + buffered * 8, 8);
    }
    return key;
  }
  std::string output_key(const char *result) const {
    std::string key(groups * 9, '\0');
    for (int c = 0; c < groups; ++c) {
      key[c * 9] = result[c * 17];
      if (key[c * 9]) std::memcpy(&key[c * 9 + 1], result + c * 17 + 1, 8);
    }
    return key;
  }
  int remember_first(ObEvalCtx &ctx, int64_t row) {
    if (!has_first) return OB_SUCCESS;
    try {
      auto key = input_key();
      if (first_rows.find(key) != first_rows.end()) return OB_SUCCESS;
      size_t bytes = key.size() + 128 + aggregates * sizeof(SavedDatum);
      std::vector<SavedDatum> saved(aggregates);
      for (int c = 0; c < aggregates; ++c) if (measures[c].first) {
        auto *datum = measures[c].input->locate_expr_datumvector(ctx).at(row);
        if (datum->len_ > 4096) return OB_SIZE_OVERFLOW;
        saved[c].metadata = *datum;
        if (!datum->is_null()) saved[c].payload.assign(datum->ptr_, datum->len_);
        bytes += saved[c].payload.size();
      }
      if (first_bytes + bytes > 512 * 1024 * 1024) return OB_SIZE_OVERFLOW;
      first_rows.emplace(std::move(key), std::move(saved)); first_bytes += bytes;
      return OB_SUCCESS;
    } catch (const std::bad_alloc &) { return OB_ALLOCATE_MEMORY_FAILED; }
  }
  void clear_masks() {
    for (int i = 0; i < columns; ++i) std::memset(input + i * column_bytes, 0xff, mask_bytes);
    buffered = 0;
  }
  int key_id(int column, const ObDatum &datum, int64_t &value) {
    if (!strings[column]) {
      const auto type = keys[column]->datum_meta_.type_;
      value = type == ObDateType ? datum.get_date() : type == ObMySQLDateType
          ? datum.get_mysql_date().date_ : sirius_signed_integer(datum, type);
      return OB_SUCCESS;
    }
    // utf8mb4_bin equality is byte equality ignoring trailing U+0020. Preserve
    // the first original spelling for the SQL output, including embedded NULs.
    if (datum.len_ > 1024) return OB_SIZE_OVERFLOW;
    try {
      std::string key(datum.ptr_, datum.len_);
      while (!key.empty() && key.back() == ' ') key.pop_back();
      auto &dict = dictionaries[column];
      auto found = dict.ids.find(key);
      if (found != dict.ids.end()) { value = found->second; return OB_SUCCESS; }
      const size_t bytes = key.size() + datum.len_ + 128;
      if (dict.values.size() >= 4096 || dictionary_bytes + bytes > 8 * 1024 * 1024) return OB_SIZE_OVERFLOW;
      value = dict.values.size();
      dict.values.emplace_back(datum.ptr_, datum.len_);
      dict.ids.emplace(std::move(key), value);
      dictionary_bytes += bytes;
      return OB_SUCCESS;
    } catch (const std::bad_alloc &) { return OB_ALLOCATE_MEMORY_FAILED; }
  }
};

void ObHashGroupByOp::reset_sirius_exact()
{
  sirius_exact_active_ = false;
  if (sirius_exact_) {
    auto &s = *sirius_exact_;
    s.client.close(); s.loaded = false; s.buffered = s.position = s.result_rows = 0;
    s.rows = s.dictionary_bytes = 0; s.cancel_ret = OB_SUCCESS;
    s.first_rows.clear(); s.first_bytes = 0;
    for (auto &dict : s.dictionaries) dict.clear();
  }
}

void ObHashGroupByOp::destroy_sirius_exact()
{
  if (sirius_exact_) {
    ctx_.get_allocator().free(sirius_exact_->input);
    ctx_.get_allocator().free(sirius_exact_->result);
    sirius_exact_->~SiriusGroupedExactState();
    ctx_.get_allocator().free(sirius_exact_); sirius_exact_ = nullptr;
  }
}

int ObHashGroupByOp::init_sirius_exact()
{
  const auto &spec = static_cast<const ObHashGroupBySpec &>(spec_);
  const char *enabled = std::getenv("SEEKDB_SIRIUS_NATIVE");
  const auto *session = ctx_.get_my_session();
  const ObObj *setting = session ? session->get_user_variable_value(ObString("sirius_gpu")) : nullptr;
  const char *profile = std::getenv("SEEKDB_SIRIUS_PROFILE");
  if (enabled && !std::strcmp(enabled, "1") && setting && setting->is_int() && setting->get_int() == 1
      && !sirius_native_depth && profile && !std::strcmp(profile, "1")) {
    std::fprintf(stderr, "Sirius exact shape: vector=%d serial_child=%d groups=%lld aggregates=%lld rows=%lld dup=%lld/%lld distinct=%lld\n",
        spec.is_vectorized(), child_ && sirius_serial_input(child_->get_spec()),
        static_cast<long long>(spec.group_exprs_.count()), static_cast<long long>(spec.aggr_infos_.count()),
        static_cast<long long>(spec.get_rows()), static_cast<long long>(spec.org_dup_cols_.count()),
        static_cast<long long>(spec.new_dup_cols_.count()), static_cast<long long>(spec.distinct_exprs_.count()));
    for (int64_t i = 0; i < spec.group_exprs_.count(); ++i) {
      const auto *key = spec.group_exprs_.at(i);
      std::fprintf(stderr, "Sirius exact key: type=%d collation=%d obj_collation=%d\n",
          key->datum_meta_.type_, key->datum_meta_.cs_type_, key->obj_meta_.get_collation_type());
    }
    for (int64_t i = 0; i < spec.aggr_infos_.count(); ++i) {
      const auto &info = spec.aggr_infos_.at(i);
      std::fprintf(stderr, "Sirius exact measure: function=%d implicit_first=%d arguments=%lld output_type=%d\n",
          info.get_expr_type(), info.is_implicit_first_aggr_, static_cast<long long>(info.param_exprs_.count()),
          info.expr_ ? info.expr_->datum_meta_.type_ : -1);
    }
  }
  if (!enabled || std::strcmp(enabled, "1") || !setting || !setting->is_int() || setting->get_int() != 1
      || sirius_native_depth || !spec.is_vectorized() || !spec.get_phy_plan()->is_select_plan()
      || spec.get_phy_plan()->get_px_dop() > 1 || !child_ || !sirius_serial_input(child_->get_spec())
      || spec.aggr_stage_ != ObThreeStageAggrStage::NONE_STAGE
      || spec.group_exprs_.count() < 1 || spec.group_exprs_.count() > 8
      || spec.aggr_infos_.empty() || spec.aggr_infos_.count() > 16
      || !spec.org_dup_cols_.empty() || !spec.new_dup_cols_.empty() || !spec.distinct_exprs_.empty()) return OB_SUCCESS;
  SiriusGroupedExactState shape;
  shape.groups = spec.group_exprs_.count(); shape.columns = shape.groups;
  for (int c = 0; c < shape.groups; ++c) {
    auto *key = spec.group_exprs_.at(c);
    const auto type = key->datum_meta_.type_;
    const bool string = type == ObVarcharType || type == ObCharType;
    if (string ? key->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN
               : !(type >= ObTinyIntType && type <= ObIntType) && type != ObDateType && type != ObMySQLDateType) return OB_SUCCESS;
    shape.keys[c] = key; shape.strings[c] = string;
  }
  shape.aggregates = spec.aggr_infos_.count();
  for (int c = 0; c < shape.aggregates; ++c) {
    const auto &info = spec.aggr_infos_.at(c);
    if (!info.expr_ || info.has_distinct_ || info.has_order_by_) return OB_SUCCESS;
    auto &m = shape.measures[c]; m.output = info.expr_; m.offset = shape.columns;
    if (info.is_implicit_first_aggr_ && info.param_exprs_.empty()) {
      // The implicit FIRST_ROW uses expr_ itself as its input and output;
      // unlike explicit aggregate functions, it has no param_exprs_.
      m.first = true; m.input = info.expr_; shape.has_first = true;
      const auto type = m.input->datum_meta_.type_;
      if (!ob_is_integer_type(type) && type != ObDecimalIntType && type != ObNumberType
          && type != ObVarcharType && type != ObCharType && type != ObDateType
          && type != ObDateTimeType && type != ObTimestampType && type != ObDoubleType
          && type != ObMySQLDateType && type != ObMySQLDateTimeType) return OB_SUCCESS;
    } else if (info.get_expr_type() == T_FUN_COUNT && info.param_exprs_.count() <= 1
        && m.output->datum_meta_.type_ == ObIntType) {
      m.count = true; m.limbs = 1;
      m.input = info.param_exprs_.empty() ? nullptr : info.param_exprs_.at(0);
    } else if (info.get_expr_type() == T_FUN_SUM && info.param_exprs_.count() == 1) {
      m.input = info.param_exprs_.at(0);
      const auto &in = m.input->datum_meta_; const auto &out = m.output->datum_meta_;
      if (in.type_ >= ObTinyIntType && in.type_ <= ObIntType && (out.type_ == ObNumberType
          || (out.type_ == ObDecimalIntType && out.precision_ >= 19 && out.precision_ <= 76 && out.scale_ == 0))) {
        m.integer = true; m.limbs = 2;
      }
      else if (in.type_ == ObDecimalIntType && in.precision_ >= 1 && in.precision_ <= 76
               && out.type_ == ObDecimalIntType && out.precision_ >= in.precision_ && out.precision_ <= 76
               && out.scale_ == in.scale_) {
        m.limbs = in.precision_ <= 9 ? 1 : in.precision_ <= 18 ? 2 : in.precision_ <= 38 ? 4 : 8;
      } else {
        std::fprintf(stderr, "seekdb Sirius grouped SUM fallback: input_type=%d precision=%d scale=%d output_type=%d precision=%d scale=%d\n",
            in.type_, in.precision_, in.scale_, out.type_, out.precision_, out.scale_);
        return OB_SUCCESS;
      }
    } else return OB_SUCCESS;
    shape.columns += m.limbs;
  }
  if (shape.columns == shape.groups || shape.columns > 64 || spec.est_group_cnt_ >
      (SiriusAggregateClient::GROUP_RESULT_BYTES - 4) / (shape.columns * 17)) return OB_SUCCESS;
  if (!sirius_exact_) {
    void *memory = ctx_.get_allocator().alloc(sizeof(SiriusGroupedExactState));
    if (!memory) return OB_ALLOCATE_MEMORY_FAILED;
    sirius_exact_ = new(memory) SiriusGroupedExactState();
  }
  auto &s = *sirius_exact_;
  s.groups = shape.groups; s.aggregates = shape.aggregates; s.columns = shape.columns;
  s.has_first = shape.has_first;
  std::copy(std::begin(shape.keys), std::end(shape.keys), s.keys);
  std::copy(std::begin(shape.strings), std::end(shape.strings), s.strings);
  std::copy(std::begin(shape.measures), std::end(shape.measures), s.measures);
  s.limit = std::min<size_t>(SiriusAggregateClient::batch_row_limit(),
      (SiriusAggregateClient::input_frame_bytes() - 4) / (s.columns * 9));
  s.mask_bytes = (s.limit + 7) / 8; s.column_bytes = s.mask_bytes + s.limit * 8;
  const size_t bytes = s.columns * s.column_bytes;
  if (s.capacity < bytes) {
    ctx_.get_allocator().free(s.input);
    s.input = static_cast<char *>(ctx_.get_allocator().alloc(bytes)); s.capacity = s.input ? bytes : 0;
  }
  if (!s.result) s.result = static_cast<char *>(ctx_.get_allocator().alloc(SiriusAggregateClient::GROUP_RESULT_BYTES));
  if (!s.input || !s.result) return OB_ALLOCATE_MEMORY_FAILED;
  s.clear_masks(); sirius_active_ = sirius_exact_active_ = true;
  return OB_SUCCESS;
}

int ObHashGroupByOp::sirius_exact_get_next_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  auto &s = *sirius_exact_;
  if (!s.loaded) {
    SiriusNativeScope scope;
    auto cancelled = [](void *context) {
      auto *state = static_cast<SiriusGroupedExactState *>(context);
      state->cancel_ret = THIS_WORKER.check_status(); return state->cancel_ret != OB_SUCCESS;
    };
    if (!s.client.open_grouped_integer_sums(s.groups, s.columns - s.groups, cancelled, &s)) ret = OB_ERR_UNEXPECTED;
    bool ended = false;
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    while (OB_SUCC(ret) && !ended) {
      clear_evaluated_flag();
      const ObBatchRows *batch = nullptr;
      if (OB_FAIL(THIS_WORKER.check_status())) {
      } else if (OB_FAIL(child_->get_next_batch(spec_.max_batch_size_, batch))) {
      } else {
        ended = batch->end_; guard.set_batch_size(batch->size_);
        for (int c = 0; OB_SUCC(ret) && c < s.groups + s.aggregates; ++c) {
          auto *expr = c < s.groups ? s.keys[c] : s.measures[c - s.groups].input;
          if (expr && OB_FAIL(expr->eval_batch(eval_ctx_, *batch->skip_, batch->size_))) {}
        }
        for (int64_t row = 0; OB_SUCC(ret) && row < batch->size_; ++row) {
          if (batch->skip_->at(row)) continue;
          for (int c = 0; OB_SUCC(ret) && c < s.groups; ++c) {
            auto *datum = s.keys[c]->locate_expr_datumvector(eval_ctx_).at(row);
            char *mask = s.input + c * s.column_bytes; int64_t value = 0;
            if (datum->is_null()) mask[s.buffered / 8] &= ~(1u << (s.buffered % 8));
            else if (OB_FAIL(s.key_id(c, *datum, value))) {}
            std::memcpy(mask + s.mask_bytes + s.buffered * 8, &value, 8);
          }
          for (int c = 0; OB_SUCC(ret) && c < s.aggregates; ++c) {
            const auto &m = s.measures[c];
            if (m.first) continue;
            const auto *datum = m.input ? m.input->locate_expr_datumvector(eval_ctx_).at(row) : nullptr;
            unsigned char value[32] = {};
            if (m.count) value[0] = !datum || !datum->is_null();
            else if (!datum->is_null() && m.integer) {
              const int64_t integer = sirius_signed_integer(*datum, m.input->datum_meta_.type_);
              std::memcpy(value, &integer, 8);
            } else if (!datum->is_null()) {
              const int len = datum->get_int_bytes();
              if (len != 4 && len != 8 && len != 16 && len != 32) { ret = OB_ERR_UNEXPECTED; break; }
              std::memset(value, static_cast<unsigned char>(datum->ptr_[len - 1]) & 0x80 ? 0xff : 0, sizeof(value));
              std::memcpy(value, datum->ptr_, len);
            }
            for (int limb = 0; limb < m.limbs; ++limb) {
              char *mask = s.input + (m.offset + limb) * s.column_bytes;
              if (!m.count && datum->is_null()) mask[s.buffered / 8] &= ~(1u << (s.buffered % 8));
              uint32_t bits = 0; int32_t signed_bits = 0;
              std::memcpy(&bits, value + limb * 4, 4); std::memcpy(&signed_bits, &bits, 4);
              const int64_t part = limb == m.limbs - 1 ? static_cast<int64_t>(signed_bits) : static_cast<int64_t>(bits);
              std::memcpy(mask + s.mask_bytes + s.buffered * 8, &part, 8);
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(s.remember_first(eval_ctx_, row))) {}
          if (OB_SUCC(ret)) {
            ++s.rows;
            if (++s.buffered == s.limit) {
              if (!s.client.batch(s.input, s.buffered, s.limit)) ret = OB_ERR_UNEXPECTED;
              s.clear_masks();
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && s.buffered && !s.client.batch(s.input, s.buffered, s.limit)) ret = OB_ERR_UNEXPECTED;
    size_t bytes = 0;
    if (OB_SUCC(ret) && !s.client.finish(s.result, SiriusAggregateClient::GROUP_RESULT_BYTES, bytes)) ret = OB_ERR_UNEXPECTED;
    s.client.close();
    if (OB_SUCC(ret)) {
      if (bytes < 4) ret = OB_ERR_UNEXPECTED;
      else {
        std::memcpy(&s.result_rows, s.result, 4);
        if (bytes != 4 + static_cast<uint64_t>(s.result_rows) * s.columns * 17) ret = OB_ERR_UNEXPECTED;
        for (size_t i = 4; OB_SUCC(ret) && i < bytes; i += 17) if (s.result[i] != 0 && s.result[i] != 1) ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_FAIL(ret)) {
      if (s.cancel_ret != OB_SUCCESS) ret = s.cancel_ret;
      LOG_WARN("native Sirius exact grouping failed", K(ret), "reason", s.client.error(), K(s.rows)); return ret;
    }
    s.loaded = true;
    std::fprintf(stderr, "seekdb Sirius native exact grouping: rows=%llu groups=%u columns=%d\n",
        static_cast<unsigned long long>(s.rows), s.result_rows, s.columns);
  }
  clear_evaluated_flag();
  brs_.size_ = std::min<int64_t>(std::min(max_row_cnt, spec_.max_batch_size_), s.result_rows - s.position);
  brs_.skip_->reset(brs_.size_); brs_.end_ = s.position + brs_.size_ == s.result_rows;
  for (int c = 0; OB_SUCC(ret) && c < s.groups + s.aggregates; ++c) {
    auto *expr = c < s.groups ? s.keys[c] : s.measures[c - s.groups].output;
    auto *datums = expr->locate_datums_for_update(eval_ctx_, expr->is_batch_result() ? brs_.size_ : 1);
    for (int64_t row = 0; OB_SUCC(ret) && row < brs_.size_; ++row) {
      auto &datum = datums[expr->is_batch_result() ? row : 0];
      const char *result = s.result + 4 + (s.position + row) * s.columns * 17;
      if (c < s.groups) {
        const char *cell = result + c * 17; int128_t value = 0; std::memcpy(&value, cell + 1, 16);
        if (!cell[0]) datum.set_null();
        else if (s.strings[c]) {
          if (value < 0 || value >= s.dictionaries[c].values.size()) ret = OB_ERR_UNEXPECTED;
          else { const auto &text = s.dictionaries[c].values[static_cast<size_t>(value)]; datum.set_string(text.data(), text.size()); }
        } else if (value < INT64_MIN || value > INT64_MAX) ret = OB_ERR_UNEXPECTED;
        else if (expr->datum_meta_.type_ == ObDateType) datum.set_date(static_cast<int32_t>(value));
        else if (expr->datum_meta_.type_ == ObMySQLDateType) datum.set_mysql_date(ObMySQLDate(static_cast<int32_t>(value)));
        else datum.set_int(static_cast<int64_t>(value));
      } else {
        const auto &m = s.measures[c - s.groups];
        if (m.first) {
          try {
            const auto found = s.first_rows.find(s.output_key(result));
            if (found == s.first_rows.end()) ret = OB_ERR_UNEXPECTED;
            else {
              const auto &saved = found->second[c - s.groups];
              datum = saved.metadata;
              if (!datum.is_null()) datum.ptr_ = saved.payload.data();
            }
          } catch (const std::bad_alloc &) { ret = OB_ALLOCATE_MEMORY_FAILED; }
          continue;
        }
        const char valid = result[m.offset * 17]; int512_t total = 0;
        for (int limb = m.limbs - 1; OB_SUCC(ret) && limb >= 0; --limb) {
          const char *cell = result + (m.offset + limb) * 17;
          if (cell[0] != valid) { ret = OB_ERR_UNEXPECTED; break; }
          int128_t part = 0; std::memcpy(&part, cell + 1, 16);
          total = total * (int64_t{1} << 32) + int512_t(part);
        }
        if (OB_SUCC(ret)) {
          if (m.count) {
            if (total < 0 || total > int512_t(INT64_MAX)) ret = OB_OPERATE_OVERFLOW;
            else datum.set_int(valid ? static_cast<int64_t>(total) : 0);
          } else if (!valid) datum.set_null();
          else if (expr->datum_meta_.type_ == ObNumberType) {
            char text[180]; int64_t length = 0; number::ObNumber value;
            if (OB_FAIL(wide::to_string(total, text, sizeof(text), length))) {}
            else if (OB_FAIL(value.from(text, length, ctx_.get_allocator()))) {}
            else datum.set_number(value);
          } else {
            int512_t bound = 1;
            for (int i = 0; i < expr->datum_meta_.precision_; ++i) bound = bound * 10;
            if (total >= bound || total <= -bound) ret = OB_OPERATE_OVERFLOW;
            else if (expr->datum_meta_.precision_ <= 38) datum.set_decimal_int(int128_t(total));
            else datum.set_decimal_int(int256_t(total));
          }
        }
      }
    }
    expr->set_evaluated_projected(eval_ctx_);
  }
  s.position += brs_.size_;
  return ret;
}
} }
