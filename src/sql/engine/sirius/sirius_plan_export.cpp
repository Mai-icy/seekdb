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
#include "sirius_plan_export.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/ob_sql_context.h"
#include "common/timezone/ob_time_convert.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include <algorithm>
#include <stdexcept>
#include <vector>

namespace oceanbase { namespace sql {
using namespace common;
namespace {
std::string quoted(const ObString &s) {
  std::string out = "\"";
  const char hex[] = "0123456789abcdef";
  for (int64_t i = 0; i < s.length(); ++i) {
    const unsigned char c = s.ptr()[i];
    if (c == '"' || c == '\\') { out += '\\'; out += c; }
    else if (c < 32) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
    else out += c;
  }
  return out + '"';
}
std::string array(const std::vector<std::string> &items) {
  std::string result = "[";
  for (size_t i = 0; i < items.size(); ++i) { if (i) result += ','; result += items[i]; }
  return result + ']';
}
std::string type(const ObRawExpr &expr) {
  const auto &t = expr.get_result_type();
  switch (t.get_type()) {
    case ObNullType: return "\"i64\"";
    case ObTinyIntType: return "\"i8\"";
    case ObSmallIntType: return "\"i16\"";
    case ObMediumIntType: case ObInt32Type: return "\"i32\"";
    case ObIntType: return "\"i64\"";
    case ObFloatType: return "\"fp32\"";
    case ObDoubleType: return "\"fp64\"";
    case ObMySQLDateType: case ObDateType: return "\"date\"";
    case ObCharType: case ObVarcharType:
      if (t.get_collation_type() != CS_TYPE_UTF8MB4_BIN)
        throw std::runtime_error("Native Sirius plan: only utf8mb4_bin text is supported");
      return "\"string\"";
    case ObNumberType: case ObDecimalIntType:
      // Preserve seekdb's declared precision in the native IR. The consumer
      // must validate its own Decimal width; silently clamping to 38 changes
      // the expression's overflow and rounding semantics.
      if (t.get_precision() > 0 && t.get_scale() >= 0
          && t.get_scale() <= t.get_precision())
        return "[\"decimal\"," + std::to_string(t.get_precision()) + ',' + std::to_string(t.get_scale()) + ']';
      if (t.get_type() == ObNumberType && t.get_precision() <= 0) return "\"number\"";
      break;
    default: break;
  }
  throw std::runtime_error("Native Sirius plan: unsupported type " + std::to_string(t.get_type())
      + " precision " + std::to_string(t.get_precision()) + " scale " + std::to_string(t.get_scale())
      + " expression " + std::to_string(expr.get_expr_type()));
}
const char *function(ObItemType t) {
  switch (t) {
    case T_OP_EQ: return "equal"; case T_OP_NE: return "not_equal";
    case T_OP_LT: return "lt"; case T_OP_LE: return "lte";
    case T_OP_GT: return "gt"; case T_OP_GE: return "gte";
    case T_OP_AND: return "and"; case T_OP_OR: return "or"; case T_OP_NOT: return "not";
    case T_OP_ADD: return "add"; case T_OP_MINUS: return "subtract";
    case T_OP_MUL: return "multiply"; case T_OP_AGG_DIV: case T_OP_DIV: return "divide";
    case T_OP_NSEQ: return "is_not_distinct_from";
    case T_OP_BTW: return "between";
    case T_OP_LIKE: return "like_escape";
    case T_FUN_SYS_SUBSTR: return "substring";
    case T_FUN_SYS_YEAR: return "year";
    case T_FUN_SYS_EXTRACT: return "extract";
    case T_FUN_AVG: return "avg";
    case T_FUN_SUM: return "sum"; case T_FUN_COUNT: return "count";
    case T_FUN_MIN: return "min"; case T_FUN_MAX: return "max";
    default: throw std::runtime_error("Native Sirius plan: unsupported expression " + std::to_string(t));
  }
}
using Columns = std::vector<const ObRawExpr *>;
std::string expression(const ObRawExpr *expr, const Columns &columns, int depth = 0) {
  if (!expr || depth > 128) throw std::runtime_error("Native Sirius plan: invalid expression depth");
  const auto found = std::find(columns.begin(), columns.end(), expr);
  if (found != columns.end()) return "{\"field\":" + std::to_string(found - columns.begin()) + ",\"type\":" + type(*expr) + '}';
  // CPU runtime filters only prune candidates. The authoritative TopN/join
  // remains in the exported tree and executes on the GPU.
  if (expr->get_expr_type() == T_OP_PUSHDOWN_TOPN_FILTER || expr->get_expr_type() == T_OP_RUNTIME_FILTER
      || expr->get_expr_type() == T_OP_LOCAL_DYNAMIC_FILTER)
    return "{\"literal\":true,\"type\":\"bool\"}";
  if (expr->is_exec_param_expr())
    return expression(static_cast<const ObExecParamRawExpr *>(expr)->get_ref_expr(), columns, depth + 1);
  if (expr->is_const_raw_expr()) {
    const auto &value = static_cast<const ObConstRawExpr *>(expr)->get_value();
    if (value.is_unknown()) return "{\"param\":" + std::to_string(value.get_unknown()) + ",\"type\":" + type(*expr) + '}';
    return "{\"literal\":" + sirius_plan_literal(value) + ",\"type\":" + type(*expr) + '}';
  }
  if (expr->is_column_ref_expr() || expr->is_aggr_expr() || expr->is_param_expr())
    throw std::runtime_error("Native Sirius plan: unbound expression " + std::to_string(expr->get_expr_type())
        + (expr->is_column_ref_expr() ? " column " + quoted(static_cast<const ObColumnRefRawExpr *>(expr)->get_column_name()) : ""));
  if (expr->is_case_op_expr()) {
    const auto *c = static_cast<const ObCaseOpRawExpr *>(expr);
    std::vector<std::string> branches;
    for (int64_t i = 0; i < c->get_when_expr_size(); ++i) {
      auto condition = expression(c->get_when_param_expr(i), columns, depth + 1);
      if (c->get_arg_param_expr())
        condition = "{\"fn\":\"equal\",\"type\":\"bool\",\"args\":["
            + expression(c->get_arg_param_expr(), columns, depth + 1) + ',' + condition + "]}";
      branches.push_back("{\"if\":" + condition + ",\"then\":"
          + expression(c->get_then_param_expr(i), columns, depth + 1) + '}');
    }
    return "{\"case\":" + array(branches) + ",\"else\":"
        + (c->get_default_param_expr() ? expression(c->get_default_param_expr(), columns, depth + 1)
            : "{\"literal\":null,\"type\":" + type(*expr) + '}') + '}';
  }
  if (expr->get_expr_type() == T_OP_IN || expr->get_expr_type() == T_OP_NOT_IN) {
    const auto *row = expr->get_param_expr(1);
    if (row->get_expr_type() != T_OP_ROW) throw std::runtime_error("Native Sirius plan: IN requires a scalar list");
    std::vector<std::string> values;
    for (int64_t i = 0; i < row->get_param_count(); ++i)
      values.push_back(expression(row->get_param_expr(i), columns, depth + 1));
    return "{\"in\":" + expression(expr->get_param_expr(0), columns, depth + 1)
        + ",\"values\":" + array(values) + ",\"negate\":"
        + (expr->get_expr_type() == T_OP_NOT_IN ? "true" : "false") + '}';
  }
  if (expr->get_expr_type() == T_OP_IS || expr->get_expr_type() == T_OP_IS_NOT) {
    auto *rhs = expr->get_param_expr(1);
    if (!rhs->is_const_raw_expr() || !static_cast<const ObConstRawExpr *>(rhs)->get_value().is_null())
      throw std::runtime_error("Native Sirius plan: IS boolean not implemented");
    return "{\"fn\":\"" + std::string(expr->get_expr_type() == T_OP_IS ? "is_null" : "is_not_null")
        + "\",\"type\":\"bool\",\"args\":[" + expression(expr->get_param_expr(0), columns, depth + 1) + "]}";
  }
  if (expr->get_expr_type() == T_FUN_SYS_CAST) {
    const auto &target = expr->get_result_type();
    const auto &source = expr->get_param_expr(0)->get_result_type();
    // A widening decimal cast changes metadata, not the numeric value. Sirius
    // uses a checked Decimal128 carrier; do not truncate a wide cast's scale.
    if ((target.get_type() == ObDecimalIntType || target.get_type() == ObNumberType)
        && target.get_precision() > 38 && source.get_precision() > 0
        && target.get_scale() >= source.get_scale()
        && target.get_precision() - target.get_scale() >= source.get_precision() - source.get_scale())
      return expression(expr->get_param_expr(0), columns, depth + 1);
    return "{\"cast\":" + expression(expr->get_param_expr(0), columns, depth + 1)
        + ",\"type\":" + type(*expr) + '}';
  }
  std::vector<std::string> args;
  for (int64_t i = 0; i < expr->get_param_count(); ++i)
    args.push_back(expression(expr->get_param_expr(i), columns, depth + 1));
  const auto *name = function(expr->get_expr_type());
  const bool boolean = expr->get_expr_type() == T_OP_EQ || expr->get_expr_type() == T_OP_NE
      || expr->get_expr_type() == T_OP_LT || expr->get_expr_type() == T_OP_LE
      || expr->get_expr_type() == T_OP_GT || expr->get_expr_type() == T_OP_GE;
  const bool logic = expr->get_expr_type() == T_OP_AND || expr->get_expr_type() == T_OP_OR || expr->get_expr_type() == T_OP_NOT;
  if (boolean) {
    for (int64_t i = 0; i < expr->get_param_count(); ++i) {
      if (expr->get_param_expr(i)->get_result_type().is_string_type())
        args[i] = "{\"fn\":\"rtrim\",\"type\":\"string\",\"args\":[" + args[i] + "]}";
    }
  }
  return "{\"fn\":\"" + std::string(name) + "\",\"args\":" + array(args)
      + ",\"type\":" + (boolean || logic || expr->get_expr_type() == T_OP_LIKE || expr->get_expr_type() == T_OP_BTW || expr->get_expr_type() == T_OP_NSEQ ? "\"bool\"" : type(*expr)) + '}';
}
struct Relation { std::string json; Columns columns; Columns pending; };
struct MaterializedRelation { const ObLogicalOperator *producer; Relation value; };
struct ExportContext {
  std::vector<MaterializedRelation> materialized;
  std::vector<const ObLogicalOperator *> active;
};
bool external(const ObRawExpr *expr, const Columns &columns, int depth = 0) {
  if (!expr || depth > 128) throw std::runtime_error("Native Sirius plan: invalid correlation depth");
  if (std::find(columns.begin(), columns.end(), expr) != columns.end()) return false;
  if (expr->is_exec_param_expr())
    return external(static_cast<const ObExecParamRawExpr *>(expr)->get_ref_expr(), columns, depth + 1);
  if (expr->is_column_ref_expr() || expr->is_aggr_expr()) return true;
  for (int64_t i = 0; i < expr->get_param_count(); ++i)
    if (external(expr->get_param_expr(i), columns, depth + 1)) return true;
  return false;
}
std::string measure(const ObAggFunRawExpr &agg, const Columns &columns) {
  std::vector<std::string> args;
  for (int64_t i = 0; i < agg.get_real_param_count(); ++i) {
    auto *arg = agg.get_real_param_exprs().at(i);
    auto value = expression(arg, columns);
    if (agg.is_param_distinct() && arg->get_result_type().is_string_type())
      value = "{\"fn\":\"rtrim\",\"type\":\"string\",\"args\":[" + value + "]}";
    args.push_back(value);
  }
  const char *name = agg.get_expr_type() == T_FUN_COUNT_SUM ? "sum" : function(agg.get_expr_type());
  return "{\"fn\":\"" + std::string(name) + "\",\"args\":" + array(args)
      + ",\"distinct\":" + (agg.is_param_distinct() ? "true" : "false") + ",\"type\":" + type(agg) + '}';
}
template<class G, class A> void aggregate(Relation &input, const G &groups, const A &aggs, const Columns &dependent = {}) {
  if (!input.pending.empty()) throw std::runtime_error("Native Sirius plan: correlation crosses aggregation");
  std::vector<std::string> keys, measures, string_keys;
  Columns output;
  for (int64_t i = 0; i < groups.count(); ++i) {
    keys.push_back(expression(groups.at(i), input.columns));
    string_keys.push_back(groups.at(i)->get_result_type().is_string_type() ? "true" : "false");
    output.push_back(groups.at(i));
  }
  for (int64_t i = 0; i < aggs.count(); ++i) {
    measures.push_back(measure(*static_cast<const ObAggFunRawExpr *>(aggs.at(i)), input.columns)); output.push_back(aggs.at(i));
  }
  for (auto *expr : dependent) {
    measures.push_back("{\"fn\":\"min\",\"args\":[" + expression(expr, input.columns)
        + "],\"distinct\":false,\"type\":" + type(*expr) + '}');
    output.push_back(expr);
  }
  input.json = "{\"op\":\"aggregate\",\"input\":" + input.json + ",\"groups\":" + array(keys)
      + ",\"string_keys\":" + array(string_keys) + ",\"measures\":" + array(measures) + '}';
  input.columns = std::move(output);
}
bool null_on_empty(const ObRawExpr *expr) {
  if (expr->is_aggr_expr()) return expr->get_expr_type() == T_FUN_SUM || expr->get_expr_type() == T_FUN_MIN || expr->get_expr_type() == T_FUN_MAX;
  switch (expr->get_expr_type()) {
    case T_FUN_SYS_CAST: return null_on_empty(expr->get_param_expr(0));
    case T_OP_ADD: case T_OP_MINUS: case T_OP_MUL: case T_OP_DIV: case T_OP_AGG_DIV:
      for (int64_t i = 0; i < expr->get_param_count(); ++i) if (null_on_empty(expr->get_param_expr(i))) return true;
      return false;
    default: return false;
  }
}
bool rejects_empty_aggregate(const ObRawExpr *expr) {
  switch (expr->get_expr_type()) {
    case T_OP_EQ: case T_OP_NE: case T_OP_LT: case T_OP_LE: case T_OP_GT: case T_OP_GE:
      return null_on_empty(expr->get_param_expr(0)) || null_on_empty(expr->get_param_expr(1));
    case T_OP_AND:
      for (int64_t i = 0; i < expr->get_param_count(); ++i) if (rejects_empty_aggregate(expr->get_param_expr(i))) return true;
      return false;
    default: return false;
  }
}
void dependent_columns(const ObRawExpr *expr, const Columns &bound, const Columns &input, Columns &extra) {
  if (std::find(bound.begin(), bound.end(), expr) != bound.end()
      || std::find(extra.begin(), extra.end(), expr) != extra.end()) return;
  if (std::find(input.begin(), input.end(), expr) != input.end()) { extra.push_back(expr); return; }
  for (int64_t i = 0; i < expr->get_param_count(); ++i)
    dependent_columns(expr->get_param_expr(i), bound, input, extra);
}
void scan_columns(const ObRawExpr *expr, uint64_t table, Columns &columns) {
  if (expr->is_exec_param_expr()) return; // outer input is supplied by the join
  if (expr->is_column_ref_expr()) {
    if (static_cast<const ObColumnRefRawExpr *>(expr)->get_table_id() == table
        && std::find(columns.begin(), columns.end(), expr) == columns.end()) columns.push_back(expr);
    return;
  }
  for (int64_t i = 0; i < expr->get_param_count(); ++i) scan_columns(expr->get_param_expr(i), table, columns);
}
void derived(Relation &input, const ObIArray<ObRawExpr *> &access, const TableItem *table) {
  if (!table || !table->ref_query_) throw std::runtime_error("Native Sirius plan: missing derived table schema");
  std::vector<std::string> values;
  Columns output;
  for (int64_t i = 0; i < access.count(); ++i) {
    if (!access.at(i)->is_column_ref_expr()) throw std::runtime_error("Native Sirius plan: invalid derived column");
    auto index = static_cast<ObColumnRefRawExpr *>(access.at(i))->get_column_id() - OB_APP_MIN_COLUMN_ID;
    if (index >= table->ref_query_->get_select_item_size()) throw std::runtime_error("Native Sirius plan: derived ordinal out of range");
    values.push_back(expression(table->ref_query_->get_select_item(index).expr_, input.columns));
    output.push_back(access.at(i));
  }
  if (!input.pending.empty()) {
    for (auto *expr : input.columns) {
      values.push_back(expression(expr, input.columns)); output.push_back(expr);
    }
  }
  input.json = "{\"op\":\"project\",\"input\":" + input.json + ",\"expressions\":" + array(values) + '}';
  input.columns = std::move(output);
}
void filter(Relation &input, const ObIArray<ObRawExpr *> &conditions) {
  for (int64_t i = 0; i < conditions.count(); ++i) {
    if (external(conditions.at(i), input.columns)) { input.pending.push_back(conditions.at(i)); continue; }
    input.json = "{\"op\":\"filter\",\"input\":" + input.json
        + ",\"condition\":" + expression(conditions.at(i), input.columns) + '}';
  }
}
void project(Relation &input, const ObIArray<ObRawExpr *> &columns) {
  if (columns.empty()) return;
  std::vector<std::string> exprs;
  Columns output;
  for (int64_t i = 0; i < columns.count(); ++i) {
    exprs.push_back(expression(columns.at(i), input.columns)); output.push_back(columns.at(i));
  }
  // A parameterized index scan must retain its keys until the enclosing join
  // can bind the lifted ON predicates.
  if (!input.pending.empty()) {
    for (auto *column : input.columns) if (std::find(output.begin(), output.end(), column) == output.end()) {
      exprs.push_back(expression(column, input.columns)); output.push_back(column);
    }
  }
  input.json = "{\"op\":\"project\",\"input\":" + input.json + ",\"expressions\":" + array(exprs) + '}';
  input.columns = std::move(output);
}
Relation relation(ObLogicalOperator &op, bool ordered, ExportContext &context, int depth = 0) {
  if (depth > 128) throw std::runtime_error("Native Sirius plan: relation depth exceeds 128");
  Relation result;
  using namespace log_op_def;
  if (op.get_type() == LOG_TABLE_SCAN) {
    auto &scan = static_cast<ObLogTableScan &>(op);
    if (scan.is_sample_scan())
      throw std::runtime_error("Native Sirius plan: sampled scans are unsupported");
    const bool storage_count = !scan.get_pushdown_aggr_exprs().empty();
    const auto *table = op.get_plan()->get_stmt()->get_table_item_by_id(scan.get_table_id());
    if (!table) throw std::runtime_error("Native Sirius plan: missing base table");
    std::vector<std::string> names, types;
    for (int64_t i = 0; i < scan.get_access_exprs().count(); ++i) {
      auto *expr = scan.get_access_exprs().at(i);
      if ((storage_count && expr->is_aggr_expr()) || expr->get_expr_type() == T_PSEUDO_GROUP_ID) continue;
      if (!expr->is_column_ref_expr()) throw std::runtime_error("Native Sirius plan: non-column scan access");
      names.push_back(quoted(static_cast<ObColumnRefRawExpr *>(expr)->get_column_name()));
      result.columns.push_back(expr);
    }
    for (int64_t i = 0; i < scan.get_range_conditions().count(); ++i)
      scan_columns(scan.get_range_conditions().at(i), scan.get_table_id(), result.columns);
    names.clear();
    for (auto *expr : result.columns) {
      names.push_back(quoted(static_cast<const ObColumnRefRawExpr *>(expr)->get_column_name()));
      types.push_back(type(*expr));
    }
    // A complete primary key bounds join multiplicity when validating the
    // consumer's Decimal128 range. Never infer uniqueness from sampled values.
    std::vector<std::string> key;
    const share::schema::ObTableSchema *schema = nullptr;
    auto *guard = op.get_plan()->get_optimizer_context().get_sql_schema_guard();
    if (!guard || guard->get_table_schema(scan.get_ref_table_id(), schema) != OB_SUCCESS || !schema)
      throw std::runtime_error("Native Sirius plan: missing scan schema");
    for (int64_t i = 0; i < schema->get_rowkey_info().get_size(); ++i) {
      const auto id = schema->get_rowkey_info().get_column(i)->column_id_;
      const auto found = std::find_if(result.columns.begin(), result.columns.end(),
          [id](const ObRawExpr *expr) {
            return static_cast<const ObColumnRefRawExpr *>(expr)->get_column_id() == id;
          });
      if (found == result.columns.end()) { key.clear(); break; }
      key.push_back(std::to_string(found - result.columns.begin()));
    }
    result.json = "{\"op\":\"read\",\"table\":" + quoted(table->table_name_)
        + ",\"database\":" + quoted(table->database_name_)
        + ",\"columns\":" + array(names) + ",\"types\":" + array(types)
        + ",\"unique_keys\":" + (key.empty() ? "[]" : '[' + array(key) + ']') + '}';
    filter(result, scan.get_range_conditions());
    if (storage_count) {
      filter(result, scan.get_startup_exprs());
      filter(result, scan.get_filter_exprs());
      aggregate(result, scan.get_pushdown_groupby_columns(), scan.get_pushdown_aggr_exprs());
    }
  } else if (op.get_type() == LOG_JOIN) {
    auto &join = static_cast<ObLogJoin &>(op);
    if (join.get_join_path() && join.get_join_path()->is_naaj_)
      throw std::runtime_error("Native Sirius plan: NULL-aware anti join requires explicit NOT IN semantics");
    auto left = relation(*op.get_child(0), false, context, depth + 1);
    auto right = relation(*op.get_child(1), false, context, depth + 1);
    result.columns = left.columns;
    result.columns.insert(result.columns.end(), right.columns.begin(), right.columns.end());
    const char *kind = nullptr;
    switch (join.get_join_type()) {
      case INNER_JOIN: kind = "inner"; break; case LEFT_OUTER_JOIN: kind = "left"; break;
      case RIGHT_OUTER_JOIN: kind = "right"; break; case FULL_OUTER_JOIN: kind = "outer"; break;
      case LEFT_SEMI_JOIN: kind = "left_semi"; break; case RIGHT_SEMI_JOIN: kind = "right_semi"; break;
      case LEFT_ANTI_JOIN: kind = "left_anti"; break; case RIGHT_ANTI_JOIN: kind = "right_anti"; break;
      default: throw std::runtime_error("Native Sirius plan: unsupported join kind");
    }
    std::vector<std::string> conditions;
    for (int64_t i = 0; i < join.get_equal_join_conditions().count(); ++i)
      conditions.push_back(expression(join.get_equal_join_conditions().at(i), result.columns));
    for (int64_t i = 0; i < join.get_other_join_conditions().count(); ++i)
      conditions.push_back(expression(join.get_other_join_conditions().at(i), result.columns));
    for (const auto *condition : left.pending) {
      if (external(condition, result.columns)) result.pending.push_back(condition);
      else conditions.push_back(expression(condition, result.columns));
    }
    for (const auto *condition : right.pending) {
      if (external(condition, result.columns)) result.pending.push_back(condition);
      else conditions.push_back(expression(condition, result.columns));
    }
    result.json = "{\"op\":\"join\",\"left\":" + left.json + ",\"right\":" + right.json
        + ",\"kind\":\"" + kind + "\",\"conditions\":" + array(conditions) + '}';
    if (join.get_join_type() == LEFT_SEMI_JOIN || join.get_join_type() == LEFT_ANTI_JOIN) result.columns = left.columns;
    if (join.get_join_type() == RIGHT_SEMI_JOIN || join.get_join_type() == RIGHT_ANTI_JOIN) result.columns = right.columns;
  } else if (op.get_type() == LOG_TEMP_TABLE_TRANSFORMATION) {
    if (op.get_num_of_child() < 1) throw std::runtime_error("Native Sirius plan: empty CTE");
    result = relation(*op.get_child(op.get_num_of_child() - 1), ordered, context, depth + 1);
  } else if (op.get_type() == LOG_TEMP_TABLE_ACCESS) {
    auto &access = static_cast<ObLogTempTableAccess &>(op);
    ObLogicalOperator *insert = nullptr;
    if (access.get_temp_table_plan(insert) != OB_SUCCESS || !insert)
      throw std::runtime_error("Native Sirius plan: missing CTE producer");
    // Preserve the optimizer's materialization identity. Expanding the producer
    // for every access repeats both the scan and aggregation (TPC-H Q15).
    auto found = std::find_if(context.materialized.begin(), context.materialized.end(),
        [insert](const MaterializedRelation &item) { return item.producer == insert; });
    size_t index = found - context.materialized.begin();
    if (found == context.materialized.end()) {
      if (std::find(context.active.begin(), context.active.end(), insert) != context.active.end()
          || context.materialized.size() + context.active.size() >= 64)
        throw std::runtime_error("Native Sirius plan: recursive or too many materialized relations");
      context.active.push_back(insert);
      const auto *statement = insert->get_plan()->get_stmt();
      auto producer = relation(*insert, statement && !statement->get_order_items().empty(), context, depth + 1);
      context.active.pop_back();
      if (!producer.pending.empty()) throw std::runtime_error("Native Sirius plan: correlated materialization");
      // Dependencies are emitted first, so every reference points backwards.
      index = context.materialized.size();
      context.materialized.push_back({insert, std::move(producer)});
    }
    result.columns = context.materialized[index].value.columns;
    result.json = "{\"op\":\"reference\",\"id\":" + std::to_string(index) + '}';
    derived(result, access.get_access_exprs(), op.get_plan()->get_stmt()->get_table_item_by_id(access.get_table_id()));
  } else {
    if (op.get_num_of_child() != 1) throw std::runtime_error("Native Sirius plan: unsupported operator " + std::string(op.get_name()));
    auto *child = op.get_child(0);
    if (op.get_type() == LOG_WINDOW_FUNCTION && child->get_type() == LOG_SORT) {
      // Unordered full-partition windows are lowered to aggregate + join.
      // CPU partition sorting (including its synthetic HASH key) is unnecessary.
      auto &sort = static_cast<ObLogSort &>(*child);
      if (!sort.get_topn_expr() && !sort.is_fetch_with_ties()
          && sort.get_filter_exprs().empty() && sort.get_startup_exprs().empty()) child = child->get_child(0);
    }
    result = relation(*child, ordered && op.get_type() != LOG_SORT && op.get_type() != LOG_GROUP_BY, context, depth + 1);
    switch (op.get_type()) {
      case LOG_GROUP_BY: {
        auto &group = static_cast<ObLogGroupBy &>(op);
        if (group.has_rollup() || group.is_push_down() || group.is_three_stage_aggr())
          throw std::runtime_error("Native Sirius plan: rollup/partial aggregation needs normalization");
        ObSEArray<ObRawExpr *, 8> groups;
        if (groups.assign(group.get_group_by_exprs()) != OB_SUCCESS) throw std::runtime_error("Native Sirius plan: group allocation failed");
        Columns pending = result.pending;
        if (!pending.empty()) {
          bool safe_empty = !groups.empty();
          for (int64_t i = 0; i < op.get_filter_exprs().count(); ++i)
            safe_empty |= rejects_empty_aggregate(op.get_filter_exprs().at(i));
          if (!safe_empty) throw std::runtime_error("Native Sirius plan: correlated scalar aggregate needs empty-row restoration");
          for (auto *condition : pending) {
            if (condition->get_expr_type() != T_OP_EQ || condition->get_param_count() != 2)
              throw std::runtime_error("Native Sirius plan: correlated aggregation requires equality keys");
            const auto *a = condition->get_param_expr(0), *b = condition->get_param_expr(1);
            const bool ae = external(a, result.columns), be = external(b, result.columns);
            if (ae == be) throw std::runtime_error("Native Sirius plan: invalid aggregate correlation key");
            auto *key = const_cast<ObRawExpr *>(ae ? b : a);
            bool found = false;
            for (int64_t i = 0; i < groups.count(); ++i) found |= groups.at(i) == key;
            if (!found && groups.push_back(key) != OB_SUCCESS) throw std::runtime_error("Native Sirius plan: group allocation failed");
          }
          result.pending.clear();
        }
        Columns bound, dependent;
        for (int64_t i = 0; i < groups.count(); ++i) bound.push_back(groups.at(i));
        for (int64_t i = 0; i < group.get_aggr_funcs().count(); ++i) bound.push_back(group.get_aggr_funcs().at(i));
        for (int64_t i = 0; i < op.get_output_exprs().count(); ++i)
          dependent_columns(op.get_output_exprs().at(i), bound, result.columns, dependent);
        // Functional dependency can also leave an ungrouped input column in
        // HAVING, even when it is absent from SELECT (TPC-H Q20). Preserve its
        // representative value before binding the post-aggregate predicates.
        for (int64_t i = 0; i < op.get_filter_exprs().count(); ++i)
          dependent_columns(op.get_filter_exprs().at(i), bound, result.columns, dependent);
        for (int64_t i = 0; i < op.get_startup_exprs().count(); ++i)
          dependent_columns(op.get_startup_exprs().at(i), bound, result.columns, dependent);
        aggregate(result, groups, group.get_aggr_funcs(), dependent);
        result.pending = std::move(pending);
        break;
      }
      case LOG_DISTINCT: {
        auto &distinct = static_cast<ObLogDistinct &>(op);
        ObSEArray<ObAggFunRawExpr *, 1> measures;
        aggregate(result, distinct.get_distinct_exprs(), measures);
        break;
      }
      case LOG_SUBPLAN_SCAN: {
        auto &scan = static_cast<ObLogSubPlanScan &>(op);
        derived(result, scan.get_access_exprs(), op.get_plan()->get_stmt()->get_table_item_by_id(scan.get_subquery_id()));
        break;
      }
      case LOG_WINDOW_FUNCTION: {
        auto &window = static_cast<ObLogWindowFunction &>(op);
        for (int64_t i = 0; i < window.get_window_exprs().count();) {
          const auto *win = window.get_window_exprs().at(i);
          if (!win->get_agg_expr() || !win->get_order_items().empty()
              || win->upper_.type_ != BOUND_UNBOUNDED || win->lower_.type_ != BOUND_UNBOUNDED
              || !win->upper_.is_preceding_ || win->lower_.is_preceding_)
            throw std::runtime_error("Native Sirius plan: requires full partition aggregate window");
          if (!result.pending.empty()) throw std::runtime_error("Native Sirius plan: correlation crosses window");
          std::vector<std::string> keys, measures;
          Columns output = result.columns;
          for (int64_t j = 0; j < win->get_partition_exprs().count(); ++j)
            keys.push_back(expression(win->get_partition_exprs().at(j), result.columns));
          // SUM and COUNT of one AVG share the same partition; one replay is
          // enough for all measures, rather than duplicating earlier windows.
          int64_t next = i;
          for (; next < window.get_window_exprs().count(); ++next) {
            const auto *candidate = window.get_window_exprs().at(next);
            bool same = candidate->get_agg_expr() && candidate->get_order_items().empty()
                && candidate->upper_.type_ == BOUND_UNBOUNDED && candidate->lower_.type_ == BOUND_UNBOUNDED
                && candidate->upper_.is_preceding_ && !candidate->lower_.is_preceding_
                && candidate->get_partition_exprs().count() == win->get_partition_exprs().count();
            for (int64_t j = 0; same && j < win->get_partition_exprs().count(); ++j)
              same = win->get_partition_exprs().at(j) == candidate->get_partition_exprs().at(j);
            if (!same) break;
            measures.push_back(measure(*candidate->get_agg_expr(), result.columns));
            output.push_back(candidate);
          }
          result.json = "{\"op\":\"window_aggregate\",\"input\":" + result.json
              + ",\"groups\":" + array(keys) + ",\"measures\":" + array(measures) + '}';
          result.columns = std::move(output);
          i = next;
        }
        break;
      }
      case LOG_SORT: {
        auto &sort = static_cast<ObLogSort &>(op);
        if (sort.get_part_cnt() || sort.is_fetch_with_ties())
          throw std::runtime_error("Native Sirius plan: partition/ties sort needs normalization");
        std::vector<std::string> keys;
        for (int64_t i = 0; i < sort.get_sort_keys().count(); ++i) {
          const auto &key = sort.get_sort_keys().at(i);
          keys.push_back("{\"expr\":" + expression(key.expr_, result.columns) + ",\"direction\":" + std::to_string(key.order_type_) + '}');
        }
        result.json = "{\"op\":\"sort\",\"input\":" + result.json + ",\"keys\":" + array(keys) + '}';
        if (sort.get_topn_expr())
          result.json = "{\"op\":\"fetch\",\"input\":" + result.json
              + ",\"count\":" + expression(sort.get_topn_expr(), {}) + ",\"offset\":null}";
        break;
      }
      case LOG_LIMIT: {
        auto &limit = static_cast<ObLogLimit &>(op);
        if (limit.get_percent_expr() || !limit.get_ties_ordering().empty()) throw std::runtime_error("Native Sirius plan: percent/ties limit");
        result.json = "{\"op\":\"fetch\",\"input\":" + result.json
            + ",\"count\":" + (limit.get_limit_expr() ? expression(limit.get_limit_expr(), {}) : "null")
            + ",\"offset\":" + (limit.get_offset_expr() ? expression(limit.get_offset_expr(), {}) : "null") + '}';
        break;
      }
      // These operators change execution placement/materialization, not rows.
      // Partial aggregate operators are independently rejected above.
      case LOG_TEMP_TABLE_INSERT: case LOG_EXCHANGE: case LOG_MATERIAL: case LOG_GRANULE_ITERATOR: break;
      default: throw std::runtime_error("Native Sirius plan: unsupported operator " + std::string(op.get_name()));
    }
  }
  if (op.get_type() != LOG_TABLE_SCAN || static_cast<ObLogTableScan &>(op).get_pushdown_aggr_exprs().empty()) {
    filter(result, op.get_startup_exprs());
    filter(result, op.get_filter_exprs());
  }
  // seekdb can satisfy ORDER BY via index direction or merge operators,
  // without a LOG_SORT. Arrow scans and GPU hash joins do not preserve that
  // physical property. Restore it explicitly before a parent LIMIT can run.
  if (ordered && (op.get_type() == LOG_TABLE_SCAN || op.get_type() == LOG_JOIN
      || op.get_type() == LOG_GROUP_BY) && !op.get_op_ordering().empty()) {
    std::vector<std::string> keys;
    for (int64_t i = 0; i < op.get_op_ordering().count(); ++i) {
      const auto &key = op.get_op_ordering().at(i);
      if (external(key.expr_, result.columns)) break;
      keys.push_back("{\"expr\":" + expression(key.expr_, result.columns)
          + ",\"direction\":" + std::to_string(key.order_type_) + '}');
    }
    if (!keys.empty()) result.json = "{\"op\":\"sort\",\"input\":" + result.json + ",\"keys\":" + array(keys) + '}';
  }
  if (op.get_type() == LOG_TABLE_SCAN) {
    auto &scan = static_cast<ObLogTableScan &>(op);
    if (scan.get_limit_expr() || scan.get_offset_expr())
      result.json = "{\"op\":\"fetch\",\"input\":" + result.json
          + ",\"count\":" + (scan.get_limit_expr() ? expression(scan.get_limit_expr(), {}) : "null")
          + ",\"offset\":" + (scan.get_offset_expr() ? expression(scan.get_offset_expr(), {}) : "null") + '}';
  }
  project(result, op.get_output_exprs());
  return result;
}
}
std::string sirius_plan_literal(const ObObj &value) {
  if (value.is_null()) return "null";
  if (value.is_string_type()) return quoted(value.get_string());
  if (value.get_type() == ObMySQLDateType) {
    const auto d = value.get_mysql_date();
    int32_t date = 0;
    if (!d.year_ || !d.month_ || !d.day_ || ObTimeConverter::mdate_to_date(d, date, 0) != OB_SUCCESS)
      throw std::runtime_error("Native Sirius plan: invalid MySQL date");
    return '\"' + std::to_string(date) + '\"';
  }
  if (value.is_date()) return '"' + std::to_string(value.get_date()) + '"';
  if (!value.is_numeric_type()) throw std::runtime_error("Native Sirius plan: unsupported parameter/literal type");
  char buffer[256]; int64_t pos = 0;
  if (value.print_plain_str_literal(buffer, sizeof(buffer), pos) != OB_SUCCESS)
    throw std::runtime_error("Native Sirius plan: cannot encode numeric literal");
  return quoted(ObString(pos, buffer));
}
std::string export_sirius_plan(ObLogPlan &plan) {
  if (!plan.get_plan_root()) throw std::runtime_error("Native Sirius plan: empty plan");
  if (!plan.get_stmt() || !plan.get_stmt()->is_select_stmt()
      || static_cast<const ObSelectStmt *>(plan.get_stmt())->has_for_update())
    throw std::runtime_error("Native Sirius plan: requires a non-locking SELECT");
  const bool ordered = plan.get_stmt() && !plan.get_stmt()->get_order_items().empty();
  ExportContext context;
  auto result = relation(*plan.get_plan_root(), ordered, context);
  if (!result.pending.empty()) throw std::runtime_error("Native Sirius plan: unresolved correlation");
  std::vector<std::string> materialized;
  for (const auto &item : context.materialized) materialized.push_back(item.value.json);
  auto document = "{\"version\":1,\"materialized\":" + array(materialized) + ",\"root\":" + result.json + '}';
  if (document.size() > 16 * 1024 * 1024) throw std::runtime_error("Native Sirius plan exceeds 16 MiB");
  return document;
}
} }
