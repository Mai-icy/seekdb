// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX SQL_ENG
#include "sirius_query_result.h"
#include "sirius_query_transport.h"
#include "sirius_binary_column.h"
#include "common/timezone/ob_time_convert.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_inner_sql_connection.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/object/ob_obj_cast.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include <algorithm>
#include <set>
#include <vector>
#include <cstring>
#include <memory>
#include <map>

namespace oceanbase { namespace sql {
using namespace common;
using namespace share;
using namespace share::schema;
namespace {
thread_local const transaction::ObTxReadSnapshot *input_snapshot = nullptr;
std::string text(const ObString &s) { return std::string(s.ptr(), s.length()); }
bool identifier(const std::string &s) {
  return !s.empty() && s.size() <= 64 && s[0] >= 'a' && s[0] <= 'z'
      && s.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_") == std::string::npos;
}
std::string format(const ObColumnSchemaV2 &column) {
  switch (column.get_data_type()) {
    case ObTinyIntType: return "c";
    case ObSmallIntType: return "s";
    case ObMediumIntType: case ObInt32Type: return "i";
    case ObIntType: return "l";
    case ObFloatType: return "f";
    case ObDoubleType: return "g";
    case ObDateType: case ObMySQLDateType: return "tdD";
    case ObCharType: case ObVarcharType:
      if (column.get_collation_type() != CS_TYPE_UTF8MB4_BIN &&
          column.get_collation_type() != CS_TYPE_BINARY)
        throw std::runtime_error("Sirius adapter requires binary text collation");
      return "u";
    case ObNumberType: case ObDecimalIntType: {
      const auto precision = column.get_accuracy().get_precision();
      const auto scale = column.get_accuracy().get_scale();
      if (precision >= 1 && precision <= 38 && scale >= 0 && scale <= precision)
        return "d:" + std::to_string(precision) + "," + std::to_string(scale);
      break;
    }
    default: break;
  }
  throw std::runtime_error("Sirius input supports signed integers, DECIMAL(<=38), DATE, CHAR/VARCHAR and FLOAT/DOUBLE");
}
struct InputTable {
  std::string name;
  std::vector<const ObColumnSchemaV2 *> columns;
  std::string identity;
};
std::vector<InputTable> input_tables(ObResultSet &result) {
  auto &ctx = result.get_exec_context();
  auto *guard = ctx.get_sql_ctx()->schema_guard_;
  auto *session = ctx.get_my_session();
  std::vector<InputTable> tables;
  std::set<uint64_t> ids;
  const auto &dependencies = result.get_physical_plan()->get_dependency_table();
  for (int64_t i = 0; i < dependencies.count(); ++i) {
    const auto &dependency = dependencies.at(i);
    if (dependency.get_type() != DEPENDENCY_TABLE) continue;
    const ObTableSchema *table = nullptr;
    const int status = guard->get_table_schema(dependency.get_object_id(), table);
    if (status != OB_SUCCESS || !table) throw std::runtime_error("Cannot resolve Sirius input table schema");
    if (table->is_index_table()) continue;
    if (!table->is_user_table() || table->is_tmp_table()
        || table->get_database_id() != session->get_database_id())
      throw std::runtime_error("Sirius whole-query input requires ordinary tables in the current database");
    if (!ids.insert(table->get_table_id()).second) continue;
    InputTable input{text(table->get_table_name_str()), {}};
    input.identity = std::to_string(table->get_table_id()) + ":" +
                     std::to_string(table->get_schema_version());
    if (!identifier(input.name)) throw std::runtime_error("Sirius input table names must be lowercase identifiers");
    for (auto it = table->column_begin(); it != table->column_end(); ++it) {
      const auto *column = *it;
      if (column->is_hidden()) continue;
      if (column->is_generated_column() || !identifier(text(column->get_column_name_str())))
        throw std::runtime_error("Sirius input requires ordinary columns with lowercase identifiers");
      (void)format(*column);
      input.columns.push_back(column);
    }
    if (input.columns.empty() || input.columns.size() > 64)
      throw std::runtime_error("Sirius input requires 1..64 columns per table");
    tables.push_back(std::move(input));
  }
  if (tables.empty() || tables.size() > 64) throw std::runtime_error("Sirius query requires 1..64 input tables");
  return tables;
}

int send_inputs(ObExecContext &ctx, const std::vector<InputTable> &tables, SiriusQueryTransport &transport) {
  int ret = OB_SUCCESS;
  auto *session = ctx.get_my_session();
  sqlclient::ObISQLConnectionGuard connection;
  if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(session, connection))) {
  } else {
    auto *conn = as_inner_sql_connection(connection.get_ptr());
    if (!conn) return OB_ERR_UNEXPECTED;
    ObSQLSessionInfo::StmtSavedValue saved;
    ObIInnerSQLConnection::SavedValue saved_connection;
    if (OB_FAIL(conn->begin_nested_session(saved, saved_connection, false))) return ret;
    struct Restore {
      ObIInnerSQLConnection &conn;
      ObSQLSessionInfo::StmtSavedValue &saved;
      ObIInnerSQLConnection::SavedValue &connection;
      int &ret;
      const transaction::ObTxReadSnapshot *prior;
      ~Restore() {
        input_snapshot = prior;
        const int status = conn.end_nested_session(saved, connection);
        if (ret == OB_SUCCESS) ret = status;
      }
    } restore{*conn, saved, saved_connection, ret, input_snapshot};
    input_snapshot = &ctx.get_das_ctx().get_snapshot();
    conn->set_check_priv(true);
    // Each cursor owns an inner connection/read context, while all use this
    // external SQL session and the outer SELECT's fixed transaction snapshot.
    // Only this SQL worker accesses them, in response to serialized requests.
    struct Cursor {
      sqlclient::ObISQLConnectionGuard connection;
      ObISQLClient::ReadResult read;
      InputTable projected;
      std::vector<uint32_t> selection;
      bool opened = false;
      bool eof = false;
    };
    std::map<uint32_t, std::pair<uint32_t, std::unique_ptr<Cursor>>> cursors;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) break;
      std::string request;
      const char kind = transport.receive(request);
      if (kind == 'D' && request.empty()) break;
      if (kind != 'N') throw std::runtime_error("Expected Sirius next-batch request");
      SiriusWireReader demand{request};
      const auto stream_id = demand.integer();
      const auto index = demand.integer();
      if (index >= tables.size()) throw std::runtime_error("Invalid Sirius input table index");
      auto found = cursors.find(stream_id);
      if (found == cursors.end()) {
        if (cursors.size() >= 64) throw std::runtime_error("Too many Sirius scan occurrences");
        found = cursors.emplace(stream_id, std::make_pair(index, std::make_unique<Cursor>())).first;
      }
      if (found->second.first != index) throw std::runtime_error("Sirius cursor table changed");
      auto &cursor = *found->second.second;
      const auto width = demand.integer();
      if (!width || width > tables[index].columns.size()) throw std::runtime_error("Invalid Sirius projected width");
      std::vector<uint32_t> selected;
      for (uint32_t i = 0; i < width; ++i) {
        const auto c = demand.integer();
        if (c >= tables[index].columns.size() || std::find(selected.begin(), selected.end(), c) != selected.end())
          throw std::runtime_error("Invalid Sirius projected column");
        selected.push_back(c);
      }
      demand.end();
      if (cursor.opened && cursor.selection != selected) throw std::runtime_error("Sirius cursor projection changed");
      if (!cursor.opened) {
        cursor.selection = selected; cursor.projected.name = tables[index].name;
        for (auto c : selected) cursor.projected.columns.push_back(tables[index].columns[c]);
      }
      const auto &table = cursor.projected;
      if (!cursor.opened) {
        if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(session, cursor.connection))) break;
        auto *scan = as_inner_sql_connection(cursor.connection.get_ptr());
        if (!scan) { ret = OB_ERR_UNEXPECTED; break; }
        scan->set_check_priv(true);
        ObSqlString sql;
        if (OB_FAIL(sql.append("SELECT /*+ PARALLEL(1) */ "))) break;
        for (size_t c = 0; OB_SUCC(ret) && c < table.columns.size(); ++c) {
          const auto name = table.columns[c]->get_column_name_str();
          ret = sql.append_fmt("%s`%.*s`", c ? "," : "", name.length(), name.ptr());
        }
        if (OB_SUCC(ret)) ret = sql.append_fmt(" FROM `%s`", table.name.c_str());
        if (OB_FAIL(ret) || OB_FAIL(scan->execute_read(sql.string(), cursor.read))) break;
        cursor.opened = true;
      }
      if (cursor.eof) { transport.send('E'); continue; }
      auto *rows = cursor.read.get_result();
      if (!rows) { ret = OB_ERR_UNEXPECTED; break; }
      std::vector<SiriusBinaryColumn> buffers;
      for (const auto *column : table.columns) {
        const auto f = format(*column);
        const unsigned width = f == "c" ? 1 : f == "s" ? 2
            : f == "i" || f == "f" || f == "tdD" ? 4 : f[0] == 'd' ? 16 : 8;
        buffers.emplace_back(width, f == "u");
      }
      uint32_t count = 0;
      auto consume = [&](const ObNewRow &row) -> int {
        if (row.get_count() != static_cast<int64_t>(table.columns.size())) return OB_ERR_UNEXPECTED;
        for (size_t c = 0; OB_SUCC(ret) && c < table.columns.size(); ++c) {
          const ObObj &value = row.get_cell(c);
          {
            auto &buffer = buffers[c]; buffer.mark(count, !value.is_null());
            if (!value.is_null()) {
              const auto type = table.columns[c]->get_data_type();
              if (buffer.variable) {
                const auto str = value.get_string(); buffer.values.append(str.ptr(), str.length());
              } else if (type == ObFloatType || type == ObDoubleType) {
                uint64_t bits = 0;
                if (type == ObFloatType) { const float v = value.get_float(); uint32_t b; std::memcpy(&b, &v, 4); bits = b; }
                else { const double v = value.get_double(); std::memcpy(&bits, &v, 8); }
                sirius_append_le(buffer.values, bits, buffer.width);
              } else if (type == ObMySQLDateType || type == ObDateType) {
                int32_t date = 0;
                if (type == ObMySQLDateType) {
                  const auto d = value.get_mysql_date();
                  if (!d.year_ || !d.month_ || !d.day_) throw std::runtime_error("Zero/incomplete dates are not supported by Sirius");
                  ret = ObTimeConverter::mdate_to_date(d, date, 0);
                } else date = value.get_date();
                sirius_append_le(buffer.values, static_cast<uint32_t>(date), 4);
              } else if (buffer.width == 16) {
                const auto accuracy = table.columns[c]->get_accuracy();
                if (value.is_decimal_int() && value.get_scale() == accuracy.get_scale()) {
                  sirius_append_decimal_storage(buffer.values, value.get_decimal_int(), value.get_int_bytes());
                } else if (value.is_number()) {
                  const auto desc = value.get_number_desc();
                  sirius_append_number_digits(buffer.values, value.get_number_digits(), desc.len_,
                      number::ObNumber::get_decode_exp(desc.desc_), desc.sign_ == number::ObNumber::NEGATIVE,
                      accuracy.get_precision(), accuracy.get_scale());
                } else {
                  char text[256]; int64_t pos = 0;
                  if (OB_SUCC(ret = value.print_plain_str_literal(text, sizeof(text), pos)))
                    sirius_append_decimal(buffer.values, text, pos, accuracy.get_precision(), accuracy.get_scale());
                }
              } else sirius_append_le(buffer.values, static_cast<uint64_t>(value.get_int()), buffer.width);
            }
            buffer.finish(value.is_null());
          }
        }
        if (OB_SUCC(ret)) {
          ++count;
          size_t bytes = 4;
          for (const auto &buffer : buffers) bytes += buffer.size();
          if (bytes >= 4 * 1024 * 1024) return OB_ITER_END;
        }
        return ret;
      };
      ret = query::ObInnerSQLConnectionAccess::consume_rows(rows, 65536,
          [](const ObNewRow &row, void *context) { return (*static_cast<decltype(consume) *>(context))(row); },
          &consume, cursor.eof);
      if (OB_FAIL(ret)) break;
      if (count) {
        SiriusWireWriter frame; frame.integer(count);
        for (const auto &buffer : buffers) {
          frame.string(buffer.validity); frame.string(buffer.offsets); frame.string(buffer.values);
        }
        transport.send('B', frame.data);
      } else transport.send('E');
    }

  }
  return ret;
}
} // namespace

const transaction::ObTxReadSnapshot *sirius_input_snapshot() { return input_snapshot; }

int SiriusQueryResult::create_if_requested(ObResultSet &result, SiriusQueryResult *&out) {
  int ret = OB_SUCCESS;
  auto &ctx = result.get_exec_context();
  auto *session = ctx.get_my_session();
  int64_t backend = 0;
  ObString snapshot;
  auto *plan = result.get_physical_plan();
  if (OB_FAIL(session->get_sys_variable(SYS_VAR_EXECUTION_BACKEND, backend))) {
  } else if (OB_FAIL(session->get_sys_variable(SYS_VAR_SIRIUS_SNAPSHOT, snapshot))) {
  } else if (plan->get_dependency_table_size() == 0) {
    // SELECT @@session variables / constants are control queries, not table reads.
  } else if (!backend && !snapshot.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sirius_snapshot requires execution_backend='gpu'; clear it before CPU table reads");
  } else if (backend) {
    if (!plan->is_plain_select() || plan->has_for_update()
        || ctx.get_sql_ctx()->is_prepare_protocol_ || ctx.get_sql_ctx()->is_text_ps_mode_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Sirius whole-query execution requires a plain, non-locking text SELECT");
    } else if (!snapshot.empty() && !OB_PRIV_HAS_ANY(session->get_user_priv_set(), OB_PRIV_SUPER)) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "SUPER for immutable Sirius snapshot reads");
    } else {
      void *memory = ctx.get_allocator().alloc(sizeof(SiriusQueryResult));
      if (!memory) ret = OB_ALLOCATE_MEMORY_FAILED;
      else out = new(memory) SiriusQueryResult(result, ctx.get_allocator());
    }
  }
  return ret;
}

int SiriusQueryResult::open(ObExecContext &ctx) {
  int ret = OB_SUCCESS;
  int cancel_ret = OB_SUCCESS;
  SiriusQueryTransport transport;
  try {
    auto *session = ctx.get_my_session();
    ObString snapshot;
    if (OB_FAIL(session->get_sys_variable(SYS_VAR_SIRIUS_SNAPSHOT, snapshot))) return ret;
    const auto tables = input_tables(result_);
    const auto *fields = result_.get_field_columns();
    if (!fields || fields->count() < 1 || fields->count() > 1024) return OB_NOT_SUPPORTED;
    auto cancelled = [](void *p) { auto &status = *static_cast<int *>(p); status = THIS_WORKER.check_status(); return status != OB_SUCCESS; };
    transport.open(std::max<int64_t>(1, THIS_WORKER.get_timeout_remain() / 1000), cancelled, &cancel_ret);
    SiriusWireWriter request;
    request.integer(5); request.string(text(snapshot));
    request.string(std::to_string(session->get_priv_user_id()));
    request.string(text(session->get_database_name()));
    const auto &read_view = ctx.get_das_ctx().get_snapshot();
    request.string(std::to_string(read_view.version().get_val_for_tx()) + ":" +
                   std::to_string(read_view.tx_id().get_id()) + ":" +
                   std::to_string(read_view.tx_seq().get_branch()) + ":" +
                   std::to_string(read_view.tx_seq().get_seq()));
    request.string(text(ctx.get_sql_ctx()->cur_sql_)); request.integer(fields->count()); request.integer(tables.size());
    for (const auto &table : tables) {
      request.string(table.name); request.string(table.identity); request.integer(table.columns.size());
      for (const auto *column : table.columns) {
        request.string(text(column->get_column_name_str())); request.string(format(*column));
      }
    }
    transport.send('W', request.data);
    {
      std::string ack;
      if (transport.receive(ack) != 'A') throw std::runtime_error("Missing Sirius input admission");
      SiriusWireReader admission{ack}; const auto live = admission.integer(); admission.end();
      if (live == 1) {
        if (OB_FAIL(send_inputs(ctx, tables, transport))) return ret;
      } else if (live != 0 || snapshot.empty()) {
        throw std::runtime_error("Invalid Sirius input admission mode");
      }
    }
    std::string response;
    if (transport.receive(response) != 'S') throw std::runtime_error("Missing Sirius result schema");
    SiriusWireReader schema{response};
    if (schema.integer() != fields->count()) throw std::runtime_error("Sirius result column count mismatch");
    schema.end();
    row_.count_ = fields->count();
    row_.cells_ = static_cast<ObObj *>(ctx.get_allocator().alloc(sizeof(ObObj) * row_.count_));
    if (!row_.cells_) return OB_ALLOCATE_MEMORY_FAILED;
    for (int64_t i = 0; i < row_.count_; ++i) new(row_.cells_ + i) ObObj();
    ObArenaAllocator cells_allocator(ObModIds::OB_SQL_ROW_STORE);
    size_t result_bytes = 0, wire_bytes = 0;
    for (;;) {
      const char kind = transport.receive(response);
      if (kind == 'Z' && response.empty()) break;
      if (kind != 'R') throw std::runtime_error("Invalid Sirius result frame");
      wire_bytes += response.size();
      if (wire_bytes > 64 * 1024 * 1024) throw std::runtime_error("Sirius wire result exceeds 64 MiB");
      SiriusWireReader data{response}; const auto count = data.integer();
      if (!count || count > 1024) throw std::runtime_error("Invalid Sirius result row count");
      for (uint32_t r = 0; r < count; ++r) {
        cells_allocator.reuse();
        for (int64_t c = 0; c < row_.count_; ++c) {
          std::string value;
          if (!data.cell(value)) row_.cells_[c].set_null();
          else {
            ObObj input;
            ObString copied;
            if (OB_FAIL(ob_write_string(cells_allocator, ObString(value.size(), value.data()), copied))) return ret;
            input.set_varchar(copied); input.set_collation_type(CS_TYPE_UTF8MB4_BIN);
            ObAccuracy accuracy = fields->at(c).accuracy_;
            ObCastCtx cast(&cells_allocator, nullptr, CM_NONE,
                           fields->at(c).type_.get_collation_type(), &accuracy);
            if (OB_FAIL(ObObjCaster::to_type(fields->at(c).type_.get_type(), cast, input, row_.cells_[c]))) return ret;
          }
        }
        result_bytes += row_.get_deep_copy_size();
        if (result_bytes > 64 * 1024 * 1024) throw std::runtime_error("Materialized Sirius result exceeds 64 MiB");
        if (OB_FAIL(rows_.add_row(row_))) return ret;
      }
      data.end();
    }
    iterator_ = rows_.begin();
    LOG_INFO("Sirius whole-query GPU execution completed", K(snapshot), K(result_bytes));
  } catch (const std::exception &error) {
    ret = cancel_ret != OB_SUCCESS ? cancel_ret : OB_NOT_SUPPORTED;
    LOG_WARN("Sirius whole-query execution failed", K(ret), "reason", error.what());
    if (cancel_ret == OB_SUCCESS) LOG_USER_ERROR(OB_NOT_SUPPORTED, error.what());
  }
  return ret;
}
int SiriusQueryResult::get_next_row(ObExecContext &ctx, const ObNewRow *&row) {
  UNUSED(ctx);
  int ret = THIS_WORKER.check_status();
  if (ret == OB_SUCCESS) ret = iterator_.get_next_row(row_);
  row = &row_; return ret;
}
int SiriusQueryResult::close(ObExecContext &ctx) {
  UNUSED(ctx); iterator_.reset(); rows_.reset(); return OB_SUCCESS;
}
} }
