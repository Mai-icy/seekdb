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
#define USING_LOG_PREFIX SQL_ENG
#include "sirius_query_result.h"
#include "sirius_plan_export.h"
#include "rust/parquet_export.h"
#include "sql/ob_result_set.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_inner_sql_connection.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/object/ob_obj_cast.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "common/timezone/ob_time_convert.h"
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <set>
#include <memory>
#include <chrono>
#include <thread>
#if defined(__linux__) || defined(__APPLE__)
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace oceanbase { namespace sql {
using namespace common;
using namespace share;
using namespace share::schema;
namespace {
std::string text(const ObString &s) { return std::string(s.ptr(), s.length()); }
std::string required_string(WireReader &reader) {
  std::string value;
  if (!reader.cell(value) || value.empty() || value.size() > 256 || value.find('\0') != std::string::npos)
    throw std::runtime_error("Invalid GPU input identifier");
  return value;
}
std::string quote_identifier(const std::string &s) {
  std::string result = "`";
  for (char c : s) { result += c; if (c == '`') result += c; }
  return result + '`';
}
uint8_t input_kind(const ObColumnSchemaV2 &column) {
  switch (column.get_data_type()) {
    case ObTinyIntType: return 0;
    case ObSmallIntType: return 1;
    case ObMediumIntType: case ObInt32Type: return 2;
    case ObIntType: return 3;
    case ObFloatType: return 4;
    case ObDoubleType: return 5;
    case ObCharType: case ObVarcharType:
      if (column.get_collation_type() == CS_TYPE_UTF8MB4_BIN) return 6;
      break;
    case ObDateType: case ObMySQLDateType: return 8;
    case ObNumberType: case ObDecimalIntType:
      if (column.get_accuracy().get_precision() > 0 && column.get_accuracy().get_precision() <= 38
          && column.get_accuracy().get_scale() >= 0
          && column.get_accuracy().get_scale() <= column.get_accuracy().get_precision()) return 9;
      break;
    default: break;
  }
  throw std::runtime_error("GPU Parquet inputs support signed integers, floats, utf8mb4_bin text, DATE and DECIMAL(<=38)");
}
std::string output_type(const ObField &field) {
  switch (field.type_.get_type()) {
    case ObTinyIntType: return "i8";
    case ObSmallIntType: return "i16";
    case ObMediumIntType: case ObInt32Type: return "i32";
    case ObIntType: return "i64";
    case ObFloatType: return "fp32";
    case ObDoubleType: return "fp64";
    case ObCharType: case ObVarcharType: return "string";
    case ObDateType: case ObMySQLDateType: return "date";
    case ObNumberType: case ObDecimalIntType:
      return "decimal:" + std::to_string(field.accuracy_.get_precision()) + ":" + std::to_string(field.accuracy_.get_scale());
    default: throw std::runtime_error("Unsupported GPU result column type");
  }
}
int cell_value(const ObObj &obj, const parquet_export::ColumnSpec &column,
    parquet_export::BatchCell &value, std::vector<uint8_t> &arena, int64_t &bytes) {
  int ret = OB_SUCCESS;
  if (obj.is_null()) value.is_null = true;
  else if (obj.is_integer_type()) value.integer = obj.get_int();
  else if (obj.is_float()) value.floating = obj.get_float();
  else if (obj.is_double()) value.floating = obj.get_double();
  else if (obj.get_type() == ObDecimalIntType && column.kind == 9
      && obj.get_scale() == column.scale
      && (obj.get_int_bytes() == sizeof(int32_t) || obj.get_int_bytes() == sizeof(int64_t))) {
    // The source already stores an exact scaled integer. Copy it without
    // formatting text, retaining schema/precision validation in the writer.
    if (!obj.get_decimal_int()) return OB_ERR_UNEXPECTED;
    if (obj.get_int_bytes() == sizeof(int32_t)) {
      int32_t integer;
      std::memcpy(&integer, obj.get_decimal_int(), sizeof(integer));
      value.integer = integer;
    } else {
      std::memcpy(&value.integer, obj.get_decimal_int(), sizeof(value.integer));
    }
    value.decimal_integer = true;
  } else if (obj.get_type() == ObNumberType || obj.get_type() == ObDecimalIntType) {
    char buffer[256];
    int64_t size = 0;
    if (OB_FAIL(obj.print_plain_str_literal(buffer, sizeof(buffer), size))) return ret;
    bytes += size;
    if (bytes > 8 * 1024 * 1024) return OB_SIZE_OVERFLOW;
    value.offset = arena.size(); value.length = size;
    arena.insert(arena.end(), buffer, buffer + size);
  } else if (obj.is_string_type()) {
    const auto str = obj.get_string();
    bytes += str.length();
    if (bytes > 8 * 1024 * 1024) return OB_SIZE_OVERFLOW;
    value.offset = arena.size(); value.length = str.length();
    if (str.length()) arena.insert(arena.end(), str.ptr(), str.ptr() + str.length());
  } else if (obj.get_type() == ObMySQLDateType) {
    const auto date = obj.get_mysql_date();
    int32_t days = 0;
    if (!date.year_ || !date.month_ || !date.day_) ret = OB_INVALID_DATE_VALUE;
    else if (OB_FAIL(ObTimeConverter::mdate_to_date(date, days, 0))) {}
    else value.integer = days;
  } else if (obj.is_date()) value.integer = obj.get_date();
  else ret = OB_NOT_SUPPORTED;
  return ret;
}
}

GpuQueryResult::~GpuQueryResult() { cleanup(); }
void GpuQueryResult::cleanup() {
  // Closing the socket cancels the isolated worker. Unlinked files already open
  // in that worker remain valid until it exits; later opens fail safely.
  transport_.close();
#if defined(__linux__) || defined(__APPLE__)
  for (const auto &path : files_) ::unlink(path.c_str());
  if (!directory_.empty()) ::rmdir(directory_.c_str());
#endif
  files_.clear();
  directory_.clear();
}

int GpuQueryResult::create_if_requested(ObResultSet &result, GpuQueryResult *&out) {
  int ret = OB_SUCCESS;
  auto &ctx = result.get_exec_context();
  int64_t backend = 0;
  auto *plan = result.get_physical_plan();
  if (OB_FAIL(ctx.get_my_session()->get_sys_variable(SYS_VAR_EXECUTION_BACKEND, backend))) {}
  else if (!backend || plan->get_dependency_table_size() == 0) {}
  else if (!plan->is_plain_select() || plan->is_contains_assignment()
      || ctx.get_sql_ctx()->is_prepare_protocol_ || ctx.get_sql_ctx()->is_text_ps_mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "GPU execution requires a read-only text SELECT");
  } else {
    void *memory = ctx.get_allocator().alloc(sizeof(GpuQueryResult));
    if (!memory) ret = OB_ALLOCATE_MEMORY_FAILED;
    else out = new(memory) GpuQueryResult(result, ctx.get_allocator());
  }
  return ret;
}

int GpuQueryResult::describe_snapshot(ObExecContext &ctx, WireWriter &request) {
  int ret = OB_SUCCESS;
  auto *session = ctx.get_my_session();
  ObString label;
  if (OB_FAIL(session->get_sys_variable(SYS_VAR_GPU_SNAPSHOT, label))) return ret;
  const auto name = text(label);
  if (name.size() > 128 || name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-") != std::string::npos)
    throw std::runtime_error("gpu_snapshot must contain at most 128 letters, digits, underscores, dots or hyphens");
  request.string(name);
  const char *source = std::getenv("SEEKDB_SIRIUS_SOURCE_ID");
  if (!name.empty() && (!source || !*source || std::strlen(source) > 256))
    throw std::runtime_error("Set SEEKDB_SIRIUS_SOURCE_ID to a stable unique source identifier");
  request.string(source ? source : "");
  request.string(std::to_string(session->get_user_id()));
  request.string(std::to_string(session->get_database_id()));
  request.string(std::to_string(ctx.get_das_ctx().get_snapshot().version().get_val_for_tx()));
  snapshot_tables_.clear();
  if (name.empty()) { request.integer(0); return ret; }
  bool autocommit = false;
  if (OB_FAIL(session->get_autocommit(autocommit))) return ret;
  if (!autocommit || session->has_explicit_start_trans() || ctx.get_das_ctx().get_snapshot().has_write_state())
    throw std::runtime_error("Named GPU snapshots require autocommit outside an explicit transaction; use gpu_snapshot='' for transactional reads");
  ObSEArray<const ObTableSchema *, 16> schemas;
  if (OB_FAIL(ctx.get_sql_ctx()->schema_guard_->get_table_schemas_in_database(session->get_database_id(), schemas))) return ret;
  std::vector<const ObTableSchema *> tables;
  for (int64_t i = 0; i < schemas.count(); ++i) {
    if (schemas.at(i)->is_user_table() && !schemas.at(i)->is_tmp_table() && !schemas.at(i)->is_index_table())
      tables.push_back(schemas.at(i));
  }
  if (tables.empty() || tables.size() > 64) throw std::runtime_error("Named GPU snapshots require 1..64 ordinary tables in the current database");
  std::sort(tables.begin(), tables.end(), [](const auto *a, const auto *b) { return a->get_table_id() < b->get_table_id(); });
  request.integer(tables.size());
  for (const auto *table : tables) {
    snapshot_tables_.push_back(table->get_table_id());
    request.string(text(session->get_database_name()));
    request.string(text(table->get_table_name_str()));
    request.string(std::to_string(table->get_table_id()));
    request.string(std::to_string(table->get_schema_version()));
    std::vector<std::string> columns;
    for (auto it = table->column_begin(); it != table->column_end(); ++it) {
      if ((*it)->is_hidden()) continue;
      if ((*it)->is_generated_column()) throw std::runtime_error("Named GPU snapshots do not support generated columns");
      input_kind(**it); // Reject unsupported types before exporting any table.
      columns.push_back(text((*it)->get_column_name_str()));
    }
    if (columns.empty() || columns.size() > 1024) throw std::runtime_error("Invalid GPU snapshot column count");
    request.integer(columns.size());
    for (const auto &column : columns) request.string(column);
  }
  return ret;
}

int GpuQueryResult::export_input(ObExecContext &ctx, const std::string &request) {
  int ret = OB_SUCCESS;
  WireReader reader{request};
  const auto database = required_string(reader);
  const auto name = required_string(reader);
  const auto count = reader.integer();
  if (count > 1024) throw std::runtime_error("Too many GPU inputs or columns");
  auto *session = ctx.get_my_session();
  const ObTableSchema *table = nullptr;
  // Resolve only dependencies of the authorized outer query. The service cannot
  // ask this connection to read unrelated tables.
  const auto &dependencies = result_.get_physical_plan()->get_dependency_table();
  for (int64_t i = 0; i < dependencies.count(); ++i) {
    if (dependencies.at(i).get_type() != DEPENDENCY_TABLE) continue;
    const ObTableSchema *candidate = nullptr;
    if (OB_FAIL(ctx.get_sql_ctx()->schema_guard_->get_table_schema(dependencies.at(i).get_object_id(), candidate))) return ret;
    if (candidate && !candidate->is_index_table() && text(candidate->get_table_name_str()) == name
        && candidate->get_database_id() == session->get_database_id()
        && database == text(session->get_database_name())) table = candidate;
  }
  if (!table) {
    for (const auto id : snapshot_tables_) {
      const ObTableSchema *candidate = nullptr;
      if (OB_FAIL(ctx.get_sql_ctx()->schema_guard_->get_table_schema(id, candidate))) return ret;
      if (candidate && text(candidate->get_table_name_str()) == name && database == text(session->get_database_name())) table = candidate;
    }
  }
  if (!table || !table->is_user_table() || table->is_tmp_table())
    throw std::runtime_error("GPU inputs require ordinary tables in the current database");
  std::vector<parquet_export::ColumnSpec> specs;
  std::set<std::string> selected;
  std::string query = "SELECT /*+ PARALLEL(1) */ ";
  auto add_column = [&](const ObColumnSchemaV2 *column) {
    if (!column || column->is_hidden() || column->is_generated_column())
      throw std::runtime_error("GPU input requires ordinary visible columns");
    const auto column_name = text(column->get_column_name_str());
    if (!selected.insert(column_name).second) throw std::runtime_error("Duplicate GPU input column");
    if (!specs.empty()) query += ',';
    query += quote_identifier(column_name);
    const uint8_t kind = input_kind(*column);
    specs.push_back({rust::String(column_name), kind,
        static_cast<uint8_t>(kind == 9 ? column->get_accuracy().get_precision() : 0),
        static_cast<uint8_t>(kind == 9 ? column->get_accuracy().get_scale() : 0)});
  };
  for (uint32_t i = 0; i < count; ++i) {
    const auto column = required_string(reader);
    add_column(table->get_column_schema(column.c_str()));
  }
  reader.end();
  // COUNT(*) still needs a physical row-count carrier in the Parquet file.
  if (specs.empty()) {
    for (auto it = table->column_begin(); it != table->column_end(); ++it) {
      if (!(*it)->is_hidden() && !(*it)->is_generated_column()) { add_column(*it); break; }
    }
  }
  if (specs.empty()) throw std::runtime_error("GPU input table has no supported columns");
  query += " FROM " + quote_identifier(database) + '.' + quote_identifier(name);
  uint64_t limit = 64 * 1024 * 1024;
  if (const char *configured = std::getenv("SEEKDB_SIRIUS_PART_BYTES")) {
    char *end = nullptr; limit = std::strtoull(configured, &end, 10);
    if (!*configured || *end || limit < 8192 || limit > 1024 * 1024 * 1024ULL)
      throw std::runtime_error("SEEKDB_SIRIUS_PART_BYTES must be 8192..1073741824");
  }
  uint64_t workers = 4;
  if (const char *configured = std::getenv("SEEKDB_SIRIUS_EXPORT_WORKERS")) {
    char *end = nullptr; workers = std::strtoull(configured, &end, 10);
    if (!*configured || *end || workers < 1 || workers > 16)
      throw std::runtime_error("SEEKDB_SIRIUS_EXPORT_WORKERS must be 1..16");
  }
  // Only owned batches enter Rust workers. The SQL session and its snapshot stay
  // on this query thread, including transactional reads of this session's writes.
  auto writer = parquet_export::open_parts(rust::Str(directory_),
      rust::Slice<const parquet_export::ColumnSpec>(specs), workers, limit);
  // Destroy the uploader before the Rust writer removes its private files.
  auto uploader = std::make_unique<PartUploader>(transport_);
  const auto export_started = std::chrono::steady_clock::now();
  uint64_t exported_rows = 0;
  uint64_t decimal_integer_cells = 0;
  int64_t upload_wait_us = 0;
  auto check_status = [&]() {
    cancel_ret_ = ctx.check_status();
    return cancel_ret_;
  };
  std::vector<parquet_export::BatchCell> values, row_values(specs.size());
  std::vector<uint8_t> arena, row_arena;
  const size_t max_batch_bytes = 8 * 1024 * 1024;
  auto poll = [&]() -> int {
    // Check errors even when the encoder has not completed another file yet.
    uploader->idle();
    for (const auto &file : parquet_export::poll_parts(*writer)) {
      const std::string path(file.data(), file.size());
      const auto started = std::chrono::steady_clock::now();
      while (!uploader->submit(path)) {
        const int status = check_status();
        if (status != OB_SUCCESS) return status;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      upload_wait_us += std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - started).count();
    }
    return OB_SUCCESS;
  };
  auto submit = [&]() -> int {
    if (values.empty()) return OB_SUCCESS;
    parquet_export::submit_parts(*writer, rust::Slice<const parquet_export::BatchCell>(values),
        rust::Slice<const uint8_t>(arena));
    values.clear(); arena.clear();
    do {
      const int status = check_status();
      if (status != OB_SUCCESS) return status;
      const int polled = poll();
      if (polled != OB_SUCCESS) return polled;
      if (!parquet_export::parts_ready(*writer)) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (!parquet_export::parts_ready(*writer));
    return OB_SUCCESS;
  };
  sqlclient::ObISQLConnectionGuard connection;
  if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(session, connection))) return ret;
  auto *conn = as_inner_sql_connection(connection.get_ptr());
  if (!conn) return OB_ERR_UNEXPECTED;
  ObSQLSessionInfo::StmtSavedValue saved;
  ObIInnerSQLConnection::SavedValue saved_connection;
  if (OB_FAIL(conn->begin_nested_session(saved, saved_connection, false))) return ret;
  {
    struct Restore {
      ObIInnerSQLConnection &conn;
      ObSQLSessionInfo &session;
      ObSQLSessionInfo::StmtSavedValue &saved;
      ObIInnerSQLConnection::SavedValue &connection;
      const transaction::ObTxReadSnapshot *prior;
      int &ret;
      ~Restore() {
        session.set_gpu_input_snapshot(prior);
        const int status = conn.end_nested_session(saved, connection);
        if (ret == OB_SUCCESS) ret = status;
      }
    } restore{*conn, *session, saved, saved_connection, session->get_gpu_input_snapshot(), ret};
    session->set_gpu_input_snapshot(&ctx.get_das_ctx().get_snapshot());
    conn->set_check_priv(true);
    ObISQLClient::ReadResult read;
    if (OB_FAIL(conn->execute_read(ObString(query.size(), query.data()), read))) {}
    else if (OB_ISNULL(read.get_result())) ret = OB_ERR_UNEXPECTED;
    else {
      auto &rows = *read.get_result();
      while (OB_SUCC(ret) && OB_SUCC(ret = rows.next())) {
        if (OB_FAIL(check_status())) break;
        row_arena.clear();
        int64_t bytes = specs.size() * 16;
        for (size_t i = 0; OB_SUCC(ret) && i < specs.size(); ++i) {
          ObObj obj;
          row_values[i] = {};
          if (OB_FAIL(rows.get_obj(i, obj))) {}
          else if (OB_FAIL(cell_value(obj, specs[i], row_values[i], row_arena, bytes))) {}
          else if (row_values[i].decimal_integer) ++decimal_integer_cells;
        }
        if (OB_SUCC(ret)) {
          if (!values.empty() && (values.size() / specs.size() >= 4096
              || (values.size() + row_values.size()) * sizeof(parquet_export::BatchCell) > max_batch_bytes
              || arena.size() + row_arena.size() > max_batch_bytes)) {
            if (OB_FAIL(submit())) break;
          }
          ++exported_rows;
          const auto offset = arena.size();
          for (auto &value : row_values) { value.offset += offset; values.push_back(value); }
          arena.insert(arena.end(), row_arena.begin(), row_arena.end());
        }
      }
      if (ret == OB_ITER_END) ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(submit())) {}
    else {
      parquet_export::finish_parts_input(*writer);
      do {
        if (OB_FAIL(check_status())) break;
        if (OB_FAIL(poll())) break;
        if (!parquet_export::parts_done(*writer)) std::this_thread::sleep_for(std::chrono::milliseconds(1));
      } while (!parquet_export::parts_done(*writer));
      while (OB_SUCC(ret) && !uploader->idle()) {
        if (OB_FAIL(check_status())) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      const auto uploaded = uploader->stats();
      uploader.reset(); // Join and restore query-thread cancellation before F.
      if (OB_SUCC(ret)) {
        transport_.send('F'); // All parts of this table have been acknowledged.
        const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - export_started).count();
        LOG_INFO("GPU Parquet input prepared", "table", name.c_str(), "rows", exported_rows,
            "files", uploaded.files, "bytes", uploaded.bytes, "elapsed_us", elapsed_us,
            "upload_us", uploaded.upload_us, "upload_backpressure_us", upload_wait_us,
            "decimal_integer_cells", decimal_integer_cells);
      }
    }
  }
  return ret;
}

int GpuQueryResult::open(ObExecContext &ctx) {
  int ret = OB_SUCCESS;
  cleanup();
  opened_ = done_ = false;
  rows_remaining_ = 0;
  cancel_ret_ = OB_SUCCESS;
  try {
    const auto &plan = result_.get_physical_plan()->get_sirius_plan();
    if (plan.empty()) throw std::runtime_error("Missing GPU plan; recompile with execution_backend='gpu'");
    const auto *fields = result_.get_field_columns();
    if (!fields || fields->count() < 1 || fields->count() > 1024) return OB_NOT_SUPPORTED;
#if defined(__linux__) || defined(__APPLE__)
    const char *root = std::getenv("SEEKDB_SIRIUS_EXPORT_DIR");
    if (!root || root[0] != '/') throw std::runtime_error("Set SEEKDB_SIRIUS_EXPORT_DIR to a private absolute directory");
    struct stat status{};
    if (::lstat(root, &status) || !S_ISDIR(status.st_mode) || status.st_uid != geteuid()
        || (status.st_mode & 0077)) throw std::runtime_error("GPU export directory must be owned by the server UID with mode 0700");
    std::string name = std::string(root) + "/query-XXXXXX";
    if (!::mkdtemp(name.data())) throw std::runtime_error("Cannot create GPU input directory");
    directory_ = name;
#else
    throw std::runtime_error("GPU query execution requires Linux or macOS");
#endif
    auto cancelled = [](void *p) {
      auto &result = *static_cast<GpuQueryResult *>(p);
      result.cancel_ret_ = result.result_.get_exec_context().check_status();
      return result.cancel_ret_ != OB_SUCCESS;
    };
    const int64_t timeout = std::min<int64_t>(UINT32_MAX, std::max<int64_t>(1, THIS_WORKER.get_timeout_remain() / 1000));
    transport_.open(timeout, cancelled, this);
    std::string document = text(plan);
    document.pop_back();
    document += ",\"parameters\":[";
    const auto &params = ctx.get_physical_plan_ctx()->get_param_store();
    for (int64_t i = 0; i < params.count(); ++i) {
      if (i) document += ',';
      document += sirius_plan_literal(params.at(i));
    }
    document += "]}";
    WireWriter request;
    request.integer(2);
    request.integer(timeout);
    request.string(document);
    request.integer(fields->count());
    for (int64_t i = 0; i < fields->count(); ++i) request.string(output_type(fields->at(i)));
    if (OB_FAIL(describe_snapshot(ctx, request))) { cleanup(); return ret; }
    transport_.send('Q', request.data);
    while (OB_SUCC(ret)) {
      const char kind = transport_.receive(response_);
      if (kind == 'I') {
        const auto input = response_;
        if (OB_FAIL(export_input(ctx, input))) break;
      } else if (kind == 'M') {
        WireReader metadata{response_};
        if (metadata.integer() != static_cast<uint32_t>(fields->count())) throw std::runtime_error("GPU result column count mismatch");
        metadata.end();
        break;
      } else throw std::runtime_error("Unexpected GPU input response");
    }
    if (OB_SUCC(ret)) {
      row_.count_ = fields->count();
      row_.cells_ = static_cast<ObObj *>(ctx.get_allocator().alloc(sizeof(ObObj) * row_.count_));
      if (!row_.cells_) ret = OB_ALLOCATE_MEMORY_FAILED;
      else {
        for (int64_t i = 0; i < row_.count_; ++i) new(row_.cells_ + i) ObObj();
        opened_ = true;
      }
    }
  } catch (const TransportTimeout &) { ret = OB_TIMEOUT; }
  catch (const std::bad_alloc &) { ret = OB_ALLOCATE_MEMORY_FAILED; }
  catch (const std::exception &error) {
    ret = cancel_ret_ != OB_SUCCESS ? cancel_ret_ : OB_NOT_SUPPORTED;
    if (cancel_ret_ == OB_SUCCESS) LOG_USER_ERROR(OB_NOT_SUPPORTED, error.what());
  }
  if (OB_FAIL(ret)) cleanup();
  return ret;
}

int GpuQueryResult::get_next_row(ObExecContext &ctx, const ObNewRow *&row) {
  UNUSED(ctx);
  int ret = ctx.check_status();
  row = &row_;
  if (OB_SUCC(ret) && !opened_) ret = OB_NOT_INIT;
  if (OB_SUCC(ret) && done_) return OB_ITER_END;
  if (OB_SUCC(ret)) {
    try {
      if (!rows_remaining_) {
        const auto kind = transport_.receive(response_);
        if (kind == 'Z') {
          if (!response_.empty()) throw std::runtime_error("Invalid GPU result terminator");
          done_ = true;
          cleanup();
          return OB_ITER_END;
        }
        if (kind != 'R') throw std::runtime_error("Invalid GPU result frame");
        WireReader frame{response_};
        rows_remaining_ = frame.integer();
        if (!rows_remaining_ || rows_remaining_ > 8192) throw std::runtime_error("Invalid GPU result batch size");
        response_position_ = frame.position;
      }
      cells_allocator_.reuse();
      WireReader frame{response_, response_position_};
      const auto *fields = result_.get_field_columns();
      for (int64_t i = 0; OB_SUCC(ret) && i < row_.count_; ++i) {
        const char *value = nullptr;
        uint32_t size = 0;
        if (!frame.cell_view(value, size)) row_.cells_[i].set_null();
        else {
          ObObj input;
          input.set_varchar(ObString(size, value));
          input.set_collation_type(CS_TYPE_UTF8MB4_BIN);
          ObAccuracy accuracy = fields->at(i).accuracy_;
          ObCastCtx cast(&cells_allocator_, nullptr, CM_NONE, fields->at(i).type_.get_collation_type(), &accuracy);
          ret = ObObjCaster::to_type(fields->at(i).type_.get_type(), cast, input, row_.cells_[i]);
        }
      }
      response_position_ = frame.position;
      if (--rows_remaining_ == 0) frame.end();
    } catch (const TransportTimeout &) { ret = OB_TIMEOUT; }
    catch (const std::bad_alloc &) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    catch (const std::exception &error) {
      ret = cancel_ret_ != OB_SUCCESS ? cancel_ret_ : OB_NOT_SUPPORTED;
      if (cancel_ret_ == OB_SUCCESS) LOG_USER_ERROR(OB_NOT_SUPPORTED, error.what());
    }
  }
  if (OB_FAIL(ret)) { done_ = true; cleanup(); }
  return ret;
}
int GpuQueryResult::close(ObExecContext &ctx) {
  UNUSED(ctx);
  cleanup();
  cells_allocator_.reuse();
  response_.clear();
  opened_ = false;
  done_ = true;
  return OB_SUCCESS;
}
} }
