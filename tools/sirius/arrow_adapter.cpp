// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "sirius_arrow_adapter_api.h"
#include "arrow_c.h"
#include <sirius/ffi.hpp>

#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
struct InputOwner {
  ArrowArrayStream stream{};
  ArrowSchema schema{};
  ~InputOwner() {
    if (schema.release) schema.release(&schema);
    if (stream.release) stream.release(&stream);
  }
};
struct BatchOwner {
  std::shared_ptr<InputOwner> input;
  ArrowArray array{};
  ~BatchOwner() { if (array.release) array.release(&array); }
};

std::string sql_type(const ArrowSchema &s) {
  if (!s.format || s.dictionary || s.n_children || s.metadata)
    throw std::invalid_argument("Adapter requires flat, non-dictionary columns without metadata");
  const std::string f(s.format);
  if (f == "b") return "BOOLEAN";
  if (f == "c") return "TINYINT";
  if (f == "C") return "UTINYINT";
  if (f == "s") return "SMALLINT";
  if (f == "S") return "USMALLINT";
  if (f == "i") return "INTEGER";
  if (f == "I") return "UINTEGER";
  if (f == "l") return "BIGINT";
  if (f == "L") return "UBIGINT";
  if (f == "f") return "FLOAT";
  if (f == "g") return "DOUBLE";
  if (f == "u") return "VARCHAR";
  if (f == "tdD") return "DATE";
  if (f == "tss:") return "TIMESTAMP_S";
  if (f == "tsm:") return "TIMESTAMP_MS";
  if (f == "tsu:") return "TIMESTAMP";
  if (f == "tsn:") return "TIMESTAMP_NS";
  int precision = 0, scale = 0, end = 0;
  if (std::sscanf(f.c_str(), "d:%d,%d%n", &precision, &scale, &end) == 2 &&
      (f.substr(end).empty() || f.substr(end) == ",128") &&
      precision > 0 && precision <= 38 && scale >= 0 && scale <= precision) {
    return "DECIMAL(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
  }
  throw std::invalid_argument("Unsupported Arrow format: " + f);
}

class Runtime {
public:
  Runtime() : worker_([this] { run(); }) {
    try { ready_.get_future().get(); }
    catch (...) { worker_.join(); throw; }
  }
  ~Runtime() { shutdown(); }
  void shutdown() {
    std::lock_guard<std::mutex> caller(callers_);
    if (!worker_.joinable()) return;
    { std::lock_guard<std::mutex> lock(mutex_); stop_ = true; }
    wake_.notify_one();
    worker_.join();
  }
  void execute(std::string plan, std::vector<sirius::ffi::ArrowInput> inputs, uintptr_t output) {
    std::lock_guard<std::mutex> caller(callers_);
    if (stop_) throw std::runtime_error("Sirius adapter has been shut down");
    std::packaged_task<void(sirius::ffi::Context &)> task(
      [plan = std::move(plan), inputs = std::move(inputs), output](auto &context) mutable {
        context.execute_substrait_arrow(plan, std::move(inputs), output);
      });
    auto result = task.get_future();
    { std::lock_guard<std::mutex> lock(mutex_); task_ = std::move(task); }
    wake_.notify_one();
    result.get();
  }
private:
  void run() {
    std::unique_ptr<sirius::ffi::Context> context;
    try {
      auto path = std::getenv("SEEKDB_SIRIUS_CONFIG");
      if (!path || !*path) throw std::runtime_error("Set SEEKDB_SIRIUS_CONFIG to an explicit memory configuration");
      context = sirius::ffi::make_context_from_config(path);
    } catch (...) { ready_.set_exception(std::current_exception()); return; }
    ready_.set_value();
    while (true) {
      std::unique_lock<std::mutex> lock(mutex_);
      wake_.wait(lock, [this] { return stop_ || task_.valid(); });
      if (stop_) break;
      auto task = std::move(task_);
      lock.unlock();
      task(*context);
    }
    // Destroy the engine before this owner's thread-local telemetry is gone.
  }
  std::mutex callers_, mutex_;
  std::condition_variable wake_;
  std::promise<void> ready_;
  std::packaged_task<void(sirius::ffi::Context &)> task_;
  bool stop_ = false;
  std::thread worker_;
};
Runtime &runtime() { static Runtime r; return r; }
} // namespace

extern "C" __attribute__((visibility("default")))
int seekdb_sirius_execute_inputs_v1(const uint8_t *plan, size_t plan_size,
    SeekdbSiriusArrowInputV1 *sources, size_t count, uintptr_t output,
    char *error, size_t error_capacity) {
  if (!error || !error_capacity) return 1;
  error[0] = 0;
  try {
    if (!plan || !plan_size || plan_size > 16 * 1024 * 1024 || !sources ||
        !count || count > 64 || !output)
      throw std::invalid_argument("Invalid adapter invocation");
    auto &result = *reinterpret_cast<ArrowArrayStream *>(output);
    if (result.release) throw std::invalid_argument("Output stream is already owned");
    std::set<uint64_t> ids;
    std::set<uintptr_t> streams;
    std::string snapshot;
    // Validate ALL declarations before moving any source. An invalid call
    // leaves every caller stream untouched; a valid call consumes all of them.
    for (size_t i = 0; i < count; ++i) {
      auto &s = sources[i];
      if (s.struct_size != sizeof(s) || !s.stream || !s.snapshot_identity ||
          !*s.snapshot_identity || !s.cancel || !s.max_batch_rows ||
          s.max_batch_rows > INT32_MAX || !s.max_batch_bytes ||
          s.max_batch_bytes > (1ULL << 30) || s.id > INT64_MAX ||
          !ids.insert(s.id).second || !streams.insert(s.stream).second)
        throw std::invalid_argument("Invalid source declaration");
      if (!i) snapshot = s.snapshot_identity;
      if (snapshot != s.snapshot_identity) throw std::invalid_argument("Mixed host snapshots refused");
      const auto &stream = *reinterpret_cast<ArrowArrayStream *>(s.stream);
      if (!stream.release || !stream.get_schema || !stream.get_next)
        throw std::invalid_argument("Invalid source Arrow stream");
    }
    std::vector<std::shared_ptr<InputOwner>> owners;
    owners.reserve(count);
    // Allocate owners first: allocation failure must not partially move inputs.
    for (size_t i = 0; i < count; ++i) owners.push_back(std::make_shared<InputOwner>());
    for (size_t i = 0; i < count; ++i) {
      auto &stream = *reinterpret_cast<ArrowArrayStream *>(sources[i].stream);
      owners[i]->stream = stream;
      stream = {};
    }
    std::vector<sirius::ffi::ArrowInput> inputs;
    for (size_t i = 0; i < count; ++i) {
      auto owner = owners[i];
      auto &s = sources[i];
      if (owner->stream.get_schema(&owner->stream, &owner->schema))
        throw std::runtime_error("Host Arrow schema callback failed");
      const auto &schema = owner->schema;
      if (!schema.release || !schema.format || std::strcmp(schema.format, "+s") ||
          schema.dictionary || schema.metadata || schema.n_children < 1 ||
          schema.n_children > 1024 || !schema.children)
        throw std::invalid_argument("Expected a flat record batch schema");
      sirius::ffi::ArrowInput input;
      input.id = s.id;
      input.snapshot_identity = snapshot;
      input.max_batch_rows = s.max_batch_rows;
      input.max_batch_bytes = s.max_batch_bytes;
      if (s.estimated_rows != UINT64_MAX) input.estimated_rows = s.estimated_rows;
      std::set<std::string> names;
      for (int64_t c = 0; c < schema.n_children; ++c) {
        auto col = schema.children[c];
        if (!col || !col->name || !*col->name || !names.insert(col->name).second)
          throw std::invalid_argument("Missing or duplicate Arrow column name");
        input.names.emplace_back(col->name);
        input.types.push_back(sql_type(*col));
      }
      const auto bound = s.max_batch_bytes;
      input.next = [owner, bound]() -> sirius::ffi::ArrowBatch {
        auto batch = std::make_shared<BatchOwner>();
        batch->input = owner;
        if (owner->stream.get_next(&owner->stream, &batch->array)) {
          auto msg = owner->stream.get_last_error ? owner->stream.get_last_error(&owner->stream) : nullptr;
          throw std::runtime_error(msg ? msg : "Host Arrow reader failed");
        }
        if (!batch->array.release) return {};
        return {reinterpret_cast<uintptr_t>(&owner->schema),
                reinterpret_cast<uintptr_t>(&batch->array), batch, bound};
      };
      input.cancel = [cancel = s.cancel, state = s.cancel_state] { cancel(state); };
      inputs.push_back(std::move(input));
    }
    runtime().execute(std::string(reinterpret_cast<const char *>(plan), plan_size), std::move(inputs), output);
    return 0;
  } catch (const std::exception &e) {
    std::snprintf(error, error_capacity, "%s", e.what());
  } catch (...) { std::snprintf(error, error_capacity, "Unknown adapter failure"); }
  return 3;
}

extern "C" __attribute__((visibility("default")))
int seekdb_sirius_adapter_shutdown_v1(char *error, size_t capacity) {
  if (!error || !capacity) return 1;
  error[0] = 0;
  try { runtime().shutdown(); return 0; }
  catch (const std::exception &e) { std::snprintf(error, capacity, "%s", e.what()); }
  catch (...) { std::snprintf(error, capacity, "Unknown adapter shutdown failure"); }
  return 3;
}
