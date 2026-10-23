// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "sirius_bridge_api.h"
#include "arrow_json.h"
#include <sirius/ffi.hpp>
#include <cstdio>
#include <cstdlib>
#include <condition_variable>
#include <future>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <chrono>

namespace {
// Own the engine on a dedicated thread. Its Rust telemetry uses thread-local
// state during destruction; C++ static destructors on the main thread run after
// that thread's TLS teardown. Destroying the engine before this worker returns
// keeps TLS alive. Waiting for initialization in the constructor also ensures
// the runtime destructor is registered after Sirius's lazy logging globals.
class Runtime {
public:
  Runtime() : worker_([this] { run(); }) {
    try { ready_.get_future().get(); }
    catch (...) { worker_.join(); throw; }
  }
  ~Runtime() {
    { std::lock_guard<std::mutex> lock(mutex_); stop_ = true; }
    wake_.notify_one();
    worker_.join();
  }
  size_t execute(const uint8_t *plan, size_t plan_size, char *output, size_t capacity) {
    return submit([&](auto &context) {
      ArrowArrayStream stream{};
      try {
        context.execute_substrait(std::string(reinterpret_cast<const char *>(plan), plan_size),
                                  reinterpret_cast<std::uintptr_t>(&stream));
        return seekdb_sirius::arrow_to_json(stream, output, capacity);
      } catch (...) {
        if (stream.release) { stream.release(&stream); }
        throw;
      }
    });
  }
#ifdef SEEKDB_SIRIUS_ARROW_INPUT
  size_t execute_stream(const uint8_t *plan, size_t plan_size, ArrowArrayStream &input,
                        char *output, size_t capacity) {
    return submit([&](auto &context) {
      const char *profile_env = std::getenv("SEEKDB_SIRIUS_PROFILE");
      const bool profile = profile_env && std::string(profile_env) == "1";
      auto clock_us = [profile]() -> int64_t {
        return profile ? std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count() : 0;
      };
      const int64_t started_us = clock_us();
      int64_t input_us = 0, copy_us = 0, rows = 0, batches = 0;
      struct StreamGuard { ArrowArrayStream &s; ~StreamGuard() { if (s.release) s.release(&s); } } guard{input};
      if (!input.release || !input.get_schema || !input.get_next) {
        throw std::invalid_argument("Invalid input Arrow stream");
      }
      ArrowSchema schema{};
      struct SchemaGuard { ArrowSchema &s; ~SchemaGuard() { if (s.release) s.release(&s); } } sg{schema};
      if (input.get_schema(&input, &schema) || !schema.release || !schema.format
          || std::string(schema.format) != "+s" || schema.n_children < 1
          || schema.n_children > 64 || !schema.children || schema.dictionary) {
        throw std::invalid_argument("Expected a flat Arrow schema with 1..64 columns");
      }
      auto fragment = sirius::ffi::make_fragment(context);
      for (int64_t i = 0; i < schema.n_children; ++i) {
        auto *column = schema.children[i];
        if (!column || !column->format || !column->name || column->dictionary || column->n_children) {
          throw std::invalid_argument("Invalid Arrow input column");
        }
        const std::string format(column->format);
        if (format != "l" && format != "g") {
          throw std::invalid_argument("Live input currently supports int64 and float64 only");
        }
        fragment->declare_input_column(0, column->name, format == "l" ? "BIGINT" : "DOUBLE");
      }
      fragment->build(std::string(reinterpret_cast<const char *>(plan), plan_size));
      const int64_t build_us = clock_us() - started_us;
      size_t input_bytes = 0;
      bool received = false;
      for (;;) {
        ArrowArray batch{};
        struct BatchGuard { ArrowArray &a; ~BatchGuard() { if (a.release) a.release(&a); } } bg{batch};
        const int64_t input_start = clock_us();
        if (input.get_next(&input, &batch)) {
          const char *error = input.get_last_error ? input.get_last_error(&input) : nullptr;
          throw std::runtime_error(error ? error : "Arrow input producer failed");
        }
        input_us += clock_us() - input_start;
        if (!batch.release) break;
        if (batch.length < 0 || batch.length > 8 * 1024 * 1024) {
          throw std::invalid_argument("Arrow input batch row limit exceeded");
        }
        const size_t bytes = (static_cast<size_t>(batch.length) * 8 + (batch.length + 7) / 8)
                             * schema.n_children;
        if (bytes > 128 * 1024 * 1024 || input_bytes > 512 * 1024 * 1024 - bytes) {
          throw std::invalid_argument("Live input exceeds 128 MiB per batch or 512 MiB per fragment");
        }
        input_bytes += bytes;
        const int64_t copy_start = clock_us();
        fragment->push_arrow(0, reinterpret_cast<uintptr_t>(&schema), reinterpret_cast<uintptr_t>(&batch));
        copy_us += clock_us() - copy_start;
        rows += batch.length;
        ++batches;
        received = true;
      }
      // A typed empty batch gives the aggregate pipeline a task, so global
      // aggregates over no rows can produce SQL's single (NULL, 0) result row.
      if (!received) {
        const void *buffers[2] = {nullptr, nullptr};
        std::vector<ArrowArray> children(schema.n_children);
        std::vector<ArrowArray *> pointers;
        for (auto &child : children) {
          child.n_buffers = 2;
          child.buffers = buffers;
          pointers.push_back(&child);
        }
        ArrowArray empty{};
        empty.n_buffers = 1;
        empty.buffers = buffers;
        empty.n_children = schema.n_children;
        empty.children = pointers.data();
        fragment->push_arrow(0, reinterpret_cast<uintptr_t>(&schema), reinterpret_cast<uintptr_t>(&empty));
      }
      fragment->close_input(0, 0);
      const int64_t run_start = clock_us();
      fragment->run();
      const int64_t run_us = clock_us() - run_start;
      ArrowArrayStream result{};
      try {
        fragment->result_to_arrow(reinterpret_cast<uintptr_t>(&result));
        const size_t size = seekdb_sirius::arrow_to_json(result, output, capacity);
        if (profile) {
          const int64_t total_us = clock_us() - started_us;
          std::fprintf(stderr, "sirius stream profile: rows=%lld batches=%lld build_us=%lld input_us=%lld copy_us=%lld run_us=%lld result_us=%lld total_us=%lld\n",
              static_cast<long long>(rows), static_cast<long long>(batches), static_cast<long long>(build_us),
              static_cast<long long>(input_us), static_cast<long long>(copy_us), static_cast<long long>(run_us),
              static_cast<long long>(clock_us() - run_start - run_us), static_cast<long long>(total_us));
        }
        return size;
      } catch (...) {
        if (result.release) result.release(&result);
        throw;
      }
    });
  }
#endif
private:
  size_t submit(std::function<size_t(sirius::ffi::Context &)> operation) {
    // One context/connection and one outstanding request. The calling SQL
    // worker owns all buffers and waits until the GPU worker releases Arrow.
    std::lock_guard<std::mutex> caller(callers_);
    std::packaged_task<size_t(sirius::ffi::Context &)> task(std::move(operation));
    auto result = task.get_future();
    { std::lock_guard<std::mutex> lock(mutex_); task_ = std::move(task); }
    wake_.notify_one();
    return result.get();
  }
  void run() {
    std::unique_ptr<sirius::ffi::Context> context;
    try {
      const char *config = std::getenv("SEEKDB_SIRIUS_CONFIG");
      if (!config || !*config) {
        throw std::runtime_error("Set SEEKDB_SIRIUS_CONFIG to an explicit GPU memory configuration before starting seekdb");
      }
      context = sirius::ffi::make_context_from_config(config);
    } catch (...) {
      ready_.set_exception(std::current_exception());
      return;
    }
    ready_.set_value();
    for (;;) {
      std::unique_lock<std::mutex> lock(mutex_);
      wake_.wait(lock, [this] { return stop_ || task_.valid(); });
      if (stop_) { break; }
      auto task = std::move(task_);
      lock.unlock();
      task(*context); // packaged_task transports execution exceptions to the caller.
    }
    // Context dies here, on its owner thread, before that thread's TLS.
  }
  std::mutex callers_, mutex_;
  std::condition_variable wake_;
  std::promise<void> ready_;
  std::packaged_task<size_t(sirius::ffi::Context &)> task_;
  bool stop_ = false;
  std::thread worker_;
};

Runtime &runtime() { static Runtime instance; return instance; }

int execute(const uint8_t *plan, size_t plan_size,
            char *output, size_t capacity, size_t *output_size,
            char *error, size_t error_capacity) noexcept
{
  if (output_size) { *output_size = 0; }
  if (error && error_capacity) { error[0] = '\0'; }
  if (!plan || !plan_size || plan_size > 1024 * 1024 || !output || !capacity || !output_size
      || !error || !error_capacity) { return SEEKDB_SIRIUS_INVALID_ARGUMENT; }
  try {
    *output_size = runtime().execute(plan, plan_size, output, capacity);
    return SEEKDB_SIRIUS_OK;
  } catch (const seekdb_sirius::ResultTooLarge &e) {
    std::snprintf(error, error_capacity, "%s", e.what());
    return SEEKDB_SIRIUS_RESULT_TOO_LARGE;
  } catch (const std::exception &e) {
    std::snprintf(error, error_capacity, "%s", e.what());
  } catch (...) {
    std::snprintf(error, error_capacity, "Unknown Sirius execution error");
  }
  return SEEKDB_SIRIUS_EXECUTION_ERROR;
}
const SeekdbSiriusApiV1 api{1, sizeof(SeekdbSiriusApiV1), execute};
}

extern "C" __attribute__((visibility("default")))
const SeekdbSiriusApiV1 *seekdb_sirius_get_api_v1() noexcept { return &api; }

#ifdef SEEKDB_SIRIUS_ARROW_INPUT
// Optional sidecar-only API. Takes ownership of an exported Arrow C stream,
// including on errors after argument validation. No pointers cross processes.
extern "C" __attribute__((visibility("default")))
int seekdb_sirius_execute_stream_v1(const uint8_t *plan, size_t plan_size, uintptr_t input,
    char *output, size_t capacity, size_t *output_size, char *error, size_t error_capacity) noexcept {
  if (output_size) *output_size = 0;
  if (!plan || !plan_size || plan_size > 1024 * 1024 || !input || !output || !capacity
      || !output_size || !error || !error_capacity) return SEEKDB_SIRIUS_INVALID_ARGUMENT;
  error[0] = '\0';
  auto &stream = *reinterpret_cast<ArrowArrayStream *>(input);
  try {
    *output_size = runtime().execute_stream(plan, plan_size, stream, output, capacity);
    return SEEKDB_SIRIUS_OK;
  } catch (const std::exception &e) {
    std::snprintf(error, error_capacity, "%s", e.what());
  } catch (...) {
    std::snprintf(error, error_capacity, "Unknown live-input error");
  }
  if (stream.release) stream.release(&stream);
  return SEEKDB_SIRIUS_EXECUTION_ERROR;
}
#endif
