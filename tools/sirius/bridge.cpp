// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#include "sirius_bridge_api.h"
#include "arrow_json.h"
#include <sirius/ffi.hpp>
#include <cstdio>
#include <cstdlib>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

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
    // One context/connection and one outstanding request. The calling SQL
    // worker owns all buffers and waits until the GPU worker releases Arrow.
    std::lock_guard<std::mutex> caller(callers_);
    std::packaged_task<size_t(sirius::ffi::Context &)> task([&](auto &context) {
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
    auto result = task.get_future();
    { std::lock_guard<std::mutex> lock(mutex_); task_ = std::move(task); }
    wake_.notify_one();
    return result.get();
  }
private:
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

int execute(const uint8_t *plan, size_t plan_size,
            char *output, size_t capacity, size_t *output_size,
            char *error, size_t error_capacity) noexcept
{
  if (output_size) { *output_size = 0; }
  if (error && error_capacity) { error[0] = '\0'; }
  if (!plan || !plan_size || plan_size > 1024 * 1024 || !output || !capacity || !output_size
      || !error || !error_capacity) { return SEEKDB_SIRIUS_INVALID_ARGUMENT; }
  try {
    static Runtime runtime;
    *output_size = runtime.execute(plan, plan_size, output, capacity);
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
