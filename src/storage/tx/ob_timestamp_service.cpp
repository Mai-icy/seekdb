/*
 * Copyright (c) 2025 OceanBase.
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

#include "ob_timestamp_service.h"

namespace oceanbase
{

using namespace oceanbase::share;
namespace transaction
{

int ObTimestampService::init()
{
  service_type_ = ServiceType::TimestampService;
  ATOMIC_STORE(&last_id_, ObClockGenerator::getClock() * 1000);
  ATOMIC_STORE(&last_gts_, 0);
  ATOMIC_STORE(&last_request_ts_, 0);
  ATOMIC_STORE(&check_gts_speed_lock_, 0);
  ATOMIC_STORE(&is_ready_, false);
  return OB_SUCCESS;
}

int ObTimestampService::server_module_init(ObTimestampService *&timestamp_service)
{
  int ret = OB_SUCCESS;
  ret = timestamp_service->init();
  return ret;
}

int ObTimestampService::allocate_timestamp_(const int64_t range,
                                            const int64_t base_id,
                                            int64_t &gts)
{
  int ret = OB_SUCCESS;
  bool allocated = false;
  if (range <= 0 || base_id < 0) {
    ret = OB_INVALID_ARGUMENT;
  }
  while (OB_SUCC(ret) && !allocated) {
    const int64_t last_id = ATOMIC_LOAD(&last_id_);
    const int64_t candidate = max(last_id, base_id);
    if (candidate > INT64_MAX - range) {
      ret = OB_SIZE_OVERFLOW;
    } else if (ATOMIC_BCAS(&last_id_, last_id, candidate + range)) {
      gts = candidate;
      allocated = true;
    }
  }
  return ret;
}

int ObTimestampService::recover(const SCN &max_ls_scn)
{
  int ret = OB_SUCCESS;
  if (!max_ls_scn.is_valid() || max_ls_scn.is_max()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const uint64_t durable_gts = max_ls_scn.get_val_for_gts();
    if (durable_gts >= static_cast<uint64_t>(INT64_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      TRANS_LOG(ERROR, "durable timestamp is too large to recover", K(ret), K(max_ls_scn),
          K(durable_gts));
    } else {
      const int64_t current_time = ObClockGenerator::getClock() * 1000;
      const int64_t log_floor = static_cast<int64_t>(durable_gts) + 1;
      (void)inc_update(&last_id_, max(current_time, log_floor));
      ATOMIC_STORE(&is_ready_, true);
      TRANS_LOG(INFO, "timestamp service recovered from durable log frontier",
          K(max_ls_scn), K(log_floor), K_(last_id));
    }
  }
  return ret;
}

// In-memory monotonic allocation. Recovery obtains its floor from the durable
// LS log frontier, so no dedicated timestamp log is submitted here.
int ObTimestampService::get_timestamp(int64_t &gts)
{
  int ret = OB_SUCCESS;
  // 100ms
  const int64_t CHECK_INTERVAL = 100000000;
  const int64_t current_time = ObClockGenerator::getClock() * 1000;
  int64_t last_request_ts = ATOMIC_LOAD(&last_request_ts_);
  int64_t time_delta = current_time - last_request_ts;

  if (!ATOMIC_LOAD(&is_ready_)) {
    ret = OB_EAGAIN;
  } else {
    ret = allocate_timestamp_(1, current_time, gts);
  }

  if (OB_SUCC(ret)) {
    if ((last_request_ts == 0 || time_delta < 0) && ATOMIC_BCAS(&check_gts_speed_lock_, 0, 1)) {
      last_request_ts = ATOMIC_LOAD(&last_request_ts_);
      time_delta = current_time - last_request_ts;
      // before, we only do a fast check, and we should check again after we get the lock
      if (last_request_ts == 0 || time_delta < 0) {
        ATOMIC_STORE(&last_request_ts_, current_time);
        ATOMIC_STORE(&last_gts_, gts);
      }
      ATOMIC_STORE(&check_gts_speed_lock_, 0);
    } else if (time_delta > CHECK_INTERVAL && ATOMIC_BCAS(&check_gts_speed_lock_, 0, 1)) {
      last_request_ts = ATOMIC_LOAD(&last_request_ts_);
      time_delta = current_time - last_request_ts;
      // before, we only do a fast check, and we should check again after we get the lock
      if (time_delta > CHECK_INTERVAL) {
        const int64_t last_gts = ATOMIC_LOAD(&last_gts_);
        const int64_t gts_delta = gts - last_gts;
        const int64_t compensation_threshold = time_delta / 2;
        const int64_t compensation_value = time_delta / 10;
        // if the gts service advanced too slowly, then we add it up with `compensation_value`
        if (time_delta - gts_delta > compensation_threshold) {
          ret = allocate_timestamp_(compensation_value, current_time, gts);
          TRANS_LOG(WARN, "the gts service advanced too slowly", K(ret), K(current_time),
              K(last_request_ts), K(time_delta), K(last_gts), K(gts), K(gts_delta),
              K(compensation_value));
        }
        if (OB_SUCC(ret)) {
          ATOMIC_STORE(&last_request_ts_, current_time);
          ATOMIC_STORE(&last_gts_, gts);
        }
      }
      ATOMIC_STORE(&check_gts_speed_lock_, 0);
    }
  }

  return ret;
}

void ObTimestampService::get_virtual_info(int64_t &ts_value)
{
  ts_value = ATOMIC_LOAD(&last_id_);
  TRANS_LOG(INFO, "gts get virtual info", K_(last_id), K(ts_value));
}

}
}
