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

#define USING_LOG_PREFIX COMMON
#include "ob_io_schedule_v2.h"
#include "share/io/ob_io_struct.h"
namespace oceanbase
{
namespace common
{

int64_t ObTenantIOSchedulerV2::get_qindex(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  const ObIOGroupKey grp_key = req.get_group_key();
  if (is_sys_group(grp_key.group_id_)) {
    index = static_cast<int64_t>(grp_key.mode_);
  } else if (!is_valid_group(grp_key.group_id_)) {
  } else if (OB_FAIL(req.tenant_io_mgr_->get_group_index(grp_key, (uint64_t&)index))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      if (REACH_TIME_INTERVAL(1 * 1000L * 1000L)) {
        LOG_INFO("get group index failed, but maybe it is ok", K(ret), K(grp_key), K(index));
      }
    } else {
      LOG_WARN("get group index failed", K(ret), K(grp_key), K(index));
    }
    index = -1;
  } else if (INT64_MAX == index) {
    index = -1;
  }
  return index;
}

int ObTenantIOSchedulerV2::schedule_request(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  ObDeviceChannel *device_channel = nullptr;
  ObTimeGuard time_guard("submit_req", 100000); //100ms
  ObIOResult* result = req.io_result_;
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret), K(req));
  } else if (OB_UNLIKELY(req.is_canceled())) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(req.prepare())) {
    LOG_WARN("prepare io request failed", K(ret), K(req));
  } else if (FALSE_IT(time_guard.click("prepare_req"))) {
  } else if (OB_FAIL(OB_IO_MANAGER.get_device_channel(req, device_channel))) {
    LOG_WARN("get device channel failed", K(ret), K(req));
  } else if (FALSE_IT(result->time_log_.dequeue_ts_ = ObTimeUtility::fast_current_time())) {
  } else {
    ObThreadCondGuard guard(result->get_cond());
    if (OB_FAIL(guard.get_ret())) {
      LOG_ERROR("fail to guard master condition", K(ret));
    } else if (req.is_canceled()) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(device_channel->submit(req))) {
      LOG_WARN("submit io to device failed");
    } else {
      time_guard.click("device_submit");
    }
  }

  if (time_guard.get_diff() > 100000) {// 100ms
    LOG_INFO("submit_request cost too much time", K(ret), K(time_guard), K(req));
  }
  if (OB_FAIL(ret)) {
    if (ret == OB_EAGAIN) {
      if (REACH_TIME_INTERVAL(1 * 1000L * 1000L)) {
        LOG_INFO("device channel eagain", K(ret));
      }
      if (OB_FAIL(req.retry_io())) {
        LOG_WARN("retry io failed", K(ret), K(req));
        req.io_result_->finish(ObIORetCode(ret), &req);
      }
    } else {
      req.io_result_->finish(ObIORetCode(ret), &req);
    }
  }
  return ret;
}

}; // end namespace common
}; // end namespace oceanbase
