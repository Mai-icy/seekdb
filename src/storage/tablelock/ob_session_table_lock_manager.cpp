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

#define USING_LOG_PREFIX TABLELOCK

#include "storage/tablelock/ob_session_table_lock_manager.h"

namespace oceanbase
{
using namespace common;
namespace transaction
{
namespace tablelock
{

static const int64_t SESSION_TABLE_LOCK_BUCKET_COUNT = 64;

SessionTableLockManager::SessionTableLockManager()
  : cond_(), owner_lock_map_(), is_inited_(false)
{
}

SessionTableLockManager::~SessionTableLockManager()
{
  destroy();
}

int SessionTableLockManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    LOG_WARN("failed to init session table lock manager", K(ret));
  } else if (OB_FAIL(owner_lock_map_.create(
                 SESSION_TABLE_LOCK_BUCKET_COUNT,
                 lib::ObMemAttr("SessTableLock")))) {
    LOG_WARN("failed to create session table lock map", K(ret));
    cond_.destroy();
  } else {
    is_inited_ = true;
  }
  return ret;
}

void SessionTableLockManager::destroy()
{
  if (is_inited_) {
    {
      ObThreadCondGuard guard(cond_);
      owner_lock_map_.destroy();
      is_inited_ = false;
    }
    cond_.destroy();
  }
}

int SessionTableLockManager::acquire(const ObTableLockOwnerID &owner_id,
                                     const uint64_t table_id,
                                     const ObTableLockMode lock_mode,
                                     bool &need_lock)
{
  int ret = OB_SUCCESS;
  need_lock = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!owner_id.is_valid()
                         || !is_valid_id(table_id)
                         || !is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObThreadCondGuard guard(cond_);
    LockList *locks = owner_lock_map_.get(owner_id);
    if (OB_ISNULL(locks)) {
      LockList new_locks;
      if (OB_FAIL(new_locks.push_back(LockEntry(table_id, lock_mode, 1)))) {
      } else if (OB_FAIL(owner_lock_map_.set_refactored(owner_id, new_locks))) {
        LOG_WARN("failed to add session table lock owner", K(ret), K(owner_id));
      }
    } else {
      bool found = false;
      for (int64_t i = 0; !found && i < locks->count(); ++i) {
        LockEntry &entry = locks->at(i);
        if (entry.table_id_ == table_id && entry.lock_mode_ == lock_mode) {
          ++entry.ref_count_;
          need_lock = false;
          found = true;
        }
      }
      if (!found && OB_FAIL(locks->push_back(LockEntry(table_id, lock_mode, 1)))) {
        LOG_WARN("failed to add session table lock", K(ret), K(owner_id), K(table_id), K(lock_mode));
      }
    }
  }
  return ret;
}

int SessionTableLockManager::rollback_acquire(const ObTableLockOwnerID &owner_id,
                                              const uint64_t table_id,
                                              const ObTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!owner_id.is_valid()
                         || !is_valid_id(table_id)
                         || !is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObThreadCondGuard guard(cond_);
    LockList *locks = owner_lock_map_.get(owner_id);
    if (OB_ISNULL(locks)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      int64_t lock_idx = -1;
      for (int64_t i = 0; lock_idx < 0 && i < locks->count(); ++i) {
        const LockEntry &entry = locks->at(i);
        if (entry.table_id_ == table_id && entry.lock_mode_ == lock_mode) {
          lock_idx = i;
        }
      }
      if (lock_idx < 0) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LockEntry &entry = locks->at(lock_idx);
        if (--entry.ref_count_ == 0) {
          locks->remove(lock_idx);
        }
        if (locks->empty()) {
          ret = owner_lock_map_.erase_refactored(owner_id);
        }
      }
    }
  }
  return ret;
}

int SessionTableLockManager::get_locks(const ObTableLockOwnerID &owner_id,
                                       ObIArray<LockSnapshot> &locks)
{
  int ret = OB_SUCCESS;
  locks.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!owner_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObThreadCondGuard guard(cond_);
    const LockList *lock_list = owner_lock_map_.get(owner_id);
    if (OB_NOT_NULL(lock_list)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < lock_list->count(); ++i) {
        const LockEntry &entry = lock_list->at(i);
        if (OB_FAIL(locks.push_back(
                LockSnapshot(entry.table_id_, entry.lock_mode_, entry.ref_count_)))) {
          LOG_WARN("failed to copy session table lock snapshot", K(ret), K(owner_id));
        }
      }
    }
  }
  return ret;
}

int SessionTableLockManager::release_all(const ObTableLockOwnerID &owner_id,
                                         int64_t &release_count)
{
  int ret = OB_SUCCESS;
  release_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!owner_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObThreadCondGuard guard(cond_);
    const LockList *locks = owner_lock_map_.get(owner_id);
    if (OB_NOT_NULL(locks)) {
      for (int64_t i = 0; i < locks->count(); ++i) {
        release_count += locks->at(i).ref_count_;
      }
      ret = owner_lock_map_.erase_refactored(owner_id);
    }
  }
  return ret;
}

int SessionTableLockManager::has_lock(const ObTableLockOwnerID &owner_id,
                                      bool &has_lock)
{
  int ret = OB_SUCCESS;
  has_lock = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!owner_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObThreadCondGuard guard(cond_);
    has_lock = OB_NOT_NULL(owner_lock_map_.get(owner_id));
  }
  return ret;
}

} // namespace tablelock
} // namespace transaction
} // namespace oceanbase
