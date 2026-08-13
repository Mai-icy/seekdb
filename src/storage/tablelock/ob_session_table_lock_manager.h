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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_SESSION_TABLE_LOCK_MANAGER_H_
#define OCEANBASE_STORAGE_TABLELOCK_OB_SESSION_TABLE_LOCK_MANAGER_H_

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_thread_cond.h"
#include "storage/tablelock/ob_table_lock_common.h"

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{

// Runtime-only reverse index for MySQL LOCK TABLES. The actual conflict state
// remains in ObOBJLockMap so session table locks continue to conflict with DML
// and DDL locks. This index only replaces __all_detect_lock_info_v2 ownership,
// recursion counting, enumeration, and session cleanup.
class SessionTableLockManager final
{
public:
  struct LockSnapshot
  {
    LockSnapshot() : table_id_(OB_INVALID_ID), lock_mode_(NO_LOCK), ref_count_(0) {}
    LockSnapshot(uint64_t table_id, ObTableLockMode lock_mode, int64_t ref_count)
      : table_id_(table_id), lock_mode_(lock_mode), ref_count_(ref_count) {}

    uint64_t table_id_;
    ObTableLockMode lock_mode_;
    int64_t ref_count_;
  };

  SessionTableLockManager();
  ~SessionTableLockManager();

  int init();
  void destroy();

  int acquire(const ObTableLockOwnerID &owner_id,
              uint64_t table_id,
              ObTableLockMode lock_mode,
              bool &need_lock);
  int rollback_acquire(const ObTableLockOwnerID &owner_id,
                       uint64_t table_id,
                       ObTableLockMode lock_mode);
  int get_locks(const ObTableLockOwnerID &owner_id,
                common::ObIArray<LockSnapshot> &locks);
  int release_all(const ObTableLockOwnerID &owner_id,
                  int64_t &release_count);
  int has_lock(const ObTableLockOwnerID &owner_id, bool &has_lock);

private:
  struct LockEntry
  {
    LockEntry() : table_id_(OB_INVALID_ID), lock_mode_(NO_LOCK), ref_count_(0) {}
    LockEntry(uint64_t table_id, ObTableLockMode lock_mode, int64_t ref_count)
      : table_id_(table_id), lock_mode_(lock_mode), ref_count_(ref_count) {}

    uint64_t table_id_;
    ObTableLockMode lock_mode_;
    int64_t ref_count_;
  };

  typedef common::ObSEArray<LockEntry, 4> LockList;
  typedef common::hash::ObHashMap<ObTableLockOwnerID, LockList> OwnerLockMap;

  common::ObThreadCond cond_;
  OwnerLockMap owner_lock_map_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(SessionTableLockManager);
};

} // namespace tablelock
} // namespace transaction
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TABLELOCK_OB_SESSION_TABLE_LOCK_MANAGER_H_
