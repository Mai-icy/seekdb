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

    TO_STRING_KV(K_(table_id), K_(lock_mode), K_(ref_count));

    uint64_t table_id_;
    ObTableLockMode lock_mode_;
    int64_t ref_count_;
  };

  struct CleanupOwner
  {
    CleanupOwner()
      : owner_id_(), session_id_(0), session_create_ts_(0) {}
    CleanupOwner(const ObTableLockOwnerID &owner_id,
                 uint32_t session_id,
                 uint64_t session_create_ts)
      : owner_id_(owner_id),
        session_id_(session_id),
        session_create_ts_(session_create_ts) {}

    TO_STRING_KV(K_(owner_id), K_(session_id), K_(session_create_ts));

    ObTableLockOwnerID owner_id_;
    uint32_t session_id_;
    uint64_t session_create_ts_;
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
  int schedule_cleanup(const ObTableLockOwnerID &owner_id,
                       uint32_t session_id,
                       uint64_t session_create_ts);
  int get_pending_cleanups(common::ObIArray<CleanupOwner> &owners);

private:
  int remove_pending_cleanup_(const ObTableLockOwnerID &owner_id);

  struct LockEntry
  {
    LockEntry() : table_id_(OB_INVALID_ID), lock_mode_(NO_LOCK), ref_count_(0) {}
    LockEntry(uint64_t table_id, ObTableLockMode lock_mode, int64_t ref_count)
      : table_id_(table_id), lock_mode_(lock_mode), ref_count_(ref_count) {}

    TO_STRING_KV(K_(table_id), K_(lock_mode), K_(ref_count));

    uint64_t table_id_;
    ObTableLockMode lock_mode_;
    int64_t ref_count_;
  };

  typedef common::ObSEArray<LockEntry, 4> LockList;
  typedef common::hash::ObHashMap<ObTableLockOwnerID, LockList> OwnerLockMap;
  typedef common::ObSEArray<CleanupOwner, 4> CleanupList;

  common::ObThreadCond cond_;
  OwnerLockMap owner_lock_map_;
  CleanupList pending_cleanups_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(SessionTableLockManager);
};

} // namespace tablelock
} // namespace transaction
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TABLELOCK_OB_SESSION_TABLE_LOCK_MANAGER_H_
