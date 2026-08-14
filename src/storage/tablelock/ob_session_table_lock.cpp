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

#include "data_plane/tablelock/ob_session_table_lock.h"

#include "share/rc/ob_server_runtime.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tablelock/ob_table_lock_service.h"

namespace oceanbase
{
namespace data_plane
{
namespace
{

using namespace transaction;
using namespace transaction::tablelock;

int make_owner(const ObSessionLockOwner &source, ObTableLockOwnerID &target)
{
  return target.convert_from_session_id(source.session_id_,
                                        source.session_create_ts_);
}

int make_owner(const ObPersistedLockOwner &source, ObTableLockOwnerID &target)
{
  return target.convert_from_value(
      static_cast<ObLockOwnerType>(source.owner_type_), source.owner_id_);
}

ObTableLockTaskType task_type_for_scope(ObSessionLockScope scope)
{
  ObTableLockTaskType task_type = INVALID_LOCK_TASK_TYPE;
  if (ObSessionLockScope::NAMED_LOCK == scope) {
    task_type = LOCK_OBJECT;
  } else if (ObSessionLockScope::TABLE_LOCK == scope) {
    task_type = LOCK_TABLE;
  }
  return task_type;
}

int unlock_request(ObTxDesc &tx,
                   const ObTxParam &tx_param,
                   const ObLockRequest &request)
{
  int ret = common::OB_SUCCESS;
  ObTableLockService *service = ::oceanbase::share::server_service<::oceanbase::transaction::tablelock::ObTableLockService>();
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else {
    switch (request.type_) {
      case ObLockRequest::ObLockMsgType::UNLOCK_OBJ_REQ:
        ret = service->unlock(
            tx, tx_param, static_cast<const ObUnLockObjsRequest &>(request));
        break;
      case ObLockRequest::ObLockMsgType::UNLOCK_TABLE_REQ:
        ret = service->unlock(
            tx, tx_param, static_cast<const ObUnLockTableRequest &>(request));
        break;
      default:
        ret = common::OB_NOT_SUPPORTED;
        break;
    }
  }
  return ret;
}

} // namespace

int acquire_named_lock(const common::ObString &lock_name,
                       const ObSessionLockOwner &owner,
                       int64_t timeout_us)
{
  int ret = common::OB_SUCCESS;
  transaction::tablelock::ObTableLockOwnerID lock_owner;
  transaction::tablelock::ObTableLockService *service =
      ::oceanbase::share::server_service<::oceanbase::transaction::tablelock::ObTableLockService>();
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(make_owner(owner, lock_owner))) {
  } else if (OB_FAIL(service->get_named_lock_manager().acquire(
                 lock_name, lock_owner, timeout_us))) {
    LOG_WARN("acquire named lock failed", KR(ret), K(lock_name), K(lock_owner));
  }
  return ret;
}

int acquire_mysql_table_lock(share::ObILockMetadataSession &session_io,
                             transaction::ObTxDesc &tx,
                             const transaction::ObTxParam &tx_param,
                             const ObSessionLockOwner &owner,
                             const ObTableLockTarget &target,
                             int64_t timeout_us)
{
  int ret = common::OB_SUCCESS;
  bool need_lock = true;
  transaction::tablelock::ObLockTableRequest request;
  transaction::tablelock::ObTableLockService *service =
      ::oceanbase::share::server_service<::oceanbase::transaction::tablelock::ObTableLockService>();
  request.table_id_ = target.table_id_;
  request.lock_mode_ = target.lock_mode_;
  request.op_type_ = transaction::tablelock::OUT_TRANS_LOCK;
  request.timeout_us_ = timeout_us;
  request.is_from_sql_ = true;
  request.detect_func_no_ = transaction::tablelock::DETECT_SESSION_ALIVE;
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_UNLIKELY(transaction::tablelock::NO_LOCK == target.lock_mode_)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(make_owner(owner, request.owner_id_))) {
  } else if (OB_FAIL(
                 transaction::tablelock::ObTableLockDetector::
                     record_detect_info_to_inner_table(
                         session_io, transaction::tablelock::LOCK_TABLE,
                         request, need_lock))) {
  } else if (need_lock && OB_FAIL(service->lock(tx, tx_param, request))) {
    LOG_WARN("acquire MySQL table lock failed", KR(ret), K(target));
  }
  return ret;
}

int acquire_mysql_table_lock(transaction::ObTxDesc &tx,
                             const transaction::ObTxParam &tx_param,
                             const ObSessionLockOwner &owner,
                             const ObTableLockTarget &target,
                             int64_t timeout_us)
{
  int ret = common::OB_SUCCESS;
  bool need_lock = true;
  ObLockTableRequest request;
  ObTableLockService *service =
      ::oceanbase::share::server_service<ObTableLockService>();
  request.table_id_ = target.table_id_;
  request.lock_mode_ = target.lock_mode_;
  request.op_type_ = SESSION_LOCK;
  request.timeout_us_ = timeout_us;
  request.is_from_sql_ = true;
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_UNLIKELY(NO_LOCK == target.lock_mode_)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(make_owner(owner, request.owner_id_))) {
  } else if (OB_FAIL(service->get_session_table_lock_manager().acquire(
                 request.owner_id_, target.table_id_, target.lock_mode_, need_lock))) {
  } else if (need_lock && OB_FAIL(service->lock(tx, tx_param, request))) {
    LOG_WARN("acquire MySQL table lock failed", KR(ret), K(target));
  }
  return ret;
}

int rollback_mysql_table_lock(const ObSessionLockOwner &owner,
                              const ObTableLockTarget &target)
{
  int ret = common::OB_SUCCESS;
  ObTableLockOwnerID lock_owner;
  ObTableLockService *service =
      ::oceanbase::share::server_service<ObTableLockService>();
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(make_owner(owner, lock_owner))) {
  } else if (OB_FAIL(service->get_session_table_lock_manager().rollback_acquire(
                 lock_owner, target.table_id_, target.lock_mode_))) {
  }
  return ret;
}

int release_named_lock(const common::ObString &lock_name,
                       const ObSessionLockOwner &owner,
                       int64_t &release_count)
{
  int ret = common::OB_SUCCESS;
  transaction::tablelock::ObTableLockOwnerID lock_owner;
  transaction::tablelock::ObTableLockService *service =
      ::oceanbase::share::server_service<::oceanbase::transaction::tablelock::ObTableLockService>();
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(make_owner(owner, lock_owner))) {
  } else if (OB_FAIL(service->get_named_lock_manager().release(
                 lock_name, lock_owner, release_count))) {
  }
  return ret;
}

int release_all_named_locks(const ObSessionLockOwner &owner,
                            int64_t &release_count)
{
  int ret = common::OB_SUCCESS;
  transaction::tablelock::ObTableLockOwnerID lock_owner;
  transaction::tablelock::ObTableLockService *service =
      ::oceanbase::share::server_service<::oceanbase::transaction::tablelock::ObTableLockService>();
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(make_owner(owner, lock_owner))) {
  } else if (OB_FAIL(service->get_named_lock_manager().release_all(
                 lock_owner, release_count))) {
  }
  return ret;
}

int session_has_named_locks(const ObSessionLockOwner &owner,
                            bool &has_locks)
{
  int ret = common::OB_SUCCESS;
  transaction::tablelock::ObTableLockOwnerID lock_owner;
  transaction::tablelock::ObTableLockService *service =
      ::oceanbase::share::server_service<::oceanbase::transaction::tablelock::ObTableLockService>();
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(make_owner(owner, lock_owner))) {
  } else if (OB_FAIL(service->get_named_lock_manager().has_lock(
                 lock_owner, has_locks))) {
  }
  return ret;
}

int named_lock_is_free(const common::ObString &lock_name,
                       bool &is_free)
{
  transaction::tablelock::ObTableLockService *service =
      ::oceanbase::share::server_service<::oceanbase::transaction::tablelock::ObTableLockService>();
  return OB_ISNULL(service)
      ? common::OB_NOT_INIT
      : service->get_named_lock_manager().is_free(lock_name, is_free);
}

int get_named_lock_owner_session(const common::ObString &lock_name,
                                 uint32_t &session_id)
{
  int ret = common::OB_SUCCESS;
  transaction::tablelock::ObTableLockOwnerID lock_owner;
  transaction::tablelock::ObTableLockService *service =
      ::oceanbase::share::server_service<::oceanbase::transaction::tablelock::ObTableLockService>();
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(service->get_named_lock_manager().get_owner(
                 lock_name, lock_owner))) {
  } else if (OB_FAIL(lock_owner.convert_to_sessid(session_id))) {
  }
  return ret;
}

int release_session_locks(share::ObILockMetadataSession &session_io,
                          transaction::ObTxDesc &tx,
                          const transaction::ObTxParam &tx_param,
                          const ObSessionLockOwner &owner,
                          ObSessionLockScope scope,
                          int64_t &release_count)
{
  transaction::tablelock::ObTableLockOwnerID lock_owner;
  int ret = make_owner(owner, lock_owner);
  if (OB_SUCC(ret)) {
    const ObPersistedLockOwner persisted(lock_owner.type(), lock_owner.id());
    ret = release_persisted_locks(session_io, tx, tx_param, persisted,
                                  scope, release_count);
  }
  return ret;
}

int release_persisted_locks(share::ObILockMetadataSession &session_io,
                            transaction::ObTxDesc &tx,
                            const transaction::ObTxParam &tx_param,
                            const ObPersistedLockOwner &owner,
                            ObSessionLockScope scope,
                            int64_t &release_count)
{
  int ret = common::OB_SUCCESS;
  int tmp_ret = common::OB_SUCCESS;
  int64_t removed = 0;
  transaction::tablelock::ObTableLockOwnerID lock_owner;
  common::ObArenaAllocator allocator(common::ObModIds::OB_SQL_RES_TYPE);
  common::ObArray<transaction::tablelock::ObLockRequest *> requests;
  release_count = 0;
  if (OB_FAIL(make_owner(owner, lock_owner))) {
  } else if (OB_FAIL(
                 transaction::tablelock::ObTableLockDetector::
                     get_unlock_request_list(
                         session_io, lock_owner, task_type_for_scope(scope),
                         allocator, requests))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < requests.count(); ++i) {
      transaction::tablelock::ObLockRequest *request = requests.at(i);
      removed = 0;
      if (OB_ISNULL(request)) {
        ret = common::OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(
                     transaction::tablelock::ObTableLockDetector::
                         remove_detect_info_from_inner_table(
                             session_io, task_type_for_scope(scope), *request,
                             removed))) {
      } else if (OB_FAIL(unlock_request(tx, tx_param, *request))) {
      } else {
        release_count += removed;
      }
    }
  }
  for (int64_t i = 0; i < requests.count(); ++i) {
    transaction::tablelock::ObLockRequest *request = requests.at(i);
    if (OB_ISNULL(request)) {
      tmp_ret = common::OB_ERR_UNEXPECTED;
    } else {
      request->~ObLockRequest();
      allocator.free(request);
    }
  }
  if (OB_FAIL(ret)) {
    release_count = -2;
  }
  return ret;
}

int unlock_all_mysql_table_locks(transaction::ObTxDesc &tx,
                                 const transaction::ObTxParam &tx_param,
                                 const ObSessionLockOwner &owner,
                                 int64_t &release_count)
{
  int ret = common::OB_SUCCESS;
  ObTableLockOwnerID lock_owner;
  ObTableLockService *service =
      ::oceanbase::share::server_service<ObTableLockService>();
  common::ObSEArray<SessionTableLockManager::LockSnapshot, 4> locks;
  release_count = 0;
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(make_owner(owner, lock_owner))) {
  } else if (OB_FAIL(service->get_session_table_lock_manager().get_locks(
                 lock_owner, locks))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < locks.count(); ++i) {
      const SessionTableLockManager::LockSnapshot &lock = locks.at(i);
      ObUnLockTableRequest request;
      request.table_id_ = lock.table_id_;
      request.lock_mode_ = lock.lock_mode_;
      request.owner_id_ = lock_owner;
      request.op_type_ = SESSION_UNLOCK;
      request.timeout_us_ = 0;
      request.is_from_sql_ = true;
      const int unlock_ret = service->unlock(tx, tx_param, request);
      if (common::OB_OBJ_LOCK_NOT_EXIST == unlock_ret) {
        // Reconcile an in-memory reverse index retained after an ambiguous
        // transaction-end error: the actual LS lock is already gone.
        LOG_INFO("session table lock is already absent", K(request));
      } else if (common::OB_SUCCESS != unlock_ret) {
        ret = unlock_ret;
        LOG_WARN("unlock MySQL session table lock failed", KR(ret), K(request));
      }
      if (OB_SUCC(ret)) {
        release_count += lock.ref_count_;
      }
    }
  }
  if (OB_FAIL(ret)) {
    release_count = -2;
  }
  return ret;
}

int finish_unlock_all_mysql_table_locks(const ObSessionLockOwner &owner,
                                        int64_t &release_count)
{
  int ret = common::OB_SUCCESS;
  ObTableLockOwnerID lock_owner;
  ObTableLockService *service =
      ::oceanbase::share::server_service<ObTableLockService>();
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(make_owner(owner, lock_owner))) {
  } else if (OB_FAIL(service->get_session_table_lock_manager().release_all(
                 lock_owner, release_count))) {
  }
  return ret;
}

int session_has_locks(const ObSessionLockOwner &owner, bool &has_locks)
{
  int ret = common::OB_SUCCESS;
  bool has_named_locks = false;
  bool has_table_locks = false;
  ObTableLockOwnerID lock_owner;
  ObTableLockService *service =
      ::oceanbase::share::server_service<ObTableLockService>();
  has_locks = false;
  if (OB_ISNULL(service)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(make_owner(owner, lock_owner))) {
  } else if (OB_FAIL(service->get_named_lock_manager().has_lock(
                 lock_owner, has_named_locks))) {
  } else if (OB_FAIL(service->get_session_table_lock_manager().has_lock(
                 lock_owner, has_table_locks))) {
  } else {
    has_locks = has_named_locks || has_table_locks;
  }
  return ret;
}

int session_has_locks(share::ObILockMetadataSession &session_io,
                      const ObSessionLockOwner &owner,
                      bool &has_locks)
{
  return transaction::tablelock::ObTableLockDetector::
      check_lock_owner_exist_in_inner_table(
          session_io, owner.session_id_,
          owner.session_create_ts_, has_locks);
}

int session_lock_owners_equal(const ObSessionLockOwner &left,
                              const ObSessionLockOwner &right,
                              bool &equal)
{
  int ret = common::OB_SUCCESS;
  transaction::tablelock::ObTableLockOwnerID left_owner;
  transaction::tablelock::ObTableLockOwnerID right_owner;
  equal = false;
  if (OB_FAIL(make_owner(left, left_owner))) {
  } else if (OB_FAIL(make_owner(right, right_owner))) {
  } else {
    equal = left_owner == right_owner;
  }
  return ret;
}

int persist_session_lock_owner(const ObSessionLockOwner &owner,
                               ObPersistedLockOwner &persisted)
{
  int ret = common::OB_SUCCESS;
  transaction::tablelock::ObTableLockOwnerID storage_owner;
  if (OB_FAIL(make_owner(owner, storage_owner))) {
  } else {
    persisted.owner_type_ = storage_owner.type();
    persisted.owner_id_ = storage_owner.id();
  }
  return ret;
}

int get_persisted_lock_owner_session(const ObPersistedLockOwner &owner,
                                     uint32_t &session_id)
{
  int ret = common::OB_SUCCESS;
  transaction::tablelock::ObTableLockOwnerID storage_owner;
  if (OB_FAIL(make_owner(owner, storage_owner))) {
  } else if (OB_FAIL(storage_owner.convert_to_sessid(session_id))) {
  }
  return ret;
}

} // namespace data_plane
} // namespace oceanbase
