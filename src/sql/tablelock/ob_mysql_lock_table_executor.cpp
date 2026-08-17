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
#include "sql/tablelock/ob_mysql_lock_table_executor.h"
#include "common/ob_timeout_ctx.h"
#include "data_plane/tablelock/ob_session_table_lock.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "query/tablelock/ob_table_lock_runtime.h"
#include "share/ob_table_access_helper.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace sql;
using namespace transaction;
using namespace common;
using namespace observer;

namespace transaction
{
namespace tablelock
{

namespace
{

class ObSessionCleanupDeadlineGuard final
{
public:
  explicit ObSessionCleanupDeadlineGuard(int64_t cleanup_timeout_ts)
    : saved_timeout_ts_(THIS_WORKER.get_timeout_ts())
  {
    THIS_WORKER.set_timeout_ts(cleanup_timeout_ts);
  }

  ~ObSessionCleanupDeadlineGuard()
  {
    THIS_WORKER.set_timeout_ts(saved_timeout_ts_);
  }

private:
  int64_t saved_timeout_ts_;
};

} // namespace

int ObMySQLLockTableExecutor::execute(ObExecContext &ctx,
                                      const ObIArray<data_plane::ObTableLockTarget> &lock_targets)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *sess = ctx.get_my_session();
  uint32_t session_id = sess->get_server_sid();
  uint64_t session_create_ts = sess->get_sess_create_time();
  bool is_rollback = false;
  int64_t attempted_count = 0;
  bool rollback_completed = false;
  ObTxParam tx_param;
  int64_t timeout_us = THIS_WORKER.get_timeout_ts() - ObTimeUtility::current_time();
  const data_plane::ObSessionLockOwner owner(session_id, session_create_ts);
  OZ (ObLockContext::valid_execute_context(ctx));

  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockContext, stack_ctx) {
      OZ (stack_ctx.init(ctx, timeout_us));
      OZ (ObSqlTransControl::build_tx_param(sess, tx_param));
      CK (OB_NOT_NULL(sess->get_tx_desc()));
      for (int64_t i = 0; OB_SUCC(ret) && i < lock_targets.count(); ++i) {
        attempted_count = i + 1;
        OZ (data_plane::acquire_mysql_table_lock(*sess->get_tx_desc(),
                                                 tx_param,
                                                 owner,
                                                 lock_targets.at(i),
                                                 timeout_us));
      }
      if (OB_SUCC(ret)) {
        mark_lock_session_(sess, true);
      }

      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx.destroy(ctx, is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        COVER_SUCC(tmp_ret);
      } else if (is_rollback) {
        rollback_completed = true;
      }
    }
  }
  // Roll back the runtime registrations only when the inner transaction was
  // explicitly rolled back.  If commit itself reports an ambiguous error,
  // keep the reverse index so a later UNLOCK/disconnect can reconcile it.
  if (rollback_completed) {
    for (int64_t i = 0; i < attempted_count; ++i) {
      tmp_ret = data_plane::rollback_mysql_table_lock(owner, lock_targets.at(i));
      if (OB_SUCCESS != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
        LOG_WARN("rollback MySQL table lock registration failed", K(tmp_ret), K(i));
      }
    }
  }
  return ret;
}

int ObMySQLUnlockTableExecutor::execute(sql::ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  int64_t release_cnt = 0;
  uint32_t session_id = 0;
  uint64_t session_create_ts = 0;
  OZ (ObLockContext::valid_execute_context(ctx));
  OX (session_id = ctx.get_my_session()->get_server_sid());
  OX (session_create_ts = ctx.get_my_session()->get_sess_create_time());
  OZ (execute_(ctx, session_id, session_create_ts, release_cnt));
  return ret;
}

int ObMySQLUnlockTableExecutor::execute(uint32_t session_id,
                                        uint64_t session_create_ts,
                                        int64_t cleanup_timeout_ts)
{
  int ret = OB_SUCCESS;
  int64_t release_cnt = 0;
  const int64_t cleanup_start_ts = ObTimeUtility::current_time();
  const int64_t cleanup_timeout_us = cleanup_timeout_ts - cleanup_start_ts;
  ObSessionCleanupDeadlineGuard deadline_guard(cleanup_timeout_ts);
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
  ObTimeoutCtx timeout_ctx;
  if (OB_UNLIKELY(cleanup_timeout_us <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("session lock cleanup deadline has expired", K(ret), K(cleanup_timeout_ts));
  }
  OZ (timeout_ctx.set_abs_timeout(cleanup_timeout_ts));
  OZ (timeout_ctx.set_trx_timeout_us(cleanup_timeout_us));
  SMART_VAR(sql::ObSQLSessionInfo, session) {
    SMART_VAR(sql::ObExecContext, exec_ctx, allocator) {
      ObSqlCtx sql_ctx;
      ObSchemaGetterGuard guard;
      ObObj cleanup_timeout;
      cleanup_timeout.set_int(cleanup_timeout_us);
      const ObServerRuntimeSchema *runtime_schema = nullptr;
      LinkExecCtxGuard link_guard(session, exec_ctx);
      sql::ObPhysicalPlanCtx phy_plan_ctx(allocator);
      OZ (session.init(0 /*default session id*/, &allocator));
      OX (session.set_inner_session());
      OZ (GCTX.schema_service_->get_runtime_schema_guard(guard));
      OZ (guard.get_server_runtime_info(runtime_schema));
      OZ (session.init_runtime(runtime_schema->get_runtime_name_str()));
      OZ (session.load_all_sys_vars(guard));
      OZ (session.load_default_configs_in_pc());
      OX (session.set_query_start_time(cleanup_start_ts));
      OZ (session.update_sys_variable(
              share::SYS_VAR_OB_QUERY_TIMEOUT, cleanup_timeout));
      OZ (session.update_sys_variable(
              share::SYS_VAR_OB_TRX_TIMEOUT, cleanup_timeout));
      OX (sql_ctx.schema_guard_ = &guard);
      OX (exec_ctx.set_my_session(&session));
      OX (exec_ctx.set_sql_ctx(&sql_ctx));
      OX (exec_ctx.set_physical_plan_ctx(&phy_plan_ctx));
      OX (phy_plan_ctx.set_timeout_timestamp(cleanup_timeout_ts));

      OZ (ObLockContext::valid_execute_context(exec_ctx));
      OZ (execute_(exec_ctx, session_id, session_create_ts, release_cnt));
      OX (exec_ctx.set_physical_plan_ctx(nullptr));  // avoid core during release exec_ctx
    }
  }
  return ret;
}

int ObMySQLUnlockTableExecutor::execute_(ObExecContext &ctx,
                                         const uint32_t session_id,
                                         const uint64_t session_create_ts,
                                         int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_rollback = false;
  bool unlock_prepared = false;
  release_cnt = INVALID_RELEASE_CNT;
  OZ (ObLockContext::valid_execute_context(ctx));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockContext, stack_ctx) {
      OZ (stack_ctx.init(ctx));
      if (OB_SUCC(ret)) {
        ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
        ObTxParam tx_param;
        const data_plane::ObSessionLockOwner owner(session_id, session_create_ts);
        OZ (ObSqlTransControl::build_tx_param(session, tx_param));
        CK (OB_NOT_NULL(session->get_tx_desc()));
        OZ (data_plane::unlock_all_mysql_table_locks(
                *session->get_tx_desc(), tx_param, owner, release_cnt));
        OX (unlock_prepared = OB_SUCC(ret));
      }
      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx.destroy(ctx, is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        COVER_SUCC(tmp_ret);
      }
    }
  }
  if (OB_SUCC(ret) && unlock_prepared) {
    const data_plane::ObSessionLockOwner owner(session_id, session_create_ts);
    OZ (data_plane::finish_unlock_all_mysql_table_locks(owner, release_cnt));
    OZ (clear_lock_session_if_no_lock_(ctx, session_id, session_create_ts));
  }
  return ret;
}

} // tablelock
} // transaction

namespace query
{

int release_table_locks_for_session(uint32_t session_id,
                                    uint64_t session_create_ts,
                                    int64_t cleanup_timeout_ts)
{
  transaction::tablelock::ObMySQLUnlockTableExecutor executor;
  return executor.execute(session_id, session_create_ts, cleanup_timeout_ts);
}

} // namespace query
} // oceanbase
