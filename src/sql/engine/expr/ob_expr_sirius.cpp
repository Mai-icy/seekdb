// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_sirius.h"
#include "sql/engine/sirius/sirius_client.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase { namespace sql {
using namespace common;

ExprSiriusSubstrait::ExprSiriusSubstrait(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_SIRIUS_SUBSTRAIT, "sirius_execute_substrait", 1,
                       NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

int ExprSiriusSubstrait::calc_result_type1(ObExprResType &type, ObExprResType &arg,
                                         ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  arg.set_calc_type(ObVarcharType);
  arg.set_calc_collation_type(CS_TYPE_BINARY);
  type.set_varchar();
  type.set_length(OB_MAX_VARCHAR_LENGTH);
  type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  return OB_SUCCESS;
}

int ExprSiriusSubstrait::cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr, ObExpr &expr) const
{
  UNUSED(ctx);
  UNUSED(raw_expr);
  expr.eval_func_ = eval_substrait;
  return OB_SUCCESS;
}

int ExprSiriusSubstrait::eval_substrait(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = nullptr;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  // A Substrait plan can read server-side files independently of seekdb's
  // catalog. This experimental administrative entry point requires SUPER.
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (!OB_PRIV_HAS_ANY(session->get_user_priv_set(), OB_PRIV_SUPER)) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "SUPER");
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
  } else if (arg->is_null()) {
    result.set_null();
  } else if (arg->get_string().empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "empty Sirius Substrait plan");
  } else {
    char *output = expr.get_str_res_mem(ctx, OB_MAX_VARCHAR_LENGTH);
    if (OB_ISNULL(output)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      static SiriusClient client;
      char error[512] = {};
      size_t output_size = 0;
      const ObString plan = arg->get_string();
      const int status = client.execute(reinterpret_cast<const uint8_t *>(plan.ptr()), plan.length(),
                                        output, OB_MAX_VARCHAR_LENGTH, &output_size, error, sizeof(error));
      if (status != SEEKDB_SIRIUS_OK) {
        if (status == SEEKDB_SIRIUS_INVALID_ARGUMENT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, error);
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, error);
        }
        LOG_WARN("Sirius execution failed", K(status), K(error));
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
      } else {
        result.set_string(output, static_cast<int32_t>(output_size));
      }
    }
  }
  return ret;
}

} }
