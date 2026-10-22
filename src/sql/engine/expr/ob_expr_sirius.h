// Copyright (c) 2026 OceanBase.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase { namespace sql {

class ExprSiriusSubstrait : public ObFuncExprOperator {
public:
  explicit ExprSiriusSubstrait(common::ObIAllocator &alloc);
  int calc_result_type1(ObExprResType &type, ObExprResType &arg,
                       common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr, ObExpr &expr) const override;
  static int eval_substrait(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
};

} }
