/*
 * Copyright (c) 2026 OceanBase.
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
#pragma once
#include <string>
namespace oceanbase {
namespace common { class ObObj; }
namespace sql {
class ObLogPlan;
// Coordinator-local, immutable plan template. Parameter values are bound for
// each execution, never baked into a reusable physical plan.
std::string export_sirius_plan(ObLogPlan &plan);
std::string sirius_plan_literal(const common::ObObj &value);
} }
