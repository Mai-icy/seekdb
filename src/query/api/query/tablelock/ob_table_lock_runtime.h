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

#ifndef OCEANBASE_QUERY_API_TABLELOCK_OB_TABLE_LOCK_RUNTIME_H_
#define OCEANBASE_QUERY_API_TABLELOCK_OB_TABLE_LOCK_RUNTIME_H_

#include <stdint.h>

namespace oceanbase
{
namespace query
{

// Query-owned autonomous cleanup for runtime-only MySQL table locks.
// cleanup_timeout_ts is an absolute deadline selected by the lifecycle owner.
int release_table_locks_for_session(uint32_t session_id,
                                    uint64_t session_create_ts,
                                    int64_t cleanup_timeout_ts);

} // namespace query
} // namespace oceanbase

#endif // OCEANBASE_QUERY_API_TABLELOCK_OB_TABLE_LOCK_RUNTIME_H_
