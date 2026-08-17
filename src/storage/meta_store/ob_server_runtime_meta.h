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

#ifndef OCEANBASE_STORAGE_META_STORE_OB_SERVER_RUNTIME_META_H_
#define OCEANBASE_STORAGE_META_STORE_OB_SERVER_RUNTIME_META_H_

#include "share/resource/ob_server_runtime_config.h"
#include "storage/ob_super_block_struct.h"

namespace oceanbase
{
namespace omt
{

struct ObServerRuntimeMeta final
{
public:
  ObServerRuntimeMeta()
    : runtime_config_(),
      super_block_() {}
  ObServerRuntimeMeta(const ObServerRuntimeMeta &) = default;
  ObServerRuntimeMeta &operator=(const ObServerRuntimeMeta &) = default;

  ~ObServerRuntimeMeta() = default;

  bool is_valid() const
  {
    return runtime_config_.is_valid() && super_block_.is_valid();
  }

  int build(const share::ObServerRuntimeConfig &runtime_config,
            const storage::ObServerRuntimeSuperBlock &super_block);

  TO_STRING_KV(K_(runtime_config), K_(super_block));

  OB_UNIS_VERSION_V(2);

public:
  share::ObServerRuntimeConfig runtime_config_;
  storage::ObServerRuntimeSuperBlock super_block_;
};

}  // end namespace omt
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_META_STORE_OB_SERVER_RUNTIME_META_H_
