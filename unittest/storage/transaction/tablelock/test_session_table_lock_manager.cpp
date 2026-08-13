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

#include <gtest/gtest.h>

#include "share/ob_errno.h"
#include "storage/tablelock/ob_session_table_lock_manager.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace transaction::tablelock;

class SessionTableLockManagerTest : public ::testing::Test
{
public:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, manager_.init());
    ASSERT_EQ(OB_SUCCESS,
              owner_.convert_from_value(ObLockOwnerType::SESS_ID_OWNER_TYPE, 1001));
  }

  void TearDown() override { manager_.destroy(); }

protected:
  SessionTableLockManager manager_;
  ObTableLockOwnerID owner_;
};

TEST_F(SessionTableLockManagerTest, recursive_acquire_and_rollback)
{
  bool need_lock = false;
  bool has_lock = false;
  ObSEArray<SessionTableLockManager::LockSnapshot, 4> locks;

  ASSERT_EQ(OB_SUCCESS, manager_.acquire(owner_, 1001, EXCLUSIVE, need_lock));
  ASSERT_TRUE(need_lock);
  ASSERT_EQ(OB_SUCCESS, manager_.acquire(owner_, 1001, EXCLUSIVE, need_lock));
  ASSERT_FALSE(need_lock);
  ASSERT_EQ(OB_SUCCESS, manager_.get_locks(owner_, locks));
  ASSERT_EQ(1, locks.count());
  EXPECT_EQ(2, locks.at(0).ref_count_);

  ASSERT_EQ(OB_SUCCESS, manager_.rollback_acquire(owner_, 1001, EXCLUSIVE));
  ASSERT_EQ(OB_SUCCESS, manager_.has_lock(owner_, has_lock));
  ASSERT_TRUE(has_lock);
  ASSERT_EQ(OB_SUCCESS, manager_.rollback_acquire(owner_, 1001, EXCLUSIVE));
  ASSERT_EQ(OB_SUCCESS, manager_.has_lock(owner_, has_lock));
  ASSERT_FALSE(has_lock);
}

TEST_F(SessionTableLockManagerTest, snapshot_and_release_all)
{
  bool need_lock = false;
  int64_t release_count = 0;
  ObSEArray<SessionTableLockManager::LockSnapshot, 4> locks;

  ASSERT_EQ(OB_SUCCESS, manager_.acquire(owner_, 1001, SHARE, need_lock));
  ASSERT_EQ(OB_SUCCESS, manager_.acquire(owner_, 1002, EXCLUSIVE, need_lock));
  ASSERT_EQ(OB_SUCCESS, manager_.get_locks(owner_, locks));
  ASSERT_EQ(2, locks.count());
  EXPECT_EQ(1001, locks.at(0).table_id_);
  EXPECT_EQ(SHARE, locks.at(0).lock_mode_);
  EXPECT_EQ(1002, locks.at(1).table_id_);
  EXPECT_EQ(EXCLUSIVE, locks.at(1).lock_mode_);

  ASSERT_EQ(OB_SUCCESS, manager_.release_all(owner_, release_count));
  EXPECT_EQ(2, release_count);
  ASSERT_EQ(OB_SUCCESS, manager_.get_locks(owner_, locks));
  EXPECT_TRUE(locks.empty());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
