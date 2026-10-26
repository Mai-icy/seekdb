/*
 * Copyright (c) 2025 OceanBase.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sstream>
#include <gtest/gtest.h>
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
// Seed an already buffered linked block. Exercise the actual item reader and
// macro iterator without starting a storage service or allocating disk blocks.
#define private public
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#undef private
#include "lib/checksum/ob_crc64.h"
#include "rust/cxx.h"

// liboceanbase also contains Rust's embedded-engine bridge. This unit test
// never starts an embedded server; satisfy its otherwise unresolved entry and
// fail immediately if a test unexpectedly invokes it.
namespace oceanbase::in_process
{
int32_t run_server(rust::Str, rust::Slice<const rust::String>, rust::Fn<void()>)
{
  std::abort();
}
}

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

class TestTabletMacroIterator : public ::testing::Test
{
protected:
  char buffer_[4096] = {};
  int64_t length_ = 0;
  const MacroBlockId meta_id_{1, 101, 0};
  const MacroBlockId data_id_{2, 102, 0};
  const MacroBlockId shared_id_{3, 103, 0};

  void append(const ObTabletMacroType type, const bool nonempty)
  {
    const int64_t header_pos = length_;
    length_ += sizeof(ObLinkedMacroBlockItemHeader);
    const int64_t payload_pos = length_;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_i16(
        buffer_, sizeof(buffer_), length_, static_cast<int16_t>(type)));
    ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(
        buffer_, sizeof(buffer_), length_, nonempty ? 1 : 0));
    if (nonempty) {
      if (ObTabletMacroType::SHARED_DATA_BLOCK == type) {
        ObSharedBlockInfo shared(shared_id_, 12345);
        ASSERT_EQ(OB_SUCCESS, shared.serialize(buffer_, sizeof(buffer_), length_));
      } else {
        const MacroBlockId &id = ObTabletMacroType::META_BLOCK == type ? meta_id_ : data_id_;
        ASSERT_EQ(OB_SUCCESS, id.serialize(buffer_, sizeof(buffer_), length_));
      }
    }
    ObLinkedMacroBlockItemHeader header;
    header.payload_size_ = length_ - payload_pos;
    header.payload_crc_ = static_cast<int32_t>(ob_crc64(buffer_ + payload_pos, header.payload_size_));
    MEMCPY(buffer_ + header_pos, &header, sizeof(header));
  }

  void prepare(ObMacroInfoIterator &iter, const ObTabletMacroType target)
  {
    iter.is_inited_ = true;
    iter.is_linked_ = true;
    iter.target_type_ = target;
    iter.cur_type_ = ObTabletMacroType::MAX;
    auto &reader = iter.block_reader_;
    reader.is_inited_ = true;
    reader.buf_ = buffer_;
    reader.buf_len_ = length_;
    reader.buf_pos_ = 0;
    reader.first_item_offset_in_block_ = 64;
    reader.buf_block_id_ = MacroBlockId(4, 104, 0);
  }
};

TEST_F(TestTabletMacroIterator, empty_shared_tail_reaches_end)
{
  append(ObTabletMacroType::META_BLOCK, true);
  append(ObTabletMacroType::DATA_BLOCK, true);
  append(ObTabletMacroType::SHARED_DATA_BLOCK, false);
  ObMacroInfoIterator iter;
  prepare(iter, ObTabletMacroType::MAX);
  ObTabletBlockInfo info;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(info));
  EXPECT_EQ(meta_id_, info.macro_id_);
  ASSERT_EQ(OB_SUCCESS, iter.get_next(info));
  EXPECT_EQ(data_id_, info.macro_id_);
  EXPECT_EQ(OB_ITER_END, iter.get_next(info));
  EXPECT_EQ(OB_ITER_END, iter.get_next(info));
}

TEST_F(TestTabletMacroIterator, shared_payload_preserves_occupancy)
{
  append(ObTabletMacroType::META_BLOCK, false);
  append(ObTabletMacroType::DATA_BLOCK, true);
  append(ObTabletMacroType::SHARED_DATA_BLOCK, true);
  ObMacroInfoIterator iter;
  prepare(iter, ObTabletMacroType::MAX);
  ObTabletBlockInfo info;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(info));
  EXPECT_EQ(data_id_, info.macro_id_);
  ASSERT_EQ(OB_SUCCESS, iter.get_next(info));
  EXPECT_EQ(shared_id_, info.macro_id_);
  EXPECT_EQ(ObTabletMacroType::SHARED_DATA_BLOCK, info.block_type_);
  EXPECT_EQ(12345, info.occupy_size_);
  EXPECT_EQ(OB_ITER_END, iter.get_next(info));
}

TEST_F(TestTabletMacroIterator, targeted_shared_iteration)
{
  append(ObTabletMacroType::META_BLOCK, true);
  append(ObTabletMacroType::DATA_BLOCK, true);
  append(ObTabletMacroType::SHARED_DATA_BLOCK, true);
  ObMacroInfoIterator iter;
  prepare(iter, ObTabletMacroType::SHARED_DATA_BLOCK);
  ObTabletBlockInfo info;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(info));
  EXPECT_EQ(shared_id_, info.macro_id_);
  EXPECT_EQ(12345, info.occupy_size_);
  EXPECT_EQ(OB_ITER_END, iter.get_next(info));
}

TEST_F(TestTabletMacroIterator, targeted_data_stops_before_shared)
{
  append(ObTabletMacroType::META_BLOCK, true);
  append(ObTabletMacroType::DATA_BLOCK, true);
  append(ObTabletMacroType::SHARED_DATA_BLOCK, true);
  ObMacroInfoIterator iter;
  prepare(iter, ObTabletMacroType::DATA_BLOCK);
  ObTabletBlockInfo info;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(info));
  EXPECT_EQ(data_id_, info.macro_id_);
  EXPECT_EQ(ObTabletMacroType::DATA_BLOCK, info.block_type_);
  EXPECT_EQ(OB_ITER_END, iter.get_next(info));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
