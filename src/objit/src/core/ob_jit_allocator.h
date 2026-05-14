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

#ifndef OB_JIT_ALLOCATOR_H
#define OB_JIT_ALLOCATOR_H

#ifdef _WIN32
#include <windows.h>
#define PROT_READ   0x1
#define PROT_WRITE  0x2
#define PROT_EXEC   0x4
#else
#include <sys/mman.h>
#endif
#include "lib/oblog/ob_log.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace jit {
namespace core {

enum JitMemType{
  JMT_RO,
  JMT_RW,
  JMT_RWE
};

#ifdef _WIN32
// Lazily allocate and return the address of a 16-byte executable trampoline
// page that performs `movabs rax, &ob_pl_seh_personality; jmp rax`.
//
// Why this exists:
//   On Windows x64, UNWIND_INFO::ExceptionHandler is a 32-bit RVA relative
//   to RTDyld's ImageBase = min(loaded JIT section). The COFF relocation
//   IMAGE_REL_AMD64_ADDR32NB on the personality reference is also resolved
//   at link time as `personality_addr - ImageBase` and must fit unsigned 32
//   bits. If JIT memory is mapped >4GB away from seekdb.exe (which is
//   typical when VirtualAlloc(NULL, ...) decides JIT lives at ~0x100_xxxx),
//   the relocation truncates and SEH dispatch jumps into garbage.
//
//   This function exposes a stable trampoline address that lives near where
//   the OS hands out JIT memory. JIT registers the trampoline as the
//   "eh_personality" symbol (instead of ob_pl_seh_personality itself), and
//   the JIT allocator scans downward from this address for new JIT
//   regions. Distance trampoline - ImageBase is then bounded by the scan
//   window and always fits in 32 bits.
//
// Thread-safety: idempotent; backed by std::call_once. Returns 0 only if the
// initial VirtualAlloc fails — callers must treat that as an unrecoverable
// SEH-broken state for JIT code.
uintptr_t ob_jit_get_personality_trampoline();
#endif


class ObJitMemoryBlock;
class ObJitMemoryGroup
{
public:
  ObJitMemoryGroup()
      : header_(nullptr),
        tailer_(nullptr),
        block_cnt_(0),
        used_(0),
        total_(0)
  {
  }
  ~ObJitMemoryGroup() { free(); }
  // Traverse block_list when allocating, if there is a block with available size, then directly get memory from the block
  // is_code_memory: on macOS, code memory needs special handling with MAP_JIT
  void *alloc_align(int64_t sz, int64_t align, int64_t p_flags = PROT_READ | PROT_WRITE, bool is_code_memory = false);
  // is_code_memory: on macOS, code memory uses pthread_jit_write_protect_np instead of mprotect
  int finalize(int64_t p_flags, bool is_code_memory = false);
  //free all
  void free();
  void reset();
  void reserve(int64_t sz, int64_t align, int64_t p_flags, bool is_code_memory = false);

  DECLARE_TO_STRING;
private:
  ObJitMemoryBlock *alloc_new_block(int64_t sz, int64_t p_flags, bool is_code_memory = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJitMemoryGroup);

private:
  ObJitMemoryBlock *header_;
  ObJitMemoryBlock *tailer_;
  int64_t block_cnt_;       // number of block allocated
  int64_t used_;        // total number of bytes allocated by users
  int64_t total_;       // total number of bytes occupied by pages
};

class ObJitAllocator
{
public:
  ObJitAllocator()
      : code_mem_(),
        rw_data_mem_(),
        ro_data_mem_()
  {}

  void *alloc(const JitMemType mem_type, int64_t sz, int64_t align);
  bool finalize();
  void reserve(const JitMemType mem_type, int64_t sz, int64_t align);

private:
  ObJitMemoryGroup code_mem_;
  ObJitMemoryGroup rw_data_mem_;
  ObJitMemoryGroup ro_data_mem_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObJitAllocator);
};

}  // core
}  // jit
}  // oceanbase

#endif /* OB_JIT_ALLOCATOR_H */
