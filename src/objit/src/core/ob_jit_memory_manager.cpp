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

#include "ob_jit_memory_manager.h"

#if defined(_WIN32)
#include <cstring>

// Forward-declare the PL SEH personality adapter. We cannot include
// ob_pl_exception_handling.h here because the JIT allocator layer is a lower
// dependency than the PL layer in the build graph. The full signature lives
// in src/pl/ob_pl_exception_handling.cpp.
extern "C" EXCEPTION_DISPOSITION ob_pl_seh_personality(
    EXCEPTION_RECORD *, void *, CONTEXT *, DISPATCHER_CONTEXT *);
#endif

namespace oceanbase {
namespace jit {
namespace core {

#if defined(_WIN32)
// UNWIND_INFO flags as defined by the Microsoft x64 ABI. We parse the unwind
// block by hand rather than pulling in <ehdata.h>, which is not part of the
// public Windows SDK headers.
static const uint8_t UNW_FLAG_EHANDLER_LOCAL  = 0x01;
static const uint8_t UNW_FLAG_UHANDLER_LOCAL  = 0x02;
static const uint8_t UNW_FLAG_CHAININFO_LOCAL = 0x04;

// Given the byte pointer to an UNWIND_INFO struct, return a pointer to its
// ExceptionHandler DWORD. Returns NULL if the struct has no handler (either
// it is a chain-info entry or neither EHANDLER/UHANDLER is set).
//
// UNWIND_INFO layout (MS x64 ABI):
//   byte 0: Version:3 | Flags:5
//   byte 1: SizeOfProlog
//   byte 2: CountOfUnwindCodes
//   byte 3: FrameRegister:4 | FrameOffset:4
//   bytes 4..4+2*CountOfUnwindCodes-1: UNWIND_CODE entries (2 bytes each)
//   [2-byte pad if CountOfUnwindCodes is odd, to 4-byte align]
//   if Flags & UNW_FLAG_CHAININFO: RUNTIME_FUNCTION (12 bytes) ← no handler
//   else if Flags & (UNW_FLAG_EHANDLER|UNW_FLAG_UHANDLER):
//                                   DWORD ExceptionHandler (4 bytes)
//                                   ULONG ExceptionData[...]
static DWORD *locate_unwind_info_exception_handler_field(uint8_t *unwind_info)
{
  DWORD *result = NULL;
  if (NULL != unwind_info) {
    uint8_t flags = static_cast<uint8_t>((unwind_info[0] >> 3) & 0x1F);
    uint8_t count = unwind_info[2];
    if (0 != (flags & UNW_FLAG_CHAININFO_LOCAL)) {
      // Chain-info entry: the slot after the unwind codes holds a copy of a
      // RUNTIME_FUNCTION pointing at the parent, not a handler RVA.
    } else if (0 == (flags & (UNW_FLAG_EHANDLER_LOCAL | UNW_FLAG_UHANDLER_LOCAL))) {
      // No handler registered for this function — nothing to patch.
    } else {
      size_t codes_bytes = static_cast<size_t>(count) * 2;
      if (0 != (count & 1)) {
        codes_bytes += 2;  // pad to 4-byte alignment
      }
      result = reinterpret_cast<DWORD *>(unwind_info + 4 + codes_bytes);
    }
  }
  return result;
}
#endif

#if defined(_WIN32)
void ObJitMemoryManager::register_windows_pdata()
{
  std::lock_guard<std::mutex> lk(pdata_mutex_);

  if (0 != pdata_base_) {
    uintptr_t exe_base = reinterpret_cast<uintptr_t>(GetModuleHandleW(NULL));
    char dbg[256];
    DWORD64 diff_high = (exe_base > pdata_base_)
                          ? static_cast<DWORD64>(exe_base - pdata_base_)
                          : 0;
    bool seh_reachable = (exe_base >= pdata_base_) && (diff_high < 0x80000000ULL);
    _snprintf_s(dbg, sizeof(dbg), _TRUNCATE,
      "[OB-JIT] pdata_base=0x%016llx exe_base=0x%016llx diff=0x%llx "
      "seh_reachable=%d\r\n",
      static_cast<unsigned long long>(pdata_base_),
      static_cast<unsigned long long>(exe_base),
      static_cast<unsigned long long>(diff_high), seh_reachable ? 1 : 0);
    OutputDebugStringA(dbg);
    // stderr write removed — keep console clean, debugger channel is enough.
  }

  for (int64_t i = 0; i < static_cast<int64_t>(pdata_pending_.size()); ++i) {
    PdataPending &pending = pdata_pending_[i];
    if (0 == pending.count_) {
      continue;
    }
    // RtlAddFunctionTable registers a dynamic function table so that
    // RtlDispatchException can find unwind info for JIT-generated code.
    // The RUNTIME_FUNCTION BeginAddress/EndAddress/UnwindInfoAddress fields
    // are RVAs relative to pdata_base_. RTDyld applies
    // IMAGE_REL_AMD64_ADDR32NB relocations using pdata_base_ as the image
    // base, so absolute addresses are stored as (address - pdata_base_).
    // With pdata_base_ passed as BaseAddress, Windows computes:
    //     real_address = BaseAddress + RVA.
    if (0 != pdata_base_) {
      if (RtlAddFunctionTable(pending.table_, pending.count_, pdata_base_)) {
        PdataEntry ent;
        ent.table_ = pending.table_;
        ent.base_ = pdata_base_;
        registered_pdata_.push_back(ent);
      }
      // else: registration failed; SEH unwind will not work for this module
    }
  }
  pdata_pending_.clear();
}

void ObJitMemoryManager::install_personality_trampoline_and_patch_xdata()
{
  std::lock_guard<std::mutex> lk(pdata_mutex_);

  bool active = true;
  DWORD trampoline_rva = 0;

  if (0 == pdata_base_) {
    // No code/data sections were registered for this module — nothing to patch.
    active = false;
  }

  if (active && NULL == personality_trampoline_) {
    // Lazy-allocate the trampoline from code memory. 16-byte alignment keeps
    // the movabs immediate naturally aligned for the i-cache.
    void *p = allocator_.alloc(JMT_RWE, PERSONALITY_TRAMPOLINE_SIZE, 16);
    personality_trampoline_ = reinterpret_cast<uint8_t *>(p);
  }

  if (active && NULL == personality_trampoline_) {
    // Allocator refused — fall back to unpatched behaviour.
    active = false;
  }

  if (active) {
    DWORD64 tramp_addr = reinterpret_cast<DWORD64>(personality_trampoline_);
    if (tramp_addr < pdata_base_) {
      // Trampoline landed in a lower-address block than pdata_base_. RVA would
      // underflow; skip.
      active = false;
    } else {
      DWORD64 diff = tramp_addr - pdata_base_;
      if (diff > 0xFFFFFFFFULL) {
        active = false;
      } else {
        trampoline_rva = static_cast<DWORD>(diff);
      }
    }
  }

  if (active) {
    // Write the trampoline body.
    //   48 B8 <imm64>   movabs rax, &ob_pl_seh_personality
    //   FF E0           jmp    rax
    uint64_t target = reinterpret_cast<uint64_t>(&ob_pl_seh_personality);
    uint8_t *t = personality_trampoline_;
    t[0] = 0x48;
    t[1] = 0xB8;
    std::memcpy(t + 2, &target, sizeof(target));
    t[10] = 0xFF;
    t[11] = 0xE0;
    // Pad remainder with 0x90 (nop) so the block is well-formed if decoded.
    for (size_t i = 12; i < PERSONALITY_TRAMPOLINE_SIZE; ++i) {
      t[i] = 0x90;
    }
  }

  if (active) {
    // Walk every accumulated .pdata entry and rewrite ExceptionHandler RVAs.
    // The .xdata bytes are still writable at this point because
    // allocator_.finalize() has not run yet.
    for (int64_t i = 0; i < static_cast<int64_t>(pdata_pending_.size()); ++i) {
      PdataPending &pending = pdata_pending_[i];
      for (DWORD j = 0; j < pending.count_; ++j) {
        RUNTIME_FUNCTION &rf = pending.table_[j];
        uint8_t *uinfo = reinterpret_cast<uint8_t *>(
            static_cast<uintptr_t>(pdata_base_) + rf.UnwindInfoAddress);
        DWORD *handler_field = locate_unwind_info_exception_handler_field(uinfo);
        if (NULL != handler_field) {
          *handler_field = trampoline_rva;
        }
      }
    }
  }

  // Emit a one-line trace so post-mortem analysis can tell whether the patch
  // took effect for this module.
  {
    char dbg[192];
    _snprintf_s(dbg, sizeof(dbg), _TRUNCATE,
        "[OB-JIT] personality trampoline: active=%d tramp=0x%016llx rva=0x%08lx\r\n",
        active ? 1 : 0,
        static_cast<unsigned long long>(
            reinterpret_cast<DWORD64>(personality_trampoline_)),
        static_cast<unsigned long>(trampoline_rva));
    OutputDebugStringA(dbg);
  }
}
#endif

}  // core
}  // jit
}  // oceanbase
