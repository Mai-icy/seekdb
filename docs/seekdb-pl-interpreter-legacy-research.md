# seekdb PL 解释执行迁移后的编译执行残留调研

> 基线：`53d42a2c96218255eb1da471ecdac51424b0a8ab`
>
> 日期：2026-07-16
>
> 产品边界更新：2026-07-18，确认 PL 已完全解释执行；纯编译执行残留且不被解释执行依赖者，授权直接删除，不再逐条讨论。
>
> 证据范围：当前 seekdb 源码、Git 提交历史、仓库内构建与测试文件。未使用外部资料。
>
> 调研目的：确认 PL 已切换为解释执行后的真实执行边界，找出仍只服务于 LLVM/JIT、原生函数指针、预编译 DLL、旧 Code Generator 或双执行路径的代码，并区分“可直接删除”“迁移后删除”“解释器仍复用、必须保留”。

## 1. 去重与口径

`docs/seekdb-enum-driven-review.md` 的去重基线已经把 “PL Native/Debugger” 记为既有在途清理大类。因此本文不把“删除 PL Native/JIT”重新包装成一个新增产品候选，也不计入枚举审查的新增条数；本文是一份针对已经开始的解释器迁移的**代码收口清单**。

本文仍单独标出以下相邻功能，因为它们不只是几个旧名称，而是各自形成了可独立实施的删除闭包：

- 旧原生 dispatch、纯 SQL native shortcut 与 Code Generator Visitor 骨架；
- native DLL 持久化控制面；
- 依赖 DLL 表判断成功与否的 PL 自动重编译任务；
- Windows JIT unwind/SEH 桥；
- 因旧 Code Generator 插桩而存在、当前已经失活的 PL Profiler 状态；
- JIT 专用函数指针、栈大小、调试元数据、监控列和测试基线。

对于与主枚举审查重叠的 Oracle UDT、`FORALL`、高级 PL statement enum、Debugger 等功能，本文只说明它们与解释器迁移的关系，不重复给出产品功能删除结论。

## 2. 结论摘要

### 2.1 当前已经不存在 JIT/解释器双执行路径

当前唯一执行入口是 `src/pl/ob_pl.cpp:4050-4055`：

```cpp
int ObPLExecState::execute()
{
  ObPLInterpreter interpreter(*this);
  return interpreter.execute();
}
```

这里没有配置开关、函数指针判断、fallback，也没有按 routine 类型切换执行器。`ObPLInterpreter::execute()` 在 `src/pl/ob_pl_interpreter.cpp:1188-1217` 取得 `func.get_ast()`，从 `ast->get_body()` 开始执行。

因此当前问题不是“还有一条可达的 JIT 后门”，而是：

1. 旧执行路径已不可达，但一批为它准备的数据结构、函数入口和控制面仍在；
2. `ObPLBuilder` 仍承担 parser/resolver、表达式生成、AST 生命周期和缓存元数据构建，这部分不能因为名字里曾有 compile/codegen 就删除；
3. 部分旧功能在迁移时只删除了生产者，消费者、清理任务、内表和监控字段仍留着。

### 2.2 分类总表

| 类别 | 结论 | 代表项 |
| --- | --- | --- |
| 可直接删除 | 当前无生产者、无读取者或无调用者，且只服务于旧原生执行 | `action_`、`ObPLSqlInfo/simple_execute`、`interface_execute`、`ObPLStmtVisitor`、JIT SPI wrapper、`stack_size_`、`simple_calc_bitset_`、`di_buf_/di_len_`、旧 LLVM golden |
| 代码可直接删，schema/兼容需迁移 | 主体代码不可达，但仍带内表、配置、虚表列或升级兼容面 | PL recompile helper、persistent DLL 表/DDL 清理、`PL_CG_MEM_HOLD`、`plsql_optimize_level` |
| 需先替换后删除 | 旧 native 优化状态仍嵌在当前 runtime 数据结构里 | Object-access getter 函数指针、`obj_access_exprs_` 收集链、Windows unwind 包装 |
| 已完成产品确认，整体删除 | 当前执行链已断，但名称上是一个独立用户功能 | `ObjectMode::PROFILE` / `DBMS_PROFILER`，已按 ENUM-12 确认删除 |
| 必须保留 | 解释器正在依赖 | AST、parser/resolver、`ObStaticEngineExprCG`、`ObExprGeneratorImpl`、SPI runtime、表达式 frame、PL cache、package build lock、依赖失效、异常分类语义 |

### 2.3 规模下界

- `src/pl` 下 C/C++ 源码和头文件当前约 **62,448 行**。
- 已经完成的三个核心迁移提交分别为：
  - `fa28bea9fbd`：147 files，`+1,624/-60,637`；
  - `c21cb10901a`：42 files，`+205/-1,869`；
  - `26b3cc41296`：108 files，`+6,904/-10,126`。
- 当前仍可直接量化的独立文件：
  - PL recompile helper：747 行；
  - Windows JIT unwind/SEH：318 行；
  - 旧 LLVM IR golden：540 行；
  - `DBMS_PROFILER` spec/body：406 行；
  - `ob_pl_persistent.{h,cpp}`：285 行，其中依赖检查部分仍需迁移保留。
- 不计生成代码和 schema/result 基线，本文识别出的后续删除面保守下界仍在 **2,000 行以上**；完成内表、系统变量、虚表和生成结果清理后会更大。

### 2.4 用户授权后的自动删除清单

从 2026-07-18 起，本文采用以下硬规则：只要代码唯一服务旧 PL native/JIT/编译执行，且当前解释器、PL AST build、SQL expression runtime、SPI 与普通 PL Cache 均无依赖，就直接进入自动删除清单。名字中带 `compile/codegen` 不是充分条件，必须以当前生产者和消费者为准。

| 自动项 | 直接删除或迁移后删除的范围 | 解释执行证据与处理 |
| --- | --- | --- |
| PL-AUTO-01 | `ObFuncPtr/action_`、`ObPLSqlInfo/sql_infos_`、AST `sql_stmts_`、`simple_execute()`、`interface_execute()/interface_name_` | 旧 Code Generator 曾写入 native 地址；当前 builder 把 `action_` 保持为 0，所有可达调用直接进入 interpreter。删除 shortcut 和元数据；保留仍影响匿名块类型语义的 `is_all_sql_stmt_` |
| PL-AUTO-02 | `ObPLStmtVisitor` 与全部 `accept()`、`ObPLSPIWrapper`、只为 JIT symbol registration 存在的 callable helper | interpreter 以 statement type switch 执行，visitor 无子类/调用者；LLVM symbol 注册生产者已经删除。保留 router 使用的 child traversal、SPI 本体与普通用户类型赋值语义 |
| PL-AUTO-03 | `stack_size_`、`di_buf_/di_len_`、`simple_calc_bitset_`、`_ob_enable_pl_dynamic_stack_check` 及 bootstrap 强制赋值 | native frame、JIT debug image、LLVM simple-calc 与动态 native stack check 均无现役生产/消费链。JIT symbol debug metadata 直接并入既有 PL Debugger 删除任务，不重复立项 |
| PL-AUTO-04 | raw/runtime object-access `get_attr_func_`，包括 serialize/copy/hash/branch 与 builder 清零循环 | 历史 Code Generator 会生成 getter 并写入地址；当前唯一 writer 只写 0，runtime 恒走通用 getter。先把 `obj_access_exprs_` 承担的 external-record default-expression 提取副作用迁为显式逻辑，再删收集链 |
| PL-AUTO-05 | `win32_pl_seh.h`、`win32_unwind_stubs.c`、CMake 登记、SEH trampoline、`force_restore_pl_stack_ctx`、纯 rethrow catch | 旧桥用于异常穿越 LLVM/JIT frame；`ObJitMemoryManager`、personality 与 LSDA 生产链已删。保留 Windows 通用 crash tracing、compiler-rt `__udivti3` 和第三方通用 libunwind；带真实 cleanup 的 catch 先改 scope guard |
| PL-AUTO-06 | `PL_CG_MEM_HOLD/pl_cg_mem_hold_`、三处恒写 0、`test_compile.result` LLVM IR golden、`PlJit/PlCodeGen` 测试/benchmark label、失真日志与注释 | 历史 compile path 会写真实 JIT 内存，当前 builder 只写 0；IR golden 无测试生产者。保留仍有意义的 AST/expression build 耗时与 `_ob_pl_compile_max_concurrency`，将文案改为 build/finalize |
| PL-AUTO-07 | `T_SP_PRAGMA_INLINE`、`T_SP_PRAGMA_UDF`、resolver no-op/孤立处理、`ObPLCompileFlag::UDF` | 当前 MySQL grammar 只产生 `T_SP_PRAGMA_INTERFACE`；`INLINE` resolver 是 no-op，`UDF` flag 没有 interpreter/optimizer 消费者。删除时保留数值 3961/3962 空洞；保留 `PRAGMA INTERFACE` 及 `INTF` flag |

以下相邻项不重复或不自动删除：

- PL recompile job、persistent native DLL、PL Debugger、显式 `ALTER COMPILE`、PL Result Cache、远程 Package State 和 `PLSQL_OPTIMIZE_LEVEL` 已在既有 seekdb 审查文档中明确在途，本文发现的补充残件并入原任务；
- PL Profiler 已作为 ENUM-12 完成产品确认，不再以 PL-AUTO 重复编号；
- `RESTRICT_REFERENCES` 是 resolver 侧语义约束，`SERIALLY_REUSABLE` 是 package-state 生命周期，不属于编译执行残留；
- `src/objit/include/objit/common/ob_item_type.h` 仍有 5 个真实生产消费者。PL-INT-08 是 canonical header 归位任务，不能按目录名直接删除。

## 3. 迁移历史证据

### 3.1 主切换提交

`fa28bea9fbd647b939904f8817dbbb5d7718845f`，2026-06-11：

> `pl: replace the LLVM ORC-JIT executor with a tree-walking interpreter`

提交说明明确表示：

- 以 `ObPLInterpreter` 直接遍历 resolved `ObPLStmt` tree；
- 替换 LLVM ORC-JIT；
- 覆盖声明/default、赋值、IF/CASE、三类循环、LEAVE/ITERATE、DO、静态 SQL、RETURN、Cursor、Handler/SIGNAL、PRAGMA INTERFACE、嵌套 CALL 与 OUT/INOUT；
- 循环继续轮询 KILL/timeout；
- 当时“passes the full PL mysqltest suite”。

该提交删除了：

- `src/pl/ob_pl_code_generator.cpp`：8,480 行；
- `src/pl/ob_pl_code_generator.h`：926 行；
- `src/pl/ob_pl_adt_service.{h,cpp}`：820 行；
- 原 `src/objit` JIT engine、bundled JIT test 等大块实现。

### 3.2 后续收口提交

- `c21cb10901a8ea4b6e9cc798ab7f20cac0e9ad7b`
  - `Remove the remaining LLVM-JIT code and fix code affected by removing Oracle mode`
  - 删除 persistent DLL encode/decode/load/store、预编译 action 恢复、JIT debug/unwind 主体。
- `4aa475d8e069d0b5876f322513b788a41e460b61`
  - `fix(pl): release interpreter call temps`
- `26b3cc412969838aba1113ae56f1134ab31dc69f`
  - `[pl] remove obsolete compile code paths`
  - `ob_pl_compile.*` 改名 `ob_pl_build.*`；
  - `ObPLCompiler` 改名 `ObPLBuilder`；
  - `ObPLCompileUnit` 改名 `ObPLExecutableUnit`；
  - 删除 JIT、plan-cache-JIT 和 codegen 单测。
- `24d2e8b82e14b21a24700f16ecc536b4260711b7`
  - `fix(pl): avoid static sql buffer leak`
- `b56b41d419a4761e5978ecfafc560d5e30e8f918`
  - `fix(pl): pass cursor sql as ObString`

这条历史说明解释器不是实验分支，而是已经连续修复并成为当前主执行器。

## 4. 当前解释器调用链与覆盖形态

### 4.1 外部入口全部汇入相同执行器

当前主要入口包括：

| 形态 | 当前入口 |
| --- | --- |
| `CALL` procedure/function | `src/sql/engine/cmd/ob_routine_executor.cpp:207` |
| PS 匿名块 | `src/sql/engine/cmd/ob_routine_executor.cpp:423` |
| 文本匿名块 | `src/sql/engine/cmd/ob_routine_executor.cpp:518` |
| SQL 标量 PL UDF | `src/sql/engine/expr/ob_expr_udf.cpp:838` |
| PL 聚合 UDF | `src/sql/engine/user_defined_function/ob_pl_user_defined_agg_function.cpp:123` |
| Trigger | `src/sql/engine/dml/ob_trigger_handler.cpp:472` |
| PL 内嵌套 CALL | `src/pl/ob_pl_interpreter.cpp:828`，经 `ObPL::execute_proc()` 再进入相同路径 |

共享执行包装位于 `src/pl/ob_pl.cpp:1185-1243`：

1. 构造 `ObPLExecState`；
2. `pl.init()`；
3. `pl.execute()`；
4. deep-copy result；
5. `pl.final()`。

### 4.2 AST 在所有当前执行形态中都被长生命周期保留

- 匿名块：`src/pl/ob_pl_build.cpp:282-356`；
- 独立 routine：`src/pl/ob_pl_build.cpp:432-447`；
- package/nested routine：`src/pl/ob_pl_build.cpp:1335-1354`；
- package spec/body 的 AST allocator 生命周期：`src/pl/ob_pl_package_manager.cpp:1042-1044`、`1115-1118`。

这部分是解释器必需的新生命周期设计，不是应删除的旧 compile residue。

### 4.3 Statement enum 覆盖

`ObPLStmtType` 在 `src/pl/ob_pl_stmt.h:1760-1802` 有 39 个实际 statement 值。解释器 switch 位于 `src/pl/ob_pl_interpreter.cpp:1105-1185`，直接处理 23 个：

```text
PL_BLOCK, PL_VAR, PL_ASSIGN, PL_IF, PL_CASE,
PL_WHILE, PL_LOOP, PL_REPEAT, PL_LEAVE, PL_ITERATE,
PL_RETURN, PL_SQL, PL_CALL, PL_SIGNAL,
PL_CURSOR, PL_OPEN, PL_FETCH, PL_CLOSE,
PL_HANDLER, PL_COND, PL_EXECUTE, PL_DO, PL_INTERFACE
```

实现分布：

| 能力 | 代码 |
| --- | --- |
| Block、handler、warning/exception 匹配 | `ob_pl_interpreter.cpp:337-437` |
| 声明、赋值、user/sys/object access target | `449-535` |
| IF/CASE | `537-579` |
| WHILE/LOOP/REPEAT、LEAVE/ITERATE、KILL/timeout polling | `581-673` |
| RETURN | `675-693` |
| 静态 SQL | `695-726` |
| 嵌套 CALL、OUT/INOUT copy-back | `751-860` |
| Cursor | `863-942` |
| SIGNAL/RESIGNAL | `944-1023` |
| EXECUTE IMMEDIATE | `1025-1071` |
| PRAGMA INTERFACE、DO | `1074-1102` |

未进入 switch 的 16 个值是：

```text
PL_USER_TYPE, PL_USER_SUBTYPE, PL_FOR_LOOP, PL_CURSOR_FOR_LOOP,
PL_FORALL, PL_EXTEND, PL_DELETE, PL_INNER_CALL, PL_OPEN_FOR,
PL_NULL, PL_PIPE_ROW, PL_ROUTINE_DEF, PL_ROUTINE_DECL,
PL_RAISE_APPLICATION_ERROR, PL_GOTO, PL_TRIM
```

不能仅凭这 16 个值就得出“解释器漏了 16 项当前功能”的结论：

- `PL_ROUTINE_DECL/DEF` 主要是解析期元数据，不应作为普通 runtime body statement 执行；
- `T_SP_NULL` 在 resolver 仍有分支（`src/pl/ob_pl_resolver.cpp:474-476`），但当前 MySQL PL grammar 没有产生它的规则；
- `T_SP_INNER_CALL_STMT` 同样没有当前 grammar 生产者，实际内部 routine call 会降为 `PL_CALL`；
- `T_SP_EXTEND` grammar 仍可能生成，但 resolver 已没有对应 case，会在解释器前失败。这是另一条 Oracle/集合语法残留，不是 JIT fallback；
- 其余多项与已经在枚举审查中处理的 Oracle UDT、FORALL、复杂 collection 能力重叠。

结论是：**当前可达的普通 MySQL PL 没有发现仍走旧编译执行的 statement 形态。**

## 5. 可直接删除的旧原生执行骨架

### 5.1 `action_`、纯 SQL native shortcut 和旧 interface shortcut

#### 原用途

旧 Code Generator 会把编译后的 native 地址放入 `ObPLFunction::action_`：

- 普通 routine：旧 `ob_pl_code_generator.cpp:7014` 设置 JIT function address；
- 全部为 SQL 的 routine：旧 `generate_simple()` 在 6789 行设置 `&ObPL::simple_execute`；
- 旧执行器把 `action_` 转为函数指针并调用。

#### 当前证据

- `ObPLExecState::execute()` 已无条件解释执行；
- `src/pl/ob_pl.h:60` 的 `ObFuncPtr` 仅服务于该字段；
- `action_` getter/setter：`src/pl/ob_pl.h:447-448`；
- 字段：`src/pl/ob_pl.h:532`；
- 全仓没有实际 `get_action()/set_action()` 调用，只有 `src/pl/ob_pl_build.cpp:574` 的“stays 0 (unused)”注释。

与其绑定的纯 SQL shortcut 也已经完全失去生产者：

- `ObPLSqlInfo`：`src/pl/ob_pl.h:297-343`；
- `ObPLSqlInfo::generate()`：`src/pl/ob_pl.cpp:3915-3952`；
- `ObPL::simple_execute()`：`src/pl/ob_pl.cpp:3954-3996`；
- `sql_infos_`：`src/pl/ob_pl.h:505-507,529`；
- AST 的 `sql_stmts_`：`src/pl/ob_pl_stmt.h:1605,1656`；
- 当前只有 resolver 在 `src/pl/ob_pl_resolver.cpp:3534` 向 `sql_stmts_` push，没有任何读取者。

旧 direct-interface shortcut 同样失活：

- `ObPL::interface_execute()`：`src/pl/ob_pl.cpp:3998-4011`；
- `interface_name_` getter/field：`src/pl/ob_pl.h:453-457,545`；
- 当前解释器直接读取 `ObPLInterfaceStmt::entry_`，调用 `spi_interface_impl()`：`src/pl/ob_pl_interpreter.cpp:1078-1088`。

#### 建议

同一个提交删除：

1. `ObFuncPtr`、`action_`、getter/setter 和构造初始化；
2. `ObPLSqlInfo`、`sql_infos_`、`simple_execute()`；
3. AST `sql_stmts_` 及 resolver 的无用收集；
4. `interface_execute()`、`interface_name_`；
5. “legacy codegen path”相关失真注释。

`is_all_sql_stmt_` 暂时不能随之删除。它仍在 `src/pl/ob_pl.cpp:3472-3476` 影响匿名块参数的 NULL/cast 兼容行为，需要另行确认语义后再收缩。

### 5.2 `ObPLStmtVisitor` 整组骨架

#### 原用途

历史上 `ObPLCodeGenerateVisitor` 通过每个 statement 的 `accept(visitor)` 生成 LLVM IR。

#### 当前证据

- 基类纯虚入口：`src/pl/ob_pl_stmt.h:1816`；
- Visitor interface：`src/pl/ob_pl_stmt.h:2965-2994`；
- 27 个 `accept()` 定义：`src/pl/ob_pl_stmt.cpp:3937-4046`；
- 当前全仓没有 `ObPLStmtVisitor` 子类；
- 当前全仓没有对 PL AST `accept()` 的调用。

解释器使用显式 `switch (stmt->get_type())`，不复用这套 visitor。

#### 建议

删除：

- `ObPLStmtVisitor` 类；
- `ObPLStmt::accept()` 纯虚接口；
- 每个 statement 的 accept 声明和定义。

`get_child_size()/get_child_stmt()` 不能一起删。`src/pl/ob_pl_router.cpp:294-295` 仍用它递归分析 route SQL。

### 5.3 仅为 JIT 符号注册保留的 helper

当前以下符号只有声明和定义，没有调用者：

- `ObPLSPIWrapper`：`src/pl/ob_pl.cpp:51-62`；
- `ObPL::set_user_type_var()`：`src/pl/ob_pl.cpp:302-330`；
- `ObPL::set_implicit_cursor_in_forall()`：`src/pl/ob_pl.cpp:332-344`；
- `ObPL::unset_implicit_cursor_in_forall()`：`src/pl/ob_pl.cpp:346-362`。

历史父版本证明它们只通过 `ObLLVMHelper::add_symbol()` 暴露给 JIT IR：

- `set_user_type_var`
- `set_implicit_cursor_in_forall`
- `unset_implicit_cursor_in_forall`

建议删除 wrapper、三个函数及 `src/pl/ob_pl.h:1222-1228` 的声明。`FORALL` 产品功能是否整体删除由枚举/语法审查决定，但这三个 JIT callable 本身已经不可达。

### 5.4 栈大小、debug image 和 simple-calc 标记

#### `stack_size_`

- getter/setter：`src/pl/ob_pl.h:264-265`；
- 字段：`src/pl/ob_pl.h:287`；
- 当前没有读写者。

旧 Code Generator 在完成 machine code 后测量 JIT stack，并在旧 `ob_pl_code_generator.cpp:6990-7019` 做 stack limit 校验和 `set_stack_size()`。

配套配置也已失去消费者：

- `_ob_enable_pl_dynamic_stack_check`：`src/share/parameter/ob_parameter_seed.ipp:2161-2163`；
- bootstrap 还在 `src/rootserver/ob_root_service.cpp:4798` 强制设为 true；
- 当前 PL 执行代码没有读取该配置。

建议删除字段、配置和 bootstrap 赋值。

#### `di_buf_` / `di_len_`

- 字段：`src/pl/ob_pl.h:534-535`；
- 当前只有析构函数 `src/pl/ob_pl.cpp:4168-4170` 释放；
- JIT debug image 注册路径已在 `c21cb10901a` 删除。

建议直接删除。

#### `simple_calc_bitset_`

- 字段与 accessor：`src/pl/ob_pl_stmt.h:1511,1559-1562,1655`；
- 当前没有调用者；
- 历史用途是 LLVM calc 替代 SPI calc 的 expression 优化选择。

建议直接删除。

### 5.5 空的 C++ `try/catch` 和原生 unwind 清理包装

解释器通过 OB error code 传播 handler/SIGNAL，不再通过 native frame `_Unwind_RaiseException` 跳转。当前仍有多处为跨 native frame 异常准备的包装：

- `src/pl/ob_pl.cpp:253-272`；
- `src/pl/ob_pl.cpp:1225-1242`；
- `src/pl/ob_pl.cpp:1829-1860`；
- `src/pl/ob_pl.cpp:1942-1966`；
- `src/pl/ob_pl.cpp:2165-2188`；
- `src/sql/engine/expr/ob_expr_udf.cpp:835-878`，其中 catch 只 `throw`，没有任何处理。

这些包装可以清理，但不建议机械地一次性删除所有 cleanup：

1. 纯 `catch (...) { throw; }` 可直接删；
2. 带 schema guard、package state、`pl.final()` 恢复的分支，应先确认所有恢复都已由 scope guard/正常返回覆盖；
3. 删除后专项回归 nested CALL + CONTINUE HANDLER、UDF、anonymous block 和 failure cleanup。

## 6. 需替换后删除的 native 优化状态

### 6.1 Object-access getter 函数指针

#### 原用途

旧 Code Generator 为每个 object access expression 生成一个专用 getter：

- 遍历生成：旧 `ob_pl_code_generator.cpp:6157-6183`；
- JIT 编译后取函数地址并写回 raw expr：旧 `6272-6300`。

#### 当前状态

Raw expr 仍存函数地址：

- `src/sql/resolver/expr/ob_raw_expr.h:4774-4787,4806-4807,4821`；
- copy/equality/hash：`src/sql/resolver/expr/ob_raw_expr.cpp:3198,3259,3273`。

Runtime extra info 仍序列化并分支：

- 字段：`src/sql/engine/expr/ob_expr_obj_access.h:111-120`；
- serialize/reset/assign：`src/sql/engine/expr/ob_expr_obj_access.cpp:28-112`；
- 执行分支：`src/sql/engine/expr/ob_expr_obj_access.cpp:416-440`；
- raw-to-runtime copy：`508-555`。

当前唯一 writer 是 `src/pl/ob_pl_build.cpp:68-77`，而它只把地址设为 0。字段本身默认也是 0。于是当前实际执行永远走：

```cpp
get_attr_func(..., *ctx, ..., session)
```

不会调用 JIT getter。

#### 建议

第一步：

1. 删除 raw/runtime 两侧的 `get_attr_func_`；
2. 删除对应 serialize、copy、hash、same-as 状态；
3. `ExtraInfo::calc()` 无条件调用 runtime `get_attr_func()`；
4. 删除 `ob_pl_build.cpp:68-77` 的清零循环。

第二步再处理 `ObPLAstUnit::obj_access_exprs_`：

- 当前它主要服务于旧 getter 生成；
- 但 `ObPLBlockNS::extract_external_record_default_expr()` 在 `src/pl/ob_pl_stmt.cpp:2640-2693` 扫描 object access 时还顺便触发 external user type 展开；
- 因而不能只删数组而丢掉该 side effect，应先把“解析外部类型”改为明确逻辑，再删除 `obj_access_exprs_` 和 `extract_assoc_index()` 收集链。

回归重点：Trigger `NEW.col` 读写、record field、collection element/property、package variable、cursor parameter、OUT/INOUT object-access target。

### 6.2 JIT-only PL symbol debug metadata

当前仍有：

- `ObPLVarDebugInfo`：`src/pl/ob_pl.h:345-392`；
- `ObPLSymbolDebugInfoTable`：`src/pl/ob_pl_stmt.h:222-259`；
- AST 字段和 generator：`src/pl/ob_pl_stmt.h:1637-1641,1659`、`src/pl/ob_pl_stmt.cpp:3553-3632`；
- `variables_debuginfo_`、`name_debuginfo_`：`src/pl/ob_pl.h:410,422,428-438,527,539`。

当前没有读取 symbol debug table 的执行器。生成入口只在 `is_pl_debug_on()` 时触发，而 `src/sql/session/ob_sql_session_info.cpp:1683-1687` 恒定返回 false。

这部分原来用于 native/JIT debug info 和变量作用域映射。若既有 Debugger 删除任务已覆盖，应把这些字段纳入同一任务；不要只删 debugger API 而继续让 resolver/build 保留和传播无用元数据。

## 7. Windows JIT unwind/SEH 桥

### 7.1 独立文件

- `src/observer/win32_pl_seh.h`：97 行；
- `src/observer/win32_unwind_stubs.c`：221 行；
- Windows 构建仍在 `src/observer/CMakeLists.txt:530-531` 加入 stubs。

文件注释明确写明其服务对象：

- JIT-generated PL frame；
- LLVM IR 直接引用的 `_Unwind_*` symbol；
- `ObJitMemoryManager::register_windows_pdata`；
- JIT `eh_personality` 和 landing pad。

这些生产者都已删除。当前源码中 `_Unwind_*` 只剩这两个文件、旧注释和 `unittest/pl/test_compile.result`。

`win32_unwind_stubs.c:150-219` 后半段的 ExitProcess hook 也没有调用者；文件内 `win32_trace()` 还与 `src/observer/main.cpp:94` 重复。因而整文件而不是仅前 149 行都具备删除条件。

### 7.2 PL 执行包装

- Windows trampoline：`src/pl/ob_pl.cpp:113-156`；
- `execute_proc()` 的 Windows 分支：`src/pl/ob_pl.cpp:235-252`；
- `LinkPLStackGuard::force_restore_pl_stack_ctx()`：`src/pl/ob_pl.h:1294-1325`；
- `src/pl/ob_pl.cpp:1225-1235` 仍用 native `_Unwind_RaiseException` 解释 cleanup 风险。

建议作为独立 Windows 任务：

1. 删除两个 JIT unwind 文件和 CMake source；
2. 删除 `pl_execute_callee_seh()` 与 `force_restore_pl_stack_ctx()`；
3. 恢复普通 RAII/error-code cleanup 路径；
4. Windows 编译、Trigger/SIGNAL/handler/nested CALL 回归。

注意：`src/observer/main.cpp` 的通用 Windows crash tracing 与 PL JIT 无关，应保留。`src/observer/CMakeLists.txt:516-526` 的 compiler-rt builtins 当前还服务 `__udivti3`，不能因为变量名含 LLVM 就随本项删除。

## 8. PL Profiler：旧 Code Generator 插桩链已经完全断开

### 8.1 原功能

旧 Code Generator 在大量 PL statement 前后注入：

- `spi_pl_profiler_before_record`
- `spi_pl_profiler_after_record`

旧 `ob_pl_code_generator.cpp` 中有数十个插桩调用点，并通过 `profile_mode_` 选择带插桩的编译结果。`ObPLObjectKey::ObjectMode::PROFILE` 用来让 profiler 版本与 normal 版本使用不同 cache key。

### 8.2 当前不可达证据

- enum/key：`src/pl/pl_cache/ob_pl_cache.h:242-255`；
- 四处 key 构造仍根据 `get_pl_profiler()` 选择 PROFILE：
  - `src/pl/ob_pl.cpp:2230-2231`
  - `src/pl/ob_pl.cpp:2350-2351`
  - `src/pl/ob_pl_package_manager.cpp:1622-1623`
  - `src/pl/ob_pl_package_manager.cpp:1684-1685`
- 但 `src/sql/session/ob_sql_session_info.h:930-935` 的 `get_pl_profiler()` 恒定返回 `nullptr`；
- `src/sql/ob_spi.cpp:7552-7565` 的 before/after record 都是空实现；
- `ObPLBuilder` 仍设置并递归传播 `profiler_unit_info_`：
  - `src/pl/ob_pl_build.cpp:326,414-423,564,1392-1396`
  - `src/pl/ob_pl.h:260-262,285`
  - 当前没有读取者；
- `ObPLProfilerTimeStack` 只剩前置声明和 `ObPLExecCtx` 死字段：`src/pl/ob_pl.h:66,823-825,861`；
- allocator label `PlProfiler` 仍在 `src/pl/ob_pl_allocator.h:29`；
- `DBMS_PROFILER` SQL spec/body 共 406 行，但没有出现在当前 `src/share/inner_table/sys_package/syspack_codegen.py:187-205` 的 `syspack_config`；
- 包体引用 7 个 `DBMS_PROFILER_*` PRAGMA INTERFACE 名称，当前 C++ interface 表没有对应 entry。

### 8.3 建议

若产品确认不保留 PL 逐行 profiler，可整体删除：

1. `ObjectMode::PROFILE`，并删除整个恒为 NORMAL 的 `mode_` cache-key 维度；
2. 四处 profiler cache 分支；
3. `profiler_unit_info_` 与递归传播；
4. `ObPLProfilerTimeStack` 字段、空 session/SPI API、allocator label；
5. 未装载且无 interface 实现的 `dbms_profiler.sql`、`dbms_profiler_body.sql`；
6. `ObPLPackageManager::update_special_package_status()` 中空的 `DBMS_PROFILER` 分支：`src/pl/ob_pl_package_manager.cpp:1003-1024`。

保留边界：

- 不影响普通 PL cache、依赖失效和执行耗时统计；
- 不把 MySQL `SHOW PROFILE/PROFILES` 会话级兼容入口与 PL statement profiler 混为一谈；
- `compile_time_`、PLSQL execution time 等仍有当前读写者，不属于本项。

## 9. Persistent native DLL 控制面

### 9.1 当前只剩删除，没有生产和消费

系统表仍完整存在：

- `__all_ncomp_dll`：`src/share/inner_table/ob_inner_table_schema_def.py:3305-3324`；
- `__all_ncomp_dll_v2`：`src/share/inner_table/ob_inner_table_schema_def.py:3651-3672`；
- v2 仍含 `dll` 和 `stack_size`。

配置仍在：

- `_enable_persistent_compiled_routine`：`src/share/parameter/ob_parameter_seed.ipp:1767-1770`；
- 当前没有消费者。

`c21cb10901a` 已从 `ob_pl_persistent.cpp` 删除约 573 行：

- encode/decode DLL；
- read/store DLL；
- `gen_action_from_precompiled`；
- stack size encode/decode；
- process storage DLL。

当前 `ObRoutinePersistentInfo::delete_dll_from_disk()` 仍在 `src/pl/ob_pl_persistent.cpp:181-217`，并有 17 个真实 DDL 调用点，分布在：

- `src/rootserver/pl_ddl/ob_pl_ddl_operator.cpp`
- `src/rootserver/ob_ddl_operator.cpp`
- `src/rootserver/parallel_ddl/ob_create_view_helper.cpp`

即当前系统仍在维护“没有任何代码会再写入”的 DLL 表。

### 9.2 不能直接整文件删除

`ob_pl_persistent.{h,cpp}` 还混有两类当前依赖逻辑：

- `check_dep_schema()`：
  - `src/pl/ob_pl_package_state.cpp:869,879` 使用；
- `has_same_name_dependency_with_public_synonym()`：
  - `src/pl/ob_pl_build.cpp:805` 使用。

这些不是 native binary persistence 本身。建议：

1. 将仍需保留的 schema dependency helper 迁入 `ob_pl_dependency_util.*`；
2. 若 Synonym 删除任务会消灭第二个 helper，则按其依赖顺序直接删除，不必迁移；
3. 再删除 `ob_pl_persistent.*` 的 DLL 命名和 DDL cleanup。

### 9.3 迁移顺序

1. 先确认升级兼容窗口内是否仍可能存在旧版本写入的 DLL 行；
2. 若需要，做一次数据清空/版本门禁；
3. 删除 17 个 DDL cleanup 调用；
4. 删除 `_enable_persistent_compiled_routine`；
5. 将两个内表标记 abandoned/retired，保留 table id 不复用；
6. 更新 schema 生成结果和测试基线。

## 10. 已失活的 PL 自动重编译任务

### 10.1 规模与可达性

- `src/pl/pl_recompile/ob_pl_recompile_task_helper.cpp`：627 行；
- header：120 行；
- CMake 仍在 `src/pl/CMakeLists.txt:77-79` 编译；
- header 还被 `src/pl/ob_pl_interface_pragma.h:34` 无意义 include。

类公开 16 个静态方法。当前外部只有一个调用：

- `ObPLRecompileTaskHelper::init_tenant_recompile_job()`；
- 调用点：`src/rootserver/ob_ddl_operator.cpp:5122`；
- 函数本身在 `src/pl/pl_recompile/ob_pl_recompile_task_helper.cpp:82-90` 是 `TODO: mysql mode` 的纯 no-op。

其余 collect/batch/recompile 方法没有外部调用者。

### 10.2 仍依赖已经失去生产者的 DLL 表

`update_recomp_table()` 在 `src/pl/pl_recompile/ob_pl_recompile_task_helper.cpp:409-473` 查询 `__all_ncomp_dll_v2`，把是否存在 disk cache row 当作“重编译是否成功”的依据。当前已经没有任何 persistent DLL producer，这个成功判据不可能再形成闭环。

其他残留：

- `_enable_pl_recompile_job`：`src/share/parameter/ob_parameter_seed.ipp:2106-2108`，无消费者；
- `POLLING_ASK_JOB_FOR_PL_RECOMPILE`：`src/observer/dbms_scheduler/ob_dbms_sched_func_type.h:26`，只有 enum 声明；
- `FLUSH_NCOMP_DLL_JOB`：同文件 25 行，只有 enum 声明；
- `__all_pl_recompile_objinfo`：`src/share/inner_table/ob_inner_table_schema_def.py:3764-3779`。

### 10.3 建议

代码层可直接删除：

1. helper cpp/header；
2. CMake subtarget；
3. no-op tenant bootstrap hook；
4. `ob_pl_interface_pragma.h` 的无用 include；
5. 两个 scheduler enum。

兼容层迁移后删除：

1. `_enable_pl_recompile_job`；
2. `__all_pl_recompile_objinfo`；
3. 相关 schema/result 基线。

建议先删 recompile task，再退休 ncomp DLL 表，因为当前 helper 仍直接引用 v2 表名。

## 11. 监控、系统变量和测试幽灵

### 11.1 `pl_cg_mem_hold_` 永远写 0

- 字段：`src/pl/pl_cache/ob_pl_cache_object.h:99`；
- builder 三处只写 0：`src/pl/ob_pl_build.cpp:389,592,885`；
- 虚表读取：`src/observer/virtual_table/ob_gv_sql.cpp:570-578`；
- 内表/视图定义：`src/share/inner_table/ob_inner_table_schema_def.py:4466,15975`；
- 三份系统视图 desc result 仍展示 `PL_CG_MEM_HOLD`。

建议先迁移/删除虚表列和结果基线，再删除字段。继续保留只会稳定输出 0。

### 11.2 `PLSQL_OPTIMIZE_LEVEL`

当前系统变量和 `ObExecEnv` 仍保留 `plsql_optimize_level`：

- getter：`src/sql/session/ob_basic_session_info.h:2549`；
- setter 在 2550 行还有 `plsql_optimize_level_ = plsql_optimize_level_` 的自赋值；
- 当前 builder/interpreter没有读取 getter；
- 旧 Code Generator 曾在优化 LLVM module 时读取；
- 当前它只参与 exec-env load/serialize/compare，因此最多改变 cache environment，而不改变解释执行优化级别。

这是 public compatibility sysvar，不建议和纯死字段放在同一无迁移提交里。应单独决定：

1. 若保留兼容展示，明确标记 no-op；
2. 若删除，处理 sysvar id/生成文件、session exec-env 版本和测试；
3. 不要继续宣称“compiler optimization level”。

### 11.3 仍需保留但建议改名的 compile 术语

- `compile_time_` 仍测量 parse + resolve + expression build，建议保留，最多改名 `build_time`；
- `_ob_pl_compile_max_concurrency` 在 `src/pl/ob_pl_build.cpp:816-817` 仍限制 package build 并发，功能必须保留，可后续改名；
- `Code Generator`、`CG Time` 日志在 `src/pl/ob_pl_build.cpp:336,568,599` 已失真；
- `PL codegen lock` 日志在 `src/pl/ob_pl.cpp:2266,2367` 已失真；
- `src/pl/ob_pl_interpreter.h:34-36` 仍写“WIP/returns OB_NOT_SUPPORTED”，与当前实现矛盾；
- `src/pl/ob_pl_stmt.h:2672` 的 `is_object_udf_` 仍因“LLVM CG 方便”使用 `uint64_t`，字段语义若仍需要可改成 bool，但要先确认序列化/布局；
- `src/pl/ob_pl_type.h:485`、`src/pl/ob_pl_type.cpp:301` 只剩过时 native-code 注释。

### 11.4 旧测试和内存标签

- `unittest/pl/test_compile.result`：540 行旧 LLVM IR golden，包含 `_Unwind_RaiseException`、`eh_personality`、`llvm.stackprotector`，当前没有对应 test source，应删除；
- `tools/deploy/mysql_test/test_suite/pl/t/pl_bug_54806294_mysql.test:43-46`
  - 为检查 `PlJit/PlCodeGen` 内存泄漏 sleep 60 秒；
  - 当前 allocator label 只有 `PlBuild`；
  - result 仍期待 `PlJit` 行；
  - 应把测试目标迁到 `PlBuild` 或删除失效断言，但保留内存泄漏测试意图；
- `test/vostest/benchmark.yml:54` 仍登记 `PlJit`；
- `src/pl/ob_pl_allocator.h:28-29` 的 `PlBuild` 是当前真实构建内存，`PlProfiler` 则随 profiler 决策处理。

## 12. 明确必须保留的解释器依赖

### 12.1 SQL 表达式 “Code Generator” 不是 PL native Code Generator

`src/pl/ob_pl_build.cpp:62-113` 仍使用：

- `ObStaticEngineExprCG`
- `ObExprGeneratorImpl`

它们把 PL AST 中的 `ObRawExpr` 转为当前 SQL expression runtime 需要的 `ObExpr/ObSqlExpression`、frame info 和 eval function。解释器在 statement 层 tree-walk，但表达式仍通过 SPI 调用这些 runtime expression：

- `ObSPIService::spi_calc_expr_at_idx()`；
- 相关路径见 `src/sql/ob_spi.cpp:872-885`。

因此不能按 “code generator” 名称删除 `src/sql/code_generator` 或 `pl_finalize_expressions()`。

### 12.2 必须保留的运行时

- SPI 的静态 SQL、动态 SQL、cursor、变量读写、copy/cast；
- `ObPlCompiteWrite` object-access 写地址；
- AST 和 package cache object 的长生命周期 allocator；
- expression frame、operator factory、`exec_env`；
- PL cache、依赖表和 schema invalidation；
- `build_lock_` 和 package build 并发限制；
- loop early-exit polling；
- handler/SIGNAL 的 SQLSTATE 分类与 warning buffer 语义。

`src/pl/ob_pl_exception_handling.{h,cpp}` 当前只有 81 行，但 resolver 和 SPI 仍调用 `ObPLEH::eh_classify_exception()`。解释器内部又复制了一份 `classify_sqlstate()`。这里可以合并为一个共享 helper，但不能删除异常分类语义。

### 12.3 `src/objit` 不能按目录名直接删除

当前 `src/objit` 只剩：

- `src/objit/include/objit/common/ob_item_type.h`：3,103 行。

但它仍被 5 个生产文件直接 include：

- `src/share/roaringbitmap/ob_rb_utils.h`
- `src/share/geo/ob_geo_utils.h`
- `src/share/ob_rpc_struct.h`
- `src/share/schema/ob_schema_struct.h`
- `src/sql/engine/expr/ob_expr_operator.h`

顶层 `src/CMakeLists.txt:11` 仍提供 include path。该 header 与 `src/sql/parser/ob_item_type.h` 已发生内容分叉，不能直接删或简单换 include。

建议另立模块边界任务：

1. 把 canonical `ObItemType` 移到真正的 L1/shared 目录；
2. 合并两个副本并消除分叉；
3. 修改所有 include；
4. 再删除最后的 `src/objit` 目录和 include path。

这是目录归位，不是仍在运行的 JIT engine。

## 13. 建议拆项和依赖顺序

### PL-INT-01：删除纯死 native dispatch 骨架

范围：

- `action_`
- `ObPLSqlInfo/simple_execute/sql_infos_/sql_stmts_`
- `interface_execute/interface_name_`
- `ObPLStmtVisitor/accept`
- JIT-only SPI wrapper/helper
- `stack_size_`
- `di_buf_/di_len_`
- `simple_calc_bitset_`
- 纯 rethrow catch

特点：无 schema 迁移，适合第一批。

### PL-INT-02：Object-access runtime 去函数指针化

范围：

- raw/runtime `get_attr_func_`
- serialize/copy/hash/branch
- builder 清零循环
- 后续迁移 `obj_access_exprs_` 的 external-type side effect

特点：代码面不大，但需较强语义回归。

### PL-INT-03：删除 Windows JIT unwind/SEH

范围：

- 两个 Windows 文件
- CMake source
- SEH trampoline、stack ctx force restore
- 原生 unwind 注释

特点：必须有 Windows build 和 handler/SIGNAL 回归。

### PL-INT-04：关闭 PL Profiler 链

范围：

- `ObjectMode::PROFILE`
- profiler cache key
- unit/time-stack metadata
- 空 session/SPI API
- `DBMS_PROFILER` SQL 文件

特点：已按枚举审查 ENUM-12 完成产品确认，进入删除清单；不再重复讨论。

### PL-INT-05：删除 PL recompile job

范围：

- 747 行 helper
- CMake、include、tenant no-op hook
- config、scheduler enum
- `__all_pl_recompile_objinfo`

特点：既有配置/语法审查已明确在途删除，不新增编号；代码可先删，schema/config 走迁移。

### PL-INT-06：退休 persistent native DLL

范围：

- 依赖 helper 搬迁
- 17 个 DDL cleanup
- persistent config
- 两张 ncomp DLL 内表
- PL recompile 对 v2 表的依赖应已在上一项消失

特点：既有配置/语法审查已明确在途删除，不新增编号；实施时仍要处理升级兼容和旧数据。

### PL-INT-07：监控、sysvar、测试和术语收口

范围：

- `PL_CG_MEM_HOLD`
- `PlJit/PlCodeGen` 测试与 benchmark label
- LLVM IR golden
- `PLSQL_OPTIMIZE_LEVEL` 决策
- compile/codegen 日志和注释

特点：`PLSQL_OPTIMIZE_LEVEL` 已在既有配置审查中标记删除；其余恒 0 指标、孤立 golden、旧 label 和失真文案按 PL-AUTO-06 自动收口。`compile_time_` 与 `_ob_pl_compile_max_concurrency` 仍服务 AST/expression build，保留或只做语义改名。

### PL-INT-08：`ObItemType` canonical header 归位

范围：

- 合并 parser/objit 两个副本
- 迁移 L1 include
- 删除最后的 `src/objit`

特点：不是解释器功能删除，不应与 JIT runtime 删除混在同一提交。

推荐依赖顺序：

```text
PL-INT-01
  ├─ PL-INT-02
  ├─ PL-INT-03
  └─ PL-INT-07

PL-INT-04（ENUM-12 已确认，独立实施）

PL-INT-05
  └─ PL-INT-06

PL-INT-08（独立模块边界任务）
```

## 14. 验证建议

历史提交说明声称全套 PL mysqltest 已通过，但本次调研没有重新编译或运行测试。删除时建议至少覆盖：

1. Linux 全量编译；
2. Windows 编译，尤其 PL-INT-03；
3. PL mysqltest 全套。当前 `tools/deploy/mysql_test/test_suite/pl` 有 217 个 `.test/.sql` 源文件；
4. 重点语义：
   - DECLARE/default、assignment；
   - IF/CASE、WHILE/LOOP/REPEAT、LEAVE/ITERATE；
   - static/dynamic SQL；
   - cursor open/fetch/close；
   - handler、SIGNAL、RESIGNAL、warning completion；
   - nested CALL、OUT/INOUT；
   - SQL scalar UDF、aggregate UDF；
   - Trigger `OLD/NEW`；
   - object access 与 package variable；
5. PL cache hit/invalidation、package state 和 dependency version；
6. old-version upgrade：
   - ncomp DLL 表中存在旧行；
   - `__all_pl_recompile_objinfo` 存在旧行；
   - 删除虚表列/系统变量后的 schema compatibility；
7. 内存泄漏测试改为当前 `PlBuild` label，去掉无意义的 `PlJit` 断言。

## 15. 最终判断

PL 的核心迁移已经完成：当前所有可达调用形态都无条件进入 tree-walking interpreter，没有仍可工作的 LLVM/JIT fallback。

剩余工作主要是三层：

1. **纯代码骨架**：函数指针、Visitor、shortcut、JIT callable、栈/debug metadata，可直接清掉；
2. **旧执行控制面**：persistent DLL、recompile job 已属既有在途删除，Windows unwind 按自动清理，profiler 已由 ENUM-12 确认删除；仍需按兼容边界拆分实施；
3. **命名与观测残留**：`CG` 监控列、sysvar、测试结果和日志，需同步收口，避免系统继续表现得像仍有 native compiler。

最重要的保留边界是：不要把 PL statement 解释执行和 SQL expression runtime 混为一谈。前者已经不再生成 native PL body；后者仍必须生成表达式执行结构并由 SPI 调用，是当前解释器正常工作的组成部分。
