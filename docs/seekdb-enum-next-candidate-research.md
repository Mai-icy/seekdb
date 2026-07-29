# seekdb 枚举驱动审查：下一候选研究（ENUM-15）

> 调研基线：`53d42a2c9621`，2026-07-19
> 本文只记录候选与删除边界，不修改主审查记录，不同步语雀。

## 1. 去重结论

已对照 `docs/seekdb-enum-driven-review.md` 中 ENUM-01～14、PL-AUTO 清单，以及 `nijia.nj/public` 下五份标题含“seekdb + 审查记录”的在途删除文档。本候选涉及的 `T_OP_MULTISET`、`T_OP_COLL_PRED`、`ObMultiSetType`、`ObMultiSetModifier`、Oracle nested-table `MULTISET` 运算和 collection predicate 均无既有立项或在途项，可以作为新的 ENUM-15。

它与以下既有/现役能力只是名字相近，不能合并判断：

- MySQL JSON `MEMBER OF()`；
- 普通查询结果集的 `UNION / INTERSECT / EXCEPT`；
- hybrid search 内部的 `ObMultiSetTable`；
- PL collection 类型、collection method、BULK COLLECT 等其他独立能力。

## 2. 首选：ENUM-15 Oracle Collection MULTISET 运算与条件残链

### 2.1 枚举抓手与所服务的功能

入口枚举位于：

- `src/sql/parser/ob_item_type.h:206-207`：`T_OP_MULTISET`、`T_OP_COLL_PRED`；
- `src/objit/include/objit/common/ob_item_type.h:208-209`：同一表达式类型的镜像槽位；
- `src/sql/resolver/expr/ob_raw_expr.h:4831-4848`：
  - `ObMultiSetType::{UNION, INTERSECT, EXCEPT, SUBMULTISET, MEMBER_OF, IS_SET, EMPTY}`；
  - `ObMultiSetModifier::{ALL, DISTINCT, NOT}`。

这些状态服务的不是普通 SQL result-set set operation，而是 Oracle nested table 的集合值运算：

- `nested_table1 MULTISET UNION|INTERSECT|EXCEPT [ALL|DISTINCT] nested_table2`；
- `element [NOT] MEMBER OF nested_table`；
- `nested_table1 [NOT] SUBMULTISET OF nested_table2`；
- `nested_table IS [NOT] A SET / IS [NOT] EMPTY`；
- `CAST(MULTISET(SELECT ...) AS nested_table_type)`。

Oracle 官方定义也明确：`MULTISET` 运算的输入和输出是 nested table，而不是两个查询块的结果集。[Oracle Multiset Operators](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Multiset-Operators.html)

### 2.2 当前生产者：语法入口已经消失

当前 parser 生成脚本 `src/sql/parser/gen_parser.sh:36-37,89-90` 只读取 `sql_parser_mysql_mode.y/.l`。现行 SQL/PL MySQL grammar 中没有产生 `T_OP_MULTISET` 或 `T_OP_COLL_PRED` 的规则，也没有 `MULTISET UNION/INTERSECT/EXCEPT`、`SUBMULTISET`、`IS A SET`、`IS EMPTY`、`CAST(MULTISET(SELECT ...))` 规则。

历史反证很直接：提交 `58adac4988a` 删除 Oracle SQL parser；其父版本 `58adac4988a^:src/sql/parser/sql_parser_oracle_mode.y` 曾有完整生产规则：

- `collection_predicate_expr` 在约 1813～1866 行构造 `T_OP_COLL_PRED`；
- `bit_expr MULTISET_OP opt_multiset_modifier bit_expr` 在约 1914～1931 行构造 `T_OP_MULTISET`；
- `MULTISET select_with_parens` 在约 2285～2289 行设置 `ParseNode::is_multiset_`；
- 对应 lexer `58adac4988a^:src/sql/parser/sql_parser_oracle_mode.l:77,1524-1535` 才会把 `MULTISET UNION|INTERSECT|EXCEPT` 识别为 `MULTISET_OP`。

也就是说，现有 resolver、raw expression、codegen 和 runtime 只剩消费者，已经没有从用户 SQL/PL 到这些枚举的 AST 生产者。

现行 MySQL grammar 中仍有 `MEMBER OF`，但 `src/sql/parser/sql_parser_mysql_mode.y:1431-1441` 把它构造成函数名 `JSON_MEMBER_OF`，最终使用 `T_FUN_SYS_JSON_MEMBER_OF`，不会产生 `T_OP_COLL_PRED`。这是必须保留的 MySQL JSON 能力。

### 2.3 当前消费者：纵向链完整保留，但关键执行点均拒绝运行

死链仍贯穿多个层次：

1. Raw expression
   - `src/sql/resolver/expr/ob_raw_expr.cpp:86-87` 仍创建 `ObMultiSetRawExpr` / `ObCollPredRawExpr`；
   - `src/sql/resolver/expr/ob_raw_expr_resolver_impl.cpp:1177-1179,1290-1310` 仍解析枚举值；
   - `src/sql/resolver/expr/ob_raw_expr.h:4850-4897` 仍保留两套专用 raw-expr class。
2. Type deduction / rewrite / printer
   - `src/sql/resolver/expr/ob_raw_expr_deduce_type.cpp:757-771` 仍有专用推导；
   - `src/sql/printer/ob_raw_expr_printer.cpp:728-821` 仍反向打印完整 Oracle 语法；
   - DML 校验、pre-process、simplify-expression 中仍传播 `is_multiset_` 或为其留例外。
3. Expression construction and serialization
   - `src/sql/code_generator/ob_expr_generator_impl.cpp:1410-1420` 仍把枚举灌入 runtime operator；
   - `src/sql/engine/expr/ob_expr_extra_info_factory.cpp:98-99` 仍注册两种 extra-info；
   - `src/sql/engine/expr/ob_expr_cast.h:95-117` 仍保留 `CastMultisetExtraInfo`。
4. Runtime
   - `src/sql/engine/expr/ob_expr_multiset.cpp:61-71` 的 `calc_result_type2()` 直接返回 `OB_NOT_SUPPORTED`；
   - 同文件 `eval_multiset()`（约 515 行）直接返回 `OB_NOT_SUPPORTED`；
   - `src/sql/engine/expr/ob_expr_coll_pred.cpp:65-76` 的 `calc_result_type2()` 直接返回 `OB_NOT_SUPPORTED`；
   - 同文件 `eval_coll_pred()`（约 217 行）直接返回 `OB_NOT_SUPPORTED`；
   - `src/sql/engine/expr/ob_expr_cast.cpp:623-640` 的 `eval_cast_multiset()` 和 `cg_cast_multiset()` 也都直接返回 `OB_NOT_SUPPORTED`。

因此它不是“功能尚可通过内部入口使用”，而是“公开 producer 已删除、runtime 关键节点也明确拒绝执行”的双重不可达残链。

### 2.4 规模

按枚举、raw/runtime class、modifier/type、query-ref `is_multiset_`、factory/serializer 和专用分支的聚焦口径，共涉及约 **29 个生产源码文件、218 个命中**。

四个专用表达式文件本身共 **1,036 行**：

| 文件 | 行数 |
|---|---:|
| `src/sql/engine/expr/ob_expr_multiset.cpp` | 570 |
| `src/sql/engine/expr/ob_expr_multiset.h` | 137 |
| `src/sql/engine/expr/ob_expr_coll_pred.cpp` | 232 |
| `src/sql/engine/expr/ob_expr_coll_pred.h` | 97 |

此外还有 raw expr、resolver、printer、type deduction、cast、code generator、extra-info factory、rewrite、CMake 和两份 item-type 槽位。规模已经明显超过“零碎几行可以忽略”的阈值。

### 2.5 为什么建议删除

- **产品不匹配**：这是 Oracle object-relational nested-table SQL，不是 seekdb 的 MySQL collection 或 query set operation。
- **入口不可达**：唯一真实 producer 随 Oracle parser 删除；现行 MySQL grammar 不产生对应 AST。
- **实现已自证不可用**：type check、cast codegen 和 runtime eval 多处硬返回 `OB_NOT_SUPPORTED`。
- **恢复成本高**：恢复它不能只加回两个关键字；还需要恢复 Oracle nested-table SQL 类型、构造/CAST、比较语义、NULL/重复元素规则、parser、resolver、optimizer、序列化和执行测试，属于完整纵向功能重建。
- **维护成本真实存在**：表达式枚举、镜像槽、factory、序列化、printer、rewrite 仍要求所有通用框架理解一个永远不会产生且无法执行的节点类型。

结论：**建议列为 ENUM-15 并删除，无需保留兼容桩。**

### 2.6 建议删除面

1. 删除 `T_OP_MULTISET`、`T_OP_COLL_PRED` 及 Objit 镜像槽，并按仓库规则处理编号/生成物兼容。
2. 删除 `ObMultiSetType`、`ObMultiSetModifier`、`ObMultiSetRawExpr`、`ObCollPredRawExpr` 及其 factory/assign/hash/same-as 分支。
3. 删除四个专用 expression 文件及 CMake 注册。
4. 删除 resolver、type deduction、printer、code generator、extra-info factory 中的专用分支。
5. 删除 `ParseNode::is_multiset_`、`ObQueryRefRawExpr::is_multiset_` 及 cast-multiset/check-cast-multiset/rewrite 特判。
6. 删除只为该链存在的 `CastMultisetExtraInfo`、错误处理和序列化注册。
7. 删除失去被测对象的 Oracle MULTISET SQLQA/golden；补充禁止误删边界的 MySQL 回归测试。

### 2.7 必须保留或迁移的边界

| 边界 | 处理 |
|---|---|
| MySQL JSON `expr MEMBER OF(json_array)` | 保留。它走 `T_FUN_SYS_JSON_MEMBER_OF` 和 `ob_expr_json_member_of.*`。MySQL 官方把它定义为 JSON array membership。[MySQL Built-In Function Reference](https://dev.mysql.com/doc/refman/8.4/en/built-in-function-reference.html) |
| 查询 `UNION / INTERSECT / EXCEPT` | 保留。它们走 `T_SET_UNION / T_SET_INTERSECT / T_SET_EXCEPT` 和 set operator，不经过 `T_OP_MULTISET`。MySQL 官方支持的是组合 query block 的 set operation。[MySQL Set Operations](https://dev.mysql.com/doc/refman/8.4/en/set-operations.html) |
| hybrid search `ObMultiSetTable` | 保留。它是请求树中组合多个子查询的内部容器，与 Oracle nested-table expression 无继承或枚举关系。 |
| 普通 PL collection 类型/方法 | 不随本项整体删除；只删除 SQL `MULTISET` 运算/条件残链。其他 collection 能力需按各自入口和使用情况另审。 |
| `ObExprIn` 的 composite comparison helper | 先迁移再删文件。`src/sql/engine/expr/ob_expr_in.cpp:700` 唯一复用 `ObExprMultiSet::eval_composite_relative_anonymous_block()`，服务 UDT `IN/NOT IN` 比较而非 MULTISET。应移到 `ObExprIn` 或通用 PL-expression helper。 |

### 2.8 MySQL 有没有

**MySQL 没有 Oracle nested-table `MULTISET` 类型运算、`SUBMULTISET`、collection `IS A SET/IS EMPTY` 或 `CAST(MULTISET(SELECT ...) AS nested_table_type)`。**

MySQL 有两个必须区分的近似名字：

- `MEMBER OF()`：判断值是否属于 JSON array；
- `UNION / INTERSECT / EXCEPT`：组合多个 query block 的行结果集。

二者都不是 Oracle collection value semantics，所以删除 ENUM-15 不构成 MySQL 兼容性缺口。

### 2.9 验证建议

- 编译 parser、SQL expression/runtime、PL 相关目标，确认 enum/factory/serialization 无遗漏；
- 跑 JSON `MEMBER OF()` 回归；
- 跑 `UNION / INTERSECT / EXCEPT [ALL|DISTINCT]` 回归；
- 跑 hybrid-search 多子查询组合回归；
- 跑包含 composite UDT `IN/NOT IN` 的 PL 回归，确认迁移后的 anonymous-block helper 行为不变；
- 对 `T_OP_MULTISET|T_OP_COLL_PRED|ObMultiSetType|ObMultiSetModifier|is_multiset_` 做最终零引用检查。

## 3. 备选：Oracle DBMS_SQL 数字游标包残链

若 ENUM-15 实施后继续下一项，备选是 `ObPLCursorFlag::DBMS_SQL_CURSOR`（`src/pl/ob_pl_type.h:820-828`）、`ParseMode::DBMS_SQL_MODE`（`src/sql/parser/parse_node.h:89-100`）及 `is_dbms_sql_` 传播链。

它服务 Oracle `DBMS_SQL.OPEN_CURSOR/PARSE/BIND_VARIABLE/DEFINE_COLUMN|ARRAY/EXECUTE/FETCH_ROWS/COLUMN_VALUE/DESCRIBE_COLUMNS/TO_REFCURSOR` 数字游标 API。当前 `syspack_codegen.py` 的 19 个 MySQL 自动装载包没有 `dbms_sql`，`dbms_sql.sql/body.sql` 虽仍有 874 行和大量 `PRAGMA INTERFACE`，但 `ob_pl_interface_pragma.h` 对 `dbms_sql_*` entry 零注册；历史提交 `7978d94b54c` 已删除 package 装载项、20 个 C++ interface entry 及 3,414 行实现/接线，现状属于未收完的 Oracle PL 删除。

MySQL 没有 Oracle `DBMS_SQL` package/数字游标 API；有 SQL `PREPARE/EXECUTE/DEALLOCATE` 和 binary-protocol server-side prepared-statement cursor。[MySQL Prepared Statements](https://dev.mysql.com/doc/refman/8.4/en/sql-prepared-statements.html)

该项排在 MULTISET 之后，是因为当前 MySQL `COM_STMT_EXECUTE` cursor / `COM_STMT_FETCH` 复用了 `ObDbmsCursorInfo`、`make_dbms_cursor()` 和部分 `dbms_cursor_*` 实现。实施前必须先提取/改名为通用 `ObServerCursorInfo`，保留 MySQL PS/server cursor 所需的 SQL、参数、字段、open/fetch/close 子集，再删除 DBMS_SQL-only parse mode、flag、bind/define/array/package/errno 分支；不能机械删除整个类和所有同名前缀函数。

去重说明：主审查 ENUM-11 只有一句把旧 DBMS_SQL SQL 文件作为 NOCOPY 残件举例，并未形成完整功能候选；五份在途文档也无 DBMS_SQL 立项。若主审查维护者把该旁注视为已覆盖，则跳过本备选即可，不影响 ENUM-15 的独立性。

## 4. 推荐顺序

1. **ENUM-15 Oracle Collection MULTISET**：入口零生产者、runtime 硬拒绝、删除边界清晰，优先级最高。
2. **Oracle DBMS_SQL**：产品上同样应删，但先拆开当前 MySQL server cursor 的实现复用，再做纵向收口。
