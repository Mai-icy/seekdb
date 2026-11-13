# Sirius GPU 查询集成分支

本分支包含 seekdb 侧的原生计划导出、Parquet writer、查询传输客户端和 GPU 结果回传。Python 适配服务、Substrait 编码器、Sirius 公开 FFI 桥接和端到端测试在独立仓库维护：

[Mai-icy/seekdb-sirius-adapter](https://github.com/Mai-icy/seekdb-sirius-adapter)

适配仓库通过 submodule 固定本分支的功能提交和未修改的 Sirius 上游提交。开发时更新本仓库；部署和验收时以适配仓库记录的固定版本组合为准。适配仓库的旧提取清单仅描述首次拆分来源，不是新的部署版本选择依据。

## 查询路径

```text
SQL/MySQL → seekdb 解析、权限、优化和读取快照
         → 原生 IR + 按快照生成的 Parquet 分片
         → 双向 TLS → 适配服务编码 Substrait
         → Sirius 原生读取 Parquet 并执行
         → 分批结果协议 → seekdb MySQL 回包
```

seekdb 导出 `EXPLAIN FORMAT=SIRIUS_JSON` 的原生 IR v1，适配器生成标准 Substrait Protobuf 字节。网络查询协议为 v2。跨机器无需共享目录，Parquet 文件内容通过原查询连接上传；结果在该连接上返回。Arrow C Stream 仅用于 Sirius 进程内结果接口，当前路径不提供 Arrow 输入或跨查询 GPU pin。

```sql
SET parallel_degree_policy = 'MANUAL';
SET _force_parallel_query_dop = 1;
SET execution_backend = 'gpu';
SET gpu_snapshot = '';
SELECT COUNT(*) FROM your_table;
```

默认执行后端仍为 CPU。GPU 路径要求启动数据库前配置适配端 endpoint、TLS、稳定且唯一的源实例 ID 和私有导出目录，具体环境变量见适配仓库 README。GPU 路径不支持的计划明确报错，不自动回退 CPU。

`gpu_snapshot=''` 按当前语句快照导出所需列，结束后回收输入。非空版本名首次导出当前数据库所有普通持久表及可见列；完整发布后复用，后续 DML 不改变该版本。命名快照要求 autocommit 且不在显式事务内，刷新需使用新版本名。它是不可变磁盘输入，不是增量同步。

原生多分片 writer 通过现有 CXX 桥接接收有界批次，编码、压缩和上传可重叠；上传确认后删除源端分片。全部输入完整后才调用 Sirius，因此不承诺边接收输入边执行 GPU 查询。

## 独立 Parquet 导出

```sql
SELECT id, amount
INTO OUTFILE '/allowed/export/sales.parquet'
FORMAT=(TYPE='PARQUET')
FROM sales;
```

沿用 FILE 权限及 `secure_file_priv` 检查，路径位于 seekdb 主机，父目录必须存在，目标文件不得已存在。支持有符号整数、浮点、字符串、DATE 和不超过 38 位的 Decimal；不支持的类型明确报错。空结果仍生成带 schema 的文件。完成 footer 并同步后以不覆盖方式发布；SQL 事务回滚不会撤销已经完成的导出。

`tools/parquet_export.py` 可在源主机或共享文件系统上协调固定 SCN 的主键范围并行导出，使用 PyMySQL 和 PyArrow。数据仍由原生 `INTO OUTFILE` 写入，Python 不逐行重写数据。运行条件和参数通过 `--help` 查看；它与 SQL 查询路径中的自动快照准备是两个入口。

## 验证入口

原生 writer 检查：

```sh
cargo test --locked -p parquet-export
```

Linux 原生上传组件检查（需要 OpenSSL 开发头文件和库）：

```sh
g++ -std=c++17 -Wall -Wextra -Werror -pthread -Isrc \
  unittest/sql/sirius/transport_upload.cpp \
  src/sql/engine/sirius/sirius_query_transport.cpp \
  -lssl -lcrypto -o /tmp/sirius-transport-upload
/tmp/sirius-transport-upload
```

单文件 SQL 导出检查需使用隔离测试实例，并配置允许写出的私有目录：

```sh
SEEKDB_PORT=2881 python unittest/sql/parquet_export/integration.py \
  --output-dir /absolute/new-export-validation
```

完整 SQL/TLS/GPU 及 TPC-H Q1–Q22 验证入口位于适配仓库 `tests/integration/`。历史机器专用日志、大体积结果、证书与 Parquet 不随源码提交；本次整理提交不代表重新执行了完整 GPU/SF500 验收。
