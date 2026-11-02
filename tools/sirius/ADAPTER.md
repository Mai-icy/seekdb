# seekdb host adapter for upstream Sirius

This experimental adapter executes SELECT plans in Sirius while seekdb owns
the SQL connection, catalog, permissions, storage and transaction read view.
It targets the upstream Super Sirius/Substrait interface and requires the
companion `Mai-icy/sirius` branch `feat/seekdb-adapter` (based on upstream
`5bc93ffffd91829cbbf176e550d0fdb84bc0142d`). The seekdb branch is
`feat/sirius-substrait-adapter`, based on `main` at
`00e3a70c58e95d31484f906ea81b3b33e0d26351`.

## Execution and ownership

1. seekdb parses, binds and authorizes the statement, then captures its normal
   transaction snapshot. `execution_backend='gpu'` selects the adapter result
   provider before the seekdb CPU physical plan is opened.
2. The provider sends SQL, result width, table schema identities and an opaque
   transaction identity over a same-UID Unix socket (protocol version 5).
3. A **separate, schema-only** DuckDB 1.5.6 process produces Substrait. It sees
   empty catalog declarations and SQL, never table rows or database files.
   Statistics-based empty-table elimination is disabled. Each read occurrence
   becomes an independent `sirius_stream_<id>` input, including self joins.
4. Sirius's upstream-pinned DuckDB 2.0 consumer lowers that plan. The new public
   `Context::execute_substrait_arrow` binds bounded Arrow sources to the official
   `arrow_push_queue` / `arrow_import_ingestible` / `GPU_SCAN` path. Operators
   execute in Sirius; DuckDB is not used to store or execute the input data.
5. A single socket worker requests batches. The original seekdb SQL thread
   opens native cursors with its external session and the **same outer snapshot**
   for every table. It serializes visible rows directly into typed column
   buffers. Arrow wraps those buffers, and Sirius performs admitted host
   staging and GPU transfer. No CSV, Parquet or DuckDB data export is involved.
6. Arrow results return through the seekdb result provider and ordinary MySQL
   TCP protocol. Errors are returned to the caller; partially consumed inputs
   are never silently rerun on CPU.

The compiler is provisional: this is not yet a native seekdb physical-plan to
Substrait translator. The C ABI and data transport are independent of that
compiler, so a native translator can replace it later.

The companion Sirius branch adds two reproducible build-tree patches to its
pinned Substrait reader: anti-join enum mapping and literal expression LIMIT /
OFFSET support. It does not modify the submodule checkout. DECIMAL128 transport
is normalized by an explicit GPU projection to Sirius's SQL decimal carrier.
The schema-only producer's FIRST→any_value encoding is reversed; explicit
ANY_VALUE SQL is refused because that encoding is ambiguous.

## Build and run

Build Sirius using its own official Pixi workflow, in its independent checkout:

```sh
pixi run -e cuda12 cmake --preset release -DCMAKE_CUDA_ARCHITECTURES=89-real
pixi run -e cuda12 cmake --build build/release --target sirius_library sirius_arrow_embedder_test -j8
```

Choose the CUDA environment/architecture appropriate to the machine. Build
seekdb with its documented toolchain (`python3 build.py build -j10
--strategy=CppCompile=local seekdb`). Then build the C ABI plugin, using absolute
paths for `SIRIUS_ROOT` and `SEEKDB_ROOT`:

```sh
cmake -S "$SEEKDB_ROOT/tools/sirius" -B "$SEEKDB_ROOT/build-sirius-adapter" \
  -DBUILD_TESTING=OFF -DSEEKDB_SIRIUS_BUILD_BRIDGE=ON \
  -DSEEKDB_SIRIUS_BUILD_ADAPTER=ON \
  -DSIRIUS_INCLUDE_DIR="$SIRIUS_ROOT/include" \
  -DSIRIUS_LIBRARY="$SIRIUS_ROOT/build/release/libsirius.so"
cmake --build "$SEEKDB_ROOT/build-sirius-adapter" --target seekdb_sirius_adapter -j4
```

Install `requirements-adapter.txt` in an isolated Python environment. Install
the compiler extension once, using that environment's Python:

```sh
python -c 'import duckdb; c=duckdb.connect(); c.install_extension("substrait", repository="community")'
```

The compiler's shebang uses `python3` on PATH; ensure the same environment is
first on PATH. Queries only LOAD the installed extension. Supply an explicit
Sirius memory configuration; the adapter refuses to initialize without one.
An example for one visible GPU is `adapter.example.yaml`. Its 16 MiB host
blocks avoid fragmented multi-block allocations for the native ~4 MiB batches.

Start the adapter under the same OS user as seekdb:

```sh
export SEEKDB_SIRIUS_CONFIG="$SEEKDB_ROOT/tools/sirius/adapter.example.yaml"
export CUDA_VISIBLE_DEVICES=1
export LD_LIBRARY_PATH="$SIRIUS_ROOT/build/release:$SIRIUS_ROOT/.pixi/envs/cuda12/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
python "$SEEKDB_ROOT/tools/sirius/adapter_service.py" \
  --socket /absolute/private-run/adapter.sock \
  --library "$SEEKDB_ROOT/build-sirius-adapter/libseekdb_sirius_adapter.so" \
  --compiler "$SEEKDB_ROOT/tools/sirius/adapter_plan_compiler.py" \
  --pin-bytes 4294967296
```

The socket parent must be owned by the current user and mode 0700. Start a
**separate** seekdb instance with `SEEKDB_SIRIUS_QUERY_SOCKET` set to this socket.
Use its own data directory and unused TCP port. Do not enable the older
`SEEKDB_SIRIUS_NATIVE` scalar-aggregate bridge for this adapter.

```sql
SET execution_backend='gpu';
SET sirius_snapshot='';
SELECT l_returnflag, SUM(l_extendedprice) FROM lineitem GROUP BY l_returnflag;
SET execution_backend='cpu';
```

## Live and pinned reads

With `sirius_snapshot=''`, each query reads its current seekdb transaction.
Own writes and rollback are handled by native seekdb scans; the adapter does
not maintain its own MVCC mask.

A nonempty pin name selects an **immutable historical snapshot**, not a live
cache. It requires SUPER as well as normal statement privileges. Pins are
scoped by authenticated user ID, database and name. To capture all eight TPC-H
tables in one read view:

```sql
SET execution_backend='gpu';
SET sirius_snapshot='tpch_snapshot';
SELECT (SELECT COUNT(*) FROM region), (SELECT COUNT(*) FROM nation),
       (SELECT COUNT(*) FROM supplier), (SELECT COUNT(*) FROM customer),
       (SELECT COUNT(*) FROM part), (SELECT COUNT(*) FROM partsupp),
       (SELECT COUNT(*) FROM orders), (SELECT COUNT(*) FROM lineitem);
-- Subsequent SELECTs with this name reuse these captured rows.
-- Return to current transaction visibility:
SET sirius_snapshot='';
```

Capture eagerly completes every declared table, even when the first query has
a LIMIT. Publication occurs only after full capture and successful execution.
Missing tables cannot be appended using a later transaction. A table identity
or schema version change rejects reuse. DML intentionally leaves the historical
snapshot unchanged. Use a new name for a new snapshot. Pins are process-local;
restarting the adapter clears them. There is no automatic eviction or incremental
pin refresh in this version, and at most 16 snapshots are retained.

`--pin-bytes` is a separate hard host budget (0 disables pin). Payloads use
`cudaHostAllocPortable`, with Arrow retaining the allocation until its last
reader releases it. This is **host pin**, not permanent GPU residency: every
execution still uses Sirius's admitted staging copy and GPU upload. The service
logs `pin_build_s`, `planning_s`, `execute_s`, `pin_reused`, input bytes and pin
bytes separately. Exclude capture time only when reporting the explicitly
identified post-pin measurement.

## Bounds and present limits

- One query executes at a time in a service instance. The engine has a dedicated
  ownership thread; input producers and cancellation are thread-neutral.
- Native batches target ~4 MiB / at most 65,536 rows; wire frames are limited to
  16 MiB, declared retained backing to 32 MiB per input, and returned results
  to 64 MiB. At most 64 read occurrences and 64 columns per table are accepted.
- Current inputs: ordinary tables in the current database, lowercase identifiers,
  signed integer, FLOAT/DOUBLE, DECIMAL precision ≤38, DATE, CHAR/VARCHAR.
  Generated columns, temporary tables, prepared statements and locking SELECTs
  are refused. Unsupported plans fail explicitly.
- Text columns require binary collation. Full MySQL/DuckDB dialect equivalence
  is not claimed: trailing-space comparison rules, implicit casts, functions and
  session SQL modes still need broader conformance work. NULL sort placement
  is explicitly set to seekdb's MySQL behavior.
- Input IDs identify read occurrences, not catalog tables. Schema identities and
  the native read view are authoritative; a callback cannot replace them mid-query.
- Large results are collected before returning to seekdb. Streaming result
  delivery, native plan translation, filter/projection pushdown, incremental
  pin/MVCC invalidation and permanent GPU-resident input reuse remain future work.

## Validation commands

Use a disposable test database. SQL fixture tests create/drop their own tables.
TPC-H tests use existing seekdb data and perform no export or reimport:

```sh
python unittest/sql/sirius/test_adapter_protocol.py
SIRIUS_ADAPTER_LIBRARY=/absolute/libseekdb_sirius_adapter.so \
  python unittest/sql/sirius/adapter_gpu_integration.py
python unittest/sql/sirius/adapter_sql_integration.py --port 2902 --database adapter_tpch001
python unittest/sql/sirius/adapter_pin_integration.py --port 2902 --database adapter_tpch001
python unittest/sql/sirius/adapter_sql_integration.py --port 2902 --database adapter_tpch1 --tpch
python unittest/sql/sirius/adapter_sql_integration.py --port 2902 --database adapter_tpch1 --tpch --pin tpch_snapshot
```

The GPU fixture checks decimal extremes, NULLs, dates, Unicode, multi-batch reads,
self joins, anti joins, empty aggregates, NULL sorting, LIMIT and reader failure
recovery. SQL tests cover own writes, rollback and repeatable-read visibility.
Pin tests verify actual CUDA pin flags, backing lifetime, budget refusal,
historical reuse, fresh live reads, missing tables and schema invalidation.
