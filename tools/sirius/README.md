# Experimental Sirius GPU integration

This prototype adds `SIRIUS_EXECUTE_SUBSTRAIT(binary_plan)` to seekdb. The function
loads an optional bridge, executes a binary Substrait plan with Sirius's public
C++ FFI, and returns its Arrow results as JSON. Normal seekdb builds require no
CUDA, Sirius, Arrow, or Protobuf dependency.

**Next prototype:** [SIDECAR.md](SIDECAR.md) adds an isolated GPU process and
direct Arrow input from a live seekdb statement, without a Parquet intermediate.
It includes build instructions, transaction checks, failure recovery and honest
end-to-end measurements. The original snapshot path below remains available.

The first working integration uses **explicit Parquet snapshots**:

```text
seekdb table → streaming SQL export → immutable Parquet file
                                            ↓
Substrait plan → seekdb SQL function → Sirius GPU → Arrow → JSON
```

It does not automatically translate seekdb SQL plans, scan seekdb storage from
the GPU, or keep snapshots synchronized. Export is a separate operation; changes
committed after the export statement starts are absent from that snapshot.
The same absolute Parquet path must be visible to the helper and seekdb server.

## Why this integration point

Sirius is built around DuckDB logical plans and GPU operators. Its public
`sirius::ffi::Context::execute_substrait` accepts a portable plan boundary and
exports an Arrow C stream. seekdb has its own parser, optimizer, physical operators
and storage interfaces, so a DuckDB extension cannot be loaded as a seekdb plugin.

The adapter keeps these systems separate at a small versioned C ABI. A built-in
seekdb expression is the initial native entry point. Later work can replace the
manual plan builder with a seekdb physical operator and add a snapshot-aware
storage scan. That requires supported-operator checks, type/NULL/collation rules,
costing, cancellation and snapshot ownership before enabling automatic offload.

## Build

Build seekdb using its regular toolchain:

```sh
./build.py init
./build.py build -j16 seekdb
```

On a Linux NVIDIA GPU host, build Sirius separately following its upstream
instructions. The bridge needs a built `libsirius.so` with the public FFI and the
matching `include/sirius/ffi.hpp`. Set these paths to that build:

```sh
cmake -S tools/sirius -B build-sirius-linux \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DBUILD_TESTING=ON \
  -DSEEKDB_SIRIUS_BUILD_BRIDGE=ON \
  -DSIRIUS_INCLUDE_DIR=/absolute/path/to/sirius/include \
  -DSIRIUS_LIBRARY=/absolute/path/to/sirius/build/release/extension/sirius/libsirius.so
cmake --build build-sirius-linux -j4
ctest --test-dir build-sirius-linux --output-on-failure
```

Tests require GTest. `-DBUILD_TESTING=OFF` builds just the tools. With
`SEEKDB_SIRIUS_BUILD_BRIDGE=OFF` (the default), the loader, JSON converter and
15 tests can be built without a GPU or Sirius installation.

The runtime must find Sirius's dependent shared libraries. If using its Pixi
environment, add that environment's `lib` directory to `LD_LIBRARY_PATH`.
The tested source revisions and measurements are in [RESULTS.md](RESULTS.md).

## Start an isolated seekdb instance

Copy `sirius.example.yaml` to a writable location, set its spill directory, and
adjust the GPU/host budgets for the machine. The bridge requires an explicit
configuration file. All variables below must be set **before** starting seekdb.

```sh
export CUDA_VISIBLE_DEVICES=0
export SEEKDB_SIRIUS_LIBRARY="$PWD/build-sirius-linux/libseekdb_sirius_bridge.so"
export SEEKDB_SIRIUS_CONFIG=/absolute/path/to/sirius.yaml
export LD_LIBRARY_PATH=/absolute/path/to/sirius/.pixi/envs/default/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

build/bin/src/observer/seekdb \
  --base-dir /absolute/path/to/new-seekdb-instance --port 2886 \
  --log-level WARN \
  --parameter memory_limit=16G \
  --parameter datafile_size=10G --parameter datafile_maxsize=30G \
  --parameter log_disk_size=10G
```

No runtime is loaded unless the function is called. Without
`SEEKDB_SIRIUS_LIBRARY`, calls fail with an explanatory SQL error. The loader
reads this setting once; restarting seekdb is required to change the library.

## Export, plan and execute

Install helper dependencies in a virtual environment:

```sh
python3 -m venv /absolute/path/to/venv
/absolute/path/to/venv/bin/pip install -r tools/sirius/requirements.txt
export SEEKDB_HOST=127.0.0.1 SEEKDB_PORT=2886 SEEKDB_USER=root SEEKDB_DATABASE=test
# Supply SEEKDB_PASSWORD through your environment when required.
```

For a table `sales(id BIGINT, region BIGINT, amount DOUBLE)`:

```sh
python tools/sirius/snapshot.py sales /absolute/path/to/sales-v1.parquet \
  --columns id:int64,region:int64,amount:float64
python tools/sirius/make_plan.py /absolute/path/to/sales-v1.parquet sales.pb \
  --group-by region --sum amount --count
python tools/sirius/execute_plan.py sales.pb
```

Use the virtual environment's Python (or activate it) for these commands.
The exporter requires explicit Arrow types, handles NULL-only batches, streams
bounded batches, and refuses to replace an existing snapshot. A new export must
use a new path. The plan helper supports scans, numeric `--minimum column=value`
filters, grouping, SUM and COUNT; it is deliberately not a general SQL compiler.

The SQL entry point is callable directly with a bound binary parameter:

```python
cursor.execute("SELECT SIRIUS_EXECUTE_SUBSTRAIT(%s)", (plan_bytes,))
```

Example response:

```json
{"columns":[{"name":"sum_amount","format":"g"},{"name":"count","format":"l"}],"rows":[[40.5,3]]}
```

`columns[].format` is the Arrow C format string. Decimal128 values are JSON
strings to preserve precision. Signed/unsigned integers are emitted exactly as
JSON numbers; clients must use a JSON parser that preserves their precision.
Rows are positional so duplicate result names do not overwrite values.

For troubleshooting outside seekdb, the same loader is available as
`build-sirius-linux/sirius_execute_plan PLAN.pb` with the same environment.

## Verification and benchmark

Run on the GPU host with the server's credentials/environment. These tests create
temporary tables and a temporary restricted account, and clean them up:

```sh
python unittest/sql/sirius/integration.py \
  --shared-directory /absolute/path/to/shared-data \
  --cli "$PWD/build-sirius-linux/sirius_execute_plan"
```

The benchmark creates a new table, loads deterministic data, exports a snapshot,
validates every timed result, alternates CPU/GPU execution order, and records CPU
EXPLAIN output plus all timings. It leaves its table and files for reuse:

```sh
python unittest/sql/sirius/benchmark.py --create --rows 5000000 --repeats 7 \
  --parallel 1 8 --directory /absolute/path/to/benchmark
```

Omit `--create` to repeat measurements against the existing unchanged table and
snapshot. Use `--export` with a new directory to re-export an existing table.
GPU timings include SQL round trip, Substrait lowering, GPU execution,
Arrow conversion and JSON transfer. Export cost is reported separately. First
calls per plan and warm medians are separate (the runtime may already be warm);
this benchmark is not a comparison of the
same storage layout or a claim about arbitrary SQL workloads.

Storage state matters substantially. For a compacted CPU baseline, execute
`ALTER SYSTEM MAJOR FREEZE` on the isolated test instance and wait until
`SELECT STATUS, IS_ERROR FROM oceanbase.DBA_OB_MAJOR_COMPACTION` reports `IDLE`
and `NO`, then rerun without `--create`. Record this separately from the
freshly inserted data. The published results include both states.

## Current limits and lifecycle

- **SUPER is required**, including for plans over server-side files. Do not grant
  access to this administrative entry point to ordinary query users.
- Plan size and JSON output are each limited to 1 MiB. Oversized results fail;
  they are never silently truncated. Prefer small aggregate results.
- Flat NULL, boolean, integer, float, UTF-8 and decimal128 outputs are supported.
  Nested/dictionary/date/time outputs and non-finite floats fail explicitly.
- GPU calls are serialized within one process. The seekdb worker waits
  synchronously; timeout/cancellation is checked before and after the call and
  does not interrupt an executing GPU plan. There is no CPU fallback on this FFI
  path. Unsupported plans return an error.
- The GPU context is owned by one dedicated thread. When C++ destructors run
  (for example, in the native CLI), that thread destroys the context before its Rust TLS, then joins before
  Sirius's logging globals are destroyed. This fixes shutdown crashes observed
  when the engine was destroyed directly from a main-thread static destructor.
  Per-query Arrow streams, schemas and batches are released on success and
  error. The library stays loaded and memory reservations persist until shutdown;
  restart to change the engine configuration. GPU/host allocations are outside
  seekdb's tenant memory accounting. The standalone seekdb server's existing
  shutdown path uses `_Exit`; in that case the OS reclaims process resources.
- The original bridge described above runs Sirius in-process. It is an experimental integration, without
  fault isolation from native engine failures. It has not been validated for
  production multi-tenant operation. The optional sidecar proxy isolates that
  process; see [SIDECAR.md](SIDECAR.md) for its remaining limits.
