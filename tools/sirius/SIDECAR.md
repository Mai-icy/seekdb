# Sirius sidecar and live Arrow input

This second prototype adds two independently usable paths:

```text
Live input:
seekdb SELECT (one statement snapshot)
  → Python streaming cursor → Arrow IPC over local Unix socket
  → Sirius Fragment input → GPU operators → Arrow → JSON

Existing native SQL entry:
SIRIUS_EXECUTE_SUBSTRAIT(plan)
  → CPU-only proxy library → local Unix socket
  → Sirius execute_substrait → GPU operators → JSON
```

The live helper reads current seekdb data on every invocation. It does not write
Parquet, create a DuckDB table, or maintain a cache. The second path still accepts
the existing explicit file-based plans and requires SUPER in seekdb. These are
separate entry points: ordinary SQL does **not** automatically offload to the GPU,
and the live helper is **not yet** a seekdb physical operator.

Sirius still links DuckDB and uses its types and logical planning internally.
Removing the requirement for DuckDB tables does not remove that implementation
dependency. The new API takes a borrowed host Arrow schema/array, copies it to
cuDF on the configured GPU, synchronizes the copy, and enqueues the resulting
batch on `sirius_stream_0`. No host pointer crosses a process boundary.

## Build the Arrow-input backend

The adapter targets Sirius commit
`3fcff6a0488516db09ddc7e993564eb9a98b72d3`. That revision has Fragment execution but
no public host Arrow input method. `build_arrow_overlay.py` generates the small
`Fragment::push_arrow` addition, an internal batch enqueue hook, and an auditable
patch. It checks the source revision and refuses tracked source/header edits.

On the experiment machine, an already built matching Sirius library is reused:

```sh
# SIRIUS_SRC is a clean checkout; SIRIUS_BUILD is its existing Ninja build.
# OVERLAY must be outside both trees. Stop any process using this output before rebuilding.
pixi run --manifest-path "$SIRIUS_SRC/pixi.toml" python \
  tools/sirius/build_arrow_overlay.py "$SIRIUS_SRC" "$SIRIUS_BUILD" "$OVERLAY"
```

Only the FFI object is recompiled and the library relinked into `OVERLAY`.
The script validates the original library SHA-256, redirects the linker
dependency file, and leaves the borrowed source/build tree unchanged. Reuse is
intentionally specific to the validated existing binary; its hash is in the
script. Compiler/link invocations and the patch are saved beside the new library.
The overlay header directories precede the original include paths.

For a different machine, generate the same patch without requiring those binary
artifacts, apply it to a **dedicated** checkout at the pinned revision, then use
Sirius's normal Pixi build instructions:

```sh
python3 tools/sirius/build_arrow_overlay.py \
  "$SIRIUS_SRC" "$SIRIUS_SRC/build/release" "$OVERLAY" --patch-only
git -C "$SIRIUS_SRC" apply "$OVERLAY/sirius-arrow-input.patch"
# Build this dedicated Sirius checkout with its own documented Pixi workflow.
```

For the tested overlay build, compile the bridge as follows. For a full source
rebuild, point `SIRIUS_INCLUDE_DIR` and `SIRIUS_LIBRARY` at that patched build.

```sh
cmake -S tools/sirius -B build-sirius-arrow \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=ON \
  -DSEEKDB_SIRIUS_BUILD_BRIDGE=ON -DSEEKDB_SIRIUS_ARROW_INPUT=ON \
  -DSIRIUS_INCLUDE_DIR="$OVERLAY/include" \
  -DSIRIUS_LIBRARY="$OVERLAY/libsirius.so"
cmake --build build-sirius-arrow -j4
ctest --test-dir build-sirius-arrow --output-on-failure
```

The Linux `libseekdb_sirius_proxy.so` target needs neither CUDA nor Sirius. It is
also built with the default `SEEKDB_SIRIUS_BUILD_BRIDGE=OFF` configuration.
The seekdb binary from the first integration needs no kernel rebuild for this
proxy: it uses the same versioned C ABI.

## Run

Install the existing `requirements.txt` in a virtual environment. Configure a
private socket directory owned by the same OS user as seekdb (mode 0700).
Set `SIRIUS_DEPS` to the Sirius Pixi environment's `lib` directory and use absolute
paths below. The GPU configuration is the same explicit bounded configuration
described in [README.md](README.md).

```sh
CUDA_VISIBLE_DEVICES=1 \
SEEKDB_SIRIUS_CONFIG=/absolute/path/to/sirius.yaml \
LD_LIBRARY_PATH="$OVERLAY:$SIRIUS_DEPS" \
python tools/sirius/sidecar.py \
  --socket /absolute/private/run/sirius.sock \
  --library "$PWD/build-sirius-arrow/libseekdb_sirius_bridge.so"
```

In a separate shell, start seekdb using its existing instance parameters plus:

```sh
export SEEKDB_SIRIUS_LIBRARY="$PWD/build-sirius-arrow/libseekdb_sirius_proxy.so"
export SEEKDB_SIRIUS_SOCKET=/absolute/private/run/sirius.sock
# Start seekdb as in README.md. No CUDA environment is needed in this process.
```

The proxy has a 60-second total request deadline, configurable before server
startup by `SEEKDB_SIRIUS_TIMEOUT_MS` in `[1,600000]`. It catches disconnection,
truncated frames and excessive result sizes; it can reconnect after sidecar
restart without restarting seekdb. A timeout closes the connection but does not
cancel a running GPU kernel. The sidecar serializes requests and has a default
60-second socket I/O timeout. SIGTERM/SIGINT stop it after the active request;
a stuck native operation may require terminating that process.

For `sales(id BIGINT, region BIGINT, amount DOUBLE)`, set the normal seekdb
connection environment and run:

```sh
SEEKDB_PORT=2886 python tools/sirius/live_query.py sales \
  --columns region:int64,amount:float64 --group-by region --sum amount --count \
  --socket /absolute/private/run/sirius.sock
```

The helper executes one `SELECT region,amount FROM sales`; filtering/grouping is
done by Sirius. Identifiers are quoted. Database credentials and permissions
govern that SELECT; no credentials are sent to the sidecar. Separate calls see
subsequent committed updates/deletes. A change committed while a SELECT is being
transferred does not change that statement's view. This prototype does not attach
to a caller's existing transaction or provide multi-statement snapshot reuse.

## Bounds and supported behavior

- Input: 1–64 uniquely named `int64`/`float64` columns, NULLs supported. Schema
  types are explicit; callers must choose types matching the table. Other input
  types require additional conversion and SQL-semantic work.
- Plans: 1 MiB; schema frame: 64 KiB; batch frame: 16 MiB; each batch: at most
  1,048,576 rows; decoded fixed-width input: at most 512 MiB per query. The helper
  normally sends at most 65,536 rows per batch. One batch is in flight and its
  acknowledgement follows completion of the host-to-GPU copy.
- The current Fragment API runs **after all input is closed**. This is bounded
  store-and-forward execution, not arbitrary-size pipelined streaming. GPU
  intermediates can consume more than input size and remain subject to the
  configured engine memory limits. The IPC decoder also allocates host memory.
- Results retain the existing 1 MiB JSON bound. Use aggregate queries rather
  than returning millions of rows. Integer SUM exports decimal128 as a string.
- An empty stream is fed as a typed zero-row batch, so global SUM/COUNT returns
  `[null,0]`. Empty scans and grouped aggregates return no rows. Rejected plans,
  malformed batches and client disconnects release per-query input resources.
- This is a trusted local administrative endpoint. Socket mode is 0600, the
  directory is private, and both server and native proxy check peer OS UID.
  Same-user clients may submit arbitrary Sirius plans (including file reads).
  The sidecar is not a tenant authorization boundary and must not be exposed to
  database clients over a network.
- There is no automatic CPU fallback, cost model or GPU kernel cancellation.
  One GPU worker owns the runtime and destroys it before that worker's TLS.
  Failed sidecar processes can be restarted; the proxy does not restart them.

## Verification on 2026-10-23

On the existing isolated seekdb instance and one RTX 5880 GPU:

| Check | Result |
|---|---|
| Existing C++ loader/Arrow suite | 15 passed |
| CPU-only proxy protocol suite | 7 passed |
| Live Arrow/data visibility suite | 18 checks passed |
| Existing SQL suite through the proxy | 11 checks plus native CLI lifecycle passed |
| Force-kill/restart sidecar | CPU SQL survived; GPU SQL recovered on the same connection |
| Process mapping | seekdb loaded the proxy; no CUDA or Sirius engine library |

Tests live under `unittest/sql/sirius`:

```sh
python unittest/sql/sirius/test_sidecar_transport.py \
  --library "$PWD/build-sirius-arrow/libseekdb_sirius_proxy.so" -v
python unittest/sql/sirius/live_integration.py --socket "$SEEKDB_SIRIUS_SOCKET"
python unittest/sql/sirius/integration.py --shared-directory /absolute/shared/data \
  --cli "$PWD/build-sirius-arrow/sirius_execute_plan"
# Stop the existing sidecar first. This test starts/kills only its own child.
# Its environment needs the GPU configuration/dependencies shown above.
python unittest/sql/sirius/fault_isolation.py --socket "$SEEKDB_SIRIUS_SOCKET" \
  --library "$PWD/build-sirius-arrow/libseekdb_sirius_bridge.so" \
  --plan /absolute/shared/smoke.pb --seekdb-pid ACTUAL_SEEKDB_PID
```

### Live-path benchmark

Five million rows in the existing compacted `sirius_benchmark` table, 100 groups,
`grp,SUM(amount),COUNT(*)`, CPU parallelism 8, three timed repetitions with
alternating order. Every timed result matched. Both systems were already warm;
the live path selected only the two needed columns and sent 77 batches.

| Path | Median |
|---|---:|
| seekdb CPU SQL | 53.61 ms |
| Fresh SELECT → Python/MySQL → Arrow → sidecar/GPU → JSON | 8,859.13 ms |

The live prototype is about **165× slower** on this workload. This measurement
includes fresh data transfer on every invocation. It establishes correctness,
not a performance win. The earlier snapshot result of 2.25× excluded export and
does not apply here. The next performance step is native batch extraction from
seekdb; bypassing Python/MySQL is expected to help but has not been measured.
Raw measurements and test records are in
[`results/2026-10-23`](../../unittest/sql/sirius/results/2026-10-23).

```sh
python unittest/sql/sirius/live_benchmark.py --socket "$SEEKDB_SIRIUS_SOCKET" \
  --table sirius_benchmark --parallel 8 --repeats 3 --output live-results.json
```

## Toward native seekdb SQL

The intended next boundary is a seekdb physical operator feeding native batches
directly into this transport, followed by automatic optimizer selection:

1. A narrowly scoped read-only GPU physical operator consumes seekdb scan
   batches while retaining seekdb's statement snapshot and privilege checks.
2. Translate a supported subset of seekdb expressions/operators to Substrait;
   keep explicit checks for NULLs, integer/decimal overflow, casts, collations,
   timezone behavior and ordering. Unsupported queries stay on CPU.
3. Return typed batches to the seekdb executor instead of bounded JSON, and add
   real cancellation, backpressure and memory accounting.
4. Add costing for scan, conversion, transport and GPU work. CPU fallback must
   preserve the same snapshot and happen before any result is exposed.

That can give ordinary `SELECT` a native seekdb experience while GPU execution
remains in a separate process. In-process execution is a separate deployment
choice; it is not required for native SQL integration. Full removal of DuckDB
from Sirius would be a substantially larger upstream engine change.
