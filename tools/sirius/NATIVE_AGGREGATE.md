# Native seekdb aggregation with Sirius

Follow-up: [NATIVE_PERFORMANCE.md](NATIVE_PERFORMANCE.md) records input profiling,
packing optimization and later measurements with scan-batch hints. The original
stage's measurements below remain as its historical baseline.
Optional bounded double-buffer receive-ahead and its before/after measurements
are documented in [PREFETCH.md](PREFETCH.md).

The third prototype integrates GPU execution into `ObHashGroupByOp`'s batch
execution path. Users run ordinary SQL; no external SELECT, Parquet export or
manual Substrait plan is needed for supported aggregates.

```text
seekdb SELECT and transaction snapshot
  → table scan / filter / serial join batches
  → native hash aggregate adapter: coalesce fixed-width columns
  → private Unix socket → reusable pinned host buffer (optional)
  → Arrow views → Sirius GPU aggregation
  → typed results in seekdb expression frames → HAVING / ORDER BY / SQL client
```

Sirius remains in its own process, with its existing internal DuckDB dependency.
The seekdb process has no CUDA, Arrow or Protobuf dependency. The Python sidecar
constructs Arrow views over received column buffers; it does not decode input
rows through Python or issue database queries. Small aggregate outputs currently
pass through the bridge's bounded JSON representation before being encoded as
typed binary results for seekdb.

## Enable and query

Build seekdb normally, and build the Arrow-input backend from [SIDECAR.md](SIDECAR.md).
The existing Sirius Arrow overlay is unchanged by this stage. In the sidecar's
CUDA/Pixi environment, start it with:

```sh
python tools/sirius/sidecar.py \
  --socket /absolute/private/run/sirius.sock \
  --library /absolute/path/libseekdb_sirius_bridge.so \
  --host-pin-mib 16 --warmup
```

The CUDA configuration and dependency library paths are described in SIDECAR.md.
`libcudart.so` must be discoverable in that matching environment to enable pinning.
Set these variables **before starting seekdb**:

```sh
export SEEKDB_SIRIUS_NATIVE=1
export SEEKDB_SIRIUS_SOCKET=/absolute/private/run/sirius.sock
# Start the newly built seekdb with your isolated instance parameters.
```

Then, for example:

```sql
SET @sirius_gpu = 1;
SELECT /*+ PARALLEL(1) USE_HASH_AGGREGATION */
       grp, SUM(amount), COUNT(*)
FROM sales
WHERE id >= 100
GROUP BY grp;

SET @sirius_gpu = 0; -- CPU comparison, same server and data
```

Both the server environment gate and integer session variable must be enabled.
Selection occurs when the physical aggregate executes, including when a
cached plan is reused. EXPLAIN still shows HASH GROUP BY; this prototype does not
add an optimizer cost model or a distinct GPU plan node. The sidecar logs native
request input/result row counts. The startup test also proves a qualifying query
fails while the sidecar is absent, so its GPU timing cannot silently be CPU timing.

## Exact scalar decimal SUM

`ObScalarAggregateOp` supports 1–16 decimal SUM and COUNT expressions over
serial seekdb input trees. This includes the arithmetic used by TPC-H Q6:

```sql
SET @sirius_gpu = 1;
SELECT /*+ PARALLEL(1) OPT_PARAM('pushdown_storage_level',2) */
       SUM(l_extendedprice * l_discount)
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24;
```

seekdb owns the scan, transaction visibility, filters, and argument expression
evaluation. Sirius performs the SUM. Storage aggregation pushdown is excluded:
the hint above disables it so the adapter receives individual visible rows.
EXPLAIN still shows SCALAR GROUP BY. This is an initial operator integration,
not full GPU execution of Q6 or of all 22 TPC-H queries.

Eligible input expressions use DecimalInt with precision 1–38. Output precision
must be at least the input precision and at most 76, with the same scale.
COUNT(*) and COUNT(expr) use exact sums of non-NULL indicators. AVG uses
seekdb's SUM/COUNT lowering and final division. DISTINCT, parallel aggregation,
and child operators outside the serial whitelist use CPU before consuming input. The serial SELECT and environment/session
gates above apply to this path too.

Protocol v3 carries one, two or four integer limbs per unscaled decimal value,
according to its declared precision (up to 9, 18 or 38 digits respectively), with
the same NULL bitmap on each limb. Low limbs are unsigned 32-bit values;
the highest limb is signed. Each limb is promoted to DECIMAL(38,0) **before** GPU
SUM; specifying only a decimal result type permits a BIGINT overflow in Sirius.
The sidecar merges partial sums using exact integers and returns signed 128-bit
cells. seekdb reconstructs the decimal in a 256-bit accumulator, checks the
declared precision, and restores the scale. No floating-point conversion occurs.
Empty and all-NULL inputs produce NULL SUM and zero COUNT in a single row.

Use bounded fragments, for example `--native-chunk-mib 128`, for large tables.
The existing 16/64/128 MiB frame and optional pinned receive buffers are reused.
The scalar protocol has a one-hour transport ceiling; seekdb statement
cancellation remains active. `SEEKDB_SIRIUS_PROFILE=1` additionally records
scan, expression evaluation, packing, transfer wait, and final-result wait times.
Sidecar startup/warmup and host-pin allocation are outside per-query timing;
scanning and transferring fresh seekdb rows are part of every query.

Validation on an isolated GPU host:

```sh
SIRIUS_TEST_LIBRARY=/absolute/path/libseekdb_sirius_bridge.so \
  python unittest/sql/sirius/test_native_exact_gpu.py
SEEKDB_PORT=2898 python unittest/sql/sirius/scalar_native_integration.py \
  --trace /absolute/path/native-sidecar.log
```

The SQL test requires per-query sidecar evidence, checks exact CPU/GPU results,
correlated rescans, own writes, another transaction's uncommitted writes,
rollback, committed changes, and repeatable-read visibility. The bridge exposes
an idempotent shutdown entry point so the sidecar destroys its GPU runtime before
interpreter teardown. The earlier v1/v2 grouped protocol remains unchanged.

For full-query correctness first, run the existing TPC-H runner with
`--modes cpu8`, alongside its source, verified import, database, scale-factor,
output-directory and sidecar-log arguments. This explicitly disables native GPU
execution for all 22 queries and does not require a running sidecar. The default
modes remain `cpu8 native1`; reports record actual GPU events separately from
successful CPU fallback. Neither mode name alone proves GPU coverage.

## Exact grouped aggregates and window MIN

The native TPC-H branch adds an exact grouped adapter before the legacy DOUBLE
adapter. It accepts 1–8 signed integer, DATE/MySQL DATE or `utf8mb4_bin` CHAR/VARCHAR
keys, with up to 16 SUM/COUNT and implicit FIRST_ROW outputs. String keys use
per-query dictionaries (trailing spaces follow the binary collation's equality);
there is no cross-query cache. Computed keys are evaluated by seekdb.

Decimal SUM inputs can have up to 76 digits. Protocol v4 carries up to eight
32-bit limbs per unscaled value; a 512-bit CPU accumulator reconstructs the exact
GPU result and checks the declared decimal precision. Signed integer SUM and
COUNT indicators are also supported. seekdb retains representative fields for
its implicit FIRST_ROW outputs; GPU computes the grouped arithmetic. AVG and
HAVING retain seekdb's normal expression semantics.

Eligible scalar/grouped inputs may include serial joins, sorts, limits,
materialization, distinct, windows and nested aggregates. PX/exchange,
subplan-filter and storage aggregation pushdown are excluded. While an outer
GPU operator consumes input, nested GPU candidates run on CPU, preventing
recursive requests to the single-stream sidecar. Enabling GPU does not mean
that every operator in a query runs on GPU.

`ObWindowFunctionOp` additionally supports one whole-partition MIN with one
BIGINT partition key and a DecimalInt argument of precision 1–18, preserving
scale. This handles the optimizer's window transformation of TPC-H Q2.
Protocol v7 reduces exact unscaled signed integers with GPU MIN; seekdb replays
its child rows in their original order and attaches the per-partition result.
NULL partition keys, all-NULL partitions and ties preserve SQL semantics.
Bounded frames, multiple window functions and parallel window plans remain CPU.

These adapters intentionally have prototype bounds:

- Grouped result: 256 MiB of binary cells, at most 64 transported columns;
  estimated group counts above that bound select CPU before reading input.
- String dictionary: 4,096 values per key, 8 MiB combined, 1,024 bytes per value.
- Implicit FIRST_ROW storage: 512 MiB estimated budget, 4,096 bytes per field.
- Window MIN: 262,144 retained rows, 64 projected fields, 256 MiB estimated
  retained-row budget, 4,096 bytes per field. Estimated larger inputs select CPU.

Actual bounds exceeded after consuming input return a SQL error; there is no
snapshot-changing retry. Python grouped merges and C++ containers have overhead
beyond wire payload sizes. These bounds must be revisited before scaling tests.
The legacy v1/v2 and scalar v3 result limits remain 1 MiB. Grouped JSON replies
and binary frames explicitly opt in to the larger bound; default framing is
unchanged. Partial SUM/MIN results from GPU fragments are combined on CPU.

Run the SQL regression tests with a private sidecar log:

```sh
SEEKDB_PORT=2898 python unittest/sql/sirius/grouped_exact_integration.py \
  --trace /absolute/path/native-sidecar.log
SEEKDB_PORT=2898 python unittest/sql/sirius/window_min_integration.py \
  --trace /absolute/path/native-sidecar.log
SIRIUS_TEST_LIBRARY=/absolute/path/libseekdb_sirius_bridge.so \
  python unittest/sql/sirius/test_native_grouped.py
```

For an existing verified TPC-H import, add `--modes native1`,
`--native-operator-hints` and `--require-gpu` to `tpch_benchmark.py`. The hints request
serial hash aggregation, disable storage aggregation pushdown and use 4,096-row
batches. Each query must match the independent reference and produce a native
GPU completion event; successful CPU fallback fails this coverage check.
Do not run unrelated queries against the same sidecar during log attribution.
This validates native operator coverage, not whole-query GPU execution or speedup.

## Legacy DOUBLE hash eligibility and shared semantics

- Linux; vectorized, serial SELECT plan (`PARALLEL(1)`), hash aggregation directly
  over a table scan. Parallel plans and other operator shapes use existing CPU
  execution. This hash path does not handle DML, joins or nested
  aggregate input trees.
- One to eight BIGINT grouping columns, DOUBLE column SUM, COUNT(*), or grouping
  without measures. At most 64 input columns. NULL keys and values are supported.
  COUNT(column), DISTINCT, integer/decimal SUM, casts/computed grouping keys,
  implicit aggregates and special aggregate stages remain on CPU.
- The table scan keeps seekdb permissions, filters, MVCC and transaction context.
  The native operator sees its transaction's own writes; rollback restores the
  previous view. It performs no independent query and maintains no cross-query
  data cache. Tests cover committed/uncommitted visibility and correlated rescan.
- GPU floating-point aggregation can use a different summation order. The exact
  result comparisons here use binary-exact quarter-unit input values; they do
  not establish bit-identical floating-point sums for arbitrary data.
- Native transport failure returns a SQL error. There is no retry against a new
  snapshot and no fallback after consuming input. Unsupported shapes select CPU
  before reading input.
- Output datums are written through `locate_datums_for_update`, restoring the
  expression-owned payload pointers after child scan consumption. Initial
  integration testing found and fixed an invalid write through scan-owned datum
  pointers; the SQL regression suite exercises nullable results and multiple keys.

## Batching, bounds and cancellation

By default the adapter merges seekdb's small batches into up to **262,144 rows**. Wide inputs
use fewer rows to keep a frame under 16 MiB. Input uses column validity bitmaps
and little-endian 64-bit values; skipped scan rows are excluded. Each batch is
acknowledged only after its GPU copy completes. The input buffer belongs to the
query allocator and is reused on rescan; the legacy native result buffer is 1 MiB.

For larger pinned transfers, set `SEEKDB_SIRIUS_FRAME_MIB=64` or `128` before
starting seekdb and use a matching or larger `--host-pin-mib` on the sidecar.
The larger protocol negotiates the frame bound before reading the scan, admits
up to 8,388,608 rows subject to the byte cap, and requires the updated Arrow
overlay (the older overlay rejected batches above 1,048,576 rows). Default
16 MiB framing remains protocol-version-1 compatible. Pin allocation failure
still permits bounded pageable input; an allocated pin buffer smaller than the
negotiated frame is rejected before acknowledgment. Input size arithmetic is
64-bit before enforcing the frame cap.

The existing Fragment implementation runs after input closes. By default total
decoded input remains limited to 512 MiB per query. With `--native-chunk-mib 128`
(also accepts 64 or 256), the sidecar closes each fragment at the target boundary,
runs its GPU aggregate, and merges its small grouped SUM/COUNT results on CPU.
The target can be exceeded by one frame, at most the configured 16/64/128 MiB. Each fragment retains
the 512 MiB guard; the complete query can exceed that size. NULL sums and keys
are preserved, grouping-only outputs are deduplicated, and non-finite partial or
merged sums fail the query. Floating-point summation order can change further.

For legacy v1/v2 aggregation, JSON per fragment and final binary output remain limited to 1 MiB; the merge
checks that limit before adding groups. Python merge objects have additional
bounded overhead beyond their binary representation. GPU intermediates can
exceed input size. Scan/transfer and fragment execution remain sequential, with
socket backpressure. This is not yet a fully overlapped scan/compute engine or
an arbitrary-size streaming result interface. See [SF500.md](SF500.md) for the
full 75-million-row customer projection experiment.

The grouped native transport has a 60-second total deadline and polls for query
cancellation at intervals of at most 100 ms while waiting on I/O. Disconnects and
broken pipes return errors rather than sending SIGPIPE to seekdb. An already
executing GPU kernel is not interrupted by closing the socket.

## Host pin and startup behavior

`--host-pin-mib 16` (also accepts 64 or 128) allocates one bounded buffer with `cudaHostAlloc`. The socket
receives native input directly into this buffer, and Arrow borrows its slices.
It is reused after the synchronized GPU copy, retained across queries and freed
with `cudaFreeHost` on orderly shutdown. This avoids per-batch allocation/pinning
and an extra staging copy in the adapter. It is a fixed additional host budget,
outside Sirius's configured host pool. On allocation/library failure the sidecar
logs a warning and uses pageable input. The default is `--host-pin-mib 0`.

Increasing only the allocation does not enlarge incoming frames. Match the
server frame setting when evaluating larger transfers, and require actual
GPU runtime evidence. Storage-level aggregate pushdown can change a qualifying
plan into a CPU partial/final aggregate. The microbenchmark's
`--disable-storage-aggregation --sidecar-log /path/to/log` applies the same
`pushdown_storage_level=2` hint to CPU/GPU modes and rejects measurements with no
observed native GPU execution. Default CPU plans should also be reported.

This follows NVIDIA's guidance to use pinned memory for transfers while keeping
pinning operations infrequent and bounded; pinning itself is relatively costly.
See [CUDA Best Practices: Pinned Memory](https://docs.nvidia.com/cuda/cuda-c-best-practices-guide/index.html#pinned-memory).

`--warmup` executes a small verified group/SUM/COUNT through the real backend
before announcing readiness. It initializes the context and relevant operators
at service startup. It does not eliminate initialization work or warm seekdb's
storage; it moves work before the first client request. Both options are explicit.

Sirius also has `pin_table(..., tier='host')`, which caches table columns. That
source-cache mechanism currently understands its Parquet/DuckDB sources. This
integration does **not** reuse such cached data for seekdb: safe table caching
requires seekdb snapshot ownership, schema identity and mutation invalidation,
including the current transaction's own writes. The pinned receive buffer stores
no reusable table snapshot.

## Measurements: 2026-10-23

One RTX 5880 GPU, the same compacted 5-million-row seekdb table as earlier stages,
100 groups. Every timed result was compared with CPU output. Times include the
SQL roundtrip, current scan, transport, GPU aggregation and final SQL results;
there is no excluded snapshot export.

Final run: 15 repetitions per mode and query, rotating CPU1/CPU8/GPU order,
262,144-row maximum batches, 16 MiB pinned input and startup warmup:

| Query | CPU serial | CPU parallel 8 | Native GPU | Speedup vs serial |
|---|---:|---:|---:|---:|
| GROUP BY + SUM + COUNT | 321.55 ms | 52.27 ms | 152.76 ms | 2.10× |
| GROUP BY + COUNT | 247.44 ms | 38.66 ms | 79.91 ms | 3.10× |

CPU parallel 8 remains faster on these workloads. GPU is therefore explicit;
these results do not justify automatic selection over that parallel CPU plan.
Compared with the previous Python/MySQL live SUM/COUNT path (8.86 seconds), the
native path removes most data extraction overhead. Earlier 65,536-row native
batches measured 168.64 ms for SUM/COUNT and 101.45 ms for COUNT in seven repeats.

### Fresh-process pin/warmup experiment

Three fresh sidecar processes per configuration (12 total), one first query and
five subsequent queries per process. Mode order rotates. **seekdb storage remains
warm**; this measures process/GPU startup rather than cold disk reads. Each value
below is the median of the three process trials (warm column: median of their
per-process warm medians).

| Host pin | Startup warmup | Process ready | First query after ready | Ready + first result | Warm query |
|---|---|---:|---:|---:|---:|
| Off | Off | 201.62 ms | 723.82 ms | 916.91 ms | 161.17 ms |
| 16 MiB | Off | 377.19 ms | 614.05 ms | 991.23 ms | 151.94 ms |
| Off | On | 791.43 ms | 174.08 ms | 987.49 ms | 173.09 ms |
| 16 MiB | On | 760.55 ms | 182.14 ms | 942.69 ms | 133.48 ms |

Pinning improves transfer behavior but has allocation/startup cost. Warmup makes
the first request after readiness much faster; launch-to-first-result remains
roughly 0.9–1.0 seconds. Warm timings vary with surrounding CPU work; the 133 ms
microbenchmark median is not substituted for the 153 ms final alternating test.
All 12 processes shut down normally, including those owning pinned allocations.

## Reproduce and inspect evidence

```sh
ctest --test-dir build-sirius-arrow --output-on-failure
python unittest/sql/sirius/test_sidecar_transport.py \
  --library build-sirius-arrow/libseekdb_sirius_proxy.so
python unittest/sql/sirius/native_integration.py
python unittest/sql/sirius/native_benchmark.py --repeats 15 --output native.json
# Stop the existing sidecar first; this test creates and stops its own children.
python unittest/sql/sirius/native_startup_benchmark.py \
  --socket "$SEEKDB_SIRIUS_SOCKET" --library /absolute/path/libseekdb_sirius_bridge.so \
  --trials 3 --output startup.json
```

Use the existing connection/GPU environment described above. The benchmark uses
the already populated `sirius_benchmark` table; create it using the earlier
benchmark only if it does not exist. The startup test needs GPU dependencies in
its environment because it starts sidecar children.

Verification: seekdb rebuild passed; 20 C++ tests, 9 protocol tests, 16 native SQL
checks and all 90 final timing comparisons passed. Raw reports, EXPLAIN output,
build tail and startup records are in
[`results/2026-10-23-native`](../../unittest/sql/sirius/results/2026-10-23-native).

Historical SF1000 host-pin configurations, their preparation costs and applicability
to this adapter are recorded in [HOST_PIN_RESEARCH.md](HOST_PIN_RESEARCH.md).
