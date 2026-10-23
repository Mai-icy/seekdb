# Native seekdb hash aggregation with Sirius

Follow-up: [NATIVE_PERFORMANCE.md](NATIVE_PERFORMANCE.md) records input profiling,
packing optimization and later measurements with scan-batch hints. The original
stage's measurements below remain as its historical baseline.

The third prototype integrates GPU execution into `ObHashGroupByOp`'s batch
execution path. Users run ordinary SQL; no external SELECT, Parquet export or
manual Substrait plan is needed for supported aggregates.

```text
seekdb SELECT and transaction snapshot
  → table scan / filter batches
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
Selection occurs when the physical hash aggregate executes, including when a
cached plan is reused. EXPLAIN still shows HASH GROUP BY; this prototype does not
add an optimizer cost model or a distinct GPU plan node. The sidecar logs native
request input/result row counts. The startup test also proves a qualifying query
fails while the sidecar is absent, so its GPU timing cannot silently be CPU timing.

## Eligibility and semantics

- Linux; vectorized, serial SELECT plan (`PARALLEL(1)`), hash aggregation directly
  over a table scan. Parallel plans and other operator shapes use existing CPU
  execution. No GPU choice is made for scalar aggregation, DML, joins or nested
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

The adapter merges seekdb's small batches into up to **262,144 rows**. Wide inputs
use fewer rows to keep a frame under 16 MiB. Input uses column validity bitmaps
and little-endian 64-bit values; skipped scan rows are excluded. Each batch is
acknowledged only after its GPU copy completes. The input buffer belongs to the
query allocator and is reused on rescan; the native result buffer is 1 MiB.

The existing Fragment implementation runs after input closes. Total decoded
input remains limited to 512 MiB per query, and JSON/binary output each remains
limited to 1 MiB. GPU intermediates can exceed input size. This is not yet a fully
pipelined scan/compute engine or an arbitrary-size streaming result interface.

The native transport has a 60-second total deadline and polls for query
cancellation at intervals of at most 100 ms while waiting on I/O. Disconnects and
broken pipes return errors rather than sending SIGPIPE to seekdb. An already
executing GPU kernel is not interrupted by closing the socket.

## Host pin and startup behavior

`--host-pin-mib 16` allocates one 16 MiB buffer with `cudaHostAlloc`. The socket
receives native input directly into this buffer, and Arrow borrows its slices.
It is reused after the synchronized GPU copy, retained across queries and freed
with `cudaFreeHost` on orderly shutdown. This avoids per-batch allocation/pinning
and an extra staging copy in the adapter. It is a fixed additional host budget,
outside Sirius's configured host pool. On allocation/library failure the sidecar
logs a warning and uses pageable input. The default is `--host-pin-mib 0`.

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
