# SF500 input-path comparison

This experiment compares the same 75,000,000 customer rows, 25 BIGINT grouping
keys, and DOUBLE integer-cent amounts. It measures grouped SUM/COUNT and grouped
COUNT, not the complete TPC-H suite. The source is the existing read-only SF500
DuckDB database; seekdb already contains the corresponding projection.

The three paths are:

1. Native DuckDB/Sirius, with direct scans and explicit uncompressed host pin.
2. Ordinary seekdb SQL, scanning each statement and streaming native batches to
   the Sirius Arrow bridge.
3. A benchmark-only immutable snapshot of those same seekdb input batches,
   copied into bounded pinned memory and replayed through the same bridge.

The third path uses the SQL function `SIRIUS_EXECUTE_SUBSTRAIT` with an explicit
benchmark token. It measures the opportunity from avoiding repeated scans and
transport. It is not an automatic cache for ordinary SQL, and has no production
snapshot invalidation, mutation visibility or eviction policy. Each replay
executes the GPU aggregation again; it does not reuse query results.

## Results, October 26

**Removing repeated seekdb input production reduces hot SUM/COUNT from 1.113 s
to 0.094 s and COUNT from 0.794 s to 0.049 s.** The resident path is 3.94× / 6.88×
faster than the faster measured seekdb CPU8 configuration. Its preparation cost
means this is a repeated-query result, not a single-query speedup over CPU8.

Hot medians in milliseconds:

| Path | SUM + COUNT | COUNT |
|---|---:|---:|
| seekdb CPU1, matched hints | 2,259.33 | 2,043.02 |
| seekdb CPU8, matched hints | 370.19 | 335.45 |
| seekdb CPU8, default choices | 513.11 | 431.29 |
| seekdb live input → Sirius | 1,112.89 | 794.19 |
| seekdb resident host input → Sirius | **94.04** | **48.78** |
| Native DuckDB CPU8 | 53.63 | 28.08 |
| Native DuckDB → Sirius direct scan | 30.83 | 16.79 |
| Native DuckDB → Sirius host pin | 32.29 | 9.53 |

seekdb has ten hot samples per cell; native cells pool twenty hot samples from
the before/after runs. Native pinned SUM/COUNT medians were 32.292 / 32.270 ms;
COUNT was 9.464 / 9.803 ms. Native Sirius remains about 2.91× / 5.12× faster than
our resident bridge. Pinning did not improve native SUM/COUNT over its direct
scan in this experiment. Increasing pin capacity alone is not an explanation
or a universal optimization.

All **292 full-input calls** passed exact result checks: 260 hot timings,
26 first calls, two seekdb capture calls and four separate diagnostics.
Each seekdb GPU call consumed all 75 million rows; live SUM/COUNT used seven
fragments and 21 frames, COUNT four fragments and 11 frames. Captured input
occupied 1,218,751,232 bytes for SUM/COUNT and 609,375,616 additional bytes for
COUNT, below the shared 2 GiB allocation.

### Preparation and amortization

The 267,137,024-byte temporary DuckDB projection took **0.917 s** to create
with warm source caches. Native host pin took 0.210–0.221 s for SUM/COUNT and
0.0276–0.0282 s for COUNT. seekdb capture took **1.263 s / 0.808 s**, including
one complete live GPU query and its input copy. Allocating the reusable 2 GiB
pinned cache took **0.595 s** once per process. Resident first calls after
capture took 0.0969 s / 0.0461 s.

Estimated totals in seconds below include capture and the full cache allocation
for each resident case considered independently. The allocation is shared when
both cases run in the same process, so do not add these case totals and charge
it twice. Engine startup is excluded from this table and reported below.

| Workload | Path | 1 query | 5 queries | 10 queries |
|---|---|---:|---:|---:|
| SUM + COUNT | seekdb CPU8, matched hints | 0.405 | 1.886 | 3.737 |
| SUM + COUNT | seekdb live GPU | 1.190 | 5.642 | 11.207 |
| SUM + COUNT | resident, allocation + capture included | 1.955 | 2.331 | 2.801 |
| COUNT | seekdb CPU8, matched hints | 0.350 | 1.692 | 3.369 |
| COUNT | seekdb live GPU | 0.745 | 3.922 | 7.893 |
| COUNT | resident, allocation + capture included | 1.448 | 1.643 | 1.887 |

The estimated break-even against matched CPU8 is **7 / 5 queries** for
SUM/COUNT and COUNT. Bridge startup/warmup additionally takes 0.620 s; charging
that as well moves break-even to **9 / 7 queries**. Native startup/warmup was
0.607–0.629 s. These are estimates, not independently timed multi-query totals;
they assume unchanged captured data and comparable hot performance.

### Where the time goes

Separate bridge diagnostics, summed over all fragments (milliseconds):

| Case/path | Input wait | Host-to-GPU copy | Fragment build | GPU execution call | Bridge total |
|---|---:|---:|---:|---:|---:|
| SUM/COUNT live | 1,001.35 | 50.36 | 12.27 | 38.71 | 1,103.67 |
| SUM/COUNT resident | 0.28 | 48.82 | 7.99 | 31.91 | 89.62 |
| COUNT live | 661.09 | 25.12 | 5.51 | 20.92 | 713.05 |
| COUNT resident | 0.12 | 24.33 | 4.94 | 18.18 | 47.90 |

Input wait accounts for **90.7% / 92.7%** of the live bridge interval. It
includes waiting for seekdb production and transport; it cannot isolate storage
scan from packing or socket work. Resident input removes that wait, but retains
copying, repeated fragment construction and serial fragment execution. The
GPU execution column is host wall time around `fragment.run()`, including its
scheduling overhead, not isolated CUDA kernel time. Do not add these diagnostics
to the separately measured SQL intervals.

This supports prioritizing parallel seekdb input production and bounded overlap
between production, transfer and GPU execution. The current adapter's serial
eligibility guard must remain until that lifecycle and backpressure are
implemented correctly. For repeated analyses, a real prepared-column facility
would also need explicit snapshot/schema identity, own-write handling,
invalidation and eviction. The benchmark snapshot does not implement those
contracts. After input work, reducing per-fragment setup and synchronization is
the next measured opportunity; expanding the pin allocation without changing
the data path does not remove the observed wait.

### Resources and cleanup

The shared host had two existing VLLM processes. One-second GPU telemetry
recorded no positive SM utilization for either during these runs, but cannot
exclude shorter bursts or other host activity. Both native and bridge workers
reached 7,644 MiB device allocation. Worker peak RSS was about 2.04 GiB for
native and 3.38 GiB for the bridge; the latter excludes the separate seekdb
server. Native read-cache mode defaults to `none` in this source revision;
explicit host pin is a separate path.

The original socket was restored and verified with a small native SQL query.
The comparison helper exited, releasing its GPU and pinned allocations. Both
owned temporary DuckDB files were deleted; the original SF500 file's size and
mtime remained unchanged. About **545.4 GiB** filesystem space remained.

The original full-suite preparation hit its 30-minute timeout while collecting
lineitem statistics serially. The comparison ran with that preparation stopped,
then the full pipeline was restarted with `--analyze-parallel 8`. This setting
belongs only to the maintenance session; benchmark CPU/GPU settings are unchanged.
All statistics completed at 12:06:16 CST (lineitem: 307.15 seconds), and TPC-H22
started one second later. Full-suite results are separate from this projection
experiment and remain pending.

Raw timing arrays, plans, GPU evidence, diagnostics, configuration, binary hashes
and cleanup evidence are in
[results/2026-10-26-paths](../../unittest/sql/sirius/results/2026-10-26-paths/).

## Method

The runner is [path_comparison.py](../../unittest/sql/sirius/path_comparison.py).
Each measured call is checked against the independent source oracle. Native
Sirius has DuckDB fallback disabled. For the seekdb GPU paths, every call must
emit bridge events totaling exactly 75,000,000 input rows; a CPU fallback cannot
be reported as a GPU measurement.

All GPU paths use physical GPU 0 of the same two-RTX-5880-Ada host, one visible
GPU, the same YAML, a 15% GPU pool, 8,000,000,000-byte Sirius host budget, four
pipeline workers and two task-creator workers. The native source scan target is
64 MiB. seekdb uses a 64 MiB pinned receive buffer and 128 MiB fragment target;
the captured snapshot additionally reserves at most 2 GiB of pinned host memory.
Native host pin is uncompressed. These are shared Sirius budgets, not equal
whole-process RSS limits; actual memory and other GPU processes are recorded.

The seekdb CPU1, CPU8 and live GPU cases use identical logical expressions,
`rowsets_max_rows=1024`, `pushdown_storage_level=2` and hash-aggregate hints.
The GPU adapter currently requires a serial scan. Native DuckDB CPU1 and CPU8
are also recorded, and should not be confused with seekdb CPU results.
An additional seekdb `cpu8_default` reference uses only `PARALLEL(8)`, leaving
scan batches and aggregation choices at their defaults.

First calls and ten subsequent executions are separate. seekdb modes are
shuffled with a fixed seed each round. Native runs bracket the seekdb run to
detect temporal drift; this is not full cross-engine randomization. OS caches
are warm and never globally flushed. The first call is not a cold-disk result.
Native timings use in-process DuckDB execute/fetch; seekdb timings include its
SQL round trip. Result normalization is outside the measured intervals.
Separate diagnostic calls after each case record bridge input wait, copy and
execution durations. They do not contribute to the medians. These are host
wall-clock measurements; `run_us` is not a pure CUDA-kernel measurement, and
input wait combines seekdb production and transport.

Startup/warmup, pin allocation and snapshot preparation are recorded separately.
seekdb preparation includes an extra complete live GPU query and the copy of
its input. Native preparation uses `pin_table`. Totals for 1, 5 and 10 queries
are explicitly estimates from measured preparation, first call and hot median,
not separately timed workloads. Engine startup remains separately reported.

Native Sirius requires a file-backed DuckDB table. Preparation creates only
the two-column projection, with a 2 GiB file limit and a 180 GiB filesystem
reserve, and checks original source size/mtime before and after. Its creation
cost is separate from pin preparation. No copy of the complete SF500 database
is needed. The temporary projection is removed after the measurements.

The full TPC-H runner pauses at a query boundary using its `benchmark/PAUSE`
file. The comparison requires no other active seekdb query. It temporarily
renames the owned sidecar socket and restores the original listener on normal
exit or Python exceptions. An abrupt process kill can leave a
`.comparison-backup` socket; restore that original socket before resuming the
TPC-H runner. The full suite resumes after the comparison.

Native Sirius uses the existing DuckDB extension from the borrowed checkout;
seekdb uses the owned Arrow-input overlay. Both artifacts' hashes are recorded.
They are different binaries and input interfaces, not a bit-identical engine
experiment. The borrowed checkout and original SF500 database remain unchanged.

## Validation

The focused resident-input tests check independent ownership of copied values
and validity masks, capacity bounds, NULL partial aggregation, repeated replay,
and restoration of the original socket after an exception. An optional real
GPU test validates the replay through the actual bridge:

```sh
SIRIUS_TEST_LIBRARY=/absolute/path/libseekdb_sirius_bridge.so \
  python unittest/sql/sirius/test_resident_input.py
```

Use the same CUDA, dynamic-library and explicit Sirius configuration environment
as the sidecar. The native probe additionally validates file-backed direct and
host-pinned queries before the full measurement.
All five resident-input tests, including the real GPU test, passed on the
measurement host. The complete comparison then exercised both aggregation
shapes through the actual seekdb SQL transport.
