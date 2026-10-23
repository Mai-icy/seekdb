# Host pin: existing SF1000 experiments and seekdb application

Inspected read-only on `30.249.210.115`, 2026-10-23. Historical measurements below
are existing DuckDB/Sirius results, not new seekdb measurements. Existing data,
benchmark scripts, source and binaries were not changed.

## Data and reproducible sources

All paths below are relative to `/data/wuhao.xwh`:

| Source | Contents |
|---|---|
| `duckdb-sirius-data/tpch_sf1000.duckdb` | Existing 283,617,800,192-byte (264.14 GiB) database |
| `duckdb-sirius-upstream/data/tpch_sf1000.duckdb` | Symlink to the same database |
| `duckdb-sirius-upstream/benchmark/official-hotpin/sf1000/run_all.sh` | Generation, CPU oracle, GPU host-pin tests |
| `duckdb-sirius-upstream/benchmark/run_sf1000_tuning.sh` | One-variable sweeps on Q3/Q9/Q18/Q21 |
| `duckdb-sirius-upstream/results/official-hotpin-sf1000-20261009/REPORT.md` | SF1000 timings, correctness, pin cost and limitations |
| `duckdb-sirius-upstream/results/config-tuning-sf1000-20261010/` | Per-iteration CSVs and validation for each tuning variant |
| `duckdb-sirius-upstream/results/sf1000-operator-microbench-20261010/REPORT.md` | Individual operator comparisons; corrected low-cardinality case linked there |
| `duckdb-sirius-upstream/results/sf1000-join-microbench-20261010/REPORT.md` | Join, semi/anti join cases and CPU comparisons |
| `sirius-benchmark-hot/REPORT-SF10-HOT-PIN.md` | Older SQL-keyed GPU scan cache experiment |
| `sirius-official-repro/bench/sf1000-repro/` | Separate GB300 reproduction recipe; different hardware and source version |

The SF1000 database was located and its size/symlink checked. It was not regenerated
or loaded into seekdb in this stage. The current live adapter's 512 MiB input bound
does not support running that entire data set through a single fragment.

## Actual Ada SF1000 configuration

The historical runner uses `--pin host --pin-compression`, per-column Simpatico
plans, `ast_interpret`, fused scan/filter, and explicitly disables DuckDB fallback.
It pins columns needed by each query, executes three times, then unpins. CPU uses
up to 64 threads. Linux page cache is not cleared.

The later annotated configuration is preserved in
[history/recommended-sf1000-2x22g-annotated.yaml](../../unittest/sql/sirius/results/2026-10-23-native/history/recommended-sf1000-2x22g-annotated.yaml).
These values describe that experiment; they are not defaults for this adapter.

| Parameter | Historical value |
|---|---|
| GPU topology and budget | Two GPUs, 22 GiB per GPU |
| GPU downgrade trigger / stop | 0.88 / 0.72 |
| Host capacity / reservation fraction | 360 GiB per NUMA / 0.95 |
| Initial host pool | 64 pools × 8 blocks × 64 MiB = 32 GiB per NUMA |
| Host downgrade trigger / stop | 0.88 / 0.72 |
| Disk spill budget | 500 GiB |
| Scan batch / concat batch | 512 MiB / 512 MiB |
| Hash partition / build threshold | 1 GiB / 2 GiB |
| Pipeline / scan manager / task creator threads | 8 / 8 / 4 |
| Downgrade workers / monitoring interval | 2 / 10 ms |
| Scan-manager read cache | none; explicit host pin is separate |
| Memory prefetcher | Disabled in final annotated dual-GPU configuration |

Historical report: CPU suite median **111.655 s**, one GPU **102.565 s**, two GPUs
**52.704 s** (2.12×). The two-GPU run spent **284.955 s pinning**. One pass estimated
from per-query medians was CPU 112.789 s versus pin + GPU **337.343 s**. Estimated
amortization was five repetitions, conditional on keeping the relevant data resident.
That original run also logged multi-GPU downgrade device-affinity errors; the later
join microbenchmark uses the fixed revision `3fcff6a`, also the basis of our overlay.

## What tuning did and did not establish

The sweeps vary pipeline threads (4/6/10), scan batches (256 MiB/1 GiB), hash
partitions (512 MiB/2 GiB), downgrade watermarks (0.90/0.80), and downgrade workers
(4). Raw Q3/Q9/Q18/Q21 TOTAL medians include the first execution of each query:

| Variant | TOTAL median |
|---|---:|
| Contemporary baseline | 27.066 s |
| Pipeline 4 / 6 / 10 | 25.924 / 29.970 / 25.890 s |
| Scan 256 MiB / 1 GiB | 33.909 / 27.524 s |
| Hash 512 MiB / 2 GiB | 35.689 / 26.559 s |
| Downgrade 0.90/0.80 | 29.959 s |
| Downgrade workers 4 | 26.479 s |

There is substantial per-iteration variation, especially Q18. A later full-suite
pipeline-10 run totals 58.555/51.653/51.547 s; this alone does not establish a stable
win over pipeline 8. The saved recommended configuration retains pipeline 8,
512 MiB scan batches and 1 GiB hash partitions. Increasing every budget or thread
count is not supported by these records.

The older SF10 cache (`executor.duckdb_scan.cache: table_gpu`) did record 66 cache
hits, but hot suite time changed only 16.886 → 16.805 s (~0.5%). That version keys
the scan cache by full SQL and clears it on query changes. Its interface differs
from later `pin_table` and should not be copied into the current YAML blindly.

The GB300 recipe uses GPU-tier compressed data, ~256 GB HBM, ~470 GB host capacity
and 8 GB scan batches. Although its runner has `--pin host`, it exports per-table
`SIRIUS_PIN_TIER_* = gpu` overrides. Its README and script also report different
revision-era suite numbers. These are not measurements for the two Ada GPUs.
Its compressed-pinning documentation reports host compression losing 7% on GB300
while GPU-resident compression helps; the host link, codec and residency determine
the tradeoff. This does not invalidate the Ada host-compressed measurements.

## Applied to the native seekdb adapter

Three mechanisms have distinct costs and lifetimes:

1. **Pinned transfer memory:** one reusable `cudaHostAlloc` buffer, 16 MiB, used
   by native socket receive and Arrow views. Each GPU copy completes before reuse.
   No table data is reused across queries. This is implemented as `--host-pin-mib 16`.
2. **GPU startup warmup:** execute a verified tiny aggregate before readiness.
   This is implemented as `--warmup`; it shifts initialization before the first
   request. Process startup plus the first result is still measured separately.
3. **Persistent table/column pin:** reuse a materialized data set across queries.
   This requires a seekdb snapshot identifier, schema/column identity, mutations
   and own-write visibility rules, an eviction budget and explicit prepare cost.
   It is not yet implemented for live seekdb tables.

The current test profile uses one GPU with a 15% pool, an 8 GB Sirius host budget,
512 MiB initial host pools, and a separate 16 MiB native receive buffer. Native
frames hold up to 262,144 rows and at most 16 MiB. The legacy
`scan_task_batch_size` setting controls Sirius source scans; it does not replace
the explicit native frame limit. Copying a 512 MiB historical scan target into
that setting would not enlarge seekdb's streamed batches.

New seekdb measurements, including pin-off/on and fresh-process comparisons, are
in [NATIVE_AGGREGATE.md](NATIVE_AGGREGATE.md). Every live timing includes seekdb
scanning and transport. For future table-pin experiments, report preparation,
first execution, repeated executions, peak memory and validation separately;
also compare against a parallel CPU plan and confirm actual GPU execution.

The operator reports favor large repeated hash updates, distinct aggregation,
large Top-N and semi/anti joins over trivial reductions or very selective scans.
The next performance work should profile native scan/packing/transfer, overlap
bounded transfers with computation, then expand supported fragments with small
final outputs. Supporting high-cardinality results requires replacing the current
1 MiB result boundary before claiming the historical group-by benefits in seekdb.
