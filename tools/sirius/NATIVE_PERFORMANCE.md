# Native aggregate: input profiling and packing optimization

Follow-up to [NATIVE_AGGREGATE.md](NATIVE_AGGREGATE.md), 2026-10-23.
The supported SQL shapes, transaction behavior and memory bounds are unchanged.

## Changes

- Added optional stage timings in seekdb and the GPU bridge. Enable
  `SEEKDB_SIRIUS_PROFILE=1` before starting **both** processes. Counters are emitted
  to stderr, which the isolated seekdb daemon redirects to its log. No SQL text,
  values or credentials are included. Leave this option unset for final timing.
- Reduced per-row packing work: skip bitmap lookups are avoided for batches with
  no skipped rows; eligible BIGINT/DOUBLE payloads are copied as eight bytes,
  eliminating a per-row type branch. NULL masks and excluded scan rows retain
  their previous semantics.
- Extended the benchmark to select grouping columns, multiple SUM columns and a
  scan-batch hint. Added a seeded, shuffled sweep that applies each batch setting
  equally to CPU1, CPU8 and GPU, checking every output against CPU.

## Where the time goes

For the original 5M-row / 100-group table, before the packing change, median
seekdb stage times in a diagnostic run were:

| Query | Scan | Pack | Send and wait for copy ACK | Finish and receive |
|---|---:|---:|---:|---:|
| SUM + COUNT | 109.81 ms | 32.47 ms | 25.80 ms | 9.67 ms |
| COUNT | 53.21 ms | 16.63 ms | 10.44 ms | 7.98 ms |

The bridge separately reports plan build, waiting for Arrow input, synchronized
H2D ingestion, GPU run and result materialization. Typical H2D time was 2–6 ms;
GPU run was 6–8 ms for these cases. Seekdb send time also includes socket copying,
Arrow framing and ACK scheduling. **Do not add the two processes' counters**:
their intervals overlap. The timer around `run()` is host wall time, not a CUDA
event measuring only kernels. `finish` includes backend execution and output work.

After the packing change, default-batch diagnostic medians for packing were
**18.53 ms** (SUM/COUNT) and **8.36 ms** (COUNT). Before/after diagnostic runs were
sequential, with five and seven measured repetitions respectively, so their
whole-query differences also include scheduling/frequency variation. The direct
stage measurements justify retaining the smaller packing loop; they do not
establish an identical percentage improvement in end-to-end latency.

The four-SUM / 1,000-group case read five columns. Before the packing change,
scan alone took approximately 362–426 ms in the last five timed GPU queries;
GPU execution still took about 10 ms. Serial storage scanning is the dominant
remaining cost for this wider input.

## Scan-batch tuning: final measurements without profiling

Two independent shuffled sweeps, seeds 20261023 and 20261024, each with 15
repetitions of 3 batch settings × 3 execution modes × 2 queries. **All 540 timed
outputs matched CPU**. The sidecar used pinned input and startup warmup. The
table below pools the 30 samples per cell and reports their median; all numbers
include SQL execute/fetch, current seekdb scan, transfer and GPU results.

| Query | seekdb scan batch hint | CPU1 | CPU8 | Native GPU |
|---|---:|---:|---:|---:|
| SUM + COUNT | Default | 314.32 ms | 51.14 ms | 142.13 ms |
| SUM + COUNT | 1024 | 264.08 ms | 44.04 ms | 124.31 ms |
| SUM + COUNT | 4096 | 273.42 ms | 42.10 ms | 149.71 ms |
| COUNT | Default | 236.02 ms | 37.33 ms | 75.73 ms |
| COUNT | 1024 | 213.50 ms | 32.92 ms | 69.68 ms |
| COUNT | 4096 | 217.02 ms | 31.58 ms | 69.43 ms |

GPU timings have substantial variation. For SUM/COUNT, default GPU run medians
were 139.09/145.17 ms, 1024 was 129.49/121.49 ms, and 4096 was 149.75/140.91 ms.
The pooled 10th–90th percentile range for the 1024 setting is 115.31–155.87 ms.
An earlier **profile-enabled** sweep gave 115.51 ms for 4096, but that ranking did
not survive the unprofiled runs. It is not presented as the final result.

For this SUM/COUNT query, 1024 improved both unprofiled runs; its pooled GPU median
is 12.5% below the same-run default. It also improves CPU. This is a useful
per-query experiment, not a universal tuning rule or a change to engine defaults:

```sql
SET @sirius_gpu = 1;
SELECT /*+ PARALLEL(1) USE_HASH_AGGREGATION OPT_PARAM('rowsets_max_rows' 1024) */
       grp, SUM(amount), COUNT(*)
FROM sirius_benchmark GROUP BY grp;
```

Compare with `@sirius_gpu=0` using the same hint, including `PARALLEL(8)`.
The GPU/CPU1 speedup for this tuned SUM/COUNT case is **2.12×**; CPU8 remains
faster. COUNT's best setting changes between runs, so no preferred hint is
established for it. A 16,384-row preliminary hint returned an internal error on
the first CPU query, before GPU testing. The benchmark accepts only 0..4096.

The seekdb scan hint and the adapter's 262,144-row / 16 MiB transport limit are
different: many small scan batches are merged into one transport frame.

## Group cardinality and width

A new isolated 5M-row table adds 10/1,000/10,000-group keys and four binary-exact
DOUBLE measures. It was major-compacted before measuring. These runs preceded
the packing change and use seven repetitions per mode:

| Query shape | CPU1 | CPU8 | GPU |
|---|---:|---:|---:|
| 1,000 groups, four SUMs + COUNT | 901.12 ms | 153.20 ms | 571.92 ms |
| 1,000 groups, COUNT | 400.75 ms | 49.67 ms | 97.83 ms |
| 10,000 groups, SUM + COUNT | 452.76 ms | 121.47 ms | 196.82 ms |
| 10,000 groups, COUNT | 313.21 ms | 92.11 ms | 103.47 ms |

More aggregate columns increase live scan/transfer work. More groups increase
CPU hash work and SQL result cost; they do not by themselves guarantee GPU wins
against parallel CPU. These results strengthen the case for improving scan and
input concurrency before adding more GPU memory or persistent pinning.

## Next implementation boundary

The current operator deliberately accepts serial plans only. A useful next step
is parallel seekdb scan input feeding a bounded GPU fragment, with explicit
backpressure, cancellation and final aggregate ownership. Simply removing the
parallel-plan guard would be incorrect: partial/final aggregate stages and
multiple producers need their own protocol, and the current sidecar serializes
requests. Persistent table pinning also needs snapshot/invalidation semantics.

Build, 20 C++ tests and 16 SQL/transaction checks passed after the packing change.
The profiled shuffled sweep adds 270 checked timings to the 540 final unprofiled
timings. Reproduction scripts, stage logs, raw samples, EXPLAIN output and shape
generation SQL are in
[results/2026-10-23-profile](../../unittest/sql/sirius/results/2026-10-23-profile).
