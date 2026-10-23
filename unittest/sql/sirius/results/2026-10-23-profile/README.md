# Input profiling and scan-batch experiments

2026-10-23, same isolated seekdb instance / single RTX 5880 GPU as the native
aggregate stage. Server and GPU bridge were rebuilt on `fdb86b71` with the changes
committed alongside these records. Original Sirius source/overlay unchanged.

## Measurement files

- `profile-baseline*.json`: preliminary default-batch measurements before the
  packing change. The v2 run has both seekdb and bridge stage counters.
- `profile-shapes1000.json`: 5M rows, 1,000 groups, four DOUBLE sums; plus COUNT.
- `profile-shapes10000.json`: 5M rows, 10,000 groups, one DOUBLE sum; plus COUNT.
- `profile-pack-batch{0,1024,4096}.json`: preliminary sequential batch sweep after
  removing the per-row type branch and avoiding per-row skip lookup on unfiltered
  batches. Zero selects the engine default.
- `profile-batch-sweep.json`: 15 shuffled repetitions of 3 batch settings ×
  3 execution modes × 2 queries, 270 checked timings, profiling enabled.
- `tuned-batch-sweep.json`: same shuffled experiment with profiling disabled in
  both seekdb and the sidecar; use this for final performance comparisons.
- `tuned-batch-sweep-repeat.json`: second unprofiled sweep with seed 20261024.
- `tuned-summary.json`: pooled 30-sample medians/deciles and individual run medians;
  deciles use Python `statistics.quantiles(..., n=10)`'s default exclusive method.
- `profile-seekdb-before.txt`: 44 profiles before the packing change: 12 baseline,
  16 shape-1000, 16 shape-10000 (including first executions).
- `profile-seekdb-all.txt`: these 44 followed by 16 per batch setting (0, 1024,
  4096), 96 profiles from the shuffled sweep, then 12 qualifying regression queries.
- `profile-bridge-all.txt`: corresponding bridge profiles, plus earlier startup
  and diagnostic runs; row counts distinguish small warmup/regression queries.
- `profile-stage-summary.json`: medians from the first 12 before profiles and
  profiles 44..59 after; omit each case's first execution. Before/after runs are
  sequential, so whole-query deltas also include machine scheduling variation.
- `profile-integration.json`, `profile-cpp-tests.txt`: 16 SQL checks and 20 C++
  tests after the packing change.
- `shape-table.sql`: exact generation SQL. 5M rows inserted in 12.606 s; major
  compaction finished at 11:33:32 local time, frozen SCN 1792726395887545006.

All successful measured results were checked against CPU. Timings include SQL
execute/fetch, while Python sorting used only for validation is excluded in these
new scripts. The earlier native benchmark included that sorting for its 100-row
outputs, so use within-run batch comparisons for the cleanest evidence.

## Failed configuration

The preliminary 16,384-row hint failed on its first **CPU1** warmup query with
`4016 Internal error`; CPU8/GPU were not timed for that setting. This is not a GPU
speed result or a successful configuration. The benchmark CLI now restricts the
hint to the validated 0..4096 range. No seekdb-wide batch default was changed.

## Reproduction

Start the sidecar with `--host-pin-mib 16 --warmup`; enable the native server gate.
Set `SEEKDB_SIRIUS_PROFILE=1` on both processes only when stage logs are wanted.
Use the existing port-2886 connection settings:

```sh
python unittest/sql/sirius/native_batch_sweep.py --repeats 15 --output sweep.json
python unittest/sql/sirius/native_benchmark.py \
  --table sirius_shapes_20261023 --group-column g1000 \
  --sum-columns a b c d --repeats 7 --output shapes1000.json
python unittest/sql/sirius/native_benchmark.py \
  --table sirius_shapes_20261023 --group-column g10000 \
  --sum-columns a --repeats 7 --output shapes10000.json
```

The shape runs preceded the packing change. See
[NATIVE_PERFORMANCE.md](../../../../../tools/sirius/NATIVE_PERFORMANCE.md) for
interpretation and final no-profile results.
