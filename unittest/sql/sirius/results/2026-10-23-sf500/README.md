# SF500 projection and chunked aggregate evidence

2026-10-23. See [SF500.md](../../../../../tools/sirius/SF500.md) for scope,
commands, data preparation, limits, results and disk accounting.

- `supplier-import.json`, `customer-import.json`: source fingerprints, full
  source group/count/cents-sum oracle, verified chunks and CSV/disk/timing data.
  supplier's final free-space observation was refreshed by the completed-resume
  verification; its original per-chunk observations remain.
- `*-import-output.txt`: raw import output. Customer was intentionally interrupted after
  18 completed million-row chunks, then resumed with four bounded workers. The
  KeyboardInterrupt is this pause; the final import completed and verified 75M.
  Some early chunks used an accepted direct-load hint that did not select a
  demonstrated fast path; the final importer explicitly uses NO_DIRECT.
- `supplier-resume-verified.txt`: final importer successfully revalidates the
  existing completed manifest without reloading any ranges.
- `compaction.json`: polls through successful major compaction, final IDLE/NO.
- `supplier-chunk128-batch1024.json`, `customer-chunk128-batch1024.json`: seven
  repeated timings per query/mode, first timings, CPU/GPU equality and EXPLAIN.
- `customer-chunk{64,256}-batch1024.json`: sequential three-repeat target-size
  comparisons. All CPU/GPU modes receive the same 1024-row scan hint.
- `chunk-multisum-correctness.json`: real two-fragment four-SUM/1,000-group query
  on the earlier 5M synthetic table. Import was active; do not use its timings as
  uncontended performance results.
- `chunk-tests.txt`: eight focused partial-merge/boundary/buffer/overflow tests.
- `protocol-tests.txt`: nine existing socket/proxy tests.
- `native-integration.json`: sixteen SQL/transaction checks with chunking enabled.
- `sidecar-output.txt`: runtime readiness, pin/chunk settings and native input/group/
  fragment counts. An early "already running" startup error was a rejected
  second process; it did not replace the active sidecar. Later ready lines show
  successful orderly restarts with 128/64/256 MiB settings.
- `space-after-import.json`: intermediate filesystem and catalog observations.
  TABLES row/size statistics are stale; SSTABLES rows include retained versions.
  Neither is treated as the final table count or a unique physical-size total.
- `final-state.json`: immutable source fingerprint, no leftover CSV, unchanged
  preallocated data-file size, free-space samples and stopped processes/GPUs.

The source is the existing 140,138,786,816-byte SF500 DuckDB database. Both source
relations were read in full, projected to three columns and validated. These
are native seekdb aggregate tests on full SF500 supplier/customer projections;
this is not the complete TPC-H22 workload. No data files are committed here.
