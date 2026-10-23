# Larger pinned frames and full TPC-H evidence

See [TPCH500.md](../../../../../tools/sirius/TPCH500.md) for configuration,
interpretation and limitations. Full SF500 import/query results are pending.

- `customer-pin{16,64,128}-frame{16,64,128}.json`: five-repeat medians on the
  existing full 75-million-row, three-column customer projection, with matched
  storage-aggregation hints, CPU equality checks and per-query GPU log events.
  The 64/16 file is the allocation-only control.
- `customer-pin16-frame16-cpu-fallback.json`: **excluded diagnostic**, produced
  before requiring GPU log evidence. Storage aggregation caused CPU fallback.
  Its `gpu` timing label must not be interpreted as actual GPU execution.
- `pin-compaction.json`: compacted storage state used for the pin comparison.
- `native-integration-frame*.json`: 16 SQL/transaction checks per frame size.
- `cpp-tests.txt`, `chunk-tests.txt`, `protocol-tests.txt`: focused transport and
  operator tests. These files contain no full dataset or binary artifacts.
- `smoke/`: complete SF0.01 import/compaction/analyze/query-pipeline result.
  All 22 queries match the DuckDB oracle in both modes. None uses a native GPU
  aggregate. Smoke timings are not SF500 timings.

The SF500 source remains a read-only external DuckDB file. Full-table manifests
and 22-query results will be added after validation completes; the projection
pin comparison is not a full TPC-H GPU performance result.
