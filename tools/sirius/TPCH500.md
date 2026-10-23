# Larger pinned frames and full TPC-H validation

This extends the [SF500 projection experiment](SF500.md). Larger native frames
are implemented and tested; full SF500 TPC-H import/validation is in progress.
The 22-query smoke run passes, but **none of its queries executes a GPU aggregate**.
The supported native operator still requires a serial direct scan, BIGINT group
keys and DOUBLE SUM / COUNT. Increasing pinned memory does not add JOIN, DECIMAL,
AVG or string-group support.

## Pinned transfer experiment

The sidecar now accepts `--host-pin-mib 16|64|128` (or `0` for pageable input).
The seekdb process accepts `SEEKDB_SIRIUS_FRAME_MIB=64|128`; unset/other values keep
the existing 16 MiB / 262,144-row behavior. Larger frames use a version-2 native
descriptor and negotiate the capacity before consuming scan input. An allocated
pin buffer smaller than the requested frame rejects admission. Allocation
failure retains the existing pageable fallback. Each sidecar owns one reused
pin buffer, not a pinned copy of a table.

Both bridge and owned Sirius Arrow overlay must be rebuilt: their input bounds
now allow up to 8,388,608 rows **and** at most 128 MiB per batch. The fragment
retains its 512 MiB total input limit. Size arithmetic uses 64 bits before the
transport's 32-bit length conversion. Result bounds are unchanged.

Measurements below use all 75 million rows of the previous three-column
`test.sirius_sf500_customer` projection. They include the SQL round trip, seekdb
scan, packing, transfer and GPU aggregation. Each configuration has five timed
repetitions after first-call measurement, rotating CPU1/CPU8/GPU execution order.
Data was compacted before measurement, and full-table imports were paused.
These are warm-storage measurements, not a cold-start claim.

| Pin / frame MiB | SUM+COUNT GPU | CPU1 | CPU8 | COUNT GPU | CPU1 | CPU8 |
|---|---:|---:|---:|---:|---:|---:|
| 16 / 16 | 1.185 s | 2.193 s | 0.390 s | 0.884 s | 2.019 s | 0.347 s |
| 64 / 16 (allocation-only control) | 1.257 s | 2.208 s | 0.384 s | 0.831 s | 1.991 s | 0.340 s |
| **64 / 64** | **1.059 s** | 2.200 s | 0.383 s | **0.720 s** | 1.983 s | 0.337 s |
| 128 / 128 | 1.115 s | 2.190 s | 0.374 s | 0.712 s | 1.971 s | 0.339 s |

64 / 64 is retained for the full-suite configuration: 2.08× / 2.75× faster than
CPU1 on these two aggregates, while CPU8 remains faster. Compared with 16 / 16,
the observed GPU medians decrease by 10.6% / 18.6%. Five samples in sequential
configuration runs do not establish a stable ranking between 64 and 128 MiB.
The benefit combines larger batches and pinning; allocation-only results cannot
isolate a pinning speedup. Defaults remain unchanged.

The 64 / 64 run actually transferred frames up to 60,584,392 bytes. The 128 MiB
fragment target produced seven SUM+COUNT fragments and four COUNT fragments per
query. Merely allocating a 64 MiB buffer while retaining the old frame setting
still sent approximately 4.26 / 2.13 MB frames.

All GPU measurements in the table have per-execution native sidecar events and
matching CPU results. The benchmark uses the same
`OPT_PARAM('pushdown_storage_level' 2)` hint in all three modes to exercise the
hash aggregate. Without it, compaction can enable storage aggregation and leave
an unsupported SUM(SUM)/COUNT_SUM shape above the scan. The initial such run is
retained as `customer-pin16-frame16-cpu-fallback.json` and **excluded** from GPU
comparisons. The full TPC-H runner does not force this operator choice.

```sh
# Before starting the isolated seekdb process:
export SEEKDB_SIRIUS_NATIVE=1 SEEKDB_SIRIUS_FRAME_MIB=64
# Start its sidecar with:
# --host-pin-mib 64 --warmup --native-chunk-mib 128
export SEEKDB_PORT=2886
python unittest/sql/sirius/native_benchmark.py \
  --table sirius_sf500_customer --scan-batch-rows 1024 --repeats 5 \
  --disable-storage-aggregation --sidecar-log /absolute/path/sidecar.log \
  --output customer-pin64-frame64.json
```

## Full-table preparation and query runner

`prepare_tpch.py` reads the existing SF500 DuckDB file read-only. It imports all
eight tables and all columns, retaining DECIMAL and DATE types. VARCHAR data is
hex-encoded in staging and decoded by LOAD DATA to avoid CSV escaping changes.
It uses seekdb's normal LOAD DATA path; no DuckDB data copy or persistent Parquet
export is created. Sparse order keys use key ranges rather than OFFSET scans.

Each staging CSV is limited to 128 MiB; at 16 workers at most 2 GiB is staged per
round (4 GiB at the optional 32 workers). The importer also applies a process
file-size limit so an oversized range fails during COPY, with cleanup in the
worker's `finally` block. Files are removed after each range. Manifests record source path, size,
mtime, target identity and completed ranges. Resume retries only unrecorded
ranges in manifest-owned tables and refuses to adopt an existing unowned table.
Validation checks affected rows, warnings, range counts and row samples, then
full-table counts, per-column numeric/date sums or string lengths, non-NULL
counts, and boundary samples. These fingerprints are not a cryptographic proof
of every imported value; query-result comparisons provide additional validation.

The default free-space reserve is 180 GiB. A growth guard and the isolated
seekdb datafile maximum provide additional bounds. Creating `PAUSE` in the job
directory stops import after the current bounded round; remove it to resume.
The importer owns a nonblocking file lock. Run only one pipeline per job.

`tpch_pipeline.py` resumes import, verifies all eight manifests, major-compacts,
analyzes all tables, then runs `tpch_benchmark.py`. An optional sidecar launcher
starts the GPU process only for queries and stops that owned process on exit.
Progress and results are saved incrementally. A failed query is reported and can
be rerun with `--resume`; it is never counted as passed.

The 22 templates are exported from DuckDB 1.5.6 `tpch_queries()`. Q11's threshold
is scaled identically in both engines. Each query is compared against read-only
DuckDB results in CPU8 and native-enabled CPU1 modes. Native events determine
actual GPU use. Comparison checks row multisets, exact non-floating numerics,
and floating values with relative tolerance 1e-9 / absolute tolerance 1e-6;
ORDER BY sequence is not compared. One measured execution per mode is a
correctness/timing survey, not a statistically stable benchmark or audited
TPC-H score. The DuckDB timing is an oracle-generation diagnostic, not a matched
performance baseline.

```sh
export SEEKDB_PORT=2886
python unittest/sql/sirius/tpch_pipeline.py \
  --source /absolute/path/tpch_sf500.duckdb \
  --directory /absolute/path/tpch500-job --database tpch500_sirius \
  --workers 16 --scale-factor 500 --host-pin-mib 64 \
  --sidecar-log /absolute/path/tpch500-job/sidecar.log \
  --sidecar-launcher /absolute/path/start-sidecar.sh
```

The launcher must configure matching bridge/overlay paths, private socket and
GPU limits. The seekdb instance for this stage explicitly uses
`memory_budget=64G`, `memstore_memory_limit=16G`, `kvcache_memory_limit=16G`,
`datafile_maxsize=384G` and `datafile_next=10G`. This fork accepts the deprecated
`memory_limit` option but ignores it; earlier command lines using `--memory_limit`
did not impose that budget. Pin comparisons above share the same effective
64 GiB budget, but cross-stage historical runs should not be treated as having
identical memory settings.

## Validation and provenance

- Complete seekdb, bridge and owned Sirius Arrow-overlay builds succeeded.
- 21 C++ tests, 11 native chunk tests and 9 sidecar transport tests passed.
- An importer integration test lowers its child file limit to 128 KiB and
  verifies an oversized export raises EFBIG, removes its CSV and inserts zero
  target rows. The regular importer ceiling is 128 MiB.
- 16 SQL/transaction checks passed for each frame setting: 16, 64 and 128 MiB.
- The complete SF0.01 import/compaction/analyze/TPC-H22 pipeline passed in both
  modes. All 22 queries used CPU fallback; no full-suite GPU speedup is claimed.
- Full SF500 preparation and query validation are still running.
- Borrowed Sirius checkout `3fcff6a0488516db09ddc7e993564eb9a98b72d3`
  remains unmodified. Its original library SHA-256 is
  `cf5c490ac32f3cf7169bea8011688eedaefb634ee53741a3f475b7d729bd78ae`.
  The rebuilt owned overlay SHA-256 is
  `50315164bf8469d811af60ffe5fabc6e2eac7ef40f96ce612d3853677fe1db10`.
- Remote job directory: `/data/wuhao.xwh/seekdb-sirius-tpch500-20261023`.
  The existing SF500 source is 140,138,786,816 bytes; its fingerprint is checked
  before/after preparation and queries.

Raw evidence: [results/2026-10-23-tpch](../../unittest/sql/sirius/results/2026-10-23-tpch/).
