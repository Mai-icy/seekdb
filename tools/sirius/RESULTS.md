# GPU integration validation — 2026-10-22

## Outcome

The native seekdb SQL function executes Sirius GPU plans successfully. With
5,000,000 rows and a **compacted seekdb table**, the grouped aggregate took
**21.97 ms on GPU versus 49.40 ms with seekdb DOP 8: 2.25× faster** (15-run
medians). Every timed result matched. Simple SUM/COUNT was faster on the CPU.

These are repeated queries over an already exported, immutable snapshot.
Export took **11.59 seconds** and is excluded from query latency. A single
export-plus-query is slower than the original seekdb query. For the grouped
query, amortizing that export alone would require roughly **423** equivalent
queries at the measured median saving; refresh and engine startup costs are
additional.

## Environment and provenance

- CPU: Intel Xeon w7-3565X, 32 physical cores / 64 logical CPUs; 503 GiB host RAM.
- GPU: one NVIDIA RTX 5880 Ada Generation, 49,140 MiB; driver 580.173.02.
  A second card was present but not used for the benchmark.
- Linux kernel 5.15.0-191-generic. Measurements were made on a shared machine,
  without CPU affinity or clock locking.
- seekdb base: `835320078f1944a6dadb3ed1bcc22dd4b3a9bd2e`, built from source with
  this branch's native expression/loader. Default `RelWithDebInfo` build,
  `memory_limit=16G`, SQL `PARALLEL(1)` and `PARALLEL(8)` hints; EXPLAIN confirmed
  `dop=8`. No column groups or extra indexes were added.
- Reused an existing Sirius shared-library build on the host with its matching
  public header and Pixi dependencies. The adjacent clean Sirius checkout was
  `3fcff6a0488516db09ddc7e993564eb9a98b72d3`; Sirius was **not rebuilt** during
  this experiment, so that checkout is not an independently verified build
  provenance claim. The tested `libsirius.so` SHA-256 is
  `cf5c490ac32f3cf7169bea8011688eedaefb634ee53741a3f475b7d729bd78ae`.
- Sirius used the [example configuration](sirius.example.yaml), with a task-local
  spill directory: one GPU, 15% GPU memory budget, 8 GB host capacity, 4 pipeline
  threads. `nvidia-smi` attributed approximately 7,646 MiB to the seekdb process
  after initialization. Execution used `Context::execute_substrait`, whose FFI
  path does not fall back to DuckDB CPU execution.

## Dataset and method

The deterministic dataset has `id BIGINT PRIMARY KEY`, `grp BIGINT`,
`amount DOUBLE`, and `qty BIGINT`. IDs are 0…4,999,999; `grp=id%100`,
`qty=id%20`, and `amount=(id%10000)*0.25`, except NULL when `id%97=0`.
The values are synthetic and highly compressible. Exact result comparisons are
possible because the floating point inputs are multiples of 0.25.

The final CPU baseline was measured after `ALTER SYSTEM MAJOR FREEZE` completed
(`DBA_OB_MAJOR_COMPACTION`: `IDLE`, `IS_ERROR=NO`); history reported a 5,000,000-row
major merge. The table uses seekdb's default indexed organization, dynamic row
format and `zstd_1.3.8` compression. The GPU reads a separate Snappy Parquet
snapshot with five row groups (maximum 1,048,576 rows), 30,777,744 bytes. This
comparison includes different scan/storage implementations; it does not isolate
GPU arithmetic speed from the storage-format effect.

Each query/parallelism pair was warmed first, then measured 15 times. CPU and
GPU calls alternated, including alternating which ran first. Timing includes SQL
round trip and result fetch, plus Substrait lowering, Arrow conversion and JSON
for the GPU call. Results were sorted and compared on every iteration. The GPU
runtime and OS caches were warm; `gpu_first_seconds` in the raw record means
first call of that plan, **not** a cold runtime or cold disk test.

## Final compacted-table results

GPU medians in this table are from the samples paired with DOP 8 CPU queries.
Speedup is CPU median divided by GPU median; less than 1 means GPU is slower.

| Query | seekdb DOP 8 | Sirius GPU | Speedup |
| --- | ---: | ---: | ---: |
| `SUM(amount), COUNT(*)` | 13.54 ms | 17.46 ms | 0.78× |
| `grp, SUM(amount), COUNT(*) GROUP BY grp` | 49.40 ms | 21.97 ms | **2.25×** |
| Same grouping with `WHERE qty >= 10` | 31.89 ms | 22.92 ms | 1.39× |

Against DOP 1, the respective speedups were 3.12×, 13.09× and 7.19×, using
separate paired GPU samples. The result supports selective offload; it does not
support sending every aggregate to the GPU.

Raw samples: [compacted.json](../../unittest/sql/sirius/results/2026-10-22/compacted.json).
CPU plan: [DOP 8 grouped aggregate](../../unittest/sql/sirius/results/2026-10-22/group-cpu8-explain.txt).

## Earlier experiments and useful changes

The initial exporter wrote one Parquet row group per 65,536-row fetch batch,
producing 77 small groups. Changing it to accumulate compact Arrow buffers and
write groups of up to 1,048,576 rows reduced latency. Fewer scan tasks and the
smaller compressed file are likely contributors; their individual effects were
not measured. Python fetch batches remain bounded at 65,536 rows.

Before seekdb major compaction, seven-run medians were:

| Snapshot layout | GPU latency across three queries | Speedup against DOP 8 |
| --- | ---: | ---: |
| 77 small groups | 156–191 ms | 1.30–1.59× |
| 5 larger groups | 19–26 ms | 10.73–11.11× |

The large speedups also reflected the freshly inserted CPU storage state.
They are **not** the primary performance claim. Compaction substantially improved
seekdb CPU latency, which is why the final table above uses the compacted state.
The earlier runs used the initial bridge lifecycle; final measurements use the
dedicated runtime thread.

Historical records: [small groups](../../unittest/sql/sirius/results/2026-10-22/fresh-small-groups.json),
[large groups](../../unittest/sql/sirius/results/2026-10-22/fresh-large-groups.json).
Only machine-specific snapshot paths were replaced by basenames in these records.

## Correctness and lifecycle evidence

- Full seekdb build succeeded: 1,853 Bazel actions.
- All 15 loader/Arrow C++ tests passed on Linux.
- Live SQL integration passed: NULL, invalid/empty plans, seekdb snapshot export,
  repeated calls, SUM/COUNT, integer SUM as decimal128, filter/group NULL behavior,
  four concurrent clients, result size errors, SUPER enforcement and recovery.
- The native CLI completed three separate successful GPU processes and an
  invalid-plan process with the expected ordinary error exit code. This exercises
  destruction of the context on its owner thread before Rust TLS teardown.
- The final benchmark checked 180 timed CPU/GPU results (3 queries × 2 CPU
  parallelism settings × 15 repetitions × 2 execution paths), plus warmups.

See [README.md](README.md) for reproduction and current limits. The implemented
scope is an explicit administrative SQL entry point over snapshots, with no
automatic SQL optimizer offload or live seekdb storage scan.
