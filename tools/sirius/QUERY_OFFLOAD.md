# Experimental whole-query offload

`query_offload.py` runs an explicit SELECT through Sirius using tables read from
seekdb. This is an opt-in developer CLI and in-process C ABI, not an automatic
replacement for seekdb's SQL executor or a network service.

```text
seekdb repeatable-read transaction → typed Arrow batches → GPU table materialization
                                                                ↓
SELECT → DuckDB binder/optimizer → Sirius GPU operators → typed Arrow results
```

The seekdb connection owns one consistent snapshot across every input table.
MySQL protocol decoding and Arrow construction currently run on CPU. DuckDB
plans SQL but does not hold a persistent copy of the source database. Sirius
executes the relational plan; unsupported GPU plans return an error instead of
falling back to DuckDB or seekdb CPU execution.

Input types include signed integers, Boolean, FLOAT/DOUBLE, DATE, UTF-8 strings
and DECIMAL up to precision 38. Decimal Arrow input is converted to the matching
Sirius decimal32/64/128 representation without converting money to floating
point. Output stays in Arrow, preserving decimal types and NULLs.

Each table is a materialized GPU CTE so multiple references (self joins,
correlated subqueries, and user CTEs) can share the input. The pinned Sirius
fragment interface otherwise permits only one consumer per input stream.
The overlay also fixes the result collector's schema check for a CTE root:
the result schema belongs to the consumer child, while the CTE operator's own
schema describes its producer passthrough.

## Build and run

Use the pinned Sirius source/build required by `build_arrow_overlay.py`, with
that build's compiler/linker environment activated. Output must be outside the
borrowed Sirius tree. The script recompiles the two edited translation units
and relinks into the output directory; it records the patch and commands.

```sh
python tools/sirius/build_arrow_overlay.py "$SIRIUS_SOURCE" "$SIRIUS_BUILD" "$OVERLAY"
cmake -S tools/sirius -B "$BRIDGE_BUILD" -DBUILD_TESTING=OFF \
  -DSEEKDB_SIRIUS_BUILD_BRIDGE=ON -DSEEKDB_SIRIUS_ARROW_INPUT=ON \
  -DSIRIUS_INCLUDE_DIR="$OVERLAY/include" -DSIRIUS_LIBRARY="$OVERLAY/libsirius.so"
cmake --build "$BRIDGE_BUILD" -j4

export SEEKDB_SIRIUS_CONFIG=/absolute/path/to/gpu.yaml
export SIRIUS_TEST_LIBRARY="$BRIDGE_BUILD/libseekdb_sirius_bridge.so"
export SEEKDB_DATABASE=tpch_smoke_sirius
# Set SEEKDB_HOST, SEEKDB_PORT, SEEKDB_USER, SEEKDB_PASSWORD as needed.
# LD_LIBRARY_PATH must resolve this overlay and the matching Sirius dependencies.
python tools/sirius/query_offload.py --library "$SIRIUS_TEST_LIBRARY" \
  --sql unittest/sql/sirius/tpch_queries/q03.sql --tables customer orders lineitem
```

The Python environment needs PyArrow and PyMySQL. The table list is explicit;
names must be lowercase SQL identifiers and every declared table must actually
be used by the optimized plan. Inputs are read sequentially through one seekdb
connection. Schema discovery and exact row counts precede the unbuffered scans.
Run against a stable schema: concurrent DDL is outside this prototype's contract.

```sh
python unittest/sql/sirius/test_query_offload.py -v
# Optional snapshot test creates and drops uniquely named tables in the test DB:
SEEKDB_TEST_SNAPSHOT=1 python unittest/sql/sirius/test_query_offload.py SnapshotTests -v
python unittest/sql/sirius/tpch_query_offload.py --library "$SIRIUS_TEST_LIBRARY" \
  --scale-factor 0.01 --output /outside/the/repository/tpch-query-results.json
```

The sweep compares GPU output with seekdb CPU output in the same transaction.
It checks row multisets with exact integer/decimal comparisons and float
tolerance (relative 1e-9, absolute 1e-6); dedicated integration tests also check
ordering, empty input, NULLs, multi-table types, repeated reads and recovery.
Timings include input preparation and are not pin-excluded GPU kernel timings.

## Boundaries and next work

- This is not yet an SF500 path: the fragment API requires every input to close
  before execution. Total decoded Arrow input is limited to 512 MiB, with a
  128 MiB batch limit. This is an input budget, not a bound on join intermediates
  or result size. Keep datasets/results small and configure the GPU memory pool.
- All columns are currently fetched and converted through Python. This proves
  execution coverage, not a speedup. There is no persistent pinned-input cache.
- SQL uses DuckDB expression, collation, date and decimal rules. TPC-H agreement
  does not establish general MySQL compatibility. Unsigned and complex input
  types are rejected. Use trusted SELECTs; this local API is not an SQL sandbox.
- The result callback runs on the engine thread, must drain/release its Arrow
  stream before returning, and must not call back into the bridge. Requests are
  serialized. Cancellation, authentication and transport are not implemented.

For scale and performance, replace Python row extraction with a seekdb native
column-batch scan adapter, project only required columns, and add bounded
producer/consumer flow with pinned host buffers and spill-aware repeated scans.
That requires a real streaming scan source, not independently executing chunks
of joins and concatenating their results. Then connect the whole-plan boundary
to a seekdb physical operator, with snapshot lifetime, cancellation, semantic
capability checks and costing. Direct seekdb SSTable GPU decoding is a separate
storage-format integration and is not needed to validate whole-query execution.
