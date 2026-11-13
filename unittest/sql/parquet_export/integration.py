"""SQL correctness and failure-path tests for native seekdb Parquet export.

Run on the server host so PyArrow can independently inspect the output files.
"""
import argparse
import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import secrets
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pymysql


def connect():
    return pymysql.connect(host=os.getenv("SEEKDB_HOST", "127.0.0.1"),
                           port=int(os.getenv("SEEKDB_PORT", "2881")),
                           user=os.getenv("SEEKDB_USER", "root"),
                           password=os.getenv("SEEKDB_PASSWORD", ""),
                           database=os.getenv("SEEKDB_DATABASE", "test"),
                           charset="utf8mb4", autocommit=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output_dir.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    table = "export_" + secrets.token_hex(5)
    results = []

    def passed(name):
        results.append(name)
        (directory / "results.json").write_text(json.dumps({"passed": results}, indent=2) + "\n")
        print("PASS", name, flush=True)

    with connect() as connection, connection.cursor() as cursor:
        cursor.execute("SET parallel_degree_policy='MANUAL'")
        cursor.execute("SET _force_parallel_query_dop=1")
        cursor.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, small SMALLINT, v DOUBLE, label VARCHAR(64), d DATE)")
        try:
            rows = [(i, i % 100, i / 4, None if i % 3 == 0 else "中文\t\n" + str(i), datetime.date(2024, 1, 2)) for i in range(17000)]
            for start in range(0, len(rows), 1000):
                cursor.executemany(f"INSERT INTO {table} VALUES (%s,%s,%s,%s,%s)", rows[start:start+1000])
            path = directory / "data.parquet"
            cursor.execute(f"SELECT /*+ OPT_PARAM('ROWSETS_MAX_ROWS', 256) */ id, small, v, label, d INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {table} ORDER BY id", (str(path),))
            assert cursor.rowcount == len(rows), cursor.rowcount
            actual = pq.read_table(path)
            assert actual.schema.names == ["id", "small", "v", "label", "d"], actual.schema
            assert actual.schema.types == [pa.int64(), pa.int16(), pa.float64(), pa.string(), pa.date32()], actual.schema
            assert list(zip(*(c.to_pylist() for c in actual.columns))) == rows
            assert pq.ParquetFile(path).num_row_groups == 3
            passed("streaming_17000_rows_types_null_unicode_date")

            # Exceed the byte budget before reaching the 4,096-row batch cap.
            wide_table = table + '_wide'
            cursor.execute(f"CREATE TABLE {wide_table}(id BIGINT PRIMARY KEY,payload VARCHAR(16000))")
            try:
                payload = '中' * 15000
                wide_rows = [(i, None if i % 11 == 0 else payload) for i in range(220)]
                for start in range(0, len(wide_rows), 10):
                    cursor.executemany(f"INSERT INTO {wide_table} VALUES (%s,%s)", wide_rows[start:start+10])
                wide_path = directory / "wide-batches.parquet"
                cursor.execute(f"SELECT id,payload INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {wide_table} ORDER BY id", (str(wide_path),))
                assert cursor.rowcount == len(wide_rows)
                actual = pq.read_table(wide_path)
                assert list(zip(actual.column(0).to_pylist(), actual.column(1).to_pylist())) == wide_rows
                passed("batch_byte_limit_preserves_large_utf8_and_nulls")
            finally:
                cursor.execute(f"DROP TABLE {wide_table}")

            empty = directory / "empty.parquet"
            cursor.execute(f"SELECT id AS renamed, label INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {table} WHERE id < 0", (str(empty),))
            result = pq.read_table(empty)
            assert result.num_rows == 0 and result.schema.names == ["renamed", "label"]
            passed("empty_result_preserves_schema_and_aliases")

            # Force the scalar executor as well as the normal vectorized path.
            scalar = directory / "scalar.parquet"
            cursor.execute(f"SELECT /*+ OPT_PARAM('ROWSETS_MAX_ROWS', 0) */ id+1 AS next_id, v*2 AS doubled INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {table} WHERE id<3 ORDER BY id", (str(scalar),))
            assert pq.read_table(scalar).to_pydict() == {"next_id": [1, 2, 3], "doubled": [0.0, 0.5, 1.0]}
            passed("scalar_executor_computed_columns")

            # Check exact values through both scalar and vectorized CXX paths.
            for batch_size in (0, 256):
                decimal_path = directory / f"decimal-{batch_size}.parquet"
                cursor.execute(f"SELECT /*+ OPT_PARAM('ROWSETS_MAX_ROWS', {batch_size}) */ "
                    "CAST('-999999999999999999999999999999999999.99' AS DECIMAL(38,2)) AS wide, "
                    "CAST('-0.01' AS DECIMAL(15,2)) AS amount, "
                    "CAST('0.000000000000000000000000000001' AS DECIMAL(38,30)) AS fraction, "
                    "CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS integer_value, "
                    "CAST(NULL AS DECIMAL(15,2)) AS missing "
                    f"INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {table} WHERE id<3", (str(decimal_path),))
                actual = pq.read_table(decimal_path)
                assert actual.schema.types == [pa.decimal128(38,2), pa.decimal128(15,2), pa.decimal128(38,30), pa.decimal128(38,0), pa.decimal128(15,2)], actual.schema
                assert actual.to_pydict() == {
                    "wide": [Decimal('-999999999999999999999999999999999999.99')]*3,
                    "amount": [Decimal('-0.01')]*3,
                    "fraction": [Decimal('0.000000000000000000000000000001')]*3,
                    "integer_value": [Decimal('99999999999999999999999999999999999999')]*3,
                    "missing": [None]*3}
            decimal_path = directory / "decimal-column.parquet"
            cursor.execute(f"ALTER TABLE {table} ADD amount DECIMAL(15,2)")
            cursor.execute(f"UPDATE {table} SET amount=CAST(v AS DECIMAL(15,2)) WHERE id%3<>0")
            cursor.execute(f"SELECT amount INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {table} ORDER BY id", (str(decimal_path),))
            with connection.cursor() as expected_cursor:
                expected_cursor.execute(f"SELECT amount FROM {table} ORDER BY id")
                assert pq.read_table(decimal_path).column(0).to_pylist() == [r[0] for r in expected_cursor.fetchall()]
            passed("decimal_exact_precision_scale_null_scalar_and_vectorized")

            prior = path.read_bytes()
            rejected = [
                (f"SELECT id INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {table}", path),
                (f"SELECT id,id INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {table}", directory / "duplicate.parquet"),
                (f"SELECT CAST(v AS DECIMAL(40,2)) AS value INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {table}", directory / "decimal.parquet"),
                (f"SELECT id INTO OUTFILE %s FORMAT=(TYPE='PARQUET' FIELD_DELIMITER=',') FROM {table}", directory / "options.parquet"),
                (f"SELECT id INTO OUTFILE %s FORMAT=(TYPE='PARQUET') SINGLE FALSE FROM {table}", directory / "split.parquet"),
            ]
            for sql, target in rejected:
                try:
                    cursor.execute(sql, (str(target),))
                except pymysql.MySQLError:
                    pass
                else:
                    raise AssertionError(("Expected rejection", sql))
                if target != path:
                    assert not target.exists()
            assert path.read_bytes() == prior
            assert not list(directory.glob(".parquet-export-*"))
            passed("no_overwrite_unsupported_types_options_and_duplicate_names")

            # Cancellation must not publish a partial file, even after row groups.
            failed = directory / "cancelled.parquet"
            with connect() as worker:
                with worker.cursor() as setup:
                    setup.execute("SET parallel_degree_policy='MANUAL'")
                    setup.execute("SET _force_parallel_query_dop=1")
                    setup.execute("SELECT CONNECTION_ID()")
                    connection_id = setup.fetchone()[0]
                errors = []
                def export():
                    try:
                        with worker.cursor() as other:
                            other.execute(f"SELECT a.id AS a,b.id AS b INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {table} a CROSS JOIN {table} b", (str(failed),))
                    except pymysql.MySQLError as error:
                        errors.append(str(error))
                thread = threading.Thread(target=export)
                thread.start()
                deadline = time.monotonic() + 15
                while not any(p.stat().st_size > 4 for p in directory.glob(".parquet-export-*")) and thread.is_alive() and time.monotonic() < deadline:
                    time.sleep(.05)
                assert thread.is_alive(), errors
                assert any(p.stat().st_size > 4 for p in directory.glob(".parquet-export-*"))
                cursor.execute(f"KILL QUERY {int(connection_id)}")
                thread.join(15)
                assert not thread.is_alive() and errors
            assert not failed.exists() and not list(directory.glob(".parquet-export-*"))
            passed("query_cancellation_cleans_unpublished_file")

            csv = directory / "existing.csv"
            cursor.execute(f"SELECT id INTO OUTFILE %s FROM {table} WHERE id<2 ORDER BY id", (str(csv),))
            assert csv.read_text() == "0\n1\n"
            passed("existing_csv_export")
        finally:
            cursor.execute(f"DROP TABLE {table}")


if __name__ == "__main__":
    main()
