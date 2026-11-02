#!/usr/bin/env python3
"""Public adapter GPU checks; no seekdb/Parquet/DuckDB storage is involved."""
from datetime import date
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools" / "sirius"))
import duckdb
import pyarrow as pa
from adapter_backend import Backend, compile_plan


def main():
    compiler = str(
        Path(__file__).resolve().parents[3] / "tools/sirius/adapter_plan_compiler.py"
    )
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("grp", pa.int64()),
            ("v", pa.decimal128(15, 2)),
            ("label", pa.string()),
            ("day", pa.date32()),
            ("valid", pa.bool_()),
        ]
    )
    a = pa.table(
        {
            "id": [1, 2, 3, 4],
            "grp": [1, 1, 2, 2],
            "v": [
                Decimal("12.34"),
                None,
                Decimal("-0.01"),
                Decimal("9999999999999.99"),
            ],
            "label": ["a", "中文", "", None],
            "day": [date(1960, 1, 1), date(2026, 1, 1), None, date(2000, 2, 29)],
            "valid": [True, False, None, True],
        },
        schema=schema,
    )
    tables = {"a": a, "empty_table": a.slice(0, 0)}
    queries = [
        "select * from a order by id",
        "select grp,sum(v),count(*),count(v) from a group by grp order by grp",
        "select id from a where day < date '2000-01-01' and valid order by id",
        "select x.id,y.id from a x left join a y on x.id=y.grp order by x.id,y.id",
        "select count(*),sum(v) from empty_table",
        "select id from a where v is null order by id",
        "select id from a where not exists (select 1 from a b where b.grp=a.id) order by id",
        "select id from a order by id limit 1",
        "select id,v from a order by v,id",
        "select id,v from a order by v desc,id",
        "select (select sum(v) from empty_table) from a order by id limit 1",
    ]
    checks = []
    backend = Backend(os.environ["SIRIUS_ADAPTER_LIBRARY"])
    try:
        with duckdb.connect() as cpu:
            cpu.execute("SET default_null_order='NULLS_FIRST_ON_ASC_LAST_ON_DESC'")
            for name, table in tables.items():
                cpu.register(name, table)
            names = list(tables)
            for sql in queries:
                plan, scans = compile_plan(
                    compiler, sql, {k: v.schema for k, v in tables.items()}
                )
                expected = cpu.execute(sql).fetchall()

                def batches(scan):
                    yield from tables[names[scan.table_index]].to_batches(
                        max_chunksize=2
                    )

                start = time.perf_counter()
                result = backend.execute(
                    plan, scans, "fixture:1", batches, lambda _: None
                )
                actual = list(zip(*(c.to_pylist() for c in result.columns)))
                assert actual == expected, (sql, expected, actual)
                check = {
                    "sql": sql,
                    "status": "match",
                    "scans": len(scans),
                    "rows": len(result),
                    "seconds": time.perf_counter() - start,
                }
                checks.append(check)
                print(json.dumps(check), flush=True)
            plan, scans = compile_plan(
                compiler, "select count(*) from a", {"a": schema}
            )

            def broken(_):
                yield a.slice(0, 1).to_batches()[0]
                raise RuntimeError("deliberate host reader failure")

            try:
                backend.execute(plan, scans, "fixture:2", broken, lambda _: None)
            except RuntimeError as error:
                assert "deliberate host reader failure" in str(error), error
            else:
                raise AssertionError("reader failure was ignored")
            result = backend.execute(
                plan,
                scans,
                "fixture:3",
                lambda _: iter(a.to_batches(max_chunksize=1)),
                lambda _: None,
            )
            assert result.column(0).to_pylist() == [4]
            print(
                json.dumps(
                    {"check": "producer_failure_and_recovery", "status": "passed"}
                ),
                flush=True,
            )
    finally:
        backend.close()


if __name__ == "__main__":
    main()
