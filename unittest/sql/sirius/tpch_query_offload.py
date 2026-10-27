"""Small-data correctness sweep using the same seekdb snapshot for GPU and CPU.

This exercises every query strictly on Sirius (unsupported plans fail), without
a DuckDB data copy. It is not an SF500 performance benchmark or pin benchmark.
"""
import argparse
from decimal import Decimal
import json
import math
from pathlib import Path
import re
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from connection import connect
from query_offload import QueryBackend, snapshot, snapshot_inputs

TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")


def compare(expected, actual):
    def key(row):
        return tuple((0, "") if x is None else (1, Decimal(str(x)))
                     if isinstance(x, (int, float, Decimal)) else (2, str(x)) for x in row)
    if len(expected) != len(actual):
        raise AssertionError(f"Row count {len(expected)} != {len(actual)}")
    for a, b in zip(sorted(expected, key=key), sorted(actual, key=key)):
        if len(a) != len(b):
            raise AssertionError("Column count mismatch")
        for left, right in zip(a, b):
            numeric = isinstance(left, (int, float, Decimal)) and isinstance(right, (int, float, Decimal))
            # seekdb AVG uses a fixed decimal scale; DuckDB AVG returns DOUBLE.
            equal = math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-6) if numeric and (
                isinstance(left, float) or isinstance(right, float)) else left == right
            if not equal:
                raise AssertionError(f"Value mismatch {left!r} != {right!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument("--scale-factor", type=Decimal, required=True)
    parser.add_argument("--queries", nargs="+", type=int, default=list(range(1, 23)))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.scale_factor <= 0 or any(q < 1 or q > 22 for q in args.queries):
        parser.error("Require positive scale factor and query IDs 1..22")
    backend = QueryBackend(args.library)
    connection = connect()
    results = []
    try:
        for number in args.queries:
            sql = (Path(__file__).parent / "tpch_queries" / f"q{number:02}.sql").read_text()
            if number == 11:
                sql = sql.replace("0.0001", str(Decimal("0.0001") / args.scale_factor))
            # Fixed TPC-H fixtures only; ignore 'Customer' inside Q16's LIKE pattern.
            identifiers = re.sub(r"'(?:''|[^'])*'", "", sql)
            tables = [t for t in TABLES if re.search(r"\b" + t + r"\b", identifiers, re.I)]
            record = {"query": number, "tables": tables, "passed": False}
            try:
                with snapshot(connection):
                    start = time.perf_counter()
                    inputs = snapshot_inputs(connection, tables)
                    actual = backend.query(sql, inputs)
                    record["gpu_including_input_s"] = time.perf_counter() - start
                    start = time.perf_counter()
                    with connection.cursor() as cursor:
                        cursor.execute(sql)
                        expected = cursor.fetchall()
                    record["cpu_s"] = time.perf_counter() - start
                    compare(expected, list(zip(*(c.to_pylist() for c in actual.columns))))
                    record.update(passed=True, rows=len(actual))
            except Exception as error:
                record["error"] = str(error)
            results.append(record)
            print(json.dumps(record), flush=True)
            args.output.write_text(json.dumps(results, indent=2) + "\n")
    finally:
        connection.close()
        actual = None
        backend.shutdown()
    return 0 if all(r["passed"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
