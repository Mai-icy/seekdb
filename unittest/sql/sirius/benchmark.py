#!/usr/bin/env python3
"""Reproducible seekdb CPU versus Sirius snapshot aggregate benchmark."""
import argparse
from decimal import Decimal
import json
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from connection import connect, identifier
from execute_plan import execute
from make_plan import make_plan
from snapshot import snapshot


def normalized(rows):
    return sorted(tuple(None if v is None else Decimal(str(v)) for v in row) for row in rows)


def main(args):
    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    table = identifier(args.table)
    report = {"rows": args.rows, "repeats": args.repeats, "table": args.table,
              "snapshot_semantics": "immutable; export cost reported separately", "queries": []}
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SET ob_query_timeout=600000000")
            cur.execute("SET ob_trx_timeout=600000000")
            if args.create:
                # No IF NOT EXISTS or DROP: avoid overwriting an existing table.
                started = time.perf_counter()
                cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, grp BIGINT, amount DOUBLE, qty BIGINT)")
                seed = min(args.rows, 1000)
                cur.executemany(f"INSERT INTO {table} VALUES (%s,%s,%s,%s)",
                                [(i, i % 100, None if i % 97 == 0 else (i % 10000) * 0.25, i % 20)
                                 for i in range(seed)])
                size = seed
                while size < args.rows:
                    amount = min(size, args.rows - size)
                    cur.execute(f"INSERT INTO {table} SELECT id+%s, MOD(id+%s,100), "
                                "IF(MOD(id+%s,97)=0,NULL,MOD(id+%s,10000)*0.25), MOD(id+%s,20) "
                                f"FROM {table} WHERE id<%s", (size, size, size, size, size, amount))
                    size += amount
                    print(f"loaded {size} rows", flush=True)
                report["load_seconds"] = time.perf_counter() - started
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            assert cur.fetchone()[0] == args.rows
        parquet = directory / (args.table + ".parquet")
        if args.create or args.export:
            report["snapshot"] = snapshot(args.table, "id:int64,grp:int64,amount:float64,qty:int64", parquet)
            print(json.dumps(report["snapshot"]), flush=True)
        elif not parquet.exists():
            raise FileNotFoundError(parquet)
        cases = [
            ("sum_count", "SUM(amount),COUNT(*)", "", [], None),
            ("group_sum_count", "grp,SUM(amount),COUNT(*)", "GROUP BY grp", ["grp"], None),
            ("filtered_group", "grp,SUM(amount),COUNT(*)", "WHERE qty>=10 GROUP BY grp", ["grp"], "qty=10"),
        ]
        for name, select, suffix, groups, minimum in cases:
            plan = make_plan(parquet, groups=groups, sums=["amount"], count=True, minimum=minimum)
            (directory / (name + ".pb")).write_bytes(plan)
            item = {"name": name, "cpu": {}}
            started = time.perf_counter()
            gpu_result = normalized(execute(conn, plan)["rows"])
            item["gpu_first_seconds"] = time.perf_counter() - started
            # Warm both sides, then alternate CPU and GPU. Every timed result is
            # checked, and GPU timing includes SQL transport, lowering and JSON.
            for parallel in args.parallel:
                sql = f"SELECT /*+ PARALLEL({parallel}) */ {select} FROM {table} {suffix}"
                with conn.cursor() as cur:
                    cur.execute(sql)
                    assert normalized(cur.fetchall()) == gpu_result, name
                    cur.execute("EXPLAIN " + sql)
                    (directory / (name + f"-cpu{parallel}-explain.txt")).write_text(
                        "\n".join(str(row) for row in cur.fetchall()))
                cpu_times, gpu_times = [], []
                for iteration in range(args.repeats):
                    # Alternate ordering too, to reduce order/thermal bias.
                    for gpu in ([False, True] if iteration % 2 == 0 else [True, False]):
                        started = time.perf_counter()
                        if gpu:
                            rows = execute(conn, plan)["rows"]
                        else:
                            with conn.cursor() as cur:
                                cur.execute(sql)
                                rows = cur.fetchall()
                        elapsed = time.perf_counter() - started
                        assert normalized(rows) == gpu_result, name
                        (gpu_times if gpu else cpu_times).append(elapsed)
                cpu_median, gpu_median = statistics.median(cpu_times), statistics.median(gpu_times)
                item["cpu"][str(parallel)] = {"cpu_seconds": cpu_times, "gpu_seconds": gpu_times,
                    "cpu_median_seconds": cpu_median, "gpu_median_seconds": gpu_median,
                    "speedup": cpu_median / gpu_median}
            report["queries"].append(item)
            print(json.dumps(item), flush=True)
            (directory / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--table", default="sirius_benchmark")
    parser.add_argument("--rows", type=int, default=5000000)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--parallel", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--create", action="store_true", help="Create a new table and export its snapshot")
    parser.add_argument("--export", action="store_true", help="Export an existing table to a new directory before timing")
    args = parser.parse_args()
    if args.rows <= 0 or args.repeats < 3 or any(p < 1 for p in args.parallel):
        parser.error("Require positive rows/parallelism and at least 3 repetitions")
    main(args)
