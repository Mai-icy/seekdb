#!/usr/bin/env python3
"""Time ordinary SQL through the native GPU aggregate and CPU execution paths."""
import argparse
import json
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from connection import connect, identifier


def main(args):
    report = {"table": args.table, "repeats": args.repeats, "timing": "SQL roundtrip, scan, transfer, GPU aggregation and SQL result", "cases": []}
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute("SET ob_query_timeout=600000000")
        cursor.execute(f"SELECT COUNT(*) FROM {identifier(args.table)}")
        report["rows"] = cursor.fetchone()[0]
        for name, expressions in (("group_sum_count", "grp,SUM(amount),COUNT(*)"), ("group_count", "grp,COUNT(*)")):
            times = {"cpu1": [], "cpu8": [], "gpu": []}
            plans = {}
            expected = None
            def run(mode):
                nonlocal expected
                cursor.execute("SET @sirius_gpu=%s", (int(mode == "gpu"),))
                parallel = 8 if mode == "cpu8" else 1
                sql = f"SELECT /*+ PARALLEL({parallel}) USE_HASH_AGGREGATION */ {expressions} FROM {identifier(args.table)} GROUP BY grp"
                start = time.perf_counter()
                cursor.execute(sql)
                values = sorted(cursor.fetchall(), key=repr)
                elapsed = time.perf_counter() - start
                if expected is None:
                    expected = values
                assert values == expected, (name, mode)
                if mode not in plans:
                    cursor.execute("EXPLAIN " + sql)
                    plans[mode] = [str(row) for row in cursor.fetchall()]
                return elapsed
            first = {mode: run(mode) for mode in times}
            for i in range(args.repeats):
                modes = list(times)
                modes = modes[i % 3:] + modes[:i % 3]
                for mode in modes:
                    times[mode].append(run(mode))
            medians = {mode: statistics.median(values) for mode, values in times.items()}
            item = {"name": name, "first_seconds": first, "seconds": times, "medians": medians,
                    "cpu1_speedup": medians["cpu1"] / medians["gpu"],
                    "cpu8_speedup": medians["cpu8"] / medians["gpu"], "explain": plans}
            report["cases"].append(item)
            Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps({key: value for key, value in item.items() if key != "explain"}), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--table", default="sirius_benchmark")
    p.add_argument("--repeats", type=int, default=7)
    p.add_argument("--output", required=True)
    args = p.parse_args()
    if args.repeats < 3:
        p.error("At least three repetitions required")
    main(args)
