#!/usr/bin/env python3
"""Compare live end-to-end GPU aggregation with CPU SQL on an existing table."""
import argparse
import json
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from connection import connect, identifier
from live_query import query
from benchmark import normalized


def main(args):
    report = {"table": args.table, "parallel": args.parallel, "repeats": args.repeats,
              "gpu_timing": "fresh SELECT, MySQL decode, Arrow IPC, GPU execution and JSON; no cached snapshot",
              "cpu_seconds": [], "gpu_seconds": []}
    sql = (f"SELECT /*+ PARALLEL({args.parallel}) */ grp,SUM(amount),COUNT(*) "
           f"FROM {identifier(args.table)} GROUP BY grp")
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(sql)
        expected = normalized(cursor.fetchall())
        cursor.execute("EXPLAIN " + sql)
        report["explain"] = [str(row) for row in cursor.fetchall()]
        first = query(args.table, "grp:int64,amount:float64", args.socket,
                      groups=["grp"], sums=["amount"], count=True)
        assert normalized(first["result"]["rows"]) == expected
        report["gpu_first_seconds"] = first["end_to_end_seconds"]
        report["input_rows"], report["input_batches"] = first["input_rows"], first["input_batches"]
        print(json.dumps({"first": report["gpu_first_seconds"], "rows": report["input_rows"]}), flush=True)
        for iteration in range(args.repeats):
            for gpu in ([False, True] if iteration % 2 == 0 else [True, False]):
                if gpu:
                    result = query(args.table, "grp:int64,amount:float64", args.socket,
                                   groups=["grp"], sums=["amount"], count=True)
                    elapsed, rows = result["end_to_end_seconds"], result["result"]["rows"]
                else:
                    started = time.perf_counter()
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    elapsed = time.perf_counter() - started
                assert normalized(rows) == expected
                report["gpu_seconds" if gpu else "cpu_seconds"].append(elapsed)
            print(json.dumps({"iteration": iteration, "cpu": report["cpu_seconds"][-1],
                              "gpu": report["gpu_seconds"][-1]}), flush=True)
    report["cpu_median_seconds"] = statistics.median(report["cpu_seconds"])
    report["gpu_median_seconds"] = statistics.median(report["gpu_seconds"])
    report["speedup"] = report["cpu_median_seconds"] / report["gpu_median_seconds"]
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--table", default="sirius_benchmark")
    parser.add_argument("--parallel", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.parallel < 1 or args.repeats < 3:
        parser.error("Positive parallelism and at least 3 repetitions required")
    main(args)
