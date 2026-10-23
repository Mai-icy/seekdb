#!/usr/bin/env python3
"""Interleave scan-batch settings and CPU/GPU modes; verify every SQL result."""
import argparse
import json
from pathlib import Path
import random
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from connection import connect, identifier


def main(args):
    report = {"table": args.table, "repeats": args.repeats, "seed": args.seed,
              "timing": "SQL execute and fetch; validation sorting excluded",
              "cases": []}
    randomizer = random.Random(args.seed)
    modes = [(batch, engine) for batch in (0, 1024, 4096) for engine in ("cpu1", "cpu8", "gpu")]
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute("SET ob_query_timeout=600000000")
        for name, select in (("sum_count", "grp,SUM(amount),COUNT(*)"), ("count", "grp,COUNT(*)")):
            expected = None
            times = {f"{batch}/{engine}": [] for batch, engine in modes}
            plans = {}
            order = []
            def run(batch, engine):
                nonlocal expected
                key = f"{batch}/{engine}"
                cursor.execute("SET @sirius_gpu=%s", (int(engine == "gpu"),))
                hint = f"OPT_PARAM('rowsets_max_rows' {batch})" if batch else ""
                sql = (f"SELECT /*+ PARALLEL({8 if engine == 'cpu8' else 1}) "
                       f"USE_HASH_AGGREGATION {hint} */ {select} "
                       f"FROM {identifier(args.table)} GROUP BY grp")
                started = time.perf_counter()
                cursor.execute(sql)
                rows = cursor.fetchall()
                elapsed = time.perf_counter() - started
                rows = sorted(rows, key=repr)
                if expected is None:
                    expected = rows
                assert rows == expected, (name, key)
                if key not in plans:
                    cursor.execute("EXPLAIN " + sql)
                    plans[key] = [str(row) for row in cursor.fetchall()]
                return elapsed
            first = {f"{batch}/{engine}": run(batch, engine) for batch, engine in modes}
            for _ in range(args.repeats):
                shuffled = modes.copy()
                randomizer.shuffle(shuffled)
                order.append([f"{batch}/{engine}" for batch, engine in shuffled])
                for batch, engine in shuffled:
                    times[f"{batch}/{engine}"].append(run(batch, engine))
            item = {"name": name, "groups": len(expected), "first_seconds": first,
                    "seconds": times, "medians": {k: statistics.median(v) for k, v in times.items()},
                    "order": order, "explain": plans}
            report["cases"].append(item)
            Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps({"name": name, "medians": item["medians"]}), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--table", default="sirius_benchmark")
    p.add_argument("--repeats", type=int, default=15)
    p.add_argument("--seed", type=int, default=20261023)
    p.add_argument("--output", required=True)
    args = p.parse_args()
    if args.repeats < 3:
        p.error("At least three repetitions required")
    main(args)
