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
    report = {"table": args.table, "group_column": args.group_column, "sum_columns": args.sum_columns,
              "scan_batch_rows": args.scan_batch_rows, "repeats": args.repeats,
              "disable_storage_aggregation": args.disable_storage_aggregation,
              "gpu_execution_verified": bool(args.sidecar_log),
              "timing": "SQL roundtrip, scan, transfer, GPU aggregation and SQL result", "cases": []}
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute("SET ob_query_timeout=600000000")
        cursor.execute(f"SELECT COUNT(*) FROM {identifier(args.table)}")
        report["rows"] = cursor.fetchone()[0]
        group = identifier(args.group_column)
        sums = ",".join(f"SUM({identifier(c)})" for c in args.sum_columns)
        for name, expressions in (("group_sum_count", f"{group},{sums},COUNT(*)"), ("group_count", f"{group},COUNT(*)")):
            times = {"cpu1": [], "cpu8": [], "gpu": []}
            plans = {}
            gpu_events = []
            expected = None
            def run(mode):
                nonlocal expected
                cursor.execute("SET @sirius_gpu=%s", (int(mode == "gpu"),))
                parallel = 8 if mode == "cpu8" else 1
                batch_hint = f"OPT_PARAM('rowsets_max_rows' {args.scan_batch_rows})" if args.scan_batch_rows else ""
                if args.disable_storage_aggregation:
                    batch_hint += " OPT_PARAM('pushdown_storage_level' 2)"
                sql = f"SELECT /*+ PARALLEL({parallel}) USE_HASH_AGGREGATION {batch_hint} */ {expressions} FROM {identifier(args.table)} GROUP BY {group}"
                log = Path(args.sidecar_log) if args.sidecar_log and mode == 'gpu' else None
                offset = log.stat().st_size if log else 0
                start = time.perf_counter()
                cursor.execute(sql)
                values = cursor.fetchall()
                elapsed = time.perf_counter() - start
                values = sorted(values, key=repr)
                if expected is None:
                    expected = values
                assert values == expected, (name, mode)
                if mode not in plans:
                    cursor.execute("EXPLAIN " + sql)
                    plans[mode] = [str(row) for row in cursor.fetchall()]
                if log:
                    with log.open() as stream:
                        stream.seek(offset)
                        events = [line.strip() for line in stream if line.startswith('native aggregate:')]
                    if not events:
                        raise RuntimeError(f'No native GPU execution observed: {plans[mode]}')
                    gpu_events.append(events)
                return elapsed
            first = {mode: run(mode) for mode in times}
            for i in range(args.repeats):
                modes = list(times)
                modes = modes[i % 3:] + modes[:i % 3]
                for mode in modes:
                    times[mode].append(run(mode))
            medians = {mode: statistics.median(values) for mode, values in times.items()}
            item = {"name": name, "groups": len(expected), "first_seconds": first, "seconds": times, "medians": medians,
                    "cpu1_speedup": medians["cpu1"] / medians["gpu"],
                    "cpu8_speedup": medians["cpu8"] / medians["gpu"], "explain": plans,
                    "native_gpu_events": gpu_events}
            report["cases"].append(item)
            Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps({key: value for key, value in item.items() if key != "explain"}), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--table", default="sirius_benchmark")
    p.add_argument("--group-column", default="grp")
    p.add_argument("--sum-columns", nargs="+", default=["amount"])
    p.add_argument("--scan-batch-rows", type=int, default=0,
                   help="Apply the same rowsets_max_rows hint to every CPU/GPU plan (0: default)")
    p.add_argument("--repeats", type=int, default=7)
    p.add_argument("--output", required=True)
    p.add_argument("--disable-storage-aggregation", action="store_true",
                   help="Apply pushdown_storage_level=2 to every mode so native aggregate input remains a scan")
    p.add_argument("--sidecar-log", help="Require per-query runtime evidence of actual GPU execution")
    args = p.parse_args()
    if args.repeats < 3:
        p.error("At least three repetitions required")
    if not 0 <= args.scan_batch_rows <= 4096:
        p.error("Scan batch rows must be in the validated range [0,4096]")
    main(args)
