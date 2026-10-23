#!/usr/bin/env python3
"""Fresh sidecar processes: pageable/pinned input with/without startup warmup."""
import argparse
import json
import os
from pathlib import Path
import selectors
import statistics
import subprocess
import sys
import time

TOOLS = Path(__file__).resolve().parents[3] / "tools/sirius"
sys.path.insert(0, str(TOOLS))
from connection import connect, identifier


def main(args):
    if os.path.exists(args.socket):
        raise ValueError("Stop the existing sidecar first; this test manages its own children")
    report = {"semantics": "fresh sidecar processes; seekdb storage remains warm; every result checked", "trials": []}
    with connect() as conn, conn.cursor() as cursor:
        sql = f"SELECT /*+ PARALLEL(1) USE_HASH_AGGREGATION */ grp,SUM(amount),COUNT(*) FROM {identifier(args.table)} GROUP BY grp"
        cursor.execute("SET @sirius_gpu=0")
        cursor.execute(sql)
        expected = sorted(cursor.fetchall(), key=repr)
        cursor.execute("SET @sirius_gpu=1")
        # Prove this query requires the native GPU path, rather than silently
        # timing a CPU plan while the sidecar is stopped.
        import pymysql
        try:
            cursor.execute(sql)
        except pymysql.MySQLError:
            pass
        else:
            raise AssertionError("Test query did not require the native backend")
        for trial in range(args.trials):
            modes = [(0, False), (16, False), (0, True), (16, True)]
            modes = modes[trial % 4:] + modes[:trial % 4]
            for pin, warmup in modes:
                command = [sys.executable, str(TOOLS / "sidecar.py"), "--socket", args.socket,
                           "--library", args.library, "--host-pin-mib", str(pin)]
                if warmup:
                    command.append("--warmup")
                started = time.perf_counter()
                child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                startup = []
                try:
                    with selectors.DefaultSelector() as selector:
                        selector.register(child.stdout, selectors.EVENT_READ)
                        while time.perf_counter() - started < 60:
                            if not selector.select(timeout=1):
                                continue
                            line = child.stdout.readline()
                            if not line:
                                raise RuntimeError("Sidecar exited: " + "".join(startup))
                            startup.append(line)
                            if line.startswith("ready:"):
                                break
                        else:
                            raise TimeoutError("Sidecar startup timed out")
                    ready = time.perf_counter() - started
                    times = []
                    for _ in range(6):
                        begin = time.perf_counter()
                        cursor.execute(sql)
                        actual = sorted(cursor.fetchall(), key=repr)
                        times.append(time.perf_counter() - begin)
                        assert actual == expected
                    item = {"trial": trial, "host_pin_mib": pin, "warmup": warmup,
                            "startup_seconds": ready, "first_query_seconds": times[0],
                            "startup_plus_first_seconds": ready + times[0],
                            "warm_seconds": times[1:], "warm_median_seconds": statistics.median(times[1:]),
                            "startup_log": startup}
                    report["trials"].append(item)
                    print(json.dumps(item), flush=True)
                    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
                finally:
                    child.terminate()
                    try:
                        tail, _ = child.communicate(timeout=15)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.communicate()
                        raise
                    assert child.returncode == 0, (child.returncode, tail)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--socket", required=True)
    p.add_argument("--library", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--table", default="sirius_benchmark")
    p.add_argument("--trials", type=int, default=3)
    main(p.parse_args())
