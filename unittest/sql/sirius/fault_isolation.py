#!/usr/bin/env python3
"""Kill and restart our own sidecar child; keep the existing seekdb alive."""
import argparse
import json
import os
import socket
from pathlib import Path
import subprocess
import sys
import time

import pymysql

TOOLS = Path(__file__).resolve().parents[3] / "tools/sirius"
sys.path.insert(0, str(TOOLS))
from connection import connect
from execute_plan import execute


def main(args):
    if os.path.exists(args.socket):
        raise ValueError("Stop the existing sidecar before this test")
    child = None
    def start():
        nonlocal child
        child = subprocess.Popen([sys.executable, str(TOOLS / "sidecar.py"), "--socket", args.socket,
                                  "--library", args.library], stdout=subprocess.DEVNULL)
        for _ in range(100):
            if child.poll() is not None:
                raise RuntimeError("Sidecar exited during startup")
            if os.path.exists(args.socket):
                with socket.socket(socket.AF_UNIX) as probe:
                    probe.settimeout(0.1)
                    try:
                        probe.connect(args.socket)
                    except (ConnectionRefusedError, FileNotFoundError):
                        pass
                    else:
                        return
            time.sleep(0.1)
        raise TimeoutError("Sidecar startup timed out")
    plan = Path(args.plan).read_bytes()
    try:
        start()
        with connect() as conn:
            expected = execute(conn, plan)
            assert expected["rows"]
            gpu_pid = child.pid
            if args.seekdb_pid:
                maps = Path(f"/proc/{args.seekdb_pid}/maps").read_text()
                assert "libseekdb_sirius_proxy.so" in maps
                assert "libcuda" not in maps and "/libsirius.so" not in maps
            child.kill()  # Only the Popen child created by this test.
            child.wait(timeout=10)
            try:
                execute(conn, plan)
            except pymysql.MySQLError:
                pass
            else:
                raise AssertionError("Dead backend unexpectedly succeeded")
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                assert cursor.fetchone() == (1,)
            start()  # Sidecar removes its own stale socket after checking it.
            assert execute(conn, plan) == expected
            recovered_pid = child.pid
        print(json.dumps({"status": "passed", "killed_child_pid": gpu_pid,
                          "recovered_child_pid": recovered_pid, "seekdb_pid": args.seekdb_pid,
                          "checks": ["native_sql", "cpu_process_has_no_cuda", "gpu_process_kill",
                                     "sql_error_on_unavailable", "cpu_sql_survives", "restart_recovery"]}))
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            child.wait(timeout=15)
            assert child.returncode == 0, child.returncode


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--socket", required=True)
    p.add_argument("--library", required=True)
    p.add_argument("--plan", required=True)
    p.add_argument("--seekdb-pid", type=int, required=True)
    main(p.parse_args())
