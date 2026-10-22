#!/usr/bin/env python3
"""GPU integration tests against an isolated seekdb instance (requires SUPER)."""
import argparse
import json
from pathlib import Path
import secrets
import sys
import tempfile
import subprocess
from concurrent.futures import ThreadPoolExecutor

import pymysql
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from connection import connect, identifier
from execute_plan import execute
from make_plan import make_plan
from snapshot import snapshot


def main(directory, cli=None):
    suffix = secrets.token_hex(6)
    table = "sirius_test_" + suffix
    user = "sirius_test_" + suffix
    password = secrets.token_urlsafe(24)
    with tempfile.TemporaryDirectory(prefix="sirius-", dir=directory) as tmp, connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {identifier(table)} (id BIGINT PRIMARY KEY, amount DOUBLE)")
        try:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {identifier(table)} VALUES (1,10),(2,NULL),(3,30.5)")
                cur.execute("SELECT SIRIUS_EXECUTE_SUBSTRAIT(NULL)")
                assert cur.fetchone() == (None,)
                for plan in (b"", b"\xff"):
                    try:
                        execute(conn, plan)
                    except pymysql.MySQLError:
                        pass
                    else:
                        raise AssertionError("Invalid plan was accepted")
            parquet = Path(tmp) / "snapshot.parquet"
            report = snapshot(table, "id:int64,amount:float64", parquet)
            assert report["rows"] == 3
            scan = make_plan(parquet)
            if cli:
                # This specifically regresses logging/TLS crashes during native
                # process shutdown; SQL-only tests cannot observe that boundary.
                plan_file = Path(tmp) / "scan.pb"
                plan_file.write_bytes(scan)
                for _ in range(3):
                    child = subprocess.run([cli, str(plan_file)], capture_output=True,
                                           text=True, timeout=60, check=True)
                    assert sorted(json.loads(child.stdout)["rows"]) == [[1, 10], [2, None], [3, 30.5]]
                plan_file.write_bytes(b"\xff")
                child = subprocess.run([cli, str(plan_file)], capture_output=True, text=True, timeout=60)
                assert child.returncode == 3, (child.returncode, child.stderr)
            for _ in range(3):
                result = execute(conn, scan)
                assert sorted(result["rows"]) == [[1, 10], [2, None], [3, 30.5]], result
            result = execute(conn, make_plan(parquet, sums=["amount"], count=True))
            assert result["rows"] == [[40.5, 3]], result
            result = execute(conn, make_plan(parquet, sums=["id"]))
            assert result["rows"] == [["6"]], result  # DuckDB HUGEINT exports decimal128.
            result = execute(conn, make_plan(parquet, groups=["id"], sums=["amount"], minimum="id=2"))
            assert sorted(result["rows"]) == [[2, None], [3, 30.5]], result
            def concurrent_call(_):
                with connect() as other:
                    return sorted(execute(other, scan)["rows"])
            with ThreadPoolExecutor(max_workers=4) as pool:
                assert all(rows == [[1, 10], [2, None], [3, 30.5]]
                           for rows in pool.map(concurrent_call, range(4)))
            oversized = Path(tmp) / "oversized.parquet"
            pq.write_table(pa.table({"id": range(200000)}), oversized)
            try:
                execute(conn, make_plan(oversized))
            except pymysql.MySQLError as error:
                assert "exceeds the output limit" in str(error), error
            else:
                raise AssertionError("Oversized result was silently accepted/truncated")
            with conn.cursor() as cur:
                cur.execute(f"CREATE USER {identifier(user)} IDENTIFIED BY %s", (password,))
            try:
                # The account has no SUPER and needs no database access to call a scalar.
                import os
                with pymysql.connect(host=os.getenv("SEEKDB_HOST", "127.0.0.1"),
                                     port=int(os.getenv("SEEKDB_PORT", "2881")),
                                     user=user, password=password) as limited:
                    try:
                        execute(limited, scan)
                    except pymysql.MySQLError as error:
                        assert error.args[0] == 1227, error
                    else:
                        raise AssertionError("Non-SUPER account executed a server-file plan")
            finally:
                with conn.cursor() as cur:
                    cur.execute(f"DROP USER {identifier(user)}")
            # Invalid plans must not poison the runtime or its transaction.
            assert execute(conn, scan)["rows"]
            print(json.dumps({"status": "passed", "checks": ["null", "invalid_plans", "snapshot",
                  "repeat_calls", "aggregate", "decimal", "filter_group_null", "concurrent_calls",
                  "result_limit", "super_privilege", "recovery"], "cli_lifecycle": bool(cli)}))
        finally:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE {identifier(table)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shared-directory", required=True,
                        help="Directory visible at the same absolute path to this script and seekdb")
    parser.add_argument("--cli", help="Also test native CLI shutdown; requires bridge runtime environment")
    args = parser.parse_args()
    main(args.shared_directory, args.cli)
