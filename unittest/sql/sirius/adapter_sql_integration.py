#!/usr/bin/env python3
"""Compare actual seekdb CPU/GPU SQL results and statement transaction visibility.

Run against a disposable database; creates and drops adapter_check only.
TPC-H mode uses existing tables and never imports/exports data.
"""
import argparse
from decimal import Decimal
import json
import math
from pathlib import Path
import time

import pymysql


def compare(expected, actual):
    assert len(expected) == len(actual), (len(expected), len(actual))
    for i, (left, right) in enumerate(zip(expected, actual)):
        assert len(left) == len(right)
        for j, (a, b) in enumerate(zip(left, right)):
            if isinstance(a, (Decimal, float)) and isinstance(b, (Decimal, float, int)):
                assert math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-6), (
                    i,
                    j,
                    a,
                    b,
                )
            else:
                assert a == b, (i, j, a, b)


def run(cursor, sql, backend, pin=""):
    cursor.execute("SET sirius_snapshot=''")
    cursor.execute(f"SET execution_backend='{backend}'")
    if pin:
        cursor.execute("SET sirius_snapshot=%s", (pin,))
    started = time.perf_counter()
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows, time.perf_counter() - started


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--tpch", action="store_true")
    parser.add_argument(
        "--pin",
        default="",
        help="Capture all eight TPC-H tables together before comparison",
    )
    args = parser.parse_args()
    failures = []
    with pymysql.connect(
        host="127.0.0.1",
        port=args.port,
        user="root",
        database=args.database,
        autocommit=True,
        read_timeout=600,
        charset="utf8mb4",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET ob_query_timeout=600000000")

            def check(name, sql):
                try:
                    expected, cpu_s = run(cursor, sql, "cpu")
                    actual, gpu_s = run(cursor, sql, "gpu", args.pin)
                    compare(expected, actual)
                    print(
                        json.dumps(
                            dict(
                                test=name,
                                status="match",
                                rows=len(actual),
                                cpu_s=cpu_s,
                                gpu_s=gpu_s,
                            )
                        ),
                        flush=True,
                    )
                except Exception as error:
                    failures.append(name)
                    print(
                        json.dumps(dict(test=name, status="failed", error=str(error))),
                        flush=True,
                    )

            if args.tpch:
                if args.pin:
                    names = [
                        "region",
                        "nation",
                        "supplier",
                        "customer",
                        "part",
                        "partsupp",
                        "orders",
                        "lineitem",
                    ]
                    check(
                        "pin_capture",
                        "SELECT "
                        + ",".join(f"(SELECT COUNT(*) FROM {n})" for n in names),
                    )
                for file in sorted(
                    (Path(__file__).parent / "tpch_queries").glob("q*.sql")
                ):
                    check(file.stem, file.read_text())
            else:
                cursor.execute(
                    "CREATE TABLE adapter_check (id BIGINT PRIMARY KEY, grp BIGINT, v DECIMAL(15,2), label VARCHAR(40), day DATE) CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
                )
                try:
                    cursor.execute(
                        "INSERT INTO adapter_check VALUES (1,1,12.34,'中文','1960-01-01'),(2,1,NULL,'','2000-02-29'),(3,2,-0.01,NULL,NULL)"
                    )
                    for n, sql in enumerate(
                        [
                            "SELECT * FROM adapter_check ORDER BY id",
                            "SELECT grp,SUM(v),COUNT(*),COUNT(v) FROM adapter_check GROUP BY grp ORDER BY grp",
                            "SELECT x.id,y.id FROM adapter_check x LEFT JOIN adapter_check y ON x.id=y.grp ORDER BY x.id,y.id",
                            "SELECT id FROM adapter_check a WHERE NOT EXISTS (SELECT 1 FROM adapter_check b WHERE b.grp=a.id) ORDER BY id",
                            "SELECT COUNT(*),SUM(v) FROM adapter_check WHERE id<0",
                            "SELECT id FROM adapter_check ORDER BY id LIMIT 1",
                            "SELECT id,v FROM adapter_check ORDER BY v,id",
                            "SELECT id,v FROM adapter_check ORDER BY v DESC,id",
                        ]
                    ):
                        check(f"sql_{n}", sql)
                    cursor.execute("SET execution_backend='cpu'")
                    connection.begin()
                    cursor.execute(
                        "INSERT INTO adapter_check VALUES (4,3,91.25,'own write','2026-11-02')"
                    )
                    check(
                        "transaction_own_write",
                        "SELECT id,v FROM adapter_check ORDER BY id",
                    )
                    connection.rollback()
                    check("rollback", "SELECT id,v FROM adapter_check ORDER BY id")
                    cursor.execute(
                        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                    )
                    connection.begin()
                    before, _ = run(cursor, "SELECT COUNT(*) FROM adapter_check", "cpu")
                    with pymysql.connect(
                        host="127.0.0.1",
                        port=args.port,
                        user="root",
                        database=args.database,
                        autocommit=True,
                    ) as other:
                        with other.cursor() as writer:
                            writer.execute(
                                "INSERT INTO adapter_check VALUES (5,3,7,'concurrent',NULL)"
                            )
                    after, _ = run(cursor, "SELECT COUNT(*) FROM adapter_check", "gpu")
                    compare(before, after)
                    print(
                        json.dumps(
                            dict(
                                test="repeatable_read_concurrent_commit", status="match"
                            )
                        ),
                        flush=True,
                    )
                    connection.rollback()
                    check(
                        "committed_visibility",
                        "SELECT id,v FROM adapter_check ORDER BY id",
                    )
                finally:
                    connection.rollback()
                    cursor.execute("SET execution_backend='cpu'")
                    cursor.execute("DROP TABLE adapter_check")
    if failures:
        raise SystemExit("Failed: " + ",".join(failures))


if __name__ == "__main__":
    main()
