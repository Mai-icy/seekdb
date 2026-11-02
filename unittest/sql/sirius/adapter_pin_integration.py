#!/usr/bin/env python3
"""GPU host-pin ownership, immutable SQL reuse, schema refusal and live recovery."""
import argparse
import ctypes as ct
import gc
import json
from pathlib import Path
import sys
import uuid

import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from adapter_pin import PinnedAllocator
from adapter_sql_integration import run, compare


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--database", required=True)
    args = parser.parse_args()
    allocator = PinnedAllocator(8192)
    data = allocator.copy(bytearray(b"abc" + b"x" * 5000))
    flags = ct.c_uint()
    allocator.cuda.cudaHostGetFlags.argtypes = [ct.POINTER(ct.c_uint), ct.c_void_p]
    assert allocator.cuda.cudaHostGetFlags(ct.byref(flags), data.address) == 0
    assert flags.value & 1
    view = data.slice(0, 3)
    del data
    gc.collect()
    assert view.to_pybytes() == b"abc" and allocator.used == 8192
    try:
        allocator.copy(bytearray(b"overflow"))
    except MemoryError:
        pass
    else:
        raise AssertionError("Pin hard budget was bypassed")
    del view
    gc.collect()
    assert allocator.used == 0
    print(
        json.dumps(dict(test="pinned_backing_lifetime_and_budget", status="passed")),
        flush=True,
    )
    pin = "pin_" + uuid.uuid4().hex
    with pymysql.connect(
        host="127.0.0.1",
        port=args.port,
        user="root",
        database=args.database,
        autocommit=True,
        read_timeout=120,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE adapter_pin_check(id BIGINT PRIMARY KEY,v DECIMAL(15,2))"
            )
            cursor.execute("CREATE TABLE adapter_pin_other(id BIGINT)")
            try:
                cursor.execute(
                    "INSERT INTO adapter_pin_check VALUES(1,1.25),(2,NULL),(3,-0.01)"
                )
                cursor.execute("INSERT INTO adapter_pin_other VALUES(8)")
                old, _ = run(
                    cursor, "SELECT * FROM adapter_pin_check ORDER BY id", "cpu"
                )
                # LIMIT must not publish a partially captured table.
                result, _ = run(
                    cursor,
                    "SELECT id FROM adapter_pin_check ORDER BY id LIMIT 1",
                    "gpu",
                    pin,
                )
                assert result == ((1,),)
                cursor.execute("SET sirius_snapshot=''")
                cursor.execute("SET execution_backend='cpu'")
                cursor.execute("UPDATE adapter_pin_check SET v=99 WHERE id=1")
                cursor.execute("DELETE FROM adapter_pin_check WHERE id=2")
                cursor.execute("INSERT INTO adapter_pin_check VALUES(4,4)")
                cached, _ = run(
                    cursor, "SELECT * FROM adapter_pin_check ORDER BY id", "gpu", pin
                )
                compare(old, cached)
                cpu, _ = run(
                    cursor, "SELECT * FROM adapter_pin_check ORDER BY id", "cpu"
                )
                live, _ = run(
                    cursor, "SELECT * FROM adapter_pin_check ORDER BY id", "gpu"
                )
                compare(cpu, live)
                assert live != old
                print(
                    json.dumps(
                        dict(test="immutable_pin_and_live_freshness", status="passed")
                    ),
                    flush=True,
                )

                def refused(sql):
                    try:
                        run(cursor, sql, "gpu", pin)
                    except pymysql.Error as error:
                        assert "Pin" in str(error) or "pin" in str(error), error
                    else:
                        raise AssertionError("Incompatible pin was reused")

                refused("SELECT COUNT(*) FROM adapter_pin_other")
                cursor.execute("SET sirius_snapshot=''")
                cursor.execute("SET execution_backend='cpu'")
                cursor.execute("ALTER TABLE adapter_pin_check ADD COLUMN extra BIGINT")
                refused("SELECT id FROM adapter_pin_check")
                live, _ = run(cursor, "SELECT COUNT(*) FROM adapter_pin_check", "gpu")
                assert live == ((3,),)
                print(
                    json.dumps(
                        dict(
                            test="missing_table_schema_change_refused_and_recovery",
                            status="passed",
                        )
                    ),
                    flush=True,
                )
            finally:
                cursor.execute("SET sirius_snapshot=''")
                cursor.execute("SET execution_backend='cpu'")
                cursor.execute("DROP TABLE adapter_pin_check,adapter_pin_other")


if __name__ == "__main__":
    main()
