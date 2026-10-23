#!/usr/bin/env python3
"""Exercise live Arrow input and seekdb snapshot visibility on a GPU sidecar."""
import argparse
import json
from pathlib import Path
import secrets
import sys

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from connection import connect as database, identifier
from live_query import execute_batches, query
from make_plan import make_plan_for_schema
from wire import connect, expect, send


def main(path):
    checks = []
    schema = pa.schema([("id", pa.int64()), ("amount", pa.float64())])
    batch = pa.record_batch([[1, 2, 3], [10., None, 30.5]], schema=schema)
    aggregate = make_plan_for_schema(schema, sums=["amount"], count=True)
    def run(plan=aggregate, batches=(batch,), selected_schema=schema):
        return execute_batches(path, plan, selected_schema, batches)["rows"]
    assert run() == [[40.5, 3]]
    assert run(batches=[]) == [[None, 0]]
    assert run(batches=[batch.slice(0, 0)]) == [[None, 0]]
    assert run(make_plan_for_schema(schema)) == [[1, 10], [2, None], [3, 30.5]]
    assert run(make_plan_for_schema(schema), []) == []
    assert run(make_plan_for_schema(schema, groups=["id"], sums=["amount"]), []) == []
    assert run(make_plan_for_schema(schema, sums=["id"])) == [["6"]]
    assert run(make_plan_for_schema(schema, sums=["amount"], count=True, minimum="id=99")) == [[None, 0]]
    assert run(batches=[pa.record_batch([[1, 2], [None, None]], schema=schema)]) == [[None, 2]]
    checks.extend(["sum_count_null", "empty_global", "zero_length_batch", "scan", "empty_scan",
                   "empty_group", "decimal_sum", "filter_all_rows", "all_null_sum"])
    # Multiple GPU input batches, including a sliced Arrow array with an offset.
    large = pa.record_batch([range(50000), [1.] * 50000], schema=schema)
    assert run(batches=[large, large, batch.slice(1)]) == [[100030.5, 100002]]
    checks.append("multiple_batches_and_offsets")
    for plan, selected_schema in ((b"\xff", schema), (aggregate, pa.schema([("text", pa.string())]))):
        try:
            run(plan, [], selected_schema)
        except RuntimeError:
            pass
        else:
            raise AssertionError("Unsupported request was accepted")
        assert run() == [[40.5, 3]]
    checks.append("invalid_plan_schema_recovery")
    for payload in (None, b"malformed Arrow IPC"):
        with connect(path) as sock:
            send(sock, b"Q", aggregate)
            send(sock, b"S", schema.serialize().to_pybytes())
            expect(sock, b"A")
            if payload is not None:
                send(sock, b"B", payload)
                try:
                    expect(sock, b"A")
                except RuntimeError:
                    pass
                else:
                    raise AssertionError("Malformed batch accepted")
            # No E frame: tests input-stream teardown after client disconnect.
        assert run() == [[40.5, 3]]
    checks.append("malformed_batch_disconnect_recovery")
    table = "sirius_live_" + secrets.token_hex(6)
    name = identifier(table)
    with database() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, amount DOUBLE)")
        try:
            def live():
                return query(table, "id:int64,amount:float64", path, sums=["amount"], count=True)["result"]["rows"]
            assert live() == [[None, 0]]
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {name} VALUES (1,10),(2,NULL),(3,30.5)")
            assert live() == [[40.5, 3]]
            with database() as writer:
                writer.begin()
                with writer.cursor() as cursor:
                    cursor.execute(f"UPDATE {name} SET amount=100 WHERE id=1")
                assert live() == [[40.5, 3]]
                writer.commit()
            assert live() == [[130.5, 3]]
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {name} WHERE id=3")
            assert live() == [[100, 2]]
            checks.extend(["live_empty", "live_insert", "uncommitted_invisible", "committed_update", "delete_visible"])
            # Read one statement while a separate connection commits a change
            # between transmitted batches. Its result keeps the original view.
            with database(stream=True) as reader, reader.cursor() as cursor:
                cursor.execute(f"SELECT id,amount FROM {name} ORDER BY id")
                def batches():
                    first = cursor.fetchmany(1)
                    yield pa.record_batch([[first[0][0]], [first[0][1]]], schema=schema)
                    with conn.cursor() as writer:
                        writer.execute(f"UPDATE {name} SET amount=25 WHERE id=2")
                    rest = cursor.fetchall()
                    yield pa.record_batch([[r[0] for r in rest], [r[1] for r in rest]], schema=schema)
                assert run(batches=batches()) == [[100, 2]]
            assert live() == [[125, 2]]
            checks.append("statement_snapshot_across_batches")
        finally:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE {name}")
    print(json.dumps({"status": "passed", "checks": checks}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    main(parser.parse_args().socket)
