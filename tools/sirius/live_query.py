#!/usr/bin/env python3
"""Read one live seekdb statement snapshot and compute on Sirius without Parquet."""
import argparse
import json
import os
import time

import pyarrow as pa
from connection import connect as connect_db, identifier
from make_plan import make_plan_for_schema
from wire import connect, expect, send


def execute_batches(path, plan, schema, batches):
    with connect(path) as sock:
        send(sock, b"Q", plan)
        send(sock, b"S", schema.serialize().to_pybytes())
        expect(sock, b"A")
        for batch in batches:
            send(sock, b"B", batch.serialize().to_pybytes())
            expect(sock, b"A")
        send(sock, b"E")
        return json.loads(expect(sock, b"J", 1024 * 1024))


def query(table, columns, socket_path, groups=(), sums=(), count=False, minimum=None):
    fields = [item.split(":", 1) for item in columns.split(",")]
    schema = pa.schema([(name, pa.type_for_alias(dtype)) for name, dtype in fields])
    if any(f.type not in (pa.int64(), pa.float64()) for f in schema):
        raise ValueError("Live input supports int64/float64 only")
    plan = make_plan_for_schema(schema, groups, sums, count, minimum)
    started, rows, batches_count = time.perf_counter(), 0, 0
    with connect_db(stream=True) as conn, conn.cursor() as cursor:
        # Exactly one SELECT: seekdb owns MVCC visibility and access control.
        # Each invocation rereads the table; there is no reusable stale snapshot.
        cursor.execute("SELECT " + ",".join(identifier(f.name) for f in schema)
                       + " FROM " + identifier(table))
        def batches():
            nonlocal rows, batches_count
            batch_size = min(65536, (8 * 1024 * 1024) // (len(schema) * 9))
            while records := cursor.fetchmany(batch_size):
                arrays = [pa.array([row[i] for row in records], type=f.type)
                          for i, f in enumerate(schema)]
                rows += len(records)
                batches_count += 1
                yield pa.RecordBatch.from_arrays(arrays, schema=schema)
        result = execute_batches(socket_path, plan, schema, batches())
    return {"result": result, "input_rows": rows, "input_batches": batches_count,
            "end_to_end_seconds": time.perf_counter() - started}


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("table")
    p.add_argument("--columns", required=True)
    p.add_argument("--socket", default=os.getenv("SEEKDB_SIRIUS_SOCKET"), required="SEEKDB_SIRIUS_SOCKET" not in os.environ)
    p.add_argument("--group-by", action="append", default=[])
    p.add_argument("--sum", action="append", default=[], dest="sums")
    p.add_argument("--count", action="store_true")
    p.add_argument("--minimum")
    args = p.parse_args()
    print(json.dumps(query(args.table, args.columns, args.socket, args.group_by, args.sums,
                           args.count, args.minimum)))
