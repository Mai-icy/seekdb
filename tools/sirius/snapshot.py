#!/usr/bin/env python3
"""Export selected seekdb columns to an immutable Parquet snapshot."""
import argparse
import json
import os
from pathlib import Path
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq
from connection import connect, identifier


def snapshot(table, columns, output, batch_size=65536, row_group_size=1048576):
    # Explicit Arrow types preserve null-only batches and avoid type inference
    # changing across batches. This also makes signed/unsigned choices visible.
    fields = [part.split(":", 1) for part in columns.split(",")]
    schema = pa.schema([(name, pa.type_for_alias(dtype)) for name, dtype in fields])
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError("Use a new snapshot path; existing plans can cache this file")
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=output.name + ".", dir=output.parent)
    os.close(fd)
    started = time.perf_counter()
    rows = 0
    try:
        with connect(stream=True) as conn:
            # One SELECT supplies one statement snapshot. Changes after that
            # statement starts are intentionally absent from this Parquet file.
            with conn.cursor() as cursor, pq.ParquetWriter(temporary, schema, compression="snappy") as writer:
                cursor.execute("SELECT " + ",".join(identifier(f.name) for f in schema)
                               + " FROM " + identifier(table))
                buffered, buffered_rows = [], 0
                while batch := cursor.fetchmany(batch_size):
                    arrays = [pa.array([row[i] for row in batch], type=f.type)
                              for i, f in enumerate(schema)]
                    buffered.append(pa.RecordBatch.from_arrays(arrays, schema=schema))
                    buffered_rows += len(batch)
                    rows += len(batch)
                    # Larger Parquet row groups avoid launching many tiny GPU
                    # scan tasks. Keep Python row batches small, accumulating
                    # only compact Arrow buffers up to this bounded threshold.
                    if buffered_rows >= row_group_size:
                        writer.write_table(pa.Table.from_batches(buffered), row_group_size=row_group_size)
                        buffered, buffered_rows = [], 0
                if buffered:
                    writer.write_table(pa.Table.from_batches(buffered), row_group_size=row_group_size)
        # link fails if another exporter created the target in the meantime.
        os.link(temporary, output)
    finally:
        os.unlink(temporary)
    return {"path": str(output), "table": table, "rows": rows, "row_group_size": row_group_size,
            "seconds": time.perf_counter() - started, "bytes": output.stat().st_size}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table")
    parser.add_argument("output")
    parser.add_argument("--columns", required=True, help="NAME:ARROW_TYPE,... e.g. id:int64,amount:float64")
    args = parser.parse_args()
    print(json.dumps(snapshot(args.table, args.columns, args.output)))
