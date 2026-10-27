#!/usr/bin/env python3
"""Experimental whole-query GPU bridge over a seekdb repeatable-read snapshot.

Explicit table allowlist; no persistent DuckDB copy. The current FFI preloads
at most 512 MiB before running, so use small datasets until scan streaming lands.
SQL is planned with DuckDB semantics (not a general MySQL compatibility layer).
"""
import argparse
import ctypes as ct
from contextlib import contextmanager
import json
from pathlib import Path
import re
import time

import pyarrow as pa
import pymysql
from connection import connect, identifier


class Stream(ct.Structure):
    _fields_ = [(name, ct.c_void_p) for name in
                ("get_schema", "get_next", "get_last_error", "release", "private_data")]


class TableInput(ct.Structure):
    _fields_ = [("name", ct.c_char_p), ("stream", ct.POINTER(Stream)),
                ("estimated_rows", ct.c_uint64)]


Consumer = ct.CFUNCTYPE(ct.c_int, ct.POINTER(Stream), ct.c_void_p)


class QueryBackend:
    def __init__(self, library):
        if not Path(library).is_absolute():
            raise ValueError("Bridge library must be an absolute path")
        self.library = ct.CDLL(str(library))
        self.execute = self.library.seekdb_sirius_execute_query_v1
        self.execute.argtypes = [ct.c_char_p, ct.c_size_t, ct.POINTER(TableInput),
                                ct.c_size_t, Consumer, ct.c_void_p, ct.c_void_p, ct.c_size_t]
        self.execute.restype = ct.c_int

    def pin_snapshot(self, name, path):
        fn = self.library.seekdb_sirius_pin_snapshot_v1
        fn.argtypes = [ct.c_char_p, ct.c_char_p, ct.c_void_p, ct.c_size_t]
        fn.restype = ct.c_int
        error = ct.create_string_buffer(4096)
        if fn(name.encode(), str(path).encode(), error, len(error)):
            raise RuntimeError(error.value.decode(errors="replace"))

    def query_snapshot(self, sql):
        fn = self.library.seekdb_sirius_query_snapshot_v1
        fn.argtypes = [ct.c_char_p, ct.c_size_t, Consumer, ct.c_void_p, ct.c_void_p, ct.c_size_t]
        fn.restype = ct.c_int
        result, errors = [], []

        @Consumer
        def consume(pointer, _):
            try:
                with pa.RecordBatchReader._import_from_c(ct.addressof(pointer.contents)) as reader:
                    result.append(reader.read_all())
                return 0
            except BaseException as error:
                errors.append(error)
                return 1

        error = ct.create_string_buffer(4096)
        encoded = sql.encode()
        status = fn(encoded, len(encoded), consume, None, error, len(error))
        if errors:
            raise errors[0]
        if status:
            raise RuntimeError(error.value.decode(errors="replace"))
        return result[0]

    def query(self, sql, inputs):
        """inputs: {name: (RecordBatchReader, estimated_rows)}; consumes readers."""
        if not 1 <= len(inputs) <= 64:
            raise ValueError("Expected 1..64 input tables")
        encoded = sql.encode()
        if not 0 < len(encoded) <= 1024 * 1024:
            raise ValueError("Expected 1..1 MiB of SQL")
        streams = [Stream() for _ in inputs]
        descriptors = (TableInput * len(inputs))()
        result, errors = [], []

        @Consumer
        def consume(pointer, _):
            try:
                with pa.RecordBatchReader._import_from_c(ct.addressof(pointer.contents)) as reader:
                    result.append(reader.read_all())
                return 0
            except BaseException as error:
                errors.append(error)
                return 1

        try:
            for i, (name, (reader, count)) in enumerate(inputs.items()):
                if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name) or count < 0 or count >= 2**64:
                    raise ValueError("Invalid input name/cardinality")
                reader._export_to_c(ct.addressof(streams[i]))
                descriptors[i] = TableInput(name.encode(), ct.pointer(streams[i]), count)
            error = ct.create_string_buffer(4096)
            status = self.execute(encoded, len(encoded), descriptors, len(inputs), consume,
                                  None, error, len(error))
            if errors:
                raise errors[0]
            if status:
                raise RuntimeError(error.value.decode(errors="replace") or f"Bridge status {status}")
            return result[0]
        finally:
            for stream in streams:
                if stream.release:
                    ct.CFUNCTYPE(None, ct.POINTER(Stream))(stream.release)(ct.byref(stream))


def sql_type(data_type, column_type, precision, scale):
    if "unsigned" in column_type.lower():
        raise ValueError("Unsigned input is not supported yet")
    integers = {"tinyint": pa.int8(), "smallint": pa.int16(), "mediumint": pa.int32(),
                "int": pa.int32(), "bigint": pa.int64()}
    if data_type in integers:
        return integers[data_type]
    if data_type in ("decimal", "numeric"):
        return pa.decimal128(int(precision), int(scale))
    if data_type == "date":
        return pa.date32()
    if data_type in ("char", "varchar", "text"):
        return pa.string()
    if data_type in ("float", "double"):
        return pa.float32() if data_type == "float" else pa.float64()
    raise ValueError(f"Unsupported seekdb input type: {column_type}")


@contextmanager
def snapshot(connection):
    """Own a transaction on a dedicated connection, with one snapshot for all tables."""
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    try:
        yield
    finally:
        connection.rollback()


def snapshot_inputs(connection, tables, batch_rows=65536):
    if not 1 <= batch_rows <= 65536:
        raise ValueError("batch_rows must be in 1..65536")
    if len(tables) != len(set(tables)):
        raise ValueError("Duplicate input table")
    inputs = {}
    # Prepare all schemas/counts before starting any unbuffered data cursor.
    for table in tables:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", table):
            raise ValueError("Invalid table name")
        with connection.cursor() as cursor:
            cursor.execute("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, "
                           "NUMERIC_SCALE, IS_NULLABLE FROM information_schema.COLUMNS "
                           "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION", (table,))
            columns = cursor.fetchall()
            if not columns:
                raise ValueError(f"Missing input table {table}")
            schema = pa.schema([pa.field(name, sql_type(kind, detail, precision, scale),
                                         nullable=nullable == "YES")
                                for name, kind, detail, precision, scale, nullable in columns])
            cursor.execute(f"SELECT COUNT(*) FROM {identifier(table)}")
            count = cursor.fetchone()[0]

        def batches(table=table, schema=schema):
            # This generator is drained sequentially on the bridge worker.
            with connection.cursor(pymysql.cursors.SSCursor) as cursor:
                cursor.execute("SELECT " + ",".join(identifier(n) for n in schema.names)
                               + " FROM " + identifier(table))
                while rows := cursor.fetchmany(batch_rows):
                    yield pa.RecordBatch.from_arrays([
                        pa.array([row[i] for row in rows], type=field.type)
                        for i, field in enumerate(schema)], schema=schema)

        inputs[table] = (pa.RecordBatchReader.from_batches(schema, batches()), count)
    return inputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument("--sql", type=Path, required=True)
    parser.add_argument("--tables", nargs="+", required=True)
    parser.add_argument("--batch-rows", type=int, default=65536)
    args = parser.parse_args()
    backend = QueryBackend(args.library)
    connection = connect()
    try:
        started = time.perf_counter()
        with snapshot(connection):
            inputs = snapshot_inputs(connection, args.tables, args.batch_rows)
            result = backend.query(args.sql.read_text(), inputs)
        print(json.dumps({"seconds_including_snapshot_input": time.perf_counter() - started,
                          "schema": str(result.schema), "rows": result.to_pylist()}, default=str))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
