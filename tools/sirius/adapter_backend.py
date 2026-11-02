"""Bounded multi-input C ABI and schema-only Substrait frontend."""

import ctypes as ct
import re
import struct
import subprocess
from dataclasses import dataclass

import pyarrow as pa
from substrait import plan_pb2

Cancel = ct.CFUNCTYPE(None, ct.c_void_p)


class Input(ct.Structure):
    _fields_ = [
        ("struct_size", ct.c_uint32),
        ("id", ct.c_uint64),
        ("stream", ct.c_size_t),
        ("snapshot_identity", ct.c_char_p),
        ("estimated_rows", ct.c_uint64),
        ("max_batch_rows", ct.c_uint64),
        ("max_batch_bytes", ct.c_uint64),
        ("cancel_state", ct.c_void_p),
        ("cancel", Cancel),
    ]


@dataclass(frozen=True)
class Scan:
    id: int
    table_index: int
    schema: pa.Schema


def sql_type(t):
    names = {
        pa.int8(): "TINYINT",
        pa.int16(): "SMALLINT",
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.float32(): "FLOAT",
        pa.float64(): "DOUBLE",
        pa.string(): "VARCHAR",
        pa.date32(): "DATE",
        pa.bool_(): "BOOLEAN",
    }
    if t in names:
        return names[t]
    if pa.types.is_decimal128(t) and 0 <= t.scale <= t.precision <= 38:
        return f"DECIMAL({t.precision},{t.scale})"
    raise ValueError(f"Unsupported host type {t}")


def compile_plan(compiler, sql, tables):
    """Compile metadata only; rewrite every scan occurrence to its own input."""
    # DuckDB's producer encodes FIRST as Substrait any_value. Restore its
    # original null-preserving semantics; explicit ANY_VALUE is ambiguous in
    # this provisional frontend and must not silently become FIRST.
    if re.search(r"\bany_value\s*\(", sql, re.IGNORECASE):
        raise ValueError(
            "Explicit ANY_VALUE is unsupported by this schema-only frontend"
        )

    def quote(name):
        return '"' + name.replace('"', '""') + '"'

    ddl = "".join(
        f"CREATE TABLE {quote(name)} ("
        + ",".join(f"{quote(f.name)} {sql_type(f.type)}" for f in schema)
        + ");"
        for name, schema in tables.items()
    )
    packet = b"".join(
        struct.pack("<I", len(b)) + b for b in (ddl.encode(), sql.encode())
    )
    result = subprocess.run([compiler], input=packet, capture_output=True, timeout=120)
    if result.returncode:
        raise ValueError(
            "Substrait compilation failed: "
            + result.stderr.decode(errors="replace")[-4096:]
        )
    plan = plan_pb2.Plan.FromString(result.stdout)
    for extension in plan.extensions:
        if extension.HasField("extension_function"):
            function = extension.extension_function
            name, separator, signature = function.name.partition(":")
            if name == "any_value":
                function.name = "first" + separator + signature
    names = list(tables)
    scans = []

    def visit(message):
        if message.DESCRIPTOR.full_name == "substrait.ReadRel":
            if not message.HasField("named_table"):
                raise ValueError("Only seekdb named-table inputs are allowed")
            path = list(message.named_table.names)
            if not path or path[-1] not in tables:
                raise ValueError("Plan reads an undeclared seekdb table")
            name = path[-1]
            scan = Scan(len(scans), names.index(name), tables[name])
            scans.append(scan)
            del message.named_table.names[:]
            message.named_table.names.append(f"sirius_stream_{scan.id}")
        for descriptor, value in message.ListFields():
            if descriptor.type != descriptor.TYPE_MESSAGE:
                continue
            if descriptor.is_repeated:
                for item in value:
                    visit(item)
            else:
                visit(value)

    visit(plan)
    if not 1 <= len(scans) <= 64:
        raise ValueError(
            "Expected 1..64 host scan occurrences; empty-table rewrites refused"
        )
    return plan.SerializeToString(), scans


class Backend:
    def __init__(self, library):
        self.library = ct.CDLL(str(library))
        self.execute_inputs = self.library.seekdb_sirius_execute_inputs_v1
        self.execute_inputs.argtypes = [
            ct.c_void_p,
            ct.c_size_t,
            ct.POINTER(Input),
            ct.c_size_t,
            ct.c_size_t,
            ct.c_void_p,
            ct.c_size_t,
        ]
        self.execute_inputs.restype = ct.c_int
        self.shutdown = self.library.seekdb_sirius_adapter_shutdown_v1
        self.shutdown.argtypes = [ct.c_void_p, ct.c_size_t]
        self.shutdown.restype = ct.c_int

    def execute(self, plan, scans, snapshot, batches, cancel):
        streams, readers, callbacks = [], [], []
        declarations = (Input * len(scans))()
        identity = snapshot.encode()
        output = (ct.c_void_p * 5)()
        try:
            for i, scan in enumerate(scans):
                stream = (ct.c_void_p * 5)()
                reader = pa.RecordBatchReader.from_batches(scan.schema, batches(scan))
                readers.append(reader)
                reader._export_to_c(ct.addressof(stream))
                streams.append(stream)
                callback = Cancel(lambda _, scan=scan: cancel(scan.id))
                callbacks.append(callback)
                declarations[i] = Input(
                    ct.sizeof(Input),
                    scan.id,
                    ct.addressof(stream),
                    identity,
                    2**64 - 1,
                    65536,
                    32 * 1024**2,
                    None,
                    callback,
                )
            error = ct.create_string_buffer(4096)
            status = self.execute_inputs(
                plan,
                len(plan),
                declarations,
                len(scans),
                ct.addressof(output),
                error,
                len(error),
            )
            if status:
                raise RuntimeError(error.value.decode(errors="replace"))
            with pa.RecordBatchReader._import_from_c(ct.addressof(output)) as result:
                return result.read_all()
        finally:
            # On preflight failure C leaves source streams untouched. Importing
            # any still-owned stream releases it without advancing its iterator.
            for stream in streams:
                if stream[3]:
                    pa.RecordBatchReader._import_from_c(ct.addressof(stream)).close()
            for reader in readers:
                reader.close()

    def close(self):
        error = ct.create_string_buffer(4096)
        if self.shutdown(error, len(error)):
            raise RuntimeError(error.value.decode(errors="replace"))
