# Copyright (c) 2026 OceanBase. SPDX-License-Identifier: Apache-2.0
import re
import struct
from datetime import date
import pyarrow as pa


class Decoder:
    def __init__(self, data):
        self.data, self.position = memoryview(data), 0

    def integer(self):
        if len(self.data) - self.position < 4:
            raise ValueError("Truncated query protocol integer")
        value = struct.unpack_from("!I", self.data, self.position)[0]
        self.position += 4
        return value

    def text(self, nullable=False):
        size = self.integer()
        if size == 0xFFFFFFFF and nullable:
            return None
        if size > len(self.data) - self.position:
            raise ValueError("Truncated query protocol string")
        value = bytes(self.data[self.position : self.position + size]).decode("utf-8")
        self.position += size
        return value

    def end(self):
        if self.position != len(self.data):
            raise ValueError("Trailing query protocol bytes")

    def buffer(self):
        size = self.integer()
        if size > len(self.data) - self.position:
            raise ValueError("Truncated binary column buffer")
        value = self.data[self.position : self.position + size]
        self.position += size
        return value


def encode_text(value):
    if value is None:
        return struct.pack("!I", 0xFFFFFFFF)
    if isinstance(value, bool):
        value = int(value)
    if isinstance(value, date):
        value = value.isoformat()
    data = str(value).encode("utf-8")
    return struct.pack("!I", len(data)) + data


def arrow_type(format):
    types = {
        "c": pa.int8(),
        "s": pa.int16(),
        "i": pa.int32(),
        "l": pa.int64(),
        "f": pa.float32(),
        "g": pa.float64(),
        "u": pa.string(),
        "tdD": pa.date32(),
    }
    if format in types:
        return types[format]
    match = re.fullmatch(r"d:(\d+),(\d+)", format)
    if match:
        precision, scale = map(int, match.groups())
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return pa.decimal128(precision, scale)
    raise ValueError(f"Unsupported input type {format}")


def decode_binary_batch(payload, schema):
    d = Decoder(payload)
    count = d.integer()
    if not 1 <= count <= 65536:
        raise ValueError("Invalid binary batch row count")
    arrays = []
    for field in schema:
        validity, offsets, values = d.buffer(), d.buffer(), d.buffer()
        if len(validity) != (count + 7) // 8:
            raise ValueError("Invalid validity bitmap length")
        if pa.types.is_string(field.type):
            if len(offsets) != (count + 1) * 4:
                raise ValueError("Invalid string offset length")
            buffers = [
                pa.py_buffer(validity),
                pa.py_buffer(offsets),
                pa.py_buffer(values),
            ]
        else:
            if offsets or len(values) != count * (field.type.bit_width // 8):
                raise ValueError("Invalid fixed-width column length")
            buffers = [pa.py_buffer(validity), pa.py_buffer(values)]
        array = pa.Array.from_buffers(field.type, count, buffers)
        array.validate(full=True)
        arrays.append(array)
    d.end()
    return pa.RecordBatch.from_arrays(arrays, schema=schema)
