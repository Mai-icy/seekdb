"""Bounded immutable, page-locked host snapshots. No persistent data files.

Buffers use cudaHostAllocPortable and remain owned by Arrow views. They are
released only after every reader drops its reference. This is host pin reuse;
Sirius still performs its admitted staging copy and GPU upload on each query.
"""

import ctypes as ct
from dataclasses import dataclass
import threading
import warnings

import pyarrow as pa


class PinnedAllocator:
    def __init__(self, capacity, library="libcudart.so.12"):
        if capacity <= 0:
            raise ValueError("Pin budget must be positive")
        self.capacity = capacity
        self.used = 0
        self.lock = threading.Lock()
        self.cuda = ct.CDLL(library)
        self.cuda.cudaHostAlloc.argtypes = [
            ct.POINTER(ct.c_void_p),
            ct.c_size_t,
            ct.c_uint,
        ]
        self.cuda.cudaHostAlloc.restype = ct.c_int
        self.cuda.cudaFreeHost.argtypes = [ct.c_void_p]
        self.cuda.cudaFreeHost.restype = ct.c_int

    def copy(self, payload):
        charge = (len(payload) + 4095) // 4096 * 4096
        with self.lock:
            if charge > self.capacity - self.used:
                raise MemoryError(
                    "Immutable host pin budget exhausted; existing pins are preserved"
                )
            self.used += charge
        owner = Allocation(self, charge)
        status = self.cuda.cudaHostAlloc(ct.byref(owner.pointer), charge, 1)
        if status:
            owner.release()
            raise MemoryError(f"cudaHostAlloc failed with status {status}")
        try:
            # Arrow accepts mutable wire frames and readonly codec output
            # without another pageable intermediate allocation.
            source = pa.py_buffer(payload)
            ct.memmove(owner.pointer, source.address, len(payload))
            return pa.foreign_buffer(owner.pointer.value, len(payload), base=owner)
        except BaseException:
            owner.release()
            raise


class Allocation:
    def __init__(self, allocator, charge):
        self.allocator, self.charge = allocator, charge
        self.pointer = ct.c_void_p()

    def release(self):
        if not self.charge:
            return
        if self.pointer.value:
            status = self.allocator.cuda.cudaFreeHost(self.pointer)
            if status:
                # Retain the budget charge if CUDA cannot confirm reclamation.
                warnings.warn(
                    f"cudaFreeHost failed with status {status}; pin charge retained"
                )
                self.charge = 0
                return
            self.pointer = ct.c_void_p()
        with self.allocator.lock:
            self.allocator.used -= self.charge
        self.charge = 0

    def __del__(self):
        self.release()


@dataclass
class Snapshot:
    identity: str
    tables: dict
    table_ids: dict
    batches: dict

    def validate(self, tables, table_ids):
        for name, schema in tables.items():
            if name not in self.tables:
                raise ValueError(
                    f"Pin does not contain {name}; use a new pin capturing all required tables together"
                )
            if self.table_ids[name] != table_ids[name] or not self.tables[name].equals(
                schema
            ):
                raise ValueError(
                    f"Pinned schema identity changed for {name}; use a new pin"
                )


@dataclass
class CompressedBatch:
    data: pa.Buffer
    raw_size: int
    schema: pa.Schema
    codec: str = "zstd"

    def decode(self):
        from adapter_protocol import decode_binary_batch

        raw = pa.decompress(
            self.data, decompressed_size=self.raw_size, codec=self.codec
        )
        return decode_binary_batch(raw, self.schema)


class PinCache:
    def __init__(self, capacity, max_pins=16, codec="none"):
        if codec not in ("none", "zstd"):
            raise ValueError("Unsupported pin codec")
        self.codec = codec
        self.capacity, self.max_pins = capacity, max_pins
        self.snapshots = {}
        self.allocator = None

    def prepare(self, key):
        if key in self.snapshots:
            return self.snapshots[key]
        if len(self.snapshots) >= self.max_pins:
            raise MemoryError("Maximum immutable snapshot count reached")
        if self.capacity <= 0:
            raise ValueError("Host pin is disabled; set --pin-bytes to enable it")
        if self.allocator is None:
            self.allocator = PinnedAllocator(self.capacity)
        return None

    def clear(self):
        self.snapshots.clear()
