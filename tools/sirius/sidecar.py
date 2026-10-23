#!/usr/bin/env python3
"""Single-worker local Sirius process with bounded Arrow IPC input."""
import argparse
import ctypes as ct
import json
import math
import os
from pathlib import Path
import signal
import socket
import stat
import struct
import time

import pyarrow as pa
from wire import receive, send
from make_plan import make_plan_for_schema

EXECUTE = ct.CFUNCTYPE(ct.c_int, ct.c_void_p, ct.c_size_t, ct.c_void_p, ct.c_size_t,
                      ct.POINTER(ct.c_size_t), ct.c_void_p, ct.c_size_t)


class Api(ct.Structure):
    _fields_ = [("version", ct.c_uint32), ("size", ct.c_uint32), ("execute", EXECUTE)]


class PinnedInput:
    """One process-lifetime CUDA host allocation, reused only after GPU copy ACK."""
    def __init__(self, mib=16):
        self.runtime = ct.CDLL("libcudart.so")
        self.runtime.cudaHostAlloc.argtypes = [ct.POINTER(ct.c_void_p), ct.c_size_t, ct.c_uint]
        self.runtime.cudaHostAlloc.restype = ct.c_int
        self.runtime.cudaFreeHost.argtypes = [ct.c_void_p]
        self.runtime.cudaFreeHost.restype = ct.c_int
        self.pointer = ct.c_void_p()
        self.size = mib * 1024 * 1024
        status = self.runtime.cudaHostAlloc(ct.byref(self.pointer), self.size, 0)
        if status:
            raise RuntimeError(f"cudaHostAlloc failed with status {status}")
        self.array = (ct.c_ubyte * self.size).from_address(self.pointer.value)
        self.view = memoryview(self.array).cast("B")

    def close(self):
        if self.pointer.value:
            # serve() has completed and all per-query Arrow streams were released.
            self.view.release()
            status = self.runtime.cudaFreeHost(self.pointer)
            self.pointer.value = None
            if status:
                print(f"warning: cudaFreeHost status={status}", flush=True)


class Backend:
    def __init__(self, library):
        if not Path(library).is_absolute():
            raise ValueError("Backend library path must be absolute")
        self.library = ct.CDLL(library)
        get_api = self.library.seekdb_sirius_get_api_v1
        get_api.restype = ct.POINTER(Api)
        self.api = get_api().contents
        if self.api.version != 1 or self.api.size < ct.sizeof(Api):
            raise ValueError("Unsupported backend ABI")
        self.stream = self.library.seekdb_sirius_execute_stream_v1
        self.stream.argtypes = [ct.c_void_p, ct.c_size_t, ct.c_size_t, ct.c_void_p,
                               ct.c_size_t, ct.POINTER(ct.c_size_t), ct.c_void_p, ct.c_size_t]
        self.stream.restype = ct.c_int
        self.input_buffer = None
        self.native_chunk_bytes = 0

    def warmup(self):
        schema = pa.schema([("g", pa.int64()), ("a", pa.float64())])
        batch = pa.record_batch([[0], [0.]], schema=schema)
        with pa.RecordBatchReader.from_batches(schema, [batch]) as reader:
            plan = make_plan_for_schema(schema, ["g"], ["a"], True)
            result = json.loads(self.execute(plan, reader))
            if result["rows"] != [[0, 0, 1]]:
                raise RuntimeError("Sirius startup warmup result mismatch")

    def execute(self, plan, reader=None):
        output, error, length = ct.create_string_buffer(1024 * 1024), ct.create_string_buffer(1024), ct.c_size_t()
        # ArrowArrayStream consists of four callback pointers and private_data.
        c_stream = (ct.c_void_p * 5)()
        if reader is not None:
            reader._export_to_c(ct.addressof(c_stream))
            status = self.stream(bytes(plan), len(plan), ct.addressof(c_stream), output,
                                 len(output), ct.byref(length), error, len(error))
        else:
            status = self.api.execute(bytes(plan), len(plan), output, len(output),
                                      ct.byref(length), error, len(error))
        if status:
            raise RuntimeError(error.value.decode("utf-8", errors="replace"))
        if not 0 < length.value <= len(output):
            raise RuntimeError("Invalid backend result size")
        return output.raw[:length.value]


def native_aggregate(sock, backend, descriptor):
    if len(descriptor) not in (4, 5):
        raise ValueError("Invalid native aggregate descriptor")
    version, groups, sums, count = descriptor[:4]
    if version == 1 and len(descriptor) == 4:
        frame_limit, row_limit = 16 * 1024**2, 262144
    elif version == 2 and len(descriptor) == 5 and descriptor[4] in (64, 128):
        frame_limit, row_limit = descriptor[4] * 1024**2, 8 * 1024**2
    else:
        raise ValueError("Unsupported native aggregate protocol version/frame bound")
    if backend.input_buffer is not None and len(backend.input_buffer) < frame_limit:
        raise ValueError("Pinned receive buffer is smaller than the requested native frame bound")
    if not 1 <= groups <= 8 or groups + sums > 64 or count not in (0, 1):
        raise ValueError("Unsupported native aggregate shape")
    names = [f"g{i}" for i in range(groups)] + [f"s{i}" for i in range(sums)]
    schema = pa.schema([(name, pa.int64() if i < groups else pa.float64())
                        for i, name in enumerate(names)])
    plan = make_plan_for_schema(schema, names[:groups], names[groups:], bool(count))
    send(sock, b"A")
    total_rows = 0
    max_batch_bytes = 0
    ended = False
    chunk_limit = backend.native_chunk_bytes
    columns = groups + sums + count
    def batches():
        nonlocal total_rows, ended, max_batch_bytes
        total = 0
        while True:
            kind, payload = receive(sock, limit=frame_limit, storage=backend.input_buffer)
            if kind == b"E" and not payload:
                ended = True
                return
            if kind != b"B" or len(payload) < 4:
                raise ValueError("Expected native aggregate batch")
            rows = struct.unpack_from("<I", payload)[0]
            mask = (rows + 7) // 8
            size = mask + rows * 8
            total += size * len(schema)
            if not 0 < rows <= row_limit or len(payload) != 4 + size * len(schema) or total > 512 * 1024 * 1024:
                raise ValueError("Native aggregate input size/budget exceeded")
            max_batch_bytes = max(max_batch_bytes, len(payload))
            # The owned frame buffer backs Arrow directly; no Python row decode.
            view = memoryview(payload)
            arrays = [pa.Array.from_buffers(field.type, rows,
                      [pa.py_buffer(view[4+i*size:4+i*size+mask]),
                       pa.py_buffer(view[4+i*size+mask:4+(i+1)*size])])
                      for i, field in enumerate(schema)]
            total_rows += rows
            yield pa.RecordBatch.from_arrays(arrays, schema=schema)
            send(sock, b"A")
            if chunk_limit and total >= chunk_limit:
                return
    merged = {}
    chunks = 0
    while not ended:
        with pa.RecordBatchReader.from_batches(schema, batches()) as reader:
            result = json.loads(backend.execute(plan, reader))
        chunks += 1
        if not chunk_limit:
            result_rows = result["rows"]
            break
        for row in result["rows"]:
            if len(row) != columns:
                raise ValueError("Native aggregate result schema mismatch")
            for i in range(groups, groups + sums):
                if row[i] is not None:
                    row[i] = float(row[i])
                    if not math.isfinite(row[i]):
                        raise ValueError("Non-finite native partial SUM")
            key = tuple(row[:groups])
            if key not in merged:
                if 4 + (len(merged) + 1) * columns * 9 > 1024 * 1024:
                    raise ValueError("Native aggregate merged result exceeds 1 MiB")
                merged[key] = list(row)
            else:
                prior = merged[key]
                for i in range(groups, groups + sums):
                    if row[i] is not None:
                        prior[i] = row[i] if prior[i] is None else prior[i] + row[i]
                        if not math.isfinite(prior[i]):
                            raise ValueError("Non-finite native merged SUM")
                if count:
                    prior[-1] += row[-1]
        result_rows = merged.values()
    output = bytearray(struct.pack("<I", len(result_rows)))
    for row in result_rows:
        if len(row) != columns:
            raise ValueError("Native aggregate result schema mismatch")
        for i, value in enumerate(row):
            output += struct.pack("<Bd" if groups <= i < groups+sums else "<Bq",
                                  value is not None, value if value is not None else 0)
    if len(output) > 1024 * 1024:
        raise ValueError("Native aggregate result exceeds 1 MiB")
    print(f"native aggregate: rows={total_rows} groups={len(result_rows)} chunks={chunks} "
          f"frame_mib={frame_limit // 1024**2} max_batch_bytes={max_batch_bytes}", flush=True)
    send(sock, b"R", output)


def handle(sock, backend):
    kind, plan = receive(sock, 1024 * 1024)
    if kind == b"N":
        native_aggregate(sock, backend, plan)
        return
    if not plan or kind not in (b"P", b"Q"):
        raise ValueError("Expected P(plan) or Q(plan) request")
    if kind == b"P":
        send(sock, b"J", backend.execute(plan))
        return
    kind, encoded_schema = receive(sock, 65536)
    if kind != b"S":
        raise ValueError("Expected Arrow schema")
    schema = pa.ipc.read_schema(pa.BufferReader(encoded_schema))
    if not 1 <= len(schema) <= 64 or len(set(schema.names)) != len(schema):
        raise ValueError("Require 1..64 uniquely named columns")
    if any(f.type not in (pa.int64(), pa.float64()) for f in schema):
        raise ValueError("Live input requires int64/float64 columns")
    send(sock, b"A")

    def batches():
        total = 0
        while True:
            kind, payload = receive(sock)
            if kind == b"E" and not payload:
                return
            if kind != b"B":
                raise ValueError("Expected Arrow batch or end-of-input")
            batch = pa.ipc.read_record_batch(pa.BufferReader(payload), schema)
            batch.validate(full=True)
            # Count decoded bytes, including nullable masks, before entering CUDA.
            decoded = (batch.num_rows * 8 + (batch.num_rows + 7) // 8) * len(schema)
            total += decoded
            if decoded > 16 * 1024 * 1024 or total > 512 * 1024 * 1024 or batch.num_rows > 1048576:
                raise ValueError("Live input budget exceeded")
            yield batch
            # get_next is requested only after the previous GPU copy completed.
            send(sock, b"A")

    with pa.RecordBatchReader.from_batches(schema, batches()) as reader:
        send(sock, b"J", backend.execute(plan, reader))


def serve(path, library, timeout, host_pin_mib=0, warmup=False, native_chunk_mib=0):
    started = time.perf_counter()
    path = Path(path).absolute()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.parent.stat()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError("Socket directory must be owned by this user and have mode 0700")
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid():
            raise ValueError("Refusing to replace a non-owned socket path")
        with socket.socket(socket.AF_UNIX) as probe:
            probe.settimeout(1)
            try:
                probe.connect(str(path))
            except ConnectionRefusedError:
                path.unlink()
            else:
                raise ValueError("Sidecar already running")
    backend = Backend(library)
    backend.native_chunk_bytes = native_chunk_mib * 1024 * 1024
    pinned = None
    if host_pin_mib:
        try:
            pinned = PinnedInput(host_pin_mib)
            backend.input_buffer = pinned.view
        except (OSError, RuntimeError) as error:
            print(f"warning: host pin unavailable, using pageable input: {error}", flush=True)
    try:
        if warmup:
            backend.warmup()
    except BaseException:
        if pinned:
            pinned.close()
        raise
    stopping = False
    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(str(path))
        os.chmod(path, 0o600)
        listener.listen(8)
        listener.settimeout(0.5)
        print(f"ready: {path} host_pin_mib={host_pin_mib if pinned else 0} warmup={warmup} native_chunk_mib={native_chunk_mib} "
              f"startup_seconds={time.perf_counter()-started:.6f}", flush=True)
        try:
            while not stopping:
                try:
                    peer, _ = listener.accept()
                except socket.timeout:
                    continue
                with peer:
                    _, uid, _ = struct.unpack("3i", peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                    if uid != os.geteuid():
                        continue
                    peer.settimeout(timeout)
                    try:
                        handle(peer, backend)
                    except Exception as error:
                        print(f"request failed: {str(error)[:4096]}", flush=True)
                        try:
                            send(peer, b"X", str(error).encode("utf-8", errors="replace")[:4096])
                        except OSError:
                            pass
        finally:
            path.unlink(missing_ok=True)
            backend.input_buffer = None
            if pinned:
                pinned.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--socket", required=True)
    p.add_argument("--library", required=True)
    p.add_argument("--io-timeout", type=float, default=60)
    p.add_argument("--host-pin-mib", type=int, choices=(0, 16, 64, 128), default=0,
                   help="Reuse bounded CUDA-pinned memory for native input; fall back if unavailable")
    p.add_argument("--warmup", action="store_true", help="Initialize GPU operators before accepting queries")
    p.add_argument("--native-chunk-mib", type=int, choices=(0, 64, 128, 256), default=0,
                   help="Bound each native GPU fragment and merge SUM/COUNT/groups; 0 keeps the 512 MiB query limit")
    args = p.parse_args()
    if not 0 < args.io_timeout <= 600:
        p.error("I/O timeout must be in (0,600] seconds")
    serve(args.socket, args.library, args.io_timeout, args.host_pin_mib, args.warmup, args.native_chunk_mib)
