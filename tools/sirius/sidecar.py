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
from native_input import NativeInput

EXECUTE = ct.CFUNCTYPE(ct.c_int, ct.c_void_p, ct.c_size_t, ct.c_void_p, ct.c_size_t,
                      ct.POINTER(ct.c_size_t), ct.c_void_p, ct.c_size_t)


class Api(ct.Structure):
    _fields_ = [("version", ct.c_uint32), ("size", ct.c_uint32), ("execute", EXECUTE)]


def native_fragment_limit_bytes():
    value = os.environ.get('SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB', '512')
    if value not in ('512', '2048'):
        raise ValueError('SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB must be 512 or 2048')
    return int(value) * 1024**2


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
        # cudaHostAllocPortable: retain pinned-transfer benefits on every
        # configured device/context when Arrow batches use multiple GPUs.
        status = self.runtime.cudaHostAlloc(ct.byref(self.pointer), self.size, 1)
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
        self.native_fragment_bytes = native_fragment_limit_bytes()
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
        self.native_prefetch_buffers = ()
        self.closed = False

    def close(self):
        if self.closed:
            return
        shutdown = getattr(self.library, 'seekdb_sirius_shutdown_v1', None)
        if shutdown is not None:
            shutdown.argtypes = [ct.c_void_p, ct.c_size_t]
            shutdown.restype = ct.c_int
            error = ct.create_string_buffer(1024)
            if shutdown(error, len(error)):
                raise RuntimeError(error.value.decode('utf-8', errors='replace'))
        self.closed = True

    def warmup(self):
        schema = pa.schema([("g", pa.int64()), ("a", pa.float64())])
        batch = pa.record_batch([[0], [0.]], schema=schema)
        with pa.RecordBatchReader.from_batches(schema, [batch]) as reader:
            plan = make_plan_for_schema(schema, ["g"], ["a"], True)
            result = json.loads(self.execute(plan, reader))
            if result["rows"] != [[0, 0, 1]]:
                raise RuntimeError("Sirius startup warmup result mismatch")

    def execute_grouped_exact(self, plan, reader):
        return self.execute(plan, reader, output_capacity=256 * 1024**2)

    def execute(self, plan, reader=None, *, output_capacity=1024**2):
        if self.closed:
            raise RuntimeError('Backend has been closed')
        if not 0 < output_capacity <= 256 * 1024**2:
            raise ValueError('Invalid backend result capacity')
        output, error, length = ct.create_string_buffer(output_capacity), ct.create_string_buffer(1024), ct.c_size_t()
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
        return ct.string_at(output, length.value)


def native_aggregate(sock, backend, descriptor):
    if len(descriptor) not in (4, 5):
        raise ValueError("Invalid native aggregate descriptor")
    version, groups, sums, count = descriptor[:4]
    if version == 1 and len(descriptor) == 4:
        frame_limit, row_limit = 16 * 1024**2, 262144
    elif version in (2, 3, 4, 7) and len(descriptor) == 5 and descriptor[4] in ((16, 64, 128) if version >= 3 else (64, 128)):
        frame_limit = descriptor[4] * 1024**2
        row_limit = 262144 if descriptor[4] == 16 else 8 * 1024**2
    else:
        raise ValueError("Unsupported native aggregate protocol version/frame bound")
    if backend.input_buffer is not None and len(backend.input_buffer) < frame_limit:
        raise ValueError("Pinned receive buffer is smaller than the requested native frame bound")
    exact = version in (3, 4, 7)
    reduction = 'min' if version == 7 else 'sum'
    if exact:
        valid_shape = (1 <= sums and groups + sums <= 64 and count == 0
                       and (groups == 0 if version == 3 else 1 <= groups <= 8))
    else:
        valid_shape = 1 <= groups <= 8 and groups + sums <= 64 and count in (0, 1)
    if not valid_shape:
        raise ValueError("Unsupported native aggregate shape")
    names = [f"g{i}" for i in range(groups)] + [f"s{i}" for i in range(sums)]
    schema = pa.schema([(name, pa.int64() if exact or i < groups else pa.float64())
                        for i, name in enumerate(names)])
    plan = make_plan_for_schema(schema, names[:groups], names[groups:], bool(count),
                                wide_integer_sums=exact, reduction=reduction)
    frames = NativeInput(sock, frame_limit, row_limit, len(schema), backend.input_buffer,
                         getattr(backend, 'native_prefetch_buffers', ()))
    try:
        send(sock, b"A")
        frames.start()
        _native_aggregate(sock, backend, frames, schema, plan, frame_limit, groups, sums, count, exact, reduction)
    finally:
        # Stop ACK writes and receive before the handler emits an error or the
        # process releases pinned storage. GPU execution has already returned.
        frames.close()


def _native_aggregate(sock, backend, frames, schema, plan, frame_limit, groups, sums, count, exact=False, reduction='sum'):
    total_rows = 0
    max_batch_bytes = 0
    ended = False
    chunk_limit = backend.native_chunk_bytes
    fragment_limit = getattr(backend, 'native_fragment_bytes', 512 * 1024**2)
    columns = groups + sums + count
    result_limit = 256 * 1024**2 if exact and groups else 1024**2
    def batches():
        nonlocal total_rows, ended, max_batch_bytes
        total = 0
        while True:
            kind, payload, slot = frames.get()
            if kind == b"E" and not payload:
                ended = True
                return
            rows = struct.unpack_from("<I", payload)[0]
            mask = (rows + 7) // 8
            size = mask + rows * 8
            total += size * len(schema)
            if total > fragment_limit:
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
            # Only a subsequent get_next proves the importer consumed this
            # batch. On iterator.close() after failure, keep the slot leased
            # until frames.close() stops receive-ahead.
            frames.release(slot)
            if chunk_limit and total >= chunk_limit:
                return
    merged = {}
    chunks = 0
    while not ended:
        iterator = batches()
        try:
            with pa.RecordBatchReader.from_batches(schema, iterator) as reader:
                execute = getattr(backend, 'execute_grouped_exact', backend.execute) if exact and groups else backend.execute
                result = json.loads(execute(plan, reader))
        finally:
            iterator.close()
        chunks += 1
        if not chunk_limit:
            result_rows = result["rows"]
            break
        for row in result["rows"]:
            if len(row) != columns:
                raise ValueError("Native aggregate result schema mismatch")
            for i in range(groups, groups + sums):
                if row[i] is not None:
                    row[i] = exact_integer(row[i]) if exact else float(row[i])
                    if not exact and not math.isfinite(row[i]):
                        raise ValueError("Non-finite native partial SUM")
            key = tuple(row[:groups])
            if key not in merged:
                if 4 + (len(merged) + 1) * columns * (17 if exact else 9) > result_limit:
                    raise ValueError("Native aggregate merged result exceeds protocol bound")
                merged[key] = list(row)
            else:
                prior = merged[key]
                for i in range(groups, groups + sums):
                    if row[i] is not None:
                        prior[i] = row[i] if prior[i] is None else (
                            min(prior[i], row[i]) if reduction == 'min' else prior[i] + row[i])
                        if not exact and not math.isfinite(prior[i]):
                            raise ValueError("Non-finite native merged SUM")
                if count:
                    prior[-1] += row[-1]
        result_rows = merged.values()
    output = bytearray(struct.pack("<I", len(result_rows)))
    for row in result_rows:
        if len(row) != columns:
            raise ValueError("Native aggregate result schema mismatch")
        for i, value in enumerate(row):
            if exact:
                number = exact_integer(value) if value is not None else 0
                if not -(1 << 127) <= number < (1 << 127):
                    raise ValueError('Native exact SUM exceeds signed 128-bit result bound')
                output.append(int(value is not None))
                output += number.to_bytes(16, 'little', signed=True)
            else:
                output += struct.pack("<Bd" if groups <= i < groups+sums else "<Bq",
                                      value is not None, value if value is not None else 0)
    if len(output) > result_limit:
        raise ValueError("Native aggregate result exceeds protocol bound")
    print(f"native aggregate: rows={total_rows} groups={len(result_rows)} chunks={chunks} "
          f"frame_mib={frame_limit // 1024**2} max_batch_bytes={max_batch_bytes} "
          f"prefetch={bool(frames.buffers)} exact={exact} reduction={reduction}", flush=True)
    send(sock, b"R", output, limit=result_limit)


def exact_integer(value):
    # Decimal128 bridge results are JSON strings. Never route these through
    # binary floating point, even when chunking is disabled.
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError('Native exact SUM requires an integer result')
    return int(value)


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


def serve(path, library, timeout, host_pin_mib=0, warmup=False, native_chunk_mib=0, native_prefetch=False):
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
    if native_chunk_mib < 0 or native_chunk_mib * 1024**2 > native_fragment_limit_bytes() - 128 * 1024**2:
        raise ValueError('Native chunk target must leave 128 MiB headroom within the fragment budget')
    backend.native_chunk_bytes = native_chunk_mib * 1024 * 1024
    pinned = None
    prefetch_pin = None
    if native_prefetch and not host_pin_mib:
        raise ValueError("Native prefetch requires pinned host input")
    if host_pin_mib:
        try:
            pinned = PinnedInput(host_pin_mib)
            backend.input_buffer = pinned.view
        except (OSError, RuntimeError) as error:
            print(f"warning: host pin unavailable, using pageable input: {error}", flush=True)
    try:
        if native_prefetch and pinned:
            prefetch_pin = PinnedInput(host_pin_mib)
            backend.native_prefetch_buffers = (pinned.view, prefetch_pin.view)
        if warmup:
            backend.warmup()
    except BaseException:
        backend.close()
        backend.native_prefetch_buffers = ()
        if prefetch_pin:
            prefetch_pin.close()
        if pinned:
            pinned.close()
        raise
    stopping = False
    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(path))
            os.chmod(path, 0o600)
            listener.listen(8)
            listener.settimeout(0.5)
            print(f"ready: {path} host_pin_mib={host_pin_mib if pinned else 0} warmup={warmup} native_chunk_mib={native_chunk_mib} "
                  f"native_prefetch={bool(backend.native_prefetch_buffers)} startup_seconds={time.perf_counter()-started:.6f}", flush=True)
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
    finally:
        backend.close()
        backend.input_buffer = None
        backend.native_prefetch_buffers = ()
        if prefetch_pin:
            prefetch_pin.close()
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
    p.add_argument("--native-chunk-mib", type=int, choices=(0, 64, 128, 256, 384, 1024, 1536), default=0,
                   help="GPU fragment target; 1024/1536 require SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB=2048; 0 uses the configured query bound")
    p.add_argument("--native-prefetch", action="store_true",
                   help="Receive ahead into two bounded pinned buffers; ACK after validated reception")
    args = p.parse_args()
    if not 0 < args.io_timeout <= 600:
        p.error("I/O timeout must be in (0,600] seconds")
    serve(args.socket, args.library, args.io_timeout, args.host_pin_mib, args.warmup, args.native_chunk_mib, args.native_prefetch)
