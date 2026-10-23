#!/usr/bin/env python3
"""Single-worker local Sirius process with bounded Arrow IPC input."""
import argparse
import ctypes as ct
import os
from pathlib import Path
import signal
import socket
import stat
import struct

import pyarrow as pa
from wire import receive, send

EXECUTE = ct.CFUNCTYPE(ct.c_int, ct.c_void_p, ct.c_size_t, ct.c_void_p, ct.c_size_t,
                      ct.POINTER(ct.c_size_t), ct.c_void_p, ct.c_size_t)


class Api(ct.Structure):
    _fields_ = [("version", ct.c_uint32), ("size", ct.c_uint32), ("execute", EXECUTE)]


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


def handle(sock, backend):
    kind, plan = receive(sock, 1024 * 1024)
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


def serve(path, library, timeout):
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
        print(f"ready: {path}", flush=True)
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
                        try:
                            send(peer, b"X", str(error).encode("utf-8", errors="replace")[:4096])
                        except OSError:
                            pass
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--socket", required=True)
    p.add_argument("--library", required=True)
    p.add_argument("--io-timeout", type=float, default=60)
    args = p.parse_args()
    if not 0 < args.io_timeout <= 600:
        p.error("I/O timeout must be in (0,600] seconds")
    serve(args.socket, args.library, args.io_timeout)
