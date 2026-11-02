"""seekdb statement snapshots -> bounded Arrow sources -> Substrait GPU execution."""

import argparse
import gc
from concurrent.futures import Future, CancelledError
import json
import os
from pathlib import Path
import queue
import re
import signal
import socket
import stat
import struct
import threading
import time
import uuid

import pyarrow as pa
from adapter_backend import Backend, Scan, compile_plan
from adapter_pin import PinCache, Snapshot
from adapter_protocol import Decoder, arrow_type, decode_binary_batch, encode_text
from wire import receive, send

IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


def decode_request(payload):
    d = Decoder(payload)
    if d.integer() != 5:
        raise ValueError("Adapter requires seekdb snapshot protocol version 5")
    pin, principal, database, snapshot, sql = (
        d.text(),
        d.text(),
        d.text(),
        d.text(),
        d.text(),
    )
    width, count = d.integer(), d.integer()
    if (
        not principal
        or not snapshot
        or not sql
        or len(sql.encode()) > 1024**2
        or not 1 <= width <= 1024
        or not 1 <= count <= 64
    ):
        raise ValueError("Invalid query declaration")
    tables = {}
    table_ids = {}
    for _ in range(count):
        name, identity, columns = d.text(), d.text(), d.integer()
        if not IDENTIFIER.fullmatch(name) or name in tables or not 1 <= columns <= 64:
            raise ValueError("Invalid table declaration")
        fields = [(d.text(), arrow_type(d.text())) for _ in range(columns)]
        if len({name for name, _ in fields}) != columns:
            raise ValueError("Duplicate columns")
        tables[name] = pa.schema(fields)
        if not identity:
            raise ValueError("Missing seekdb table schema identity")
        table_ids[name] = identity
    d.end()
    if pin and not IDENTIFIER.fullmatch(pin):
        raise ValueError("Pin names must be lowercase identifiers")
    return pin, principal, database, snapshot, sql, width, tables, table_ids


class ScanPump:
    """One socket owner; each GPU scan can wait/cancel independently.

    seekdb reads all cursors on its SQL thread. A successful early LIMIT drains
    the one in-flight reply before sending D, preserving protocol framing.
    Failure shuts down the channel so neither peer waits on an abandoned reply.
    """

    def __init__(self, sock, pin_allocator=None):
        self.sock = sock
        self.pin_allocator = pin_allocator
        self.lock = threading.Lock()
        self.requests = queue.Queue(maxsize=64)
        self.pending = {}
        self.cancelled = set()
        self.stopping = False
        self.error = None
        self.bytes = self.batches = 0
        self.worker = threading.Thread(target=self.run)
        self.worker.start()

    def next(self, scan):
        with self.lock:
            if self.error:
                raise self.error
            if self.stopping or scan.id in self.cancelled:
                raise CancelledError("Host input cancelled")
            if scan.id in self.pending:
                raise RuntimeError("Concurrent reads of one scan")
            future = Future()
            self.pending[scan.id] = future
            self.requests.put_nowait((scan, future))
        try:
            return future.result()
        finally:
            with self.lock:
                self.pending.pop(scan.id, None)

    def cancel(self, stream_id):
        with self.lock:
            self.cancelled.add(stream_id)
            future = self.pending.get(stream_id)
            if future and not future.done():
                future.cancel()

    def run(self):
        while True:
            item = self.requests.get()
            if item is None:
                return
            scan, future = item
            if future.cancelled():
                continue
            try:
                columns = list(range(len(scan.schema)))
                request = struct.pack("!III", scan.id, scan.table_index, len(columns))
                request += b"".join(struct.pack("!I", c) for c in columns)
                send(self.sock, b"N", request)
                kind, payload = receive(self.sock)
                if kind == b"X":
                    raise RuntimeError(payload.decode(errors="replace"))
                if kind == b"E" and not payload:
                    result = None
                elif kind == b"B":
                    self.bytes += len(payload)
                    self.batches += 1
                    if self.pin_allocator:
                        payload = self.pin_allocator.copy(payload)
                    result = decode_binary_batch(payload, scan.schema)
                else:
                    raise ValueError("Invalid seekdb scan reply")
                with self.lock:
                    if not future.done():
                        future.set_result(result)
            except Exception as error:
                with self.lock:
                    self.error = error
                    for pending in self.pending.values():
                        if not pending.done():
                            pending.set_exception(error)
                return

    def batches_for(self, scan):
        while True:
            batch = self.next(scan)
            if batch is None:
                return
            yield batch

    def stop(self, success):
        with self.lock:
            self.stopping = True
            for future in self.pending.values():
                if not future.done():
                    future.cancel()
        if not success:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if self.worker.is_alive():
            self.requests.put(None)
        self.worker.join()
        if success and self.error:
            raise self.error


def handle(sock, backend, compiler, pins):
    kind, payload = receive(sock)
    if kind != b"W":
        raise ValueError("Expected seekdb whole-query request")
    pin, principal, database, snapshot, sql, width, tables, table_ids = decode_request(
        payload
    )
    started = time.perf_counter()
    plan, scans = compile_plan(compiler, sql, tables)
    planning = time.perf_counter() - started
    key = (principal, database, pin)
    cached = pins.prepare(key) if pin else None
    if cached:
        cached.validate(tables, table_ids)
    send(sock, b"A", struct.pack("!I", 0 if cached else 1))
    pump = None if cached else ScanPump(sock, pins.allocator if pin else None)
    pin_s = 0.0
    names = list(tables)
    pending = None
    try:
        identity = f"{principal}:{database}:{snapshot}:{uuid.uuid4()}"
        if pin and not cached:
            capture_started = time.perf_counter()
            captured = {}
            # Finish every table before publication, even if the query uses a
            # LIMIT or only one of several occurrences of that table.
            for i, (name, schema) in enumerate(tables.items()):
                captured[name] = list(pump.batches_for(Scan(i, i, schema)))
            pending = Snapshot(identity, tables, table_ids, captured)
            cached = pending
            pin_s = time.perf_counter() - capture_started
        execute_started = time.perf_counter()
        if cached:
            result = backend.execute(
                plan,
                scans,
                cached.identity,
                lambda scan: iter(cached.batches[names[scan.table_index]]),
                lambda _: None,
            )
        else:
            result = backend.execute(
                plan, scans, identity, pump.batches_for, pump.cancel
            )
        execute_s = time.perf_counter() - execute_started
    except Exception:
        # Drain the one in-flight response before reporting the original GPU
        # error. Closing the socket here would hide it as a generic I/O error
        # in seekdb. The socket timeout still bounds an unresponsive host.
        if pump:
            try:
                pump.stop(True)
            except Exception:
                pass  # worker is joined; preserve the original failure
        raise
    else:
        if pump:
            pump.stop(True)
    if pump:
        send(sock, b"D")
    if result.num_columns != width:
        raise ValueError("GPU result width differs from seekdb result metadata")
    send(sock, b"S", struct.pack("!I", width))
    total = 0
    for batch in result.to_batches(max_chunksize=1024):
        encoded = bytearray(struct.pack("!I", batch.num_rows))
        for row in zip(*(c.to_pylist() for c in batch.columns)):
            for value in row:
                encoded.extend(encode_text(value))
        total += len(encoded)
        if total > 64 * 1024**2:
            raise ValueError("Result exceeds the seekdb adapter 64 MiB bound")
        send(sock, b"R", encoded)
    send(sock, b"Z")
    if pending:
        pins.snapshots[key] = pending
    print(
        json.dumps(
            {
                "backend": "sirius_substrait",
                "source": "seekdb_statement_snapshot",
                "scans": len(scans),
                "input_bytes": pump.bytes if pump else 0,
                "input_batches": pump.batches if pump else 0,
                "pin": pin,
                "pin_build_s": pin_s,
                "execute_s": execute_s,
                "pin_reused": bool(pin and not pending),
                "pin_bytes": pins.allocator.used if pins.allocator else 0,
                "rows": len(result),
                "planning_s": planning,
                "elapsed_s": time.perf_counter() - started,
            }
        ),
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--library", required=True)
    parser.add_argument("--compiler", required=True)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument(
        "--pin-bytes",
        type=int,
        default=0,
        help="Separate hard budget for immutable cudaHostAlloc snapshots; 0 disables pin",
    )
    args = parser.parse_args()
    if not args.socket.is_absolute() or args.timeout <= 0 or args.pin_bytes < 0:
        parser.error("Require an absolute socket path and positive timeout")
    args.socket.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = args.socket.parent.stat()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError("Socket directory must be owned by this user with mode 0700")
    backend = Backend(args.library)
    pins = PinCache(args.pin_bytes)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    bound = False

    def stop(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    try:
        listener.bind(str(args.socket))
        bound = True
        os.chmod(args.socket, 0o600)
        listener.listen(8)
        print(f"Seekdb Substrait adapter ready: {args.socket}", flush=True)
        while True:
            sock, _ = listener.accept()
            with sock:
                sock.settimeout(args.timeout)
                try:
                    _, uid, _ = struct.unpack(
                        "3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
                    )
                    if uid != os.geteuid():
                        raise PermissionError("Peer must have the same UID")
                    handle(sock, backend, args.compiler, pins)
                except Exception as error:
                    print(f"query failed: {error}", flush=True)
                    try:
                        send(sock, b"X", str(error).encode()[:4096])
                    except OSError:
                        pass
            # Failed Futures can retain tracebacks containing provisional pin
            # batches. Reclaim them before admitting another snapshot budget.
            gc.collect()
    except KeyboardInterrupt:
        pass
    finally:
        listener.close()
        if bound:
            args.socket.unlink(missing_ok=True)
        pins.clear()
        backend.close()


if __name__ == "__main__":
    main()
