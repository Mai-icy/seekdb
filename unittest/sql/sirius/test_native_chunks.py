#!/usr/bin/env python3
"""Exercise chunk boundaries and partial-result merging without CUDA."""
import json
from pathlib import Path
import socket
import struct
import sys
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from sidecar import native_aggregate
from wire import receive, send


class FakeBackend:
    def __init__(self, partials, chunk_bytes):
        self.native_chunk_bytes = chunk_bytes
        self.input_buffer = memoryview(bytearray(1024))
        self.partials = iter(partials)
        self.input_rows = []

    def execute(self, plan, reader):
        # Consume each borrowed batch before requesting the next one, like the
        # synchronized GPU importer. Materialize values to catch buffer reuse.
        rows = []
        for batch in reader:
            rows.extend(batch.to_pylist())
        self.input_rows.append(rows)
        return json.dumps({"rows": next(self.partials)}).encode()


def frame(rows, sums):
    payload = bytearray(struct.pack("<I", len(rows)))
    for col in range(1 + sums):
        payload += bytes([sum((row[col] is not None) << i for i, row in enumerate(rows))])
        for row in rows:
            payload += struct.pack("<d" if col else "<q", row[col] if row[col] is not None else 0)
    return payload


class NativeChunks(unittest.TestCase):
    def run_stream(self, batches, partials, sums=1, count=True, chunk_bytes=1):
        backend = FakeBackend(partials, chunk_bytes)
        left, right = socket.socketpair()
        errors = []
        left.settimeout(3)
        right.settimeout(3)
        def serve():
            with right:
                try:
                    native_aggregate(right, backend, bytes((1, 1, sums, int(count))))
                except Exception as error:
                    errors.append(error)
                    send(right, b"X", str(error).encode())
                    # Keep the error frame readable while the client finishes
                    # its already-ACKed input/end frame, then closes the socket.
                    while right.recv(1024):
                        pass
        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        try:
            with left:
                self.assertEqual(receive(left), (b"A", b""))
                for batch in batches:
                    send(left, b"B", frame(batch, sums))
                    self.assertEqual(receive(left), (b"A", b""))
                send(left, b"E")
                kind, payload = receive(left)
                if kind == b"X":
                    return None, backend, bytes(payload).decode()
                self.assertEqual(kind, b"R")
                columns = 1 + sums + int(count)
                size = struct.unpack_from("<I", payload)[0]
                result = []
                for row in range(size):
                    values = []
                    for col in range(columns):
                        valid, value = struct.unpack_from("<Bd" if 1 <= col <= sums else "<Bq",
                                                          payload, 4 + (row * columns + col) * 9)
                        values.append(value if valid else None)
                    result.append(values)
                return sorted(result, key=repr), backend, None
        finally:
            worker.join(timeout=4)
            self.assertFalse(worker.is_alive())

    def test_nullable_sums_groups_and_counts_across_exact_boundaries(self):
        batches = [[(1, 4.), (None, None)], [(1, None), (None, 3.)], [(2, None)]]
        partials = [[[1, 4., 1], [None, None, 1]], [[1, None, 1], [None, 3., 1]], [[2, None, 1]], []]
        result, backend, error = self.run_stream(batches, partials)
        self.assertIsNone(error)
        self.assertEqual(result, sorted([[1, 4., 2], [None, 3., 2], [2, None, 1]], key=repr))
        self.assertEqual([len(r) for r in backend.input_rows], [2, 2, 1, 0])
        self.assertEqual(backend.input_rows[0][0], {"g0": 1, "s0": 4.})

    def test_group_only_deduplicates(self):
        result, _, error = self.run_stream([[(1,)], [(1,), (2,)]], [[[1]], [[1], [2]], []], sums=0, count=False)
        self.assertIsNone(error)
        self.assertEqual(result, [[1], [2]])

    def test_count_only(self):
        result, _, error = self.run_stream([[(1,)], [(1,)]], [[[1, 1]], [[1, 1]], []], sums=0)
        self.assertIsNone(error)
        self.assertEqual(result, [[1, 2]])

    def test_sum_without_count(self):
        result, _, error = self.run_stream([[(1, 2.)], [(1, -3.)]], [[[1, 2.]], [[1, -3.]], []], count=False)
        self.assertIsNone(error)
        self.assertEqual(result, [[1, -1.]])

    def test_empty_stream(self):
        result, _, error = self.run_stream([], [[]])
        self.assertIsNone(error)
        self.assertEqual(result, [])

    def test_disabled_chunking_keeps_one_fragment(self):
        result, backend, error = self.run_stream([[(1, 2.)], [(1, 3.)]], [[[1, 5., 2]]], chunk_bytes=0)
        self.assertIsNone(error)
        self.assertEqual(result, [[1, 5., 2]])
        self.assertEqual(len(backend.input_rows), 1)

    def test_merged_result_bound(self):
        first = [[i, 1., 1] for i in range(20000)]
        second = [[i, 1., 1] for i in range(20000, 40000)]
        result, _, error = self.run_stream([[(1, 1.)], [(2, 1.)]], [first, second])
        self.assertIsNone(result)
        self.assertIn("merged result exceeds", error)

    def test_sum_overflow_returns_error(self):
        result, _, error = self.run_stream([[(1, 1e308)], [(1, 1e308)]],
                                           [[[1, 1e308, 1]], [[1, 1e308, 1]]])
        self.assertIsNone(result)
        self.assertIn("Non-finite native merged SUM", error)


if __name__ == "__main__":
    unittest.main()
