import json
import unittest
import os
from pathlib import Path
import socket
import sys
import tempfile

import pyarrow as pa
from resident_input import ResidentInput, RecordingBackend


class FakeBackend:
    def execute(self, plan, reader):
        grouped = {}
        for batch in reader:
            for row in zip(*[a.to_pylist() for a in batch.columns]):
                key, value = row
                entry = grouped.setdefault(key, [key, None, 0])
                if value is not None:
                    entry[1] = value if entry[1] is None else entry[1] + value
                entry[2] += 1
        return json.dumps({'rows': list(grouped.values())}).encode()


class ResidentTests(unittest.TestCase):
    @unittest.skipUnless(os.getenv('SIRIUS_TEST_LIBRARY'), 'Requires the real GPU bridge')
    def test_gpu_snapshot_replay(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
        from sidecar import Backend, PinnedInput
        from make_plan import make_plan_for_schema
        pinned = PinnedInput(1)
        cache = ResidentInput(pinned.view)
        backend = RecordingBackend(Backend(os.environ['SIRIUS_TEST_LIBRARY']), cache)
        schema = pa.schema([('g', pa.int64()), ('v', pa.float64())])
        plan = make_plan_for_schema(schema, ['g'], ['v'], True)
        try:
            backend.capture = 'sum_count'
            for values in [([1, None], [None, 4.]), ([1, None], [3., None])]:
                batch = pa.record_batch(list(values), schema=schema)
                with pa.RecordBatchReader.from_batches(schema, [batch]) as reader:
                    backend.execute(plan, reader)
            backend.capture = None
            token = backend.token('sum_count')
            for _ in range(3):
                rows = json.loads(backend.execute(token))['rows']
                self.assertEqual(sorted(rows, key=repr), sorted([[1, 3., 2], [None, 4., 2]], key=repr))
        finally:
            cache.clear()
            pinned.close()

    def test_socket_restored_after_exception(self):
        from path_comparison import benchmark_socket
        with tempfile.TemporaryDirectory() as directory, socket.socket(socket.AF_UNIX) as original:
            os.chmod(directory, 0o700)
            path = Path(directory) / 's.sock'
            original.bind(str(path))
            original.listen(1)
            inode = path.stat().st_ino
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                with benchmark_socket(path, None):
                    self.assertNotEqual(path.stat().st_ino, inode)
                    raise RuntimeError('injected')
            self.assertEqual(path.stat().st_ino, inode)
            self.assertFalse(path.with_name('s.sock.comparison-backup').exists())
            with socket.socket(socket.AF_UNIX) as peer:
                peer.connect(str(path))
                accepted, _ = original.accept()
                accepted.close()

    def test_snapshot_owns_values_and_null_masks(self):
        import struct
        payload = bytearray(struct.pack('<qq', 7, 9))
        mask = bytearray([1])
        a = pa.Array.from_buffers(pa.int64(), 2, [pa.py_buffer(mask), pa.py_buffer(payload)])
        batch = pa.record_batch([a], names=['g'])
        cache = ResidentInput(memoryview(bytearray(1024)))
        copy = cache.copy_batch(batch)
        payload[:] = bytes(16)
        mask[0] = 3
        self.assertEqual(copy.column(0).to_pylist(), [7, None])

    def test_capacity_is_enforced(self):
        cache = ResidentInput(memoryview(bytearray(8)))
        with self.assertRaises(MemoryError):
            cache.copy_batch(pa.record_batch([pa.array([1, 2], type=pa.int64())], names=['g']))

    def test_replay_keeps_partial_null_sum_and_count(self):
        from substrait import plan_pb2
        cache = ResidentInput(memoryview(bytearray(4096)))
        backend = RecordingBackend(FakeBackend(), cache)
        backend.capture = 'sum_count'
        schema = pa.schema([('g', pa.int64()), ('v', pa.float64())])
        for values in [([1, None], [None, 4.]), ([1, None], [3., None])]:
            batch = pa.record_batch(list(values), schema=schema)
            with pa.RecordBatchReader.from_batches(schema, [batch]) as reader:
                backend.execute(plan_pb2.Plan().SerializeToString(), reader)
        backend.capture = None
        token = backend.token('sum_count')
        for _ in range(2):
            rows = json.loads(backend.execute(token))['rows']
            self.assertEqual(sorted(rows, key=repr), sorted([[1, 3., 2], [None, 4., 2]], key=repr))
        self.assertEqual([e['rows'] for e in backend.events if e['mode'] == 'resident'], [2, 2, 2, 2])


if __name__ == '__main__':
    unittest.main()
