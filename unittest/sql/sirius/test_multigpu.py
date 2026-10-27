"""Run with SIRIUS_TEST_LIBRARY and a one/two-device SEEKDB_SIRIUS_CONFIG."""
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from make_plan import make_plan_for_schema
from resident_input import ResidentInput
from sidecar import Backend, PinnedInput


@unittest.skipUnless(os.getenv('SIRIUS_TEST_LIBRARY'), 'Requires the real GPU bridge')
class MultiDeviceInputTests(unittest.TestCase):
    def test_default_limit_and_opt_in_larger_fragment(self):
        backend = Backend(os.environ['SIRIUS_TEST_LIBRARY'])
        schema = pa.schema([('g', pa.int64()), ('v', pa.float64())])
        plan = make_plan_for_schema(schema, ['g'], ['v'], True)
        rows = 1024**2
        batch = pa.record_batch([pa.array([1] * rows, type=pa.int64()),
                                 pa.array([2.] * rows, type=pa.float64())], schema=schema)
        # 32 decoded batches include validity budget and exceed 512 MiB.
        with patch.dict(os.environ, SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB='512'):
            with pa.RecordBatchReader.from_batches(schema, [batch] * 32) as reader:
                with self.assertRaisesRegex(RuntimeError, 'configured fragment limit'):
                    backend.execute(plan, reader)
        with patch.dict(os.environ, SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB='2048'):
            with pa.RecordBatchReader.from_batches(schema, [batch] * 32) as reader:
                self.assertEqual(json.loads(backend.execute(plan, reader))['rows'],
                                 [[1, 2. * rows * 32, rows * 32]])

    def test_invalid_bridge_budget_releases_input_and_recovers(self):
        backend = Backend(os.environ['SIRIUS_TEST_LIBRARY'])
        schema = pa.schema([('g', pa.int64()), ('v', pa.float64())])
        plan = make_plan_for_schema(schema, ['g'], ['v'], True)
        batch = pa.record_batch([[1], [2.]], schema=schema)
        with patch.dict(os.environ, SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB='unlimited'):
            with pa.RecordBatchReader.from_batches(schema, [batch]) as reader:
                with self.assertRaisesRegex(RuntimeError, 'must be 512 or 2048'):
                    backend.execute(plan, reader)
        with pa.RecordBatchReader.from_batches(schema, [batch]) as reader:
            self.assertEqual(json.loads(backend.execute(plan, reader))['rows'], [[1, 2., 1]])

    def test_reused_host_slot_cross_device_groups_nulls_and_empty_batches(self):
        backend = Backend(os.environ['SIRIUS_TEST_LIBRARY'])
        schema = pa.schema([('g', pa.int64()), ('v', pa.float64())])
        plan = make_plan_for_schema(schema, ['g'], ['v'], True)
        values = [([1, None, 9], [2., 4., None]),
                  ([1, None, 9], [3., None, None]),
                  ([], []),
                  ([1, None, 9], [-1., 8., None])]
        pinned = PinnedInput(1)
        cache = ResidentInput(pinned.view)
        try:
            for _ in range(3):
                def batches():
                    for columns in values:
                        # Reuse the identical host addresses on the next pull.
                        # The importer must have finished copying on that batch's
                        # target device before returning to this generator.
                        cache.used = 0
                        batch = pa.record_batch(list(columns), schema=schema)
                        yield cache.copy_batch(batch)
                with pa.RecordBatchReader.from_batches(schema, batches()) as reader:
                    rows = json.loads(backend.execute(plan, reader))['rows']
                self.assertEqual(sorted(rows, key=repr),
                                 sorted([[1, 4., 3], [None, 12., 3], [9, None, 3]], key=repr))
        finally:
            cache.clear()
            pinned.close()


if __name__ == '__main__':
    unittest.main()
