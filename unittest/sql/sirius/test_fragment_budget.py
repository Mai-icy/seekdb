import os
from pathlib import Path
import struct
import sys
import unittest
from unittest.mock import patch

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from sidecar import _native_aggregate, native_fragment_limit_bytes


class FragmentBudgetTests(unittest.TestCase):
    def test_explicit_bounded_budget(self):
        for value, expected in [('512', 512), ('2048', 2048)]:
            with patch.dict(os.environ, SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB=value):
                self.assertEqual(native_fragment_limit_bytes(), expected * 1024**2)
        for value in ('', '0', '-1', '2049', 'unlimited'):
            with patch.dict(os.environ, SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB=value):
                with self.assertRaises(ValueError):
                    native_fragment_limit_bytes()

    def test_budget_rejects_before_import_or_slot_release(self):
        class Frames:
            released = False
            def get(self):
                # Two int64 values plus a validity bitmap: 17 bytes.
                return b'B', struct.pack('<IBqq', 2, 3, 1, 2), 0
            def release(self, slot):
                self.released = True
        class Backend:
            native_chunk_bytes = 0
            native_fragment_bytes = 16
            def execute(self, plan, reader):
                next(iter(reader))
                raise AssertionError('Over-budget batch reached importer')
        frames = Frames()
        with self.assertRaisesRegex(ValueError, 'budget exceeded'):
            _native_aggregate(None, Backend(), frames, pa.schema([('g', pa.int64())]),
                              b'', 16 * 1024**2, 1, 0, 1)
        self.assertFalse(frames.released)


if __name__ == '__main__':
    unittest.main()
