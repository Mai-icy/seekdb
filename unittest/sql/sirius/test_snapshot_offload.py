"""Regression for immutable snapshot host pin and strict whole-query execution."""
from decimal import Decimal
import os
from pathlib import Path
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from query_offload import QueryBackend


@unittest.skipUnless(os.getenv("SIRIUS_TEST_LIBRARY"), "Requires the real GPU bridge")
class SnapshotOffloadTests(unittest.TestCase):
    def test_pin_repeated_reads_errors_and_typed_results(self):
        backend = QueryBackend(os.environ["SIRIUS_TEST_LIBRARY"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.parquet"
            table = pa.table({"id": [1, 2, 3], "name": ["中文", None, "z"],
                              "amount": pa.array([Decimal("1.23"), None, Decimal("2.34")],
                                                 type=pa.decimal128(15, 2))})
            pq.write_table(table, path)
            with self.assertRaises(RuntimeError):
                backend.pin_snapshot("snapshot_test", Path(directory) / "missing.parquet")
            backend.pin_snapshot("snapshot_test", path)
            for _ in range(2):
                actual = backend.query_snapshot("SELECT a.id,b.name,b.amount FROM snapshot_test a "
                                                "JOIN snapshot_test b ON a.id=b.id ORDER BY a.id")
                self.assertEqual(actual.to_pylist(), table.to_pylist())
            for sql in ("DROP VIEW snapshot_test", "SELECT * FROM snapshot_test; SELECT 1"):
                with self.assertRaises(RuntimeError):
                    backend.query_snapshot(sql)
            with self.assertRaises(RuntimeError):
                backend.pin_snapshot("snapshot_test", path)
            actual = backend.query_snapshot("WITH totals AS (SELECT sum(amount) v FROM snapshot_test) "
                                            "SELECT v FROM totals WHERE v=(SELECT max(v) FROM totals)")
            self.assertEqual(actual.column(0).to_pylist(), [Decimal("3.57")])


if __name__ == "__main__":
    unittest.main()
