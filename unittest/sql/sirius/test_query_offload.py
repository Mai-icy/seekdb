"""GPU integration coverage; set SIRIUS_TEST_LIBRARY and SEEKDB_SIRIUS_CONFIG."""
from datetime import date
from decimal import Decimal
import os
from pathlib import Path
import sys
import unittest
import uuid

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from query_offload import QueryBackend, snapshot, snapshot_inputs
from connection import connect


@unittest.skipUnless(os.getenv("SIRIUS_TEST_LIBRARY"), "Requires the real GPU bridge")
class QueryOffloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.backend = QueryBackend(os.environ["SIRIUS_TEST_LIBRARY"])

    def query(self, sql, **tables):
        return self.backend.query(sql, {name: (table.to_reader(max_chunksize=2), len(table))
                                        for name, table in tables.items()})

    def test_typed_join_filter_aggregate_order(self):
        sales = pa.table({"id": pa.array([1, 2, 1, 3], type=pa.int32()),
                          "amount": pa.array([Decimal("123.45"), None, Decimal("0.01"), Decimal("9.99")],
                                             type=pa.decimal128(15, 2)),
                          "day": pa.array([date(1994, 1, 1)] * 3 + [date(1995, 1, 1)], type=pa.date32())})
        people = pa.table({"id": pa.array([1, 2, 3], type=pa.int32()), "name": ["中文", "z", "a"]})
        actual = self.query("SELECT p.name, sum(s.amount) total, count(*) n FROM sales s "
                            "JOIN people p ON s.id=p.id WHERE s.day < DATE '1995-01-01' "
                            "GROUP BY p.name ORDER BY p.name", sales=sales, people=people)
        self.assertEqual(actual.to_pylist(), [{"name": "z", "total": None, "n": 1},
                                             {"name": "中文", "total": Decimal("123.46"), "n": 2}])
        self.assertTrue(pa.types.is_decimal(actual.schema.field("total").type))

    def test_self_join_and_correlated_not_exists(self):
        values = pa.table({"id": [1, 2, 3], "parent": [None, 1, 1]})
        actual = self.query("SELECT a.id, b.id child FROM tree a LEFT JOIN tree b "
                            "ON a.id=b.parent ORDER BY a.id,b.id", tree=values)
        self.assertEqual(actual.to_pylist(), [{"id": 1, "child": 2}, {"id": 1, "child": 3},
                                             {"id": 2, "child": None}, {"id": 3, "child": None}])
        actual = self.query("SELECT a.id FROM tree a WHERE NOT EXISTS "
                            "(SELECT 1 FROM tree b WHERE b.parent=a.id) ORDER BY a.id", tree=values)
        self.assertEqual(actual.column(0).to_pylist(), [2, 3])

    def test_decimal_widths_and_nullable_scalars(self):
        table = pa.table({"id": [1, 2],
                          "narrow": pa.array([Decimal("9999999.99"), None], type=pa.decimal128(9, 2)),
                          "wide": pa.array([Decimal("123456789012345678901234.56"), None], type=pa.decimal128(30, 2)),
                          "flag": [True, None], "small": pa.array([-128, 127], type=pa.int8())})
        actual = self.query("SELECT * FROM data ORDER BY id", data=table)
        self.assertEqual(actual.to_pylist(), table.to_pylist())

    def test_empty_input(self):
        table = pa.table({"x": pa.array([], type=pa.decimal128(15, 2)),
                          "s": pa.array([], type=pa.string())})
        actual = self.query("SELECT count(*) n, sum(x) total FROM data", data=table)
        self.assertEqual(actual.to_pylist(), [{"n": 0, "total": None}])

    def test_user_cte_reads_declared_input_and_is_reused(self):
        actual = self.query("WITH totals AS (SELECT id, sum(v) total FROM data GROUP BY id) "
                            "SELECT id, total FROM totals WHERE total=(SELECT max(total) FROM totals)",
                            data=pa.table({"id": [1, 1, 2], "v": [2, 3, 4]}))
        self.assertEqual(actual.to_pylist(), [{"id": 1, "total": Decimal(5)}])

    def test_failed_plans_release_and_recover(self):
        table = pa.table({"id": [1]})
        for sql in ("DELETE FROM data", "SELECT * FROM data; SELECT 1", "SELECT absent FROM data"):
            with self.assertRaises(RuntimeError):
                self.query(sql, data=table)
            self.assertEqual(self.query("SELECT id FROM data", data=table).column(0).to_pylist(), [1])

    def test_producer_error_and_recovery(self):
        schema = pa.schema([("id", pa.int64())])

        def fail():
            yield pa.record_batch([[1]], schema=schema)
            raise ValueError("injected input failure")

        with self.assertRaisesRegex(RuntimeError, "injected input failure"):
            self.backend.query("SELECT sum(id) FROM data",
                               {"data": (pa.RecordBatchReader.from_batches(schema, fail()), 2)})
        self.assertEqual(self.query("SELECT id FROM data", data=pa.table({"id": [2]})).column(0).to_pylist(), [2])

    def test_input_budget_failure_and_recovery(self):
        schema = pa.schema([("id", pa.int64())])
        rows = 1024 * 1024
        batch = pa.record_batch([pa.array([1] * rows, type=pa.int64())], schema=schema)
        with self.assertRaisesRegex(RuntimeError, "512 MiB prototype limit"):
            self.backend.query("SELECT sum(id) FROM data",
                               {"data": (pa.RecordBatchReader.from_batches(schema, [batch] * 64), rows * 64)})
        self.assertEqual(self.query("SELECT id FROM data", data=pa.table({"id": [3]})).column(0).to_pylist(), [3])


@unittest.skipUnless(os.getenv("SEEKDB_TEST_SNAPSHOT"), "Requires a writable seekdb test database")
class SnapshotTests(unittest.TestCase):
    def test_concurrent_commit_is_invisible_to_both_table_reads(self):
        reader, writer = connect(), connect()
        names = ["sirius_snapshot_" + uuid.uuid4().hex for _ in range(2)]
        try:
            with writer.cursor() as cursor:
                for name in names:
                    cursor.execute(f"CREATE TABLE {name} (id BIGINT PRIMARY KEY)")
                    cursor.execute(f"INSERT INTO {name} VALUES (1)")
            with snapshot(reader):
                inputs = snapshot_inputs(reader, names)
                self.assertEqual(inputs[names[0]][0].read_all().column(0).to_pylist(), [1])
                with writer.cursor() as cursor:
                    cursor.execute(f"INSERT INTO {names[1]} VALUES (2)")
                self.assertEqual(inputs[names[1]][0].read_all().column(0).to_pylist(), [1])
            with reader.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {names[1]}")
                self.assertEqual(cursor.fetchone()[0], 2)
        finally:
            reader.rollback()
            with writer.cursor() as cursor:
                for name in names:
                    cursor.execute(f"DROP TABLE IF EXISTS {name}")
            reader.close()
            writer.close()


if __name__ == "__main__":
    unittest.main()
