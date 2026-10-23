#!/usr/bin/env python3
"""Compare native hash aggregation with CPU SQL, including transaction semantics."""
import json
from pathlib import Path
import secrets
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from connection import connect, identifier


def rows(cursor, sql, gpu):
    cursor.execute("SET @sirius_gpu=%s", (int(gpu),))
    cursor.execute(sql)
    return sorted(cursor.fetchall(), key=repr)


def main():
    name = identifier("sirius_native_" + secrets.token_hex(6))
    checks = []
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, g BIGINT, h BIGINT, a DOUBLE, b DOUBLE)")
        try:
            cursor.execute(f"INSERT INTO {name} VALUES (1,1,2,10,3),(2,1,2,NULL,4),"
                           "(3,NULL,2,30.5,NULL),(4,2,NULL,NULL,NULL),(5,2,NULL,9,-1)")
            prefix = "SELECT /*+ PARALLEL(1) USE_HASH_AGGREGATION */ "
            cases = {
                "nullable_group_sum_count": f"g,SUM(a),COUNT(*) FROM {name} GROUP BY g",
                "two_keys_multiple_sums_reordered": f"COUNT(*),SUM(b),h,g,SUM(a) FROM {name} GROUP BY h,g",
                "scan_filter": f"g,SUM(a),COUNT(*) FROM {name} WHERE id>=2 GROUP BY g",
                "empty_input": f"g,SUM(a),COUNT(*) FROM {name} WHERE id>100 GROUP BY g",
                "having_order_limit": f"g,SUM(a),COUNT(*) FROM {name} GROUP BY g HAVING COUNT(*)>1 ORDER BY g LIMIT 1",
                "group_only": f"g FROM {name} GROUP BY g",
                "count_only": f"g,COUNT(*) FROM {name} GROUP BY g",
                "unsupported_count_column_cpu": f"g,COUNT(a) FROM {name} GROUP BY g",
                "unsupported_distinct_cpu": f"g,COUNT(DISTINCT a) FROM {name} GROUP BY g",
                "unsupported_integer_sum_cpu": f"g,SUM(id) FROM {name} GROUP BY g",
                "scalar_aggregate_cpu": f"SUM(a),COUNT(*) FROM {name}",
                "correlated_rescan": f"x.id,(SELECT /*+ NO_UNNEST USE_HASH_AGGREGATION */ SUM(y.a) "
                                     f"FROM {name} y WHERE y.g=x.g GROUP BY y.g) FROM {name} x",
            }
            for label, query in cases.items():
                expected = rows(cursor, prefix + query, False)
                actual = rows(cursor, prefix + query, True)
                assert actual == expected, (label, actual, expected)
                checks.append(label)
            query = prefix + cases["nullable_group_sum_count"]
            original = rows(cursor, query, False)
            with connect() as writer, writer.cursor() as other:
                writer.begin()
                other.execute(f"UPDATE {name} SET a=50 WHERE id=1")
                assert rows(cursor, query, True) == original
                checks.append("uncommitted_invisible")
                writer.commit()
            assert rows(cursor, query, True) == rows(cursor, query, False)
            assert rows(cursor, query, True) != original
            checks.append("committed_update_visible")
            # Unlike the previous external SELECT helper, this operator sees
            # writes in its own transaction and rollback restores the view.
            conn.begin()
            cursor.execute(f"UPDATE {name} SET a=75 WHERE id=1")
            pending = rows(cursor, query, False)
            assert rows(cursor, query, True) == pending
            conn.rollback()
            assert rows(cursor, query, True) != pending
            checks.append("own_transaction_and_rollback")
            cursor.execute(f"DELETE FROM {name} WHERE id=3")
            assert rows(cursor, query, True) == rows(cursor, query, False)
            checks.append("delete_visible")
            print(json.dumps({"status": "passed", "checks": checks}))
        finally:
            conn.rollback()
            cursor.execute(f"DROP TABLE {name}")


if __name__ == "__main__":
    main()
