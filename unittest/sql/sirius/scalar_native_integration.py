#!/usr/bin/env python3
"""Native exact SUM: SQL results, actual offload, rescans and MVCC visibility."""
import argparse
import json
from pathlib import Path
import secrets
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect, identifier

HINT = "/*+ PARALLEL(1) OPT_PARAM('pushdown_storage_level',2) */"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trace', type=Path, required=True,
                        help='Log of the isolated native sidecar; proves GPU execution')
    args = parser.parse_args()
    checks = []
    table = identifier('sirius_scalar_' + secrets.token_hex(6))

    def count():
        return args.trace.read_text().count('exact=True')

    def query(cursor, sql, gpu, require_gpu=False):
        before = count()
        cursor.execute('SET @sirius_gpu=%s', (int(gpu),))
        cursor.execute(sql)
        result = cursor.fetchall()
        if require_gpu:
            deadline = time.monotonic() + 2
            while count() == before and time.monotonic() < deadline:
                time.sleep(.01)
            assert count() > before, ('SQL did not enter the GPU scalar operator', sql)
        return result

    def compare(cursor, label, sql, require_gpu=True):
        expected = query(cursor, sql, False)
        actual = query(cursor, sql, True, require_gpu)
        assert actual == expected, (label, actual, expected)
        checks.append(label)
        return actual

    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {table} (id INT PRIMARY KEY, v DECIMAL(18,2), '
                       'd DECIMAL(5,2), wide DECIMAL(38,0), small DECIMAL(3,0))')
        try:
            cursor.execute(f'INSERT INTO {table} VALUES '
                           '(1,1234567890123456.78,0.06,99999999999999999999999999999999999999,999),'
                           '(2,-10.25,0.05,-99999999999999999999999999999999999999,-998),'
                           '(3,NULL,NULL,NULL,NULL),'
                           '(4,0.01,0.07,9007199254740993,1),'
                           '(5,-0.02,-0.03,-9007199254740992,-2)')
            cases = {
                'decimal_sum': f'SUM(v) FROM {table}',
                'q6_multiplication': f'SUM(v*d) FROM {table} WHERE d BETWEEN 0.05 AND 0.07',
                'multiple_scales': f'SUM(v), SUM(v*d), SUM(wide), SUM(small) FROM {table}',
                'filtered_multiple_widths': f'SUM(v), SUM(small), SUM(wide) FROM {table} WHERE id>=2',
                'decimal128_cancellation': f'SUM(wide) FROM {table}',
                'decimal128_positive': f'SUM(wide) FROM {table} WHERE id=1',
                'decimal128_negative': f'SUM(wide) FROM {table} WHERE id=2',
                'empty': f'SUM(v), SUM(wide) FROM {table} WHERE id>100',
                'all_null': f'SUM(v), SUM(wide) FROM {table} WHERE id=3',
            }
            for label, body in cases.items():
                compare(cursor, label, f'SELECT {HINT} {body}')
            cursor.execute(f'INSERT INTO {table} (id,wide) VALUES '
                           '(6,99999999999999999999999999999999999999),'
                           '(7,99999999999999999999999999999999999999)')
            compare(cursor, 'decimal256_sum', f'SELECT {HINT} SUM(wide) FROM {table} WHERE id>=6')
            cursor.execute(f'DELETE FROM {table} WHERE id>=6')
            compare(cursor, 'correlated_rescan',
                    f'SELECT {HINT} x.id, (SELECT /*+ NO_UNNEST PARALLEL(1) '
                    "OPT_PARAM('pushdown_storage_level',2) */ SUM(y.v) "
                    f'FROM {table} y WHERE y.id<=x.id) FROM {table} x ORDER BY x.id')
            for label, body in {
                'distinct_fallback': f'SUM(DISTINCT v) FROM {table}',
                'mixed_count_fallback': f'SUM(v),COUNT(*) FROM {table}',
                'integer_fallback': f'SUM(id) FROM {table}',
            }.items():
                compare(cursor, label, f'SELECT {HINT} {body}', False)

            sql = f'SELECT {HINT} SUM(v) FROM {table}'
            original = query(cursor, sql, False)
            with connect() as writer, writer.cursor() as other:
                writer.begin()
                other.execute(f'UPDATE {table} SET v=50 WHERE id=2')
                assert query(cursor, sql, True, True) == original
                checks.append('other_uncommitted_invisible')
                writer.commit()
            changed = compare(cursor, 'committed_update_visible', sql)
            assert changed != original
            conn.begin()
            cursor.execute(f'UPDATE {table} SET v=75 WHERE id=2')
            pending = compare(cursor, 'own_writes_visible', sql)
            assert pending != changed
            conn.rollback()
            assert query(cursor, sql, True, True) == changed
            checks.append('rollback_visible')

            cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            conn.begin()
            snapshot = query(cursor, sql, True, True)
            with connect() as writer, writer.cursor() as other:
                other.execute(f'DELETE FROM {table} WHERE id=2')
            assert query(cursor, sql, True, True) == snapshot
            checks.append('repeatable_read_snapshot')
            conn.commit()
            deleted = compare(cursor, 'committed_delete_visible', sql)
            assert deleted != snapshot
            print(json.dumps({'status': 'passed', 'checks': checks}, indent=2))
        finally:
            conn.rollback()
            cursor.execute(f'DROP TABLE {table}')


if __name__ == '__main__':
    main()
