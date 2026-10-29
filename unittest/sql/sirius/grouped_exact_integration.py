#!/usr/bin/env python3
"""Prove native GPU grouping, exact decimals, AVG lowering and nested safety."""
import argparse
import json
from pathlib import Path
import secrets
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect, identifier

HINT = "/*+ PARALLEL(1) USE_HASH_AGGREGATION OPT_PARAM('pushdown_storage_level',2) */"


def main(trace):
    table = identifier('sirius_grouped_' + secrets.token_hex(6))
    checks = []
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY, k VARCHAR(64), n BIGINT, '
                       'v DECIMAL(18,2), d DECIMAL(15,2), x DECIMAL(15,2)) '
                       'CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        try:
            cursor.execute(f'INSERT INTO {table} VALUES '
                           "(1,'A',1,1234567890123456.78,0.06,1.07),"
                           "(2,'A ',1,-10.25,0.05,1.06),(3,'B',2,NULL,NULL,NULL),"
                           "(4,NULL,NULL,0.01,0.07,1.05),(5,'é',2,-0.02,-0.03,1.04),"
                           "(6,'a',3,0.03,0.04,1.03),(7,'',0,0.04,0.05,1.02)")
            cursor.execute(f'INSERT INTO {table} VALUES (%s,%s,%s,%s,%s,%s)', (8,'A\0',4,'0.05','0.06','1.01'))

            def run(sql, gpu):
                cursor.execute('SET @sirius_gpu=%s', (int(gpu),))
                cursor.execute(sql)
                return sorted(cursor.fetchall(), key=repr)

            def check(label, body, native=True):
                sql = f'SELECT {HINT} {body}'
                expected = run(sql, False)
                offset = trace.stat().st_size
                actual = run(sql, True)
                assert actual == expected, (label, actual, expected)
                if native:
                    with trace.open() as stream:
                        stream.seek(offset)
                        events = stream.read()
                    assert 'exact=True' in events, ('No exact GPU aggregate', label, sql)
                checks.append(label)

            check('nullable_binary_strings', f'k,SUM(v),COUNT(*),COUNT(v),AVG(v) FROM {table} GROUP BY k')
            check('wide_expression_and_multiple_keys',
                  f'k,n,SUM(v*d*x),SUM(v*d),AVG(d),COUNT(x) FROM {table} GROUP BY k,n')
            check('integer_case_sum', f'k,SUM(CASE WHEN id>2 THEN 1 ELSE 0 END) FROM {table} GROUP BY k')
            check('computed_key', f'SUBSTR(k,1,1),SUM(v) FROM {table} GROUP BY SUBSTR(k,1,1)')
            check('having_sort', f'k,SUM(v) FROM {table} GROUP BY k HAVING SUM(v)>0 ORDER BY k')
            check('empty_grouped', f'k,SUM(v),COUNT(*) FROM {table} WHERE id>100 GROUP BY k')
            check('count_only', f'k,COUNT(*) FROM {table} GROUP BY k')
            check('count_nullable', f'k,COUNT(v) FROM {table} GROUP BY k')
            check('distinct_fallback', f'k,COUNT(DISTINCT v) FROM {table} GROUP BY k', False)
            check('nested_aggregates',
                  f'SUBSTR(k,1,1),SUM(s) FROM (SELECT /*+ NO_MERGE USE_HASH_AGGREGATION '
                  "OPT_PARAM('pushdown_storage_level',2) */ "
                  f'k,SUM(v) s FROM {table} GROUP BY k) t GROUP BY SUBSTR(k,1,1)')
            check('join_then_group', f'a.k,SUM(a.v*b.d),COUNT(*) FROM {table} a '
                  f'JOIN {table} b ON a.id=b.id GROUP BY a.k')
            check('join_then_scalar', f'SUM(a.v*b.d),COUNT(a.v) FROM {table} a '
                  f'JOIN {table} b ON a.id=b.id')
            check('scalar_avg', f'AVG(v),COUNT(*),COUNT(v) FROM {table}')
            check('scalar_empty_counts', f'AVG(v),COUNT(*),COUNT(v) FROM {table} WHERE id>100')
            cursor.execute(f'ALTER TABLE {table} ADD n32 INT')
            cursor.execute(f'UPDATE {table} SET n32=-n')
            check('signed_int32_key', f'n32,SUM(v) FROM {table} GROUP BY n32')
            check('date_key', f"CAST(DATE_ADD(DATE '2020-01-01', INTERVAL id DAY) AS DATE),SUM(v) "
                  f"FROM {table} GROUP BY CAST(DATE_ADD(DATE '2020-01-01', INTERVAL id DAY) AS DATE)")
            conn.begin()
            cursor.execute(f"UPDATE {table} SET v=75,k='C' WHERE id=2")
            check('own_write_group_change', f'k,SUM(v),COUNT(v) FROM {table} GROUP BY k')
            conn.rollback()
            check('rollback_group_change', f'k,SUM(v),COUNT(v) FROM {table} GROUP BY k')
            print(json.dumps({'status':'passed','checks':checks},indent=2))
        finally:
            conn.rollback()
            cursor.execute(f'DROP TABLE {table}')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--trace', required=True, type=Path)
    main(p.parse_args().trace)
