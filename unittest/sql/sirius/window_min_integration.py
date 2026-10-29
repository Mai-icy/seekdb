#!/usr/bin/env python3
"""Verify GPU window MIN preserves rows, exact decimals, NULLs and own writes."""
import argparse
import json
from pathlib import Path
import secrets
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect, identifier


def main(trace):
    table = identifier('sirius_window_' + secrets.token_hex(6))
    hint = "/*+ PARALLEL(1) OPT_PARAM('pushdown_storage_level',2) */"
    checks = []
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY, k BIGINT, '
                       'v DECIMAL(18,2), small DECIMAL(9,2), label VARCHAR(40)) '
                       'CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        try:
            cursor.execute(f'INSERT INTO {table} VALUES '
                           "(1,1,9999999999999999.99,12.34,'first'),"
                           "(2,1,-9999999999999999.99,-12.34,'é'),"
                           "(3,NULL,1.23,NULL,'null key'),(4,NULL,-4.56,2.34,'tie'),"
                           "(5,2,NULL,NULL,NULL),(6,2,NULL,NULL,'all null'),"
                           "(7,1,-9999999999999999.99,-12.34,'tie')")
            def check(label, expression, condition='', gpu=True):
                sql=f'SELECT {hint} id,k,v,small,label,{expression} FROM {table} {condition} ORDER BY id'
                cursor.execute('SET @sirius_gpu=0'); cursor.execute(sql); expected=cursor.fetchall()
                offset=trace.stat().st_size
                cursor.execute('SET @sirius_gpu=1'); cursor.execute(sql); actual=cursor.fetchall()
                assert actual == expected, (label,actual,expected)
                with trace.open() as stream:
                    stream.seek(offset); used='reduction=min' in stream.read()
                assert used == gpu, (label,'Unexpected GPU dispatch',used,sql)
                checks.append(label)
            check('nullable_exact_full_partition','MIN(v) OVER(PARTITION BY k)')
            check('int32_decimal','MIN(small) OVER(PARTITION BY k)')
            check('empty','MIN(v) OVER(PARTITION BY k)','WHERE id>100')
            check('bounded_frame_fallback', 'MIN(v) OVER(PARTITION BY k ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)',gpu=False)
            check('multiple_window_fallback','MIN(v) OVER(PARTITION BY k), MAX(v) OVER(PARTITION BY k)',gpu=False)
            conn.begin()
            cursor.execute(f'UPDATE {table} SET v=-9.99,k=NULL WHERE id=1')
            check('own_write','MIN(v) OVER(PARTITION BY k)')
            conn.rollback()
            check('rollback','MIN(v) OVER(PARTITION BY k)')
            # Multiple seekdb output batches, with all fields retained in order.
            cursor.execute(f'INSERT INTO {table} SELECT id+100, MOD(id,13), id/100, id/100, label FROM {table}')
            for _ in range(10):
                cursor.execute(f'INSERT INTO {table} SELECT id+(SELECT MAX(id) FROM {table}),k,v,small,label FROM {table}')
            check('multiple_batches','MIN(v) OVER(PARTITION BY k)')
            print(json.dumps({'status':'passed','checks':checks},indent=2))
        finally:
            conn.rollback(); cursor.execute(f'DROP TABLE {table}')


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--trace',required=True,type=Path)
    main(p.parse_args().trace)
