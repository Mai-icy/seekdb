#!/usr/bin/env python3
"""Benchmark native decimal SUM over seekdb rows; DuckDB is only an oracle."""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect, identifier


def main(args):
    table = identifier(args.database) + '.lineitem'
    q6 = (Path(__file__).parent / 'tpch_queries/q06.sql').read_text().strip().rstrip(';')
    cases = {'q6': q6, 'full_decimal_sum': 'SELECT SUM(l_extendedprice) FROM lineitem'}
    report = {'database': args.database, 'status': 'running', 'cases': [],
              'timing': 'TCP SQL wall time including seekdb scan, expressions, transport and GPU SUM; excludes startup/pin allocation',
              'gpu_scope': 'SUM only; scan/filter/argument expressions remain in seekdb',
              'batch_rows': args.batch_rows}
    try:
        report['gpu_processes'] = subprocess.check_output(
            ['nvidia-smi', '--query-compute-apps=pid,used_memory', '--format=csv,noheader'], text=True)
    except (OSError, subprocess.CalledProcessError):
        report['gpu_processes'] = 'unavailable'

    def save():
        tmp = args.output.with_suffix('.tmp')
        tmp.write_text(json.dumps(report, indent=2, default=str) + '\n')
        tmp.replace(args.output)

    with duckdb.connect(str(args.source), read_only=True,
                        config={'threads': 8, 'memory_limit': '4GB'}) as oracle:
        expected = {name: oracle.execute(sql).fetchall() for name, sql in cases.items()}
        report['source_rows'] = oracle.execute('SELECT COUNT(*) FROM lineitem').fetchone()[0]
    modes = ['cpu1_same_shape', 'cpu8_same_shape', 'cpu8_default', 'gpu_native']
    try:
        with connect(read_timeout=1860) as conn, conn.cursor() as cursor:
            cursor.execute('SET ob_query_timeout=1800000000')
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            report['seekdb_rows'] = cursor.fetchone()[0]
            assert report['seekdb_rows'] == report['source_rows'], 'Incomplete seekdb import'
            cursor.execute("SHOW PARAMETERS WHERE name IN ('memory_budget','kvcache_memory_limit',"
                           "'memstore_memory_limit','server_cpu_quota_min','server_cpu_quota_max')")
            report['parameters'] = cursor.fetchall()
            for name, base in cases.items():
                item = {'name': name, 'oracle': expected[name], 'first_seconds': {},
                        'seconds': {mode: [] for mode in modes}, 'explain': {}, 'gpu_events': []}
                report['cases'].append(item)

                def run(mode):
                    gpu = mode == 'gpu_native'
                    parallel = 8 if mode.startswith('cpu8') else 1
                    hints = f"PARALLEL({parallel}) OPT_PARAM('rowsets_max_rows',{args.batch_rows})"
                    if mode != 'cpu8_default':
                        hints += " OPT_PARAM('pushdown_storage_level',2)"
                    sql = base.replace('SELECT', 'SELECT /*+ ' + hints + ' */', 1).replace('FROM lineitem', 'FROM '+table)
                    # Q6 has a newline between FROM and lineitem.
                    sql = sql.replace('FROM\n    lineitem', 'FROM '+table)
                    cursor.execute('SET @sirius_gpu=%s', (int(gpu),))
                    if mode not in item['explain']:
                        cursor.execute('EXPLAIN '+sql)
                        item['explain'][mode] = cursor.fetchall()
                    offset = args.sidecar_log.stat().st_size
                    started = time.perf_counter()
                    cursor.execute(sql)
                    actual = list(cursor.fetchall())
                    elapsed = time.perf_counter() - started
                    assert actual == expected[name], (name, mode, actual, expected[name])
                    if gpu:
                        with args.sidecar_log.open() as stream:
                            stream.seek(offset)
                            events = stream.read().splitlines()
                        assert any('native aggregate:' in e and 'exact=True' in e for e in events), 'Missing GPU execution'
                        item['gpu_events'].append(events)
                    print(json.dumps({'case': name, 'mode': mode, 'seconds': elapsed}), flush=True)
                    return elapsed

                for mode in modes:
                    item['first_seconds'][mode] = run(mode)
                    save()
                for repeat in range(args.repeats):
                    offset = repeat % len(modes)
                    for mode in modes[offset:] + modes[:offset]:
                        item['seconds'][mode].append(run(mode))
                        save()
                item['median_seconds'] = {mode: statistics.median(times) for mode,times in item['seconds'].items()}
                item['speedup'] = {mode: value/item['median_seconds']['gpu_native']
                                   for mode,value in item['median_seconds'].items() if mode != 'gpu_native'}
                save()
        report['status'] = 'passed'
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        save()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database', required=True)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--sidecar-log', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--batch-rows', type=int, default=4096)
    args = p.parse_args()
    if args.repeats < 3 or not 1 <= args.batch_rows <= 4096:
        p.error('Require at least three repeats and batch rows in [1,4096]')
    main(args)
