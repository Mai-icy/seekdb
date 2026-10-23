#!/usr/bin/env python3
"""Validate all 22 native seekdb SQL queries against a read-only DuckDB oracle."""
import argparse
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import time

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect, identifier
from prepare_tpch import KEYS


def ordered(rows):
    def key(row):
        return tuple((0, '') if x is None else (1, Decimal(str(x))) if isinstance(x, (int, float, Decimal)) else (2, str(x)) for x in row)
    return sorted(rows, key=key)


def compare(expected, actual):
    if len(expected) != len(actual):
        raise AssertionError(f'Row count mismatch: {len(expected)} != {len(actual)}')
    for number, (left, right) in enumerate(zip(ordered(expected), ordered(actual))):
        if len(left) != len(right): raise AssertionError('Column count mismatch')
        for a, b in zip(left, right):
            if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
                valid = math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-6) if isinstance(a, float) or isinstance(b, float) else a == b
            else: valid = a == b
            if not valid:
                raise AssertionError(f'Value mismatch at sorted row {number}: {a!r} != {b!r}')


def main(args):
    root = Path(args.directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    source = Path(args.source).resolve()
    source_info = source.stat()
    host, port = os.getenv('SEEKDB_HOST', '127.0.0.1'), int(os.getenv('SEEKDB_PORT', '2881'))
    manifests = Path(args.import_directory)
    for table in KEYS:
        report = json.loads((manifests / f'{table}-import.json').read_text())
        if (not report.get('verified') or report['identity']['source'] != str(source)
                or report['identity']['source_bytes'] != source_info.st_size
                or report['identity']['source_mtime_ns'] != source_info.st_mtime_ns
                or report['identity']['target_database'] != args.database
                or report['identity']['host'] != host or report['identity']['port'] != port):
            raise RuntimeError(f'Require verified matching full import: {table}')
    summary_path = root / 'summary.json'
    summary = {'source': str(source), 'source_bytes': source_info.st_size,
               'source_mtime_ns': source_info.st_mtime_ns, 'scale_factor': args.scale_factor,
               'host': host, 'port': port,
               'database': args.database, 'modes': ['cpu8', 'native1'], 'queries': [],
               'comparison': 'row multiset; exact non-floating numerics, float rel=1e-9 abs=1e-6; ordering not compared',
               'timing': 'SQL execute/fetch; excludes sorting and oracle generation'}
    if args.resume and summary_path.exists():
        prior = json.loads(summary_path.read_text())
        for key in ('source', 'source_bytes', 'source_mtime_ns', 'scale_factor', 'host', 'port', 'database', 'modes'):
            if summary[key] != prior[key]: raise ValueError(f'Resume identity mismatch: {key}')
        summary = prior
    def save():
        temporary = summary_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(summary, indent=2, default=str) + '\n')
        temporary.replace(summary_path)
    with duckdb.connect(str(source), read_only=True,
                        config={'threads': 16, 'memory_limit': args.oracle_memory,
                                'temp_directory': str(root / 'oracle-spill'),
                                'max_temp_directory_size': '32GB'}) as db, connect(read_timeout=args.query_timeout + 60) as conn, conn.cursor() as cursor:
        cursor.execute(f'USE {identifier(args.database)}')
        cursor.execute('SET ob_query_timeout=%s', (args.query_timeout * 1000000,))
        cursor.execute('SET ob_trx_timeout=%s', (args.query_timeout * 1000000,))
        for number in args.queries:
            old = next((q for q in summary['queries'] if q['query'] == number), None)
            sql = (Path(__file__).with_name('tpch_queries') / f'q{number:02d}.sql').read_text().strip().rstrip(';')
            if number == 11:
                sql = sql.replace('0.0001000000', format(Decimal('0.0001') / Decimal(str(args.scale_factor)), 'f'))
            sql_hash = hashlib.sha256(sql.encode()).hexdigest()
            if args.resume and old and old.get('passed') and old.get('sql_sha256') == sql_hash: continue
            item = {'query': number, 'sql_sha256': sql_hash, 'modes': {}, 'passed': False}
            summary['queries'] = [q for q in summary['queries'] if q['query'] != number] + [item]
            try:
                started = time.perf_counter()
                expected = db.execute(sql).fetchall()
                item['oracle_seconds'] = time.perf_counter() - started
                item['oracle_rows'] = len(expected)
                item['oracle_sample'] = expected[:10]
                for mode in summary['modes']:
                    cursor.execute('SET @sirius_gpu=%s', (int(mode == 'native1'),))
                    query = re.sub(r'\bSELECT\b', 'SELECT /*+ PARALLEL(%d) */' % (1 if mode == 'native1' else 8), sql, flags=re.I)
                    cursor.execute('EXPLAIN ' + query)
                    plan = [str(row) for row in cursor.fetchall()]
                    log = Path(args.sidecar_log)
                    offset = log.stat().st_size if log.exists() else 0
                    started = time.perf_counter()
                    cursor.execute(query)
                    actual = cursor.fetchall()
                    seconds = time.perf_counter() - started
                    compare(expected, actual)
                    events = []
                    if log.exists():
                        with log.open() as stream:
                            stream.seek(offset)
                            events = [line.strip() for line in stream if line.startswith('native aggregate:')]
                    item['modes'][mode] = {'seconds': seconds, 'rows': len(actual), 'validated': True,
                                           'native_gpu_aggregates': events, 'used_gpu': bool(events), 'explain': plan}
                    save()
                    print(json.dumps({'query': number, 'mode': mode, 'seconds': seconds,
                                      'rows': len(actual), 'gpu_aggregates': len(events), 'status': 'passed'}), flush=True)
                item['passed'] = True
            except Exception as error:
                item['error'] = repr(error)
                print(json.dumps({'query': number, 'status': 'failed', 'error': repr(error)}), flush=True)
            save()
    final = source.stat()
    if (source_info.st_size, source_info.st_mtime_ns) != (final.st_size, final.st_mtime_ns):
        raise RuntimeError('Read-only source fingerprint changed')
    failed = [q['query'] for q in summary['queries'] if not q['passed']]
    if failed: raise RuntimeError(f'Queries requiring correction: {failed}')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', required=True)
    p.add_argument('--database', required=True)
    p.add_argument('--import-directory', required=True)
    p.add_argument('--directory', required=True)
    p.add_argument('--sidecar-log', required=True)
    p.add_argument('--scale-factor', type=float, required=True)
    p.add_argument('--oracle-memory', default='128GB')
    p.add_argument('--query-timeout', type=int, default=1800)
    p.add_argument('--queries', nargs='+', type=int, default=list(range(1, 23)))
    p.add_argument('--resume', action='store_true')
    args = p.parse_args()
    if args.scale_factor <= 0 or not 1 <= args.query_timeout <= 3600 or any(q < 1 or q > 22 for q in args.queries): p.error('Invalid scale factor, timeout or query number')
    main(args)
