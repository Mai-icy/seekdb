#!/usr/bin/env python3
"""Import full TPC-H relations from read-only DuckDB using bounded disposable CSVs."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
import os
from pathlib import Path
import shutil
import sys
import threading
import time

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect, identifier

KEYS = {
    'region': ('r_regionkey',), 'nation': ('n_nationkey',),
    'supplier': ('s_suppkey',), 'customer': ('c_custkey',),
    'part': ('p_partkey',), 'partsupp': ('ps_partkey', 'ps_suppkey'),
    'orders': ('o_orderkey',), 'lineitem': ('l_orderkey', 'l_linenumber'),
}
SPANS = {'region': 100000, 'nation': 100000, 'supplier': 400000, 'customer': 200000,
         'part': 250000, 'partsupp': 100000, 'orders': 1000000, 'lineitem': 500000}


def main(args):
    root, source = Path(args.directory).resolve(), Path(args.source).resolve()
    source_info = source.stat()
    identity = {'source': str(source), 'source_bytes': source_info.st_size,
                'source_mtime_ns': source_info.st_mtime_ns, 'target_database': args.database,
                'host': os.getenv('SEEKDB_HOST', '127.0.0.1'),
                'port': int(os.getenv('SEEKDB_PORT', '2881'))}
    config = {'threads': 8, 'memory_limit': '4GB', 'max_temp_directory_size': '0B'}
    def source_connection():
        return duckdb.connect(str(source), read_only=True, config=config)
    def save(path, data):
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(data, indent=2, default=str) + '\n')
        temporary.replace(path)
    job_path = root / 'import-job.json'
    if job_path.exists():
        job = json.loads(job_path.read_text())
        if job['identity'] != identity:
            raise ValueError('Import source/target identity mismatch')
    else:
        job = {'identity': identity, 'initial_free_bytes': shutil.disk_usage(root).free}
        save(job_path, job)
    def space_check():
        free = shutil.disk_usage(root).free
        if free < args.reserve_gib * 2**30 or job['initial_free_bytes'] - free > (args.growth_gib - 4) * 2**30:
            raise RuntimeError('Disk reserve/growth guard reached; manifest and imported rows preserved')
    def configure(cursor):
        cursor.execute('SET ob_query_timeout=1800000000')
        cursor.execute('SET ob_trx_timeout=1800000000')
    with source_connection() as db, connect() as conn, conn.cursor() as cur:
        configure(cur)
        cur.execute(f'CREATE DATABASE IF NOT EXISTS {identifier(args.database)} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for table in args.tables:
                path = root / f'{table}-import.json'
                target = identifier(args.database) + '.' + identifier(table)
                columns = [(row[0], row[1]) for row in db.execute(f'DESCRIBE {table}').fetchall()]
                definitions = []
                for name, kind in columns:
                    if kind == 'VARCHAR': mapped = 'VARCHAR(256)'
                    elif kind in ('BIGINT', 'INTEGER', 'DATE') or kind.startswith('DECIMAL('): mapped = kind
                    else: raise ValueError(f'Unsupported source type: {kind}')
                    definitions.append(f'{identifier(name)} {mapped}')
                key = KEYS[table][0]
                pk = ','.join(identifier(c) for c in KEYS[table])
                low, high, count = db.execute(f'SELECT min({key}),max({key}),count(*) FROM {table}').fetchone()
                span = args.key_span or SPANS[table]
                if path.exists():
                    report = json.loads(path.read_text())
                    if report['identity'] != identity:
                        raise ValueError('Table resume identity mismatch')
                else:
                    report = {'identity': identity, 'table': table, 'columns': columns,
                              'key_span': span, 'source_rows': count, 'chunks': [],
                              'created': False, 'verified': False}
                    # CREATE never adopts a pre-existing unowned table.
                    cur.execute(f'CREATE TABLE {target} ({",".join(definitions)}, PRIMARY KEY ({pk}))')
                    report['created'] = True
                    save(path, report)
                # Complement recorded ranges, including out-of-order completion.
                # This also permits a different bounded span on resume.
                pending, position = [], low
                for chunk in sorted(report['chunks'], key=lambda c: c['start']) + [{'start': high + 1, 'end': high + 1}]:
                    start, end = chunk['start'], chunk['end']
                    if start < position or end < start or end > high + 1:
                        raise ValueError('Invalid or overlapping recorded chunk layout')
                    pending.extend((left, min(left + span, start)) for left in range(position, start, span))
                    position = end
                if report.get('verified'):
                    print(json.dumps({'table': table, 'status': 'already_verified', 'rows': count}), flush=True)
                    continue
                for csv in root.glob(f'{table}-chunk-*.csv'):
                    csv.unlink()
                lock = threading.Lock()
                select = ','.join(f'hex({name})' if kind == 'VARCHAR' else name for name, kind in columns)
                fields = ','.join(f'@v{i}' if kind == 'VARCHAR' else identifier(name) for i, (name, kind) in enumerate(columns))
                setters = ','.join(f'{identifier(name)}=UNHEX(@v{i})' for i, (name, kind) in enumerate(columns) if kind == 'VARCHAR')
                def load(bounds):
                    start, end = bounds
                    space_check()
                    csv = root / f'{table}-chunk-{start:012d}.csv'
                    begin = time.perf_counter()
                    try:
                        with source_connection() as reader:
                            query = f'SELECT {select} FROM {table} WHERE {key}>={start} AND {key}<{end}'
                            quoted = str(csv).replace("'", "''")
                            rows = reader.execute(f"COPY ({query}) TO '{quoted}' (FORMAT CSV, HEADER false, DELIMITER '|', NULL '\\N')").fetchone()[0]
                            sample = reader.execute(f'SELECT * FROM {table} WHERE {key}>={start} AND {key}<{end} ORDER BY {pk} LIMIT 8'.replace('`', '"')).fetchall()
                        exported = time.perf_counter() - begin
                        size = csv.stat().st_size
                        if size > 128 * 2**20:
                            raise RuntimeError('CSV exceeds 128 MiB; choose a smaller key span in a fresh import')
                        started = time.perf_counter()
                        with connect(local_infile=True) as writer, writer.cursor() as cursor:
                            configure(cursor)
                            # Only an unrecorded range in this manifest-owned target is retried.
                            cursor.execute(f'DELETE FROM {target} WHERE {identifier(key)}>=%s AND {identifier(key)}<%s', (start, end))
                            if rows:
                                sql = (f"LOAD DATA /*+ NO_DIRECT */ LOCAL INFILE %s INTO TABLE {target} "
                                       f"FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' ({fields})")
                                if setters: sql += ' SET ' + setters
                                cursor.execute(sql, (str(csv),))
                                affected = cursor.rowcount
                                cursor.execute('SHOW WARNINGS')
                                warnings = cursor.fetchall()
                                if warnings or affected != rows:
                                    raise RuntimeError(f'LOAD discrepancy: rows={rows} affected={affected} warnings={warnings}')
                            cursor.execute(f'SELECT COUNT(*) FROM {target} WHERE {identifier(key)}>=%s AND {identifier(key)}<%s', (start, end))
                            if cursor.fetchone()[0] != rows:
                                raise RuntimeError('Loaded range count mismatch')
                            cursor.execute(f'SELECT * FROM {target} WHERE {identifier(key)}>=%s AND {identifier(key)}<%s ORDER BY {pk} LIMIT 8', (start, end))
                            if sample != list(cursor.fetchall()):
                                raise RuntimeError('Imported row sample differs from DuckDB')
                        item = {'start': start, 'end': end, 'rows': rows, 'csv_bytes': size,
                                'export_seconds': exported, 'load_seconds': time.perf_counter() - started,
                                'free_bytes': shutil.disk_usage(root).free}
                        with lock:
                            report['chunks'].append(item)
                            report['chunks'].sort(key=lambda c: c['start'])
                            save(path, report)
                            print(json.dumps({'table': table, **item}), flush=True)
                    finally:
                        csv.unlink(missing_ok=True)
                incomplete = bool(args.max_chunks and len(pending) > args.max_chunks)
                if args.max_chunks:
                    pending = pending[:args.max_chunks]
                for i in range(0, len(pending), args.workers):
                    if (root / 'PAUSE').exists():
                        print(json.dumps({'table': table, 'status': 'paused_at_chunk_boundary'}), flush=True)
                        return
                    round_start = time.perf_counter()
                    previous_rows = sum(c['rows'] for c in report['chunks'])
                    futures = [pool.submit(load, bounds) for bounds in pending[i:i + args.workers]]
                    errors = []
                    for future in futures:
                        try: future.result()
                        except Exception as error: errors.append(error)
                    if errors: raise errors[0]
                    loaded_rows = sum(c['rows'] for c in report['chunks'])
                    progress = {'table': table, 'loaded_rows': loaded_rows, 'source_rows': count,
                                'round_rows': loaded_rows - previous_rows,
                                'round_seconds': time.perf_counter() - round_start,
                                'workers': args.workers, 'free_bytes': shutil.disk_usage(root).free}
                    save(root / 'progress.json', progress)
                    print(json.dumps({'status': 'progress', **progress}), flush=True)
                if incomplete:
                    print(json.dumps({'table': table, 'status': 'partial', 'chunks': len(report['chunks'])}), flush=True)
                    continue
                # Full-column aggregate fingerprints plus exact boundary row samples.
                expressions = ['COUNT(*)']
                for name, kind in columns:
                    if kind == 'VARCHAR': expression = f'SUM(LENGTH({name}))'
                    elif kind == 'DATE': expression = f'SUM(EXTRACT(YEAR FROM {name})*10000+EXTRACT(MONTH FROM {name})*100+EXTRACT(DAY FROM {name}))'
                    else: expression = f'SUM({name})'
                    expressions.append(expression)
                    expressions.append(f'COUNT({name})')
                projection = ','.join(expressions)
                expected = db.execute(f'SELECT {projection} FROM {table}').fetchone()
                cur.execute(f'SELECT /*+ PARALLEL(8) */ {projection} FROM {target}')
                actual = cur.fetchone()
                if expected != actual:
                    raise RuntimeError(f'Full table fingerprint mismatch: {table}, {expected}, {actual}')
                for direction in ('ASC', 'DESC'):
                    order = ','.join(f'{c} {direction}' for c in KEYS[table])
                    expected_rows = db.execute(f'SELECT * FROM {table} ORDER BY {order} LIMIT 16').fetchall()
                    cur.execute(f'SELECT * FROM {target} ORDER BY {order} LIMIT 16')
                    if expected_rows != list(cur.fetchall()):
                        raise RuntimeError(f'Boundary row mismatch: {table}, {direction}')
                final = source.stat()
                if (final.st_size, final.st_mtime_ns) != (source_info.st_size, source_info.st_mtime_ns):
                    raise RuntimeError('Read-only source fingerprint changed')
                report['verified'] = True
                report['fingerprint_expressions'] = expressions
                report['source_fingerprint'] = expected
                report['final_free_bytes'] = shutil.disk_usage(root).free
                save(path, report)
                print(json.dumps({'table': table, 'status': 'verified', 'rows': count}), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', required=True)
    p.add_argument('--directory', required=True)
    p.add_argument('--database', default='tpch500_sirius')
    p.add_argument('--tables', nargs='+', choices=tuple(KEYS), default=list(KEYS))
    p.add_argument('--workers', type=int, choices=(1, 2, 4, 8, 16, 32), default=4)
    p.add_argument('--key-span', type=int, default=0, help='0 uses per-table bounded spans')
    p.add_argument('--reserve-gib', type=int, default=180)
    p.add_argument('--growth-gib', type=int, default=420)
    p.add_argument('--max-chunks', type=int, default=0, help='Bound a smoke import; 0 imports every range')
    args = p.parse_args()
    if not 0 <= args.key_span <= 1000000 or args.reserve_gib < 1 or args.growth_gib < 8 or args.max_chunks < 0:
        p.error('Invalid chunk/disk limits')
    root = Path(args.directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'import.lock').open('a') as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        main(args)
