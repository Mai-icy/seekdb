"""Export a seekdb table as parallel primary-key ranges at one fixed snapshot.

Run on the seekdb host (or a filesystem shared with it). Rows are written by
native INTO OUTFILE; Python only coordinates SQL and verifies completed files.
Requires PyMySQL and PyArrow. Credentials use SEEKDB_HOST/PORT/USER/PASSWORD.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time

import pymysql
import pyarrow.parquet as pq


def identifier(value):
    if not value or '\0' in value:
        raise ValueError('Empty or NUL-containing SQL identifier')
    return '`' + value.replace('`', '``') + '`'


def connect(args):
    return pymysql.connect(host=os.getenv('SEEKDB_HOST', '127.0.0.1'),
                           port=int(os.getenv('SEEKDB_PORT', '2881')),
                           user=os.getenv('SEEKDB_USER', 'root'),
                           password=os.getenv('SEEKDB_PASSWORD', ''),
                           database=args.database, autocommit=True,
                           charset='utf8mb4', connect_timeout=15,
                           read_timeout=args.timeout + 30, write_timeout=30)


def configure(cursor, timeout):
    cursor.execute("SET execution_backend='cpu'")
    cursor.execute("SET parallel_degree_policy='MANUAL'")
    cursor.execute('SET _force_parallel_query_dop=1')
    cursor.execute('SET ob_query_timeout=%s', (timeout * 1000000,))


def definition(cursor, table):
    cursor.execute('SHOW CREATE TABLE ' + table)
    row = cursor.fetchone()
    if not row or not row[1].startswith('CREATE TABLE'):
        raise ValueError('Only ordinary tables are supported')
    return row[1]


def table_identity(args):
    # Read current catalog state outside the coordinator's repeatable-read view.
    # Names/SHOW CREATE alone cannot detect DROP + recreate with identical DDL.
    with connect(args) as connection, connection.cursor() as cursor:
        cursor.execute(
            'SELECT t.table_id,t.schema_version FROM oceanbase.__all_table t '
            'JOIN oceanbase.__all_database d ON t.database_id=d.database_id '
            'WHERE d.database_name=%s AND t.table_name=%s', (args.database, args.table))
        rows = cursor.fetchall()
        if len(rows) != 1:
            raise ValueError('Cannot resolve a unique source table identity')
        return dict(table_id=str(rows[0][0]), schema_version=str(rows[0][1]))


def export(args):
    destination = args.output.absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f'Output already exists: {destination}')
    # An existing parent avoids accidentally creating paths on the wrong host.
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    # Cooperative exporters cannot publish to the same name concurrently.
    lock_path = parent / ('.' + destination.name + '.export-lock')
    lock_fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    work = None
    cancelled = threading.Event()
    active = {}
    mutex = threading.Lock()
    completed = False
    destination_owned = False
    try:
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(f'Output already exists: {destination}')
        work = Path(tempfile.mkdtemp(prefix='.' + destination.name + '.export-', dir=parent))
        table = identifier(args.database) + '.' + identifier(args.table)
        with connect(args) as coordinator, coordinator.cursor() as cursor:
            configure(cursor, args.timeout)
            transaction_seconds = min(86400, args.timeout * ((args.parts or args.workers * 4) + args.workers - 1) // args.workers + 120)
            cursor.execute('SET ob_trx_timeout=%s', (transaction_seconds * 1000000,))
            cursor.execute('SET ob_trx_idle_timeout=%s', (transaction_seconds * 1000000,))
            cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            coordinator.begin()
            # Hold a read transaction predating the chosen SCN until publication.
            # Every export still explicitly requests that SCN, never a fresh one.
            cursor.execute('SELECT 1 FROM ' + table + ' LIMIT 1')
            cursor.fetchall()
            schema = definition(cursor, table)
            cursor.execute('SHOW COLUMNS FROM ' + table)
            columns = cursor.fetchall()
            cursor.execute('SHOW INDEX FROM ' + table + " WHERE Key_name='PRIMARY'")
            primary = sorted(cursor.fetchall(), key=lambda row: row[3])
            if not primary:
                raise ValueError('Range export requires an explicit primary key')
            key = primary[0][4]
            column = next(c for c in columns if c[0] == key)
            if column[1].split('(')[0].split()[0].lower() not in {'tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint'} or 'unsigned' in column[1].lower():
                raise ValueError('The leading primary-key column must be a signed integer')
            identity = table_identity(args)
            if args.snapshot is None:
                cursor.execute('SELECT CURRENT_SCN()')
                snapshot = int(cursor.fetchone()[0])
            else:
                snapshot = args.snapshot
            scan = table + ' AS OF SNAPSHOT %s'
            cursor.execute('SELECT ' + identifier(key) + ' FROM ' + scan + ' ORDER BY ' + identifier(key) + ' ASC LIMIT 1', (snapshot,))
            first = cursor.fetchone()
            cursor.execute('SELECT ' + identifier(key) + ' FROM ' + scan + ' ORDER BY ' + identifier(key) + ' DESC LIMIT 1', (snapshot,))
            last = cursor.fetchone()
            if first is None:
                if last is not None:
                    raise RuntimeError('Inconsistent snapshot bounds')
                ranges = [(None, None)] # Empty table still emits a valid schema.
            else:
                lower, upper = int(first[0]), int(last[0])
                count = min(args.parts or args.workers * 4, upper - lower + 1)
                # Integer arithmetic handles negative keys and BIGINT extrema.
                cuts = [lower + (upper - lower + 1) * i // count for i in range(count)]
                ranges = [(v, cuts[i + 1] if i + 1 < count else None) for i, v in enumerate(cuts)]
            projection = ','.join(identifier(c[0]) for c in columns)
            plan = dict(version=1, database=args.database, table=args.table,
                        snapshot=snapshot, primary_key=[r[4] for r in primary],
                        split_key=key, workers=args.workers, schema=schema, identity=identity,
                        columns=[c[0] for c in columns], ranges=ranges)
            (work / 'plan.json').write_text(json.dumps(plan, indent=2) + '\n')

            def part(index, bounds):
                if cancelled.is_set():
                    raise RuntimeError('Export cancelled')
                connection = connect(args)
                try:
                    with mutex:
                        if cancelled.is_set():
                            raise RuntimeError('Export cancelled')
                        active[connection.thread_id()] = connection
                    with connection.cursor() as x:
                        configure(x, args.timeout)
                        if cancelled.is_set():
                            raise RuntimeError('Export cancelled')
                        path = work / f'part-{index:05}.parquet'
                        sql = 'SELECT /*+ PARALLEL(1) */ ' + projection
                        sql += ' INTO OUTFILE %s FORMAT=(TYPE=\'PARQUET\') FROM ' + scan
                        parameters = [str(path), snapshot]
                        low, high = bounds
                        if low is not None:
                            sql += ' WHERE ' + identifier(key) + '>=%s'
                            parameters.append(low)
                            if high is not None:
                                sql += ' AND ' + identifier(key) + '<%s'
                                parameters.append(high)
                        started = time.monotonic()
                        x.execute(sql, parameters)
                        return dict(file=path.name, lower_inclusive=low, upper_exclusive=high,
                                    rows=x.rowcount, sql_seconds=time.monotonic() - started)
                finally:
                    with mutex:
                        active.pop(connection.thread_id(), None)
                        connection.close()

            def cancel_workers():
                cancelled.set()
                # Kill owned connections, including idle ones about to execute,
                # so cancellation cannot race with the start of an export.
                with connect(args) as admin, admin.cursor() as x, mutex:
                    for thread_id in list(active):
                        try:
                            x.execute('KILL CONNECTION %s', (thread_id,))
                        except pymysql.Error:
                            pass

            began = time.monotonic()
            executor = ThreadPoolExecutor(max_workers=args.workers)
            try:
                futures = [executor.submit(part, i, bounds) for i, bounds in enumerate(ranges)]
                parts = []
                for future in as_completed(futures):
                    parts.append(future.result())
                    print(json.dumps(dict(event='part_complete', **parts[-1])), flush=True)
            except BaseException:
                cancel_workers()
                raise
            finally:
                executor.shutdown(wait=True, cancel_futures=True)
            export_seconds = time.monotonic() - began
            if table_identity(args) != identity or definition(cursor, table) != schema:
                raise RuntimeError('Table definition changed during export')
            parts.sort(key=lambda p: p['file'])
            arrow_schema = None
            verify_began = time.monotonic()
            for part in parts:
                path = work / part['file']
                meta = pq.read_metadata(path)
                current = meta.schema.to_arrow_schema()
                if meta.num_rows != part['rows'] or current.names != plan['columns']:
                    raise RuntimeError('Exported row count or columns mismatch')
                if arrow_schema is not None and not current.equals(arrow_schema, check_metadata=False):
                    raise RuntimeError('Exported schemas differ')
                arrow_schema = current
                digest = hashlib.sha256()
                with path.open('rb') as file:
                    for block in iter(lambda: file.read(8 * 1024 * 1024), b''):
                        digest.update(block)
                part.update(bytes=path.stat().st_size, sha256=digest.hexdigest())
            manifest = dict(plan, complete=True, parts=parts,
                            rows=sum(p['rows'] for p in parts),
                            bytes=sum(p['bytes'] for p in parts),
                            export_seconds=export_seconds,
                            verification_seconds=time.monotonic() - verify_began)
            with (work / 'manifest.json').open('x') as file:
                json.dump(manifest, file, indent=2)
                file.write('\n'); file.flush(); os.fsync(file.fileno())
            # Publish only after all workers have stopped and all files validate.
            if destination.exists() or destination.is_symlink():
                raise FileExistsError(f'Output already exists: {destination}')
            coordinator.rollback()
            destination.mkdir(mode=0o700) # Exclusive creation; never replace another directory.
            destination_owned = True
            # manifest.json is the completion marker. Move it last so consumers
            # can never mistake a partially published directory for a snapshot.
            for path in work.iterdir():
                if path.name != 'manifest.json':
                    os.rename(path, destination / path.name)
            os.rename(work / 'manifest.json', destination / 'manifest.json')
            work.rmdir()
            completed = True
            print(json.dumps(dict(event='complete', output=str(destination), rows=manifest['rows'],
                                  files=len(parts), export_seconds=export_seconds,
                                  verification_seconds=manifest['verification_seconds'])), flush=True)
            return manifest
    finally:
        os.close(lock_fd)
        lock_path.unlink()
        if not completed:
            if destination_owned:
                shutil.rmtree(destination)
            if work is not None and work.exists():
                shutil.rmtree(work)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--table', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--snapshot', type=int, help='Explicit shared SCN for a coordinated multi-table export; never falls back if expired')
    parser.add_argument('--parts', type=int, help='Number of key ranges; default: workers * 4')
    parser.add_argument('--timeout', type=int, default=1800, help='Seconds allowed for each SQL statement')
    args = parser.parse_args()
    if not 1 <= args.workers <= 16 or (args.parts is not None and not 1 <= args.parts <= 4096) or not 1 <= args.timeout <= 86400:
        parser.error('Require workers 1..16, parts 1..4096, timeout 1..86400')
    if args.snapshot is not None and not 0 < args.snapshot < 2**64:
        parser.error('snapshot must be a positive uint64 SCN')
    export(args)


if __name__ == '__main__':
    main()
