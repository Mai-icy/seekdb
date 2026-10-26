#!/usr/bin/env python3
"""Compare native DuckDB/Sirius, live seekdb input and fixed resident input.

Use only on an isolated seekdb test instance with all other queries paused.
The resident path is a benchmark-only immutable snapshot, not a product cache.
"""
import argparse
import contextlib
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import resource
import shutil
import signal
import socket
import stat
import statistics
import struct
import subprocess
import sys
import threading
import time

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect, identifier
from sidecar import Backend, PinnedInput, handle
from wire import send
from resident_input import ResidentInput, RecordingBackend

CASES = {'sum_count': 'grp,SUM(amount),COUNT(*)', 'count': 'grp,COUNT(*)'}


def provenance(args):
    files = {}
    for name in ('extension', 'library'):
        value = getattr(args, name, None)
        if value:
            path = Path(value).resolve(strict=True)
            with path.open('rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            files[name] = {'path': str(path), 'sha256': digest}
    config = os.environ.get('SIRIUS_CONFIG_FILE') or os.environ.get('SEEKDB_SIRIUS_CONFIG')
    return {'started_utc': datetime.now(timezone.utc).isoformat(), 'pid': os.getpid(),
            'host': socket.gethostname(), 'cpu_affinity': sorted(os.sched_getaffinity(0)),
            'cuda_visible_devices': os.environ.get('CUDA_VISIBLE_DEVICES'),
            'duckdb_version': duckdb.__version__, 'binaries': files,
            'config_path': config, 'config': Path(config).read_text() if config else None,
            'gpu_processes_at_start': subprocess.check_output(
                ['nvidia-smi', '--query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory',
                 '--format=csv'], text=True)}


@contextlib.contextmanager
def telemetry(output):
    # Keep evidence of other tenants' GPU activity on this shared machine.
    with Path(str(output) + '.gpu.log').open('w') as stream:
        process = subprocess.Popen(['nvidia-smi', 'pmon', '-s', 'um', '-o', 'DT', '-d', '1'],
                                   stdout=stream, stderr=subprocess.STDOUT)
        try:
            yield
        finally:
            process.terminate()
            process.wait(timeout=10)


def save(path, report):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, indent=2, default=str) + '\n')
    temporary.replace(path)


def normalize(rows):
    return sorted([list(row) for row in rows], key=repr)


def measured(call, expected):
    start = time.perf_counter()
    result = call()
    elapsed = time.perf_counter() - start
    if isinstance(result, (bytes, str)):
        result = json.loads(result)['rows']
    if normalize(result) != normalize(expected):
        raise AssertionError('Result differs from the exact source oracle')
    return elapsed


def summary(times, first, preparation=0):
    median = statistics.median(times)
    return {'seconds': times, 'median_seconds': median, 'first_seconds': first,
            'preparation_seconds': preparation,
            'estimated_total_seconds': {str(n): preparation + first + (n - 1) * median for n in (1, 5, 10)},
            'total_note': 'estimate using measured first call and hot median; engine startup reported separately'}


def prepare(args):
    source = Path(args.source).resolve(strict=True)
    target = Path(args.projection).resolve()
    if target.exists():
        raise FileExistsError('Refuse to overwrite a projection; reuse its metadata or choose a new path')
    if shutil.disk_usage(target.parent).free < 180 * 1024**3:
        raise RuntimeError('180 GiB filesystem reserve reached')
    before = source.stat()
    soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    ceiling = min([2 * 1024**3] + [x for x in (soft, hard) if x != resource.RLIM_INFINITY])
    resource.setrlimit(resource.RLIMIT_FSIZE, (ceiling, hard))
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    started = time.perf_counter()
    with duckdb.connect(str(target), config={'threads': 8, 'memory_limit': '8GB',
                                            'max_temp_directory_size': '0B'}) as db:
        escaped = str(source).replace("'", "''")
        db.execute(f"ATTACH '{escaped}' AS source_db (READ_ONLY)")
        db.execute('CREATE TABLE bench AS SELECT CAST(c_nationkey AS BIGINT) grp, '
                   'CAST(CAST(c_acctbal * 100 AS BIGINT) AS DOUBLE) amount FROM source_db.customer')
        db.execute('CREATE TABLE warmup AS SELECT 0::BIGINT grp, 0::DOUBLE amount')
        db.execute('CHECKPOINT')
        prepared = time.perf_counter() - started
        expected = {name: db.execute(f'SELECT {expr} FROM bench GROUP BY grp').fetchall()
                    for name, expr in CASES.items()}
        # Independently validate the materialized projection against exact
        # integer cents from the original DECIMAL source.
        source_rows = db.execute('SELECT c_nationkey,SUM(CAST(c_acctbal*100 AS BIGINT)),COUNT(*) '
                                 'FROM source_db.customer GROUP BY c_nationkey').fetchall()
        assert normalize(expected['sum_count']) == normalize(source_rows)
    after = source.stat()
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    report = {'source': str(source), 'source_bytes': before.st_size, 'source_mtime_ns': before.st_mtime_ns,
              'projection': str(target), 'projection_bytes': target.stat().st_size,
              'prepare_seconds': prepared, 'expected': expected,
              'rows': sum(row[-1] for row in expected['count']), 'schema': ['BIGINT', 'DOUBLE cents'],
              'note': 'Temporary two-column file required by the native DuckDB scanner; delete after the experiment'}
    save(Path(args.output), report)
    print(json.dumps({k: v for k, v in report.items() if k != 'expected'}), flush=True)


def native(args, oracle):
    started = time.perf_counter()
    db = duckdb.connect(args.projection, read_only=True,
                        config={'threads': 8, 'memory_limit': '8GB', 'max_temp_directory_size': '0B',
                                'allow_unsigned_extensions': 'true'})
    db.execute("LOAD '" + str(Path(args.extension).resolve()).replace("'", "''") + "'")
    db.execute('SET enable_duckdb_fallback=false')
    db.execute('SET gpu_execution=true')
    db.execute('SET pin_table_compression=false')
    db.execute('SET gpu_execution=false')
    settings = dict(db.execute("SELECT name,value FROM duckdb_settings() WHERE name IN "
                              "('scan_task_batch_size','threads','enable_duckdb_fallback',"
                              "'pin_table_compression','memory_limit')").fetchall())
    db.execute('SET gpu_execution=true')
    # Warm the engine with a real query, not the measured full-table query.
    db.execute('SELECT grp,SUM(amount),COUNT(*) FROM warmup GROUP BY grp').fetchall()
    startup_seconds = time.perf_counter() - started
    report = {'path': 'duckdb_sirius', 'startup_warmup_seconds': startup_seconds,
              'provenance': provenance(args),
              'rows': oracle['rows'], 'gpu_execution': True, 'duckdb_fallback': False,
              'pin_compression': False, 'threads': 8, 'cases': {},
              'effective_settings': settings,
              'timing': 'DuckDB execute/fetch; result normalization excluded; warm OS caches, not cold storage'}
    try:
        for name, expressions in CASES.items():
            sql = f'SELECT {expressions} FROM bench GROUP BY grp'
            expected = oracle['expected'][name]
            cases = report['cases'][name] = {}
            db.execute('SET gpu_execution=false')
            for threads in (1, 8):
                db.execute(f'SET threads={threads}')
                first = measured(lambda: db.execute(sql).fetchall(), expected)
                times = [measured(lambda: db.execute(sql).fetchall(), expected) for _ in range(args.repeats)]
                cases[f'cpu{threads}'] = summary(times, first)
            db.execute('SET gpu_execution=true')
            for pinned in (False, True):
                prepare_seconds = 0
                if pinned:
                    columns = "['grp','amount']" if name == 'sum_count' else "['grp']"
                    start = time.perf_counter()
                    assert db.execute(f"CALL pin_table(format='duckdb',tier='host',name='bench',cols={columns})").fetchone()[0]
                    prepare_seconds = time.perf_counter() - start
                try:
                    first = measured(lambda: db.execute(sql).fetchall(), expected)
                    times = [measured(lambda: db.execute(sql).fetchall(), expected) for _ in range(args.repeats)]
                    result = summary(times, first, prepare_seconds)
                    result['projection_prepare_seconds'] = oracle['prepare_seconds']
                    result['estimated_total_including_projection'] = {
                        n: value + oracle['prepare_seconds'] for n, value in result['estimated_total_seconds'].items()}
                    cases['host_pinned' if pinned else 'live_scan'] = result
                    report['process_peak_rss_kib'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    save(Path(args.output), report)
                    print(json.dumps({'case': name, 'pinned': pinned, 'median': result['median_seconds']}), flush=True)
                finally:
                    if pinned:
                        db.execute("CALL unpin_table('bench')")
    finally:
        db.close()


@contextlib.contextmanager
def benchmark_socket(path, backend):
    """Temporarily route only the isolated instance to the benchmark backend.

    Rename preserves the original live listener. A crash leaves the explicit
    .comparison-backup socket for operator recovery; never overwrite it.
    """
    path = Path(path)
    backup = path.with_name(path.name + '.comparison-backup')
    directory = path.parent.stat()
    if directory.st_uid != os.geteuid() or stat.S_IMODE(directory.st_mode) & 0o077:
        raise ValueError('Require an owned 0700 socket directory')
    if backup.exists() or backup.is_symlink():
        raise RuntimeError('A previous socket backup exists; restore it before running')
    original = path.lstat()
    if not stat.S_ISSOCK(original.st_mode) or original.st_uid != os.geteuid():
        raise ValueError('Require an existing owned sidecar socket')
    listener = socket.socket(socket.AF_UNIX)
    stopped = threading.Event()
    path.rename(backup)
    bound_inode = None
    worker = None
    try:
        listener.bind(str(path))
        bound_inode = path.stat().st_ino
        os.chmod(path, 0o600)
        listener.listen(8)
        listener.settimeout(0.25)
        def serve():
            while not stopped.is_set():
                try:
                    peer, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if stopped.is_set(): return
                    raise
                with peer:
                    _, uid, _ = struct.unpack('3i', peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                    if uid != os.geteuid(): continue
                    peer.settimeout(60)
                    try: handle(peer, backend)
                    except Exception as error:
                        try: send(peer, b'X', str(error).encode()[:4096])
                        except OSError: pass
        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        yield
    finally:
        stopped.set()
        listener.close()
        if worker is not None:
            # GPU execution may outlive the socket deadline. Do not release
            # pinned buffers until the final reader has actually returned.
            worker.join()
        if path.exists():
            if path.stat().st_ino != bound_inode:
                raise RuntimeError('Socket changed ownership; leave backup intact')
            path.unlink()
        backup.rename(path)


def seekdb(args, oracle):
    if os.environ.get('SEEKDB_SIRIUS_PROFILE') == '1':
        raise ValueError('Disable bridge profiling for measured calls; diagnostics run separately')
    pause = Path(args.tpch_pause)
    if not pause.exists():
        raise RuntimeError('Pause the full TPC-H runner before comparison')
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute('SHOW FULL PROCESSLIST')
        if any(row[4] == 'Query' and 'SHOW FULL PROCESSLIST' not in str(row[7]) for row in cursor.fetchall()):
            raise RuntimeError('Other seekdb queries are running')
        settings = {}
        for name in ('memory_budget', 'memstore_memory_limit', 'kvcache_memory_limit',
                     'datafile_maxsize', 'log_disk_size', 'freeze_trigger_percentage'):
            cursor.execute('SHOW PARAMETERS LIKE %s', (name,))
            settings[name] = cursor.fetchone()[3]
        cursor.execute("SHOW GLOBAL VARIABLES LIKE 'ob_sql_work_area_percentage'")
        settings['ob_sql_work_area_percentage'] = cursor.fetchone()[1]
    started = time.perf_counter()
    real = Backend(str(Path(args.library).resolve()))
    real.warmup()
    startup_seconds = time.perf_counter() - started
    report = {'path': 'seekdb_live_and_resident', 'rows': oracle['rows'], 'cases': {},
              'provenance': provenance(args),
              'seekdb_settings': settings,
              'startup_warmup_seconds': startup_seconds,
              'frame_mib': 64, 'fragment_mib': 128, 'resident_capacity_mib': 2048,
              'snapshot_scope': 'One completed native SQL statement per case; immutable benchmark data only',
              'timing': 'SQL execute/fetch including RPC; resident JSON decoding and normalization excluded',
              'resident_prepare_note': 'One live GPU query plus copying its input into pinned storage; includes that query execution'}
    receive_pin = cache_pin = cache = backend = None
    try:
        receive_pin = PinnedInput(64)
        start = time.perf_counter()
        cache_pin = PinnedInput(2048)
        report['resident_allocation_seconds'] = time.perf_counter() - start
        cache = ResidentInput(cache_pin.view)
        backend = RecordingBackend(real, cache)
        backend.input_buffer = receive_pin.view
        with benchmark_socket(args.socket, backend), connect(read_timeout=600) as conn, conn.cursor() as cursor:
            cursor.execute('SET ob_query_timeout=600000000')
            for name, expressions in CASES.items():
                expected = oracle['expected'][name]
                events = []
                plans = {}
                def run(mode):
                    resident = mode == 'resident'
                    cursor.execute('SET @sirius_gpu=%s', (int(mode == 'live'),))
                    parallel = 8 if mode.startswith('cpu8') else 1
                    sql = (f"SELECT /*+ PARALLEL({parallel}) USE_HASH_AGGREGATION "
                           "OPT_PARAM('rowsets_max_rows' 1024) OPT_PARAM('pushdown_storage_level' 2) */ "
                           f"{expressions} FROM {identifier(args.table)} GROUP BY grp")
                    if mode == 'cpu8_default':
                        sql = (f'SELECT /*+ PARALLEL(8) */ {expressions} '
                               f'FROM {identifier(args.table)} GROUP BY grp')
                    offset = len(backend.events)
                    def execute():
                        if resident:
                            cursor.execute('SELECT SIRIUS_EXECUTE_SUBSTRAIT(%s)', (backend.token(name),))
                            return cursor.fetchone()[0]
                        cursor.execute(sql)
                        return cursor.fetchall()
                    elapsed = measured(execute, expected)
                    if mode in ('live', 'resident'):
                        emitted = backend.events[offset:]
                        if not emitted or sum(e['rows'] for e in emitted) != oracle['rows']:
                            raise RuntimeError('Expected complete GPU input; CPU fallback is not a GPU result')
                        events.append({'mode': mode, 'chunks': emitted})
                    if mode not in plans and not resident:
                        cursor.execute('EXPLAIN ' + sql)
                        plans[mode] = cursor.fetchall()
                    return elapsed
                # First calls precede capture; capture is additional preparation.
                first = {mode: run(mode) for mode in ('cpu1', 'cpu8', 'cpu8_default', 'live')}
                backend.capture = name
                prepare_seconds = run('live')
                backend.capture = None
                first['resident'] = run('resident')
                times = {mode: [] for mode in first}
                rng = random.Random(20261026)
                for _ in range(args.repeats):
                    modes = list(times)
                    rng.shuffle(modes)
                    for mode in modes: times[mode].append(run(mode))
                item = {'modes': {mode: summary(values, first[mode], prepare_seconds if mode == 'resident' else 0)
                                  for mode, values in times.items()},
                        'resident_cached_bytes_total': cache.used, 'gpu_events': events, 'plans': plans}
                item['modes']['resident']['allocation_seconds'] = report['resident_allocation_seconds']
                item['modes']['resident']['estimated_total_with_allocation'] = {
                    n: value + report['resident_allocation_seconds']
                    for n, value in item['modes']['resident']['estimated_total_seconds'].items()}
                # Separate diagnostic calls, never mixed into the hot medians.
                # Bridge input_us includes waiting for seekdb/transport; run_us
                # is host wall time around fragment.run(), not a CUDA event.
                previous_profile = os.environ.get('SEEKDB_SIRIUS_PROFILE')
                item['diagnostic_seconds'] = {}
                try:
                    os.environ['SEEKDB_SIRIUS_PROFILE'] = '1'
                    for mode in ('live', 'resident'):
                        print(json.dumps({'diagnostic_begin': name, 'mode': mode}), flush=True)
                        item['diagnostic_seconds'][mode] = run(mode)
                        print(json.dumps({'diagnostic_end': name, 'mode': mode}), flush=True)
                finally:
                    if previous_profile is None:
                        os.environ.pop('SEEKDB_SIRIUS_PROFILE', None)
                    else:
                        os.environ['SEEKDB_SIRIUS_PROFILE'] = previous_profile
                report['cases'][name] = item
                report['process_peak_rss_kib'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                save(Path(args.output), report)
                print(json.dumps({'case': name, 'medians': {m: v['median_seconds'] for m, v in item['modes'].items()}}), flush=True)
    finally:
        if backend is not None:
            backend.input_buffer = None
        if cache is not None: cache.clear()
        if cache_pin is not None: cache_pin.close()
        if receive_pin is not None: receive_pin.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=('prepare', 'native', 'seekdb'))
    p.add_argument('--source')
    p.add_argument('--projection')
    p.add_argument('--oracle')
    p.add_argument('--extension')
    p.add_argument('--library')
    p.add_argument('--socket')
    p.add_argument('--tpch-pause')
    p.add_argument('--table', default='sirius_sf500_customer')
    p.add_argument('--repeats', type=int, default=10)
    p.add_argument('--output', required=True)
    args = p.parse_args()
    if args.repeats < 3: p.error('At least three hot executions required')
    if args.mode == 'prepare': prepare(args)
    else:
        oracle = json.loads(Path(args.oracle).read_text())
        with telemetry(args.output):
            if args.mode == 'native': native(args, oracle)
            else: seekdb(args, oracle)
