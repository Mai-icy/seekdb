#!/usr/bin/env python3
"""Compare GPU topology/memory configs in separate processes on an idle instance.

Each process interleaves live SQL, immutable host-resident replay and CPU8.
Pin/allocation/capture/warmup costs are recorded separately from query timings.
Use the same seed and reverse configuration order for a second sweep.
"""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import random
import socket
import statistics
import time

from path_comparison import (Backend, CASES, PinnedInput, benchmark_socket, connect,
                             measured, provenance, save, telemetry)
from resident_input import RecordingBackend, ResidentInput
import live_integration
import prefetch_integration


@contextlib.contextmanager
def endpoint(path, backend):
    # The test instance normally has no sidecar between experiments. Reserve
    # its absent socket with our own placeholder, then use the existing safe
    # swap/restore helper. Never unlink another process's socket.
    path = Path(path)
    placeholder = None
    inode = None
    try:
        if not path.exists():
            placeholder = socket.socket(socket.AF_UNIX)
            placeholder.bind(str(path))
            inode = path.stat().st_ino
            os.chmod(path, 0o600)
            placeholder.listen(1)
        with benchmark_socket(path, backend):
            yield
    finally:
        if placeholder is not None:
            placeholder.close()
            if inode is not None and path.exists() and path.stat().st_ino == inode:
                path.unlink()


def main(args):
    oracle = json.loads(Path(args.oracle).read_text())
    with connect() as conn, conn.cursor() as c:
        c.execute('SHOW FULL PROCESSLIST')
        if any(r[4] == 'Query' and 'SHOW FULL PROCESSLIST' not in str(r[7]) for r in c.fetchall()):
            raise RuntimeError('Other seekdb queries are active')
    report = {'provenance': provenance(args), 'cases': {}, 'rows': oracle['rows'],
              'fragment_target_mib': args.chunk_mib, 'frame_mib': 64,
              'repeats': args.repeats, 'seed': args.seed,
              'timing': 'TCP SQL execute/fetch; pin, capture, startup and warmup excluded',
              'resident_scope': 'Immutable benchmark snapshot in pinned HOST memory; H2D and aggregation repeated on every call'}
    with contextlib.ExitStack() as cleanup:
        start = time.perf_counter()
        pins = []
        for size in (64, 64, 2048):
            pin = PinnedInput(size)
            cleanup.callback(pin.close)
            pins.append(pin)
        report['pin_allocation_seconds'] = time.perf_counter() - start
        start = time.perf_counter()
        real = Backend(str(Path(args.library).resolve()))
        real.warmup()
        report['startup_warmup_seconds'] = time.perf_counter() - start
        loaded = {line.split()[-1] for line in Path('/proc/self/maps').read_text().splitlines()
                  if '/libsirius.so' in line}
        report['loaded_sirius'] = {}
        for name in sorted(loaded):
            with Path(name).open('rb') as library:
                report['loaded_sirius'][name] = hashlib.file_digest(library, 'sha256').hexdigest()
        cache = ResidentInput(pins[2].view)
        cleanup.callback(cache.clear)
        backend = RecordingBackend(real, cache)
        backend.input_buffer = pins[0].view
        backend.native_prefetch_buffers = (pins[0].view, pins[1].view)
        backend.native_chunk_bytes = args.chunk_mib * 1024**2
        if backend.native_chunk_bytes > backend.native_fragment_bytes - 128 * 1024**2:
            raise ValueError('Chunk target leaves insufficient fragment headroom')
        report['fragment_limit_bytes'] = backend.native_fragment_bytes
        with endpoint(args.socket, backend), connect() as conn, conn.cursor() as c:
            if args.checks:
                live_integration.main(args.socket)
                prefetch_integration.main(args.oracle)
                report['integration_checks'] = 'live/snapshot, native/transaction, cancellation/recovery passed'
            c.execute('SET ob_query_timeout=600000000')
            rng = random.Random(args.seed)
            for name, expr in CASES.items():
                events = []
                token = None
                def run(mode):
                    c.execute('SET @sirius_gpu=%s', (int(mode == 'live'),))
                    sql = (f"SELECT /*+ PARALLEL({8 if mode == 'cpu8' else 1}) USE_HASH_AGGREGATION "
                           "OPT_PARAM('rowsets_max_rows' 1024) OPT_PARAM('pushdown_storage_level' 2) */ "
                           f"{expr} FROM sirius_sf500_customer GROUP BY grp")
                    offset = len(backend.events)
                    def query():
                        if mode == 'resident':
                            c.execute('SELECT SIRIUS_EXECUTE_SUBSTRAIT(%s)', (token,))
                            return c.fetchone()[0]
                        c.execute(sql)
                        return c.fetchall()
                    seconds = measured(query, oracle['expected'][name])
                    if mode != 'cpu8':
                        emitted = backend.events[offset:]
                        if not emitted or sum(e['rows'] for e in emitted) != oracle['rows']:
                            raise AssertionError('Full GPU input evidence missing')
                        events.append({'mode': mode, 'chunks': emitted})
                    return seconds
                first = {mode: run(mode) for mode in ('cpu8', 'live')}
                backend.capture = name
                try:
                    preparation = run('live')
                finally:
                    backend.capture = None
                token = backend.token(name)
                first['resident'] = run('resident')
                times = {mode: [] for mode in first}
                order = []
                for _ in range(args.repeats):
                    modes = list(times)
                    rng.shuffle(modes)
                    order.append(modes)
                    for mode in modes:
                        times[mode].append(run(mode))
                item = {'first_seconds': first, 'seconds': times, 'order': order,
                        'medians': {m: statistics.median(v) for m, v in times.items()},
                        'capture_seconds': preparation, 'pinned_host_bytes': cache.used,
                        'gpu_events': events}
                try:
                    os.environ['SEEKDB_SIRIUS_PROFILE'] = '1'
                    for mode in ('live', 'resident'):
                        print(json.dumps({'diagnostic_begin': name, 'mode': mode}), flush=True)
                        run(mode)
                        print(json.dumps({'diagnostic_end': name, 'mode': mode}), flush=True)
                finally:
                    os.environ.pop('SEEKDB_SIRIUS_PROFILE', None)
                report['cases'][name] = item
                save(Path(args.output), report)
                print(json.dumps({'case': name, 'medians': item['medians']}), flush=True)
        backend.input_buffer = None
        backend.native_prefetch_buffers = ()
    report['status'] = 'passed'
    save(Path(args.output), report)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--library', required=True)
    p.add_argument('--socket', required=True)
    p.add_argument('--oracle', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--chunk-mib', type=int, choices=(128, 256, 384, 1024, 1536), default=128)
    p.add_argument('--repeats', type=int, default=10)
    p.add_argument('--seed', type=int, default=20261027)
    p.add_argument('--checks', action='store_true')
    args = p.parse_args()
    if args.repeats < 3 or os.environ.get('SEEKDB_SIRIUS_PROFILE') == '1':
        p.error('Require at least three repeats and disabled profiling')
    with telemetry(args.output):
        main(args)
