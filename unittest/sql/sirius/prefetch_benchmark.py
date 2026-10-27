#!/usr/bin/env python3
"""Interleave serial and prefetched live SQL on an idle owned test instance."""
import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import resource
import statistics
import time

from path_comparison import (Backend, PinnedInput, benchmark_socket, connect,
                             measured, provenance, save, telemetry)
import path_comparison
from resident_input import RecordingBackend, ResidentInput
import native_integration


def main(args):
    oracle = json.loads(Path(args.oracle).read_text())
    with connect() as conn, conn.cursor() as cursor:
        cursor.execute('SHOW FULL PROCESSLIST')
        if any(r[4] == 'Query' and 'SHOW FULL PROCESSLIST' not in str(r[7]) for r in cursor.fetchall()):
            raise RuntimeError('Other seekdb queries are active')
    report = {'provenance': provenance(args), 'rows': oracle['rows'], 'repeats': args.repeats,
              'seed': args.seed, 'cases': {}, 'frame_mib': 64, 'fragment_target_mib': 128,
              'note': 'Both buffers preallocated for interleaving; baseline uses one. Every GPU call rescans seekdb. No resident table cache.'}
    with contextlib.ExitStack() as cleanup:
        optimized_handle = path_comparison.handle
        baseline_handle = optimized_handle
        cleanup.callback(setattr, path_comparison, 'handle', optimized_handle)
        if args.baseline_sidecar:
            source = Path(args.baseline_sidecar).resolve(strict=True)
            spec = importlib.util.spec_from_file_location('baseline_sidecar', source)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            baseline_handle = module.handle
            report['baseline_source'] = {'path': str(source), 'sha256': hashlib.sha256(source.read_bytes()).hexdigest()}
        pins = []
        for _ in range(2):
            pin = PinnedInput(64)
            cleanup.callback(pin.close)
            pins.append(pin)
        real = Backend(str(Path(args.library).resolve()))
        real.warmup()
        backend = RecordingBackend(real, ResidentInput(memoryview(bytearray())))
        backend.input_buffer = pins[0].view
        with benchmark_socket(args.socket, backend), connect() as conn, conn.cursor() as cursor:
            backend.native_prefetch_buffers = tuple(p.view for p in pins)
            native_integration.main()
            report['sql_transaction_checks'] = 'passed'
            cursor.execute('SET ob_query_timeout=600000000')
            rng = random.Random(args.seed)
            for name, expr in [('sum_count', 'grp,SUM(amount),COUNT(*)'), ('count', 'grp,COUNT(*)')]:
                events = []
                def run(mode):
                    path_comparison.handle = baseline_handle if mode == 'baseline' else optimized_handle
                    backend.native_prefetch_buffers = tuple(p.view for p in pins) if mode == 'prefetch' else ()
                    cursor.execute('SET @sirius_gpu=%s', (int(mode != 'cpu8'),))
                    sql = (f"SELECT /*+ PARALLEL({8 if mode == 'cpu8' else 1}) USE_HASH_AGGREGATION "
                           "OPT_PARAM('rowsets_max_rows' 1024) OPT_PARAM('pushdown_storage_level' 2) */ "
                           f"{expr} FROM sirius_sf500_customer GROUP BY grp")
                    offset = len(backend.events)
                    def query():
                        cursor.execute(sql)
                        return cursor.fetchall()
                    elapsed = measured(query, oracle['expected'][name])
                    if mode != 'cpu8':
                        chunks = backend.events[offset:]
                        if not chunks or sum(c['rows'] for c in chunks) != oracle['rows']:
                            raise AssertionError('Missing full GPU execution evidence')
                        events.append({'mode': mode, 'chunks': chunks})
                    return elapsed
                modes = ['baseline', 'prefetch', 'cpu8']
                first = {mode: run(mode) for mode in modes}
                times = {mode: [] for mode in modes}
                order = []
                for _ in range(args.repeats):
                    rng.shuffle(modes)
                    order.append(list(modes))
                    for mode in modes:
                        times[mode].append(run(mode))
                item = {'first_seconds': first, 'seconds': times, 'order': order,
                        'medians': {m: statistics.median(v) for m, v in times.items()}, 'gpu_events': events}
                item['prefetch_speedup'] = item['medians']['baseline'] / item['medians']['prefetch']
                # Diagnostics use extra calls and never enter the hot samples.
                try:
                    os.environ['SEEKDB_SIRIUS_PROFILE'] = '1'
                    for mode in ('baseline', 'prefetch'):
                        print(json.dumps({'diagnostic_begin': name, 'mode': mode}), flush=True)
                        run(mode)
                        print(json.dumps({'diagnostic_end': name, 'mode': mode}), flush=True)
                finally:
                    os.environ.pop('SEEKDB_SIRIUS_PROFILE', None)
                report['cases'][name] = item
                report['peak_rss_kib'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                save(Path(args.output), report)
                print(json.dumps({'case': name, 'medians': item['medians'], 'speedup': item['prefetch_speedup']}), flush=True)
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
    p.add_argument('--repeats', type=int, default=20)
    p.add_argument('--seed', type=int, default=20261027)
    p.add_argument('--baseline-sidecar', help='Optional exact previous sidecar.py for interleaved before/after comparison')
    args = p.parse_args()
    if args.repeats < 3 or os.environ.get('SEEKDB_SIRIUS_PROFILE') == '1':
        p.error('Require at least three repeats and disabled profiling')
    with telemetry(args.output):
        main(args)
