#!/usr/bin/env python3
"""Resume a bounded full import, compact/analyze, then validate TPC-H22."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect, identifier
from prepare_tpch import KEYS


def main(args):
    root = Path(args.directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    state_path = root / 'pipeline-state.json'
    state = {'status': 'running', 'started_utc': datetime.now(timezone.utc).isoformat()}
    sidecar = None
    sidecar_output = None
    def update(stage, **more):
        state.update(stage=stage, updated_utc=datetime.now(timezone.utc).isoformat(), **more)
        temporary = state_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(state, indent=2, default=str) + '\n')
        temporary.replace(state_path)
        print(json.dumps(state, default=str), flush=True)
    def reserve():
        if shutil.disk_usage(root).free < args.reserve_gib * 2**30:
            raise RuntimeError('Filesystem free-space reserve reached')
    try:
        reserve()
        update('import')
        subprocess.run([sys.executable, str(Path(__file__).with_name('prepare_tpch.py')),
                        '--source', args.source, '--directory', str(root), '--database', args.database,
                        '--workers', str(args.workers), '--reserve-gib', str(args.reserve_gib)], check=True)
        tables = {}
        for table in KEYS:
            report = json.loads((root / f'{table}-import.json').read_text())
            if not report.get('verified'):
                raise RuntimeError(f'Import paused/incomplete at {table}; resume from existing manifest')
            tables[table] = report['source_rows']
        reserve()
        update('major_compaction', tables=tables)
        maintenance = {'compaction': [], 'analyze': []}
        with connect(read_timeout=1860) as conn, conn.cursor() as cursor:
            cursor.execute('SET ob_query_timeout=1800000000')
            cursor.execute('SET ob_trx_timeout=1800000000')
            cursor.execute('SELECT FROZEN_SCN FROM oceanbase.DBA_OB_MAJOR_COMPACTION')
            before = cursor.fetchone()[0]
            cursor.execute('ALTER SYSTEM MAJOR FREEZE')
            deadline = time.monotonic() + 3600
            while time.monotonic() < deadline:
                reserve()
                cursor.execute('SELECT * FROM oceanbase.DBA_OB_MAJOR_COMPACTION')
                row = dict(zip([c[0] for c in cursor.description], cursor.fetchone()))
                maintenance['compaction'].append(row)
                if row['IS_ERROR'] != 'NO': raise RuntimeError(f'Compaction error: {row}')
                if row['FROZEN_SCN'] > before and row['LAST_SCN'] >= row['FROZEN_SCN'] and row['STATUS'] == 'IDLE': break
                time.sleep(5)
            else: raise TimeoutError('Compaction deadline reached')
            for table in KEYS:
                update('analyze', current_table=table)
                started = time.perf_counter()
                cursor.execute(f'ANALYZE TABLE {identifier(args.database)}.{identifier(table)}')
                result = cursor.fetchall()
                maintenance['analyze'].append({'table': table, 'seconds': time.perf_counter() - started,
                                                'result': result})
                if any(len(row) > 2 and str(row[2]).lower() == 'error' for row in result):
                    raise RuntimeError(f'Analyze failed: {result}')
        (root / 'maintenance.json').write_text(json.dumps(maintenance, indent=2, default=str) + '\n')
        reserve()
        if args.sidecar_launcher:
            update('sidecar_startup')
            log = Path(args.sidecar_log)
            offset = log.stat().st_size if log.exists() else 0
            sidecar_output = log.open('ab')
            sidecar = subprocess.Popen([args.sidecar_launcher, '--host-pin-mib', str(args.host_pin_mib),
                                        '--warmup', '--native-chunk-mib', '128'],
                                       stdout=sidecar_output, stderr=subprocess.STDOUT)
            for _ in range(600):
                if sidecar.poll() is not None: raise RuntimeError('Owned sidecar failed during startup')
                with log.open('rb') as stream:
                    stream.seek(offset)
                    if b'ready: ' in stream.read(): break
                time.sleep(0.1)
            else: raise TimeoutError('Sidecar readiness deadline reached')
        update('tpch22')
        subprocess.run([sys.executable, str(Path(__file__).with_name('tpch_benchmark.py')),
                        '--source', args.source, '--database', args.database,
                        '--import-directory', str(root), '--directory', str(root / 'benchmark'),
                        '--sidecar-log', args.sidecar_log, '--scale-factor', str(args.scale_factor),
                        '--resume'], check=True)
        result = json.loads((root / 'benchmark/summary.json').read_text())
        if {q['query'] for q in result['queries'] if q['passed']} != set(range(1, 23)):
            raise RuntimeError('Not all 22 queries passed')
        update('complete', status='passed', free_bytes=shutil.disk_usage(root).free)
    except BaseException as error:
        update(state.get('stage', 'unknown'), status='failed', error=repr(error))
        raise
    finally:
        if sidecar is not None:
            sidecar.terminate()
            try: sidecar.wait(timeout=30)
            except subprocess.TimeoutExpired:
                sidecar.kill()
                sidecar.wait()
        if sidecar_output is not None:
            sidecar_output.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', required=True)
    p.add_argument('--directory', required=True)
    p.add_argument('--database', default='tpch500_sirius')
    p.add_argument('--sidecar-log', required=True)
    p.add_argument('--sidecar-launcher', help='Start an owned sidecar only for the query stage; stop it on exit')
    p.add_argument('--host-pin-mib', type=int, choices=(16, 64, 128), default=64)
    p.add_argument('--scale-factor', type=float, default=500)
    p.add_argument('--workers', type=int, choices=(4, 8, 16, 32), default=16)
    p.add_argument('--reserve-gib', type=int, default=180)
    main(p.parse_args())
