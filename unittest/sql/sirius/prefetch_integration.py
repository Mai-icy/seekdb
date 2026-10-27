#!/usr/bin/env python3
"""Run against the actual --native-prefetch sidecar on the owned SF500 instance."""
import argparse
import json
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect
import native_integration


def main(oracle):
    native_integration.main()
    expected = json.loads(Path(oracle).read_text())['expected']['sum_count']
    sql = ("SELECT /*+ PARALLEL(1) USE_HASH_AGGREGATION "
           "OPT_PARAM('rowsets_max_rows' 1024) OPT_PARAM('pushdown_storage_level' 2) */ "
           "grp,SUM(amount),COUNT(*) FROM sirius_sf500_customer GROUP BY grp")
    with connect() as query_conn, connect() as control, query_conn.cursor() as c, control.cursor() as k:
        c.execute('SELECT CONNECTION_ID()')
        query_id = c.fetchone()[0]
        c.execute('SET @sirius_gpu=1')
        errors = []
        def query():
            try:
                c.execute(sql)
                c.fetchall()
            except Exception as error:
                errors.append(repr(error))
        worker = threading.Thread(target=query)
        worker.start()
        try:
            deadline = time.monotonic() + 5
            while True:
                k.execute('SHOW FULL PROCESSLIST')
                if any(r[0] == query_id and r[4] == 'Query' and 'sirius_sf500_customer' in str(r[7]) for r in k.fetchall()):
                    break
                if not worker.is_alive() or time.monotonic() > deadline:
                    raise RuntimeError('Did not observe the owned query before cancellation')
                time.sleep(0.01)
            # Wait until multiple scan batches have started flowing.
            time.sleep(0.15)
            started = time.monotonic()
            k.execute(f'KILL QUERY {int(query_id)}')
            worker.join(timeout=65)
            if worker.is_alive():
                raise RuntimeError('Cancelled query failed to finish within the transport deadline')
            assert errors, 'Cancelled query returned success'
            cancellation_seconds = time.monotonic() - started
            c.execute('SELECT 1')
            assert c.fetchone() == (1,)
            c.execute(sql)
            actual = sorted([list(r) for r in c.fetchall()], key=repr)
            assert actual == sorted(expected, key=repr), 'GPU query after cancellation differs from oracle'
            print(json.dumps({'status': 'passed', 'cancellation_seconds': cancellation_seconds,
                              'error': errors, 'cpu_connection_recovered': True,
                              'full_gpu_query_after_cancel_verified': True}), flush=True)
        finally:
            # Never close a connection or pinned server state under an active
            # query thread. The server/transport already impose finite deadlines.
            if worker.is_alive():
                k.execute(f'KILL QUERY {int(query_id)}')
                worker.join()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--oracle', required=True)
    main(p.parse_args().oracle)
