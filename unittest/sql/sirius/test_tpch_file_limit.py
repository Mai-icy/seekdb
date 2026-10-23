#!/usr/bin/env python3
"""Integration check: an oversized COPY fails before LOAD and cleans staging."""
import json
from pathlib import Path
import resource
import subprocess
import sys
import tempfile
import uuid

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from connection import connect, identifier


def limit_child():
    _, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    resource.setrlimit(resource.RLIMIT_FSIZE, (128 * 1024, hard))


def main():
    database = 'tpch_limit_' + uuid.uuid4().hex[:12]
    with tempfile.TemporaryDirectory(prefix='tpch-file-limit-') as temporary:
        root = Path(temporary)
        source = root / 'source.duckdb'
        with duckdb.connect(str(source)) as db:
            db.execute("CREATE TABLE region AS SELECT 0::BIGINT r_regionkey, "
                       "'region' r_name, repeat('x', 80000) r_comment")
        with connect() as conn, conn.cursor() as cursor:
            try:
                result = subprocess.run([
                    sys.executable, str(Path(__file__).with_name('prepare_tpch.py')),
                    '--source', str(source), '--directory', str(root / 'job'),
                    '--database', database, '--tables', 'region', '--workers', '1',
                    '--reserve-gib', '1'], capture_output=True, text=True, preexec_fn=limit_child)
                assert result.returncode != 0, 'Oversized export unexpectedly succeeded'
                assert 'File too large' in result.stderr, result.stderr
                assert not list((root / 'job').glob('*.csv')), 'Failed CSV was not removed'
                cursor.execute(f'SELECT COUNT(*) FROM {identifier(database)}.region')
                assert cursor.fetchone()[0] == 0, 'LOAD ran despite failed export'
                print(json.dumps({'passed': True, 'limit_bytes': 128 * 1024,
                                  'child_exit_code': result.returncode,
                                  'staging_files_remaining': 0, 'target_rows': 0,
                                  'failure': next(line for line in result.stderr.splitlines()
                                                  if 'File too large' in line)}))
            finally:
                cursor.execute(f'DROP DATABASE IF EXISTS {identifier(database)}')


if __name__ == '__main__':
    main()
