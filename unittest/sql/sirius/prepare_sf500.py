#!/usr/bin/env python3
"""Read-only DuckDB projection -> bounded reusable CSV -> isolated seekdb tables."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import threading
import json
from pathlib import Path
import shutil
import sys
import time

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from connection import connect, identifier


def main(args):
    source = Path(args.database).resolve()
    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    manifest = directory / f"{args.source_table}-import.json"
    if manifest.exists() != args.resume:
        raise ValueError("Use --resume only with an existing import manifest")
    prefix = "s" if args.source_table == "supplier" else "c"
    key = prefix + ("_suppkey" if prefix == "s" else "_custkey")
    group, amount = prefix + "_nationkey", prefix + "_acctbal"
    target = identifier(args.target)
    initial = source.stat()
    initial_free = shutil.disk_usage(directory).free
    budget = args.disk_budget_gib * 1024**3
    report = {"source": str(source), "source_bytes": initial.st_size,
              "source_mtime_ns": initial.st_mtime_ns, "source_table": args.source_table,
              "target": args.target, "projection": "id, nation key, exact integer cents as DOUBLE",
              "disk_budget_bytes": budget, "initial_free_bytes": initial_free, "chunks": []}
    if args.resume:
        saved = json.loads(manifest.read_text())
        for field in ("source", "source_bytes", "source_mtime_ns", "source_table", "target", "disk_budget_bytes"):
            if report[field] != saved[field]:
                raise ValueError(f"Resume identity mismatch: {field}")
        report = saved
        initial_free = report["initial_free_bytes"]
    # Only staging files named by this import are disposable on resume.
    staged = list(directory.glob(f"{args.source_table}-chunk-*.csv"))
    staged += list(directory.glob(f"{args.source_table}-chunk.csv"))
    if staged and not args.resume:
        raise ValueError("Refusing to overwrite existing staging files")
    for csv in staged:
        csv.unlink()
    lock = threading.Lock()
    def save():
        temporary = manifest.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(report, indent=2) + "\n")
        temporary.replace(manifest)
    def source_connection():
        return duckdb.connect(str(source), read_only=True,
                              config={"threads": 4, "memory_limit": "2GB", "max_temp_directory_size": "0B"})
    def configure(cursor):
        cursor.execute("SET ob_query_timeout=600000000")
        cursor.execute("SET ob_trx_timeout=600000000")
    with source_connection() as db:
        low, high, count = db.execute(f"SELECT min({key}),max({key}),count(*) FROM {args.source_table}").fetchone()
        if low != 1 or high != count:
            raise ValueError(f"Expected dense source primary keys: {(low, high, count)}")
        report["source_rows"] = count
        report["source_groups"] = db.execute(
            f"SELECT {group},count(*),CAST(sum({amount}*100) AS BIGINT) "
            f"FROM {args.source_table} GROUP BY {group} ORDER BY {group}").fetchall()
    ranges = [(start, min(start + args.chunk_rows, high + 1)) for start in range(1, high + 1, args.chunk_rows)]
    completed = {(chunk["start"], chunk["end"]) for chunk in report["chunks"]}
    if not completed.issubset(set(ranges)) or len(completed) != len(report["chunks"]):
        raise ValueError("Resume chunk layout mismatch or duplicate records")
    with connect() as conn, conn.cursor() as cursor:
        if not args.resume:
            cursor.execute(f"CREATE TABLE {target} (id BIGINT PRIMARY KEY,grp BIGINT NOT NULL,amount DOUBLE NOT NULL)")
    report["chunk_rows"] = args.chunk_rows
    report["workers"] = args.workers
    report["verified"] = False
    save()
    def load_chunk(bounds):
        start, end = bounds
        free = shutil.disk_usage(directory).free
        if free < args.reserve_free_gib * 1024**3 or initial_free - free > budget - 1024**3:
            raise RuntimeError("Disk reserve/growth budget reached; imported table and manifest preserved")
        csv = directory / f"{args.source_table}-chunk-{start:011d}.csv"
        query = (f"SELECT {key} AS id, CAST({group} AS BIGINT) AS grp, "
                 f"CAST({amount}*100 AS BIGINT) AS amount FROM {args.source_table} "
                 f"WHERE {key}>={start} AND {key}<{end}")
        try:
            started = time.perf_counter()
            with source_connection() as db:
                escaped = str(csv).replace("'", "''")
                rows = db.execute(f"COPY ({query}) TO '{escaped}' (FORMAT CSV, HEADER false)").fetchone()[0]
            exported = time.perf_counter() - started
            size = csv.stat().st_size
            if size > 64 * 1024**2 or rows != end - start:
                raise RuntimeError("Unexpected staging size or source row count")
            started = time.perf_counter()
            with connect(local_infile=True) as conn, conn.cursor() as cursor:
                configure(cursor)
                if args.resume:
                    # LOAD DATA is not globally atomic. Retry only unrecorded ranges.
                    cursor.execute(f"DELETE FROM {target} WHERE id>=%s AND id<%s", (start, end))
                cursor.execute(f"LOAD DATA /*+ NO_DIRECT */ LOCAL INFILE %s INTO TABLE {target} "
                               "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' (id,grp,amount)", (str(csv),))
                affected = cursor.rowcount
                cursor.execute("SHOW WARNINGS")
                warnings = cursor.fetchall()
                if warnings or affected != rows:
                    raise RuntimeError(f"Import discrepancy: expected={rows} affected={affected} warnings={warnings}")
                cursor.execute(f"SELECT COUNT(*) FROM {target} WHERE id>=%s AND id<%s", (start, end))
                if cursor.fetchone()[0] != rows:
                    raise RuntimeError(f"Imported range count mismatch: {start}..{end}")
            chunk = {"start": start, "end": end, "rows": rows, "csv_bytes": size,
                     "load_hint": "NO_DIRECT", "export_seconds": exported,
                     "load_seconds": time.perf_counter() - started,
                     "free_bytes": shutil.disk_usage(directory).free}
            with lock:
                report["chunks"].append(chunk)
                report["chunks"].sort(key=lambda item: item["start"])
                save()
                print(json.dumps(chunk), flush=True)
        finally:
            csv.unlink(missing_ok=True)
    pending = [bounds for bounds in ranges if bounds not in completed]
    # Bounded rounds limit disk use and stop scheduling after any failed load.
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for offset in range(0, len(pending), args.workers):
            futures = [pool.submit(load_chunk, bounds) for bounds in pending[offset:offset + args.workers]]
            failures = []
            for future in futures:
                try:
                    future.result()
                except Exception as error:
                    failures.append(error)
            if failures:
                raise failures[0]
    with connect() as conn, conn.cursor() as cursor:
        configure(cursor)
        cursor.execute(f"SELECT grp,COUNT(*),SUM(amount) FROM {target} GROUP BY grp ORDER BY grp")
        actual = cursor.fetchall()
        if [tuple(row) for row in report["source_groups"]] != list(actual):
            raise RuntimeError(f"Full source/target group mismatch: {actual}")
    final = source.stat()
    if (initial.st_size, initial.st_mtime_ns) != (final.st_size, final.st_mtime_ns):
        raise RuntimeError("Source database fingerprint changed")
    report["verified"] = True
    report["final_free_bytes"] = shutil.disk_usage(directory).free
    report["peak_csv_bytes"] = max(c["csv_bytes"] for c in report["chunks"])
    report["concurrent_csv_upper_bound_bytes"] = args.workers * report["peak_csv_bytes"]
    save()
    print("verified", args.target, count, flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--database", required=True)
    p.add_argument("--directory", required=True)
    p.add_argument("--source-table", choices=("supplier", "customer"), required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--chunk-rows", type=int, default=1000000)
    p.add_argument("--disk-budget-gib", type=int, default=8)
    p.add_argument("--reserve-free-gib", type=int, default=100)
    p.add_argument("--resume", action="store_true", help="Continue the target owned by a matching existing manifest")
    p.add_argument("--workers", type=int, choices=(1, 2, 4), default=1)
    args = p.parse_args()
    if not 1 <= args.chunk_rows <= 1000000 or args.disk_budget_gib < 2 or args.reserve_free_gib < 1:
        p.error("Require 1..1000000 chunk rows, >=2 GiB growth budget and >=1 GiB reserve")
    # A second importer must not delete/reload ranges owned by an active one.
    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f"{args.source_table}-import.lock").open("a") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        main(args)
