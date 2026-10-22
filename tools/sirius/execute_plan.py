#!/usr/bin/env python3
"""Execute a binary Substrait plan through seekdb's SQL entry point."""
import argparse
import json
from pathlib import Path
from connection import connect


def execute(conn, plan):
    with conn.cursor() as cursor:
        cursor.execute("SELECT SIRIUS_EXECUTE_SUBSTRAIT(%s)", (plan,))
        return json.loads(cursor.fetchone()[0])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan")
    args = parser.parse_args()
    with connect() as conn:
        print(json.dumps(execute(conn, Path(args.plan).read_bytes()), ensure_ascii=False))
