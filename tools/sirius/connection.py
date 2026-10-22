"""Connection settings shared by the snapshot and execution commands."""
import os
import pymysql


def connect(stream=False):
    return pymysql.connect(
        host=os.environ.get("SEEKDB_HOST", "127.0.0.1"),
        port=int(os.environ.get("SEEKDB_PORT", "2881")),
        user=os.environ.get("SEEKDB_USER", "root"),
        password=os.environ.get("SEEKDB_PASSWORD", ""),
        database=os.environ.get("SEEKDB_DATABASE", "test"),
        charset="utf8mb4", autocommit=True,
        cursorclass=pymysql.cursors.SSCursor if stream else pymysql.cursors.Cursor,
        read_timeout=600, write_timeout=600,
    )


def identifier(name):
    return "`" + name.replace("`", "``") + "`"
