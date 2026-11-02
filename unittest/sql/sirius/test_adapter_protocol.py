import concurrent.futures
from pathlib import Path
import socket
import struct
import sys
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools" / "sirius"))
import pyarrow as pa
from adapter_backend import Scan
from adapter_service import ScanPump, decode_request
from adapter_protocol import encode_text
from wire import receive, send


def batch(value):
    return (
        struct.pack("!II", 1, 1)
        + b"\x01"
        + struct.pack("!II", 0, 8)
        + struct.pack("<q", value)
    )


class AdapterProtocolTest(unittest.TestCase):
    def test_cancel_one_waiter_drains_reply_and_preserves_other_scan(self):
        client, server = socket.socketpair()
        client.settimeout(5)
        server.settimeout(5)
        pump = ScanPump(client)
        schema = pa.schema([("a", pa.int64())])
        seen = threading.Event()
        resume = threading.Event()

        def host():
            kind, payload = receive(server)
            self.assertEqual(
                (kind, struct.unpack("!IIII", payload)), (b"N", (0, 0, 1, 0))
            )
            seen.set()
            self.assertTrue(resume.wait(5))
            send(server, b"B", batch(1))
            kind, payload = receive(server)
            self.assertEqual(
                (kind, struct.unpack("!IIII", payload)), (b"N", (1, 0, 1, 0))
            )
            send(server, b"B", batch(42))

        try:
            with concurrent.futures.ThreadPoolExecutor(3) as pool:
                peer = pool.submit(host)
                first = pool.submit(pump.next, Scan(0, 0, schema))
                self.assertTrue(seen.wait(5))
                pump.cancel(0)
                with self.assertRaises(concurrent.futures.CancelledError):
                    first.result(timeout=1)
                second = pool.submit(pump.next, Scan(1, 0, schema))
                resume.set()
                self.assertEqual(second.result(timeout=5).to_pydict(), {"a": [42]})
                peer.result(timeout=5)
            pump.stop(True)
        finally:
            resume.set()
            pump.stop(False)
            client.close()
            server.close()

    def test_live_snapshot_and_schema_are_required(self):
        payload = struct.pack("!I", 5)
        for value in ("", "1001:2001", "test", "123:0:0:0", "select count(*) from a"):
            payload += encode_text(value)
        payload += (
            struct.pack("!II", 1, 1)
            + encode_text("a")
            + encode_text("5001:1")
            + struct.pack("!I", 1)
        )
        payload += encode_text("a") + encode_text("l")
        pin, principal, database, snapshot, sql, width, tables, ids = decode_request(
            payload
        )
        self.assertEqual(snapshot, "123:0:0:0")
        self.assertEqual(tables["a"], pa.schema([("a", pa.int64())]))
        with self.assertRaises(ValueError):
            decode_request(payload + b"garbage")

    def test_disconnected_host_releases_all_waiters(self):
        client, server = socket.socketpair()
        pump = ScanPump(client)
        server.close()
        try:
            with self.assertRaises((OSError, ConnectionError)):
                pump.next(Scan(0, 0, pa.schema([("a", pa.int64())])))
        finally:
            pump.stop(False)
            client.close()


if __name__ == "__main__":
    unittest.main()
