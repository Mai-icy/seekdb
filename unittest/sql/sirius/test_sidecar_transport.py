#!/usr/bin/env python3
"""CPU-only protocol regression tests against the actual C++ proxy library."""
import argparse
import ctypes as ct
import os
from pathlib import Path
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools/sirius"))
from wire import receive, send

EXECUTE = ct.CFUNCTYPE(ct.c_int, ct.c_void_p, ct.c_size_t, ct.c_void_p, ct.c_size_t,
                      ct.POINTER(ct.c_size_t), ct.c_void_p, ct.c_size_t)
class Api(ct.Structure):
    _fields_ = [("version", ct.c_uint32), ("size", ct.c_uint32), ("execute", EXECUTE)]


class Transport(unittest.TestCase):
    def call(self, response, capacity=256, timeout=1000):
        with tempfile.TemporaryDirectory(prefix="sirius-wire-") as directory:
            path = directory + "/s"
            os.environ["SEEKDB_SIRIUS_SOCKET"] = path
            os.environ["SEEKDB_SIRIUS_TIMEOUT_MS"] = str(timeout)
            errors = []
            with socket.socket(socket.AF_UNIX) as listener:
                listener.bind(path)
                listener.listen(1)
                listener.settimeout(2)
                def serve():
                    try:
                        conn, _ = listener.accept()
                        with conn:
                            conn.settimeout(2)
                            self.assertEqual(receive(conn), (b"P", b"a\x00b"))
                            response(conn)
                    except Exception as error:
                        errors.append(error)
                thread = threading.Thread(target=serve)
                thread.start()
                output, error = ct.create_string_buffer(capacity), ct.create_string_buffer(512)
                length = ct.c_size_t(123)
                status = API.execute(b"a\x00b", 3, output, capacity, ct.byref(length), error, len(error))
                thread.join(3)
                self.assertFalse(thread.is_alive())
                if errors:
                    raise errors[0]
                if status:
                    self.assertEqual(length.value, 0)
                return status, output.raw[:length.value], error.value

    def test_binary_plan_and_fragmented_response(self):
        def response(conn):
            for value in b"J\x00\x00\x00\x02{}":
                conn.sendall(bytes([value]))
        self.assertEqual(self.call(response)[:2], (0, b"{}"))

    def test_backend_error_and_recovery(self):
        self.assertEqual(self.call(lambda s: send(s, b"X", b"unsupported"))[0::2], (3, b"unsupported"))
        self.assertEqual(self.call(lambda s: send(s, b"J", b"{}"))[0], 0)

    def test_short_response(self):
        self.assertEqual(self.call(lambda s: s.sendall(b"J\x00\x00\x00\x04{}"))[0], 2)

    def test_untrusted_length(self):
        self.assertEqual(self.call(lambda s: s.sendall(b"J" + struct.pack("!I", 2**32-1)))[0], 2)

    def test_capacity(self):
        self.assertEqual(self.call(lambda s: send(s, b"J", b"[1,2]"), capacity=2)[0], 4)

    def test_deadline(self):
        started = time.monotonic()
        status, _, error = self.call(lambda s: time.sleep(0.15), timeout=30)
        self.assertEqual(status, 2)
        self.assertIn(b"timed out", error)
        self.assertLess(time.monotonic() - started, 1)

    def test_frame_limit_before_body(self):
        left, right = socket.socketpair()
        with left, right:
            left.sendall(b"B" + struct.pack("!I", 16 * 1024 * 1024 + 1))
            with self.assertRaises(ValueError):
                receive(right)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    args, other = parser.parse_known_args()
    library = ct.CDLL(str(Path(args.library).resolve()))
    library.seekdb_sirius_get_api_v1.restype = ct.POINTER(Api)
    API = library.seekdb_sirius_get_api_v1().contents
    unittest.main(argv=[sys.argv[0]] + other)
