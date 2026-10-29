"""V4 exact grouped transport; optionally run the same cases on real Sirius."""
import json
import os
from pathlib import Path
import socket
import struct
import sys
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from sidecar import Backend, native_aggregate
from wire import receive, send
from test_native_exact import encode
from substrait import plan_pb2


class Reference:
    input_buffer = None
    native_chunk_bytes = 1
    def execute(self, plan, reader):
        decoded = plan_pb2.Plan.FromString(plan)
        minimum = any(e.extension_function.name == 'min' for e in decoded.extensions)
        groups = {}
        for batch in reader:
            for row in zip(*(column.to_pylist() for column in batch.columns)):
                key = tuple(row[:2])
                total = groups.setdefault(key, [None] * (len(row)-2))
                for i, value in enumerate(row[2:]):
                    if value is not None:
                        total[i] = value if total[i] is None else min(total[i],value) if minimum else total[i]+value
        return json.dumps({'rows': [list(k)+[str(v) if v is not None else None for v in values]
                                    for k,values in groups.items()]}).encode()


class GroupedExact(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        library = os.getenv('SIRIUS_TEST_LIBRARY')
        cls.backend = Backend(library) if library else Reference()
        cls.backend.native_chunk_bytes = 1

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls.backend, 'close'): cls.backend.close()

    def run_stream(self, batches, version=4):
        left, right = socket.socketpair()
        left.settimeout(15); right.settimeout(15)
        def serve():
            with right:
                try: native_aggregate(right, self.backend, bytes((version,2,2,0,16)))
                except Exception as error: send(right,b'X',str(error).encode())
        thread = threading.Thread(target=serve); thread.start()
        try:
            with left:
                self.assertEqual(receive(left),(b'A',b''))
                for batch in batches:
                    send(left,b'B',encode(batch));kind,payload=receive(left)
                    if kind==b'X':raise RuntimeError(bytes(payload).decode())
                    self.assertEqual(kind,b'A')
                send(left,b'E');kind,payload=receive(left)
                if kind==b'X':raise RuntimeError(bytes(payload).decode())
                self.assertEqual(kind,b'R')
                rows=struct.unpack_from('<I',payload)[0]
                self.assertEqual(len(payload),4+rows*4*17)
                result=[]
                for row in range(rows):
                    result.append(tuple(int.from_bytes(payload[5+(row*4+c)*17:21+(row*4+c)*17],'little',signed=True)
                                        if payload[4+(row*4+c)*17] else None for c in range(4)))
                return sorted(result,key=repr)
        finally:
            thread.join(16);self.assertFalse(thread.is_alive())

    def test_nullable_keys_and_wide_partial_merges(self):
        expected=[(1,-2,2**64-2,3),(None,3,7,None),(4,None,None,1)]
        actual=self.run_stream([[(1,-2,2**63-1,1),(None,3,None,None)],
                                [(1,-2,2**63-1,2),(None,3,7,None),(4,None,None,1)]])
        self.assertEqual(actual,sorted(expected,key=repr))

    def test_empty_grouped_input(self):
        self.assertEqual(self.run_stream([]),[])

    def test_exact_minimum_merges(self):
        rows=self.run_stream([[(1,None,2**63-1,None),(None,2,None,5)],
                              [(1,None,-2**63,7),(None,2,None,3)]],version=7)
        self.assertEqual(rows, sorted([(1,None,-2**63,7),(None,2,None,3)],key=repr))

    def test_invalid_shape(self):
        for descriptor in [(4,0,1,0,16),(4,9,1,0,16),(4,1,64,0,16),(4,1,0,0,16),(4,1,1,1,16)]:
            with self.assertRaisesRegex(ValueError,'shape'):
                native_aggregate(None,self.backend,bytes(descriptor))


if __name__ == '__main__': unittest.main()
