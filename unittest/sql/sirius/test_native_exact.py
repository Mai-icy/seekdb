"""Exact scalar protocol: no float round trips, nullable/chunked integer sums."""
import json
from pathlib import Path
import socket
import struct
import sys
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from sidecar import native_aggregate, exact_integer
from wire import receive, send


def encode(rows):
    payload = bytearray(struct.pack('<I', len(rows)))
    for column in zip(*rows):
        mask = bytearray((len(rows)+7)//8)
        for i, value in enumerate(column):
            if value is not None: mask[i//8] |= 1 << (i%8)
        payload.extend(mask)
        for value in column: payload.extend(struct.pack('<q', 0 if value is None else value))
    return payload


class SumBackend:
    input_buffer = None
    def __init__(self, chunk_bytes):
        self.native_chunk_bytes = chunk_bytes
    def execute(self, plan, reader):
        total = [None] * len(reader.schema)
        for batch in reader:
            for c, column in enumerate(batch.columns):
                for value in column.to_pylist():
                    if value is not None: total[c] = (total[c] or 0) + value
        # Match Arrow decimal128 JSON encoding, including values beyond 2^53.
        return json.dumps({'rows': [[str(v) if v is not None else None for v in total]]}).encode()


class NativeExact(unittest.TestCase):
    def run_stream(self, batches, columns=1, chunk_bytes=1, backend=None):
        backend = backend or SumBackend(chunk_bytes)
        left, right = socket.socketpair()
        left.settimeout(3); right.settimeout(3)
        errors = []
        def serve():
            with right:
                try: native_aggregate(right, backend, bytes((3,0,columns,0,16)))
                except Exception as error:
                    errors.append(error)
                    send(right, b'X', str(error).encode())
        thread = threading.Thread(target=serve); thread.start()
        try:
            with left:
                self.assertEqual(receive(left), (b'A',b''))
                for batch in batches:
                    send(left,b'B',encode(batch)); self.assertEqual(receive(left),(b'A',b''))
                send(left,b'E');kind,payload=receive(left)
                if kind==b'X': raise RuntimeError(bytes(payload).decode())
                self.assertEqual(kind,b'R')
                self.assertEqual(struct.unpack_from('<I',payload)[0],1)
                self.assertEqual(len(payload),4+columns*17)
                return [int.from_bytes(payload[5+c*17:21+c*17],'little',signed=True) if payload[4+c*17] else None
                        for c in range(columns)]
        finally:
            thread.join(4); self.assertFalse(thread.is_alive())

    def test_exceeds_double_exactness_and_signed_64(self):
        for chunk in [0,1]:
            self.assertEqual(self.run_stream([[(2**63-1,)],[(2**63-1,)]],chunk_bytes=chunk),[2**64-2])

    def test_null_empty_and_signed(self):
        self.assertEqual(self.run_stream([]),[None])
        self.assertEqual(self.run_stream([[(None,)],[(None,)]]),[None])
        self.assertEqual(self.run_stream([[(-2**63,)],[(7,),(None,)]]),[-2**63+7])

    def test_decimal_limbs_with_cancellation(self):
        values=[10**37+123456789,-10**37, -(2**65),2**65, None]
        def limbs(value):
            if value is None: return (None,)*4
            raw=value.to_bytes(16,'little',signed=True)
            return tuple(int.from_bytes(raw[i*4:(i+1)*4],'little',signed=i==3) for i in range(4))
        parts=self.run_stream([[limbs(v)] for v in values],columns=4)
        self.assertEqual(sum(part << (32*i) for i,part in enumerate(parts)),123456789)

    def test_float_result_is_rejected(self):
        for value in [1.0,True,'1.25']:
            with self.assertRaises((ValueError,TypeError)): exact_integer(value)

    def test_out_of_range_reply_is_rejected(self):
        class Overflow(SumBackend):
            def execute(self,plan,reader):
                list(reader)
                return json.dumps({'rows':[[str(2**127)]]}).encode()
        with self.assertRaisesRegex(RuntimeError,'128-bit'):
            self.run_stream([],backend=Overflow(0))

    def test_invalid_shape_rejected_before_ack(self):
        for descriptor in [(3,1,1,0,16),(3,0,0,0,16),(3,0,1,1,16),(3,0,65,0,16)]:
            with self.assertRaisesRegex(ValueError,'shape'):
                native_aggregate(None,SumBackend(0),bytes(descriptor))


if __name__=='__main__': unittest.main()
