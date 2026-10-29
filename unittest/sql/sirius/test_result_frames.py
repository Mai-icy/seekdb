"""Large grouped replies must opt in without widening legacy frame limits."""
from pathlib import Path
import socket
import sys
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from wire import MAX_FRAME, receive, send


class ResultFrames(unittest.TestCase):
    def test_explicit_large_reply_and_legacy_bound(self):
        payload = b'x' * (MAX_FRAME + 1)
        with self.assertRaises(ValueError): send(None,b'R',payload)
        left,right=socket.socketpair()
        left.settimeout(5);right.settimeout(5)
        errors=[]
        def produce():
            try:send(left,b'R',payload,limit=256*1024**2)
            except Exception as error:errors.append(error)
            finally:left.close()
        thread=threading.Thread(target=produce);thread.start()
        try:
            with right:
                kind,data=receive(right,limit=256*1024**2)
                self.assertEqual(kind,b'R');self.assertEqual(data,payload)
        finally:
            thread.join(6);self.assertFalse(thread.is_alive())
        self.assertEqual(errors,[])

    def test_invalid_limit(self):
        for limit in (0,-1,256*1024**2+1):
            with self.assertRaises(ValueError):send(None,b'R',limit=limit)


if __name__=='__main__':unittest.main()
