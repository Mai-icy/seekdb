import socket
import struct
import sys
import time
import unittest
from unittest.mock import MagicMock, patch
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'tools/sirius'))
from native_input import NativeInput
from wire import receive, send


class NativeInputTests(unittest.TestCase):
    def setUp(self):
        self.client, self.server = socket.socketpair()
        self.client.settimeout(1)
        self.server.settimeout(1)
        self.source = NativeInput(self.server, 64, 4, 1, None,
                                  tuple(memoryview(bytearray(64)) for _ in range(2)))
        self.source.start()

    def tearDown(self):
        self.source.close()
        self.client.close()
        self.server.close()

    def batch(self, value):
        send(self.client, b'B', struct.pack('<IBq', 1, 1, value))

    def test_early_ack_and_no_reuse_until_release(self):
        self.batch(11)
        self.assertEqual(receive(self.client), (b'A', b''))
        _, first, one = self.source.get()
        self.batch(22)
        self.assertEqual(receive(self.client), (b'A', b''))
        _, second, two = self.source.get()
        self.batch(33)
        self.client.settimeout(0.1)
        with self.assertRaises(socket.timeout):
            self.client.recv(1)
        self.client.settimeout(1)
        self.assertEqual(struct.unpack_from('<q', first, 5)[0], 11)
        self.assertEqual(struct.unpack_from('<q', second, 5)[0], 22)
        self.source.release(one)
        self.assertEqual(receive(self.client), (b'A', b''))
        _, third, three = self.source.get()
        self.assertEqual(one, three)
        self.assertEqual(struct.unpack_from('<q', third, 5)[0], 33)
        self.assertEqual(struct.unpack_from('<q', second, 5)[0], 22)
        self.source.release(two)
        self.source.release(three)
        send(self.client, b'E')
        self.assertEqual(self.source.get(), (b'E', b'', None))

    def test_close_interrupts_receive_and_preserves_error_reply(self):
        start = time.monotonic()
        self.source.close()
        self.assertLess(time.monotonic() - start, 0.5)
        send(self.server, b'X', b'backend failed')
        self.assertEqual(receive(self.client), (b'X', b'backend failed'))

    def test_close_interrupts_wait_for_free_buffer(self):
        for v in (1, 2):
            self.batch(v)
            self.assertEqual(receive(self.client), (b'A', b''))
        start = time.monotonic()
        self.source.close()
        self.assertLess(time.monotonic() - start, 0.5)

    def test_malformed_input_is_not_acknowledged(self):
        send(self.client, b'B', struct.pack('<IBq', 3, 1, 11))
        with self.assertRaisesRegex(ValueError, 'size/budget'):
            self.source.get()
        self.client.settimeout(0.1)
        with self.assertRaises(socket.timeout):
            self.client.recv(1)

    def test_partial_frame_disconnect_propagates(self):
        self.client.sendall(b'B' + struct.pack('!I', 13) + b'\0')
        self.client.shutdown(socket.SHUT_WR)
        with self.assertRaisesRegex(ConnectionError, 'disconnected'):
            self.source.get()


class AllocationTests(unittest.TestCase):
    def test_failed_thread_start_can_be_closed(self):
        source = NativeInput(None, 64, 4, 1, None,
                             tuple(memoryview(bytearray(64)) for _ in range(2)))
        with patch('threading.Thread.start', side_effect=RuntimeError('thread limit')):
            with self.assertRaisesRegex(RuntimeError, 'thread limit'):
                source.start()
        source.close()
        self.assertIsNone(source.worker)

    def test_both_pins_released_if_bind_fails(self):
        import sidecar
        pins = [MagicMock(), MagicMock()]
        backend = MagicMock()
        listener = MagicMock()
        listener.__enter__.return_value = listener
        listener.bind.side_effect = OSError('injected bind failure')
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(sidecar, 'Backend', return_value=backend), \
                patch.object(sidecar, 'PinnedInput', side_effect=pins), \
                patch.object(sidecar.signal, 'signal'), \
                patch.object(sidecar.socket, 'socket', return_value=listener):
            with self.assertRaisesRegex(OSError, 'injected bind'):
                sidecar.serve(str(Path(directory) / 's'), '/unused', 1, 64, True, 128, True)
        for pin in pins:
            pin.close.assert_called_once()
        self.assertIsNone(backend.input_buffer)
        self.assertEqual(backend.native_prefetch_buffers, ())

    def test_first_pin_released_if_second_allocation_fails(self):
        import sidecar
        first = MagicMock()
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(sidecar, 'Backend'), \
                patch.object(sidecar, 'PinnedInput', side_effect=[first, RuntimeError('pin failed')]):
            with self.assertRaisesRegex(RuntimeError, 'pin failed'):
                sidecar.serve(str(Path(directory) / 's'), '/unused', 1, 64, True, 128, True)
        first.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
