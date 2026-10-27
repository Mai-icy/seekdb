"""Bounded native frame prefetch; each buffer has one owner until GPU copy ends."""
import queue
import socket
import struct
import threading

from wire import receive, send


class NativeInput:
    def __init__(self, sock, frame_limit, row_limit, columns, storage, buffers=()):
        if buffers and (len(buffers) != 2 or any(len(b) < frame_limit for b in buffers)):
            raise ValueError('Native prefetch requires two buffers at least as large as the frame bound')
        self.sock, self.frame_limit, self.row_limit = sock, frame_limit, row_limit
        self.columns, self.storage, self.buffers = columns, storage, buffers
        self.free = queue.Queue(maxsize=2)
        self.ready = queue.Queue(maxsize=2)
        self.stopped = threading.Event()
        self.worker = None
        for index in range(len(buffers)):
            self.free.put(index)

    def _take(self, items):
        while not self.stopped.is_set():
            try:
                return items.get(timeout=0.1)
            except queue.Empty:
                pass
        raise RuntimeError('Native input stopped')

    def _put(self, items, item):
        while not self.stopped.is_set():
            try:
                items.put(item, timeout=0.1)
                return
            except queue.Full:
                pass

    def _read(self, storage):
        kind, payload = receive(self.sock, limit=self.frame_limit, storage=storage)
        if kind == b'E' and not payload:
            return kind, payload
        if kind != b'B' or len(payload) < 4:
            raise ValueError('Expected native aggregate batch')
        rows = struct.unpack_from('<I', payload)[0]
        size = ((rows + 7) // 8 + rows * 8) * self.columns
        if not 0 < rows <= self.row_limit or len(payload) != 4 + size:
            raise ValueError('Native aggregate input size/budget exceeded')
        return kind, payload

    def start(self):
        # The caller sends the opening ACK before starting this sole ACK writer.
        if self.buffers:
            worker = threading.Thread(target=self._produce, name='sirius-native-input')
            worker.start()
            self.worker = worker

    def _produce(self):
        try:
            while not self.stopped.is_set():
                index = self._take(self.free)
                kind, payload = self._read(self.buffers[index])
                if kind == b'E':
                    self._put(self.ready, (kind, b'', None))
                    return
                # This independent buffer now owns the entire validated frame.
                # ACK permits the producer's buffer reuse, not this buffer's.
                send(self.sock, b'A')
                self._put(self.ready, (kind, payload, index))
        except Exception as error:
            self._put(self.ready, error)

    def get(self):
        if not self.buffers:
            kind, payload = self._read(self.storage)
            return kind, payload, None
        item = self._take(self.ready)
        if isinstance(item, Exception):
            raise item
        return item

    def release(self, index):
        # Called only after synchronous push_arrow has finished using this frame.
        if self.buffers:
            self._put(self.free, index)
        else:
            send(self.sock, b'A')

    def close(self):
        if self.worker is not None:
            self.stopped.set()
            if self.worker.is_alive():
                # Wake a blocked recv without disabling the final error reply.
                try:
                    self.sock.shutdown(socket.SHUT_RD)
                except OSError:
                    pass
            self.worker.join()
            self.worker = None
