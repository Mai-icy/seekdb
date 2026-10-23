"""Version 1 local sidecar framing: kind byte, big-endian uint32 size, payload."""
import socket
import struct

MAX_FRAME = 16 * 1024 * 1024


def receive_exact(sock, size):
    data = bytearray(size)
    view = memoryview(data)
    offset = 0
    while offset < size:
        count = sock.recv_into(view[offset:])
        if not count:
            raise ConnectionError("Sidecar peer disconnected")
        offset += count
    return data


def receive(sock, limit=MAX_FRAME):
    header = receive_exact(sock, 5)
    size = struct.unpack("!I", header[1:])[0]
    if size > limit:
        raise ValueError("Sidecar frame exceeds limit")
    return bytes(header[:1]), receive_exact(sock, size)


def send(sock, kind, data=b""):
    if len(kind) != 1 or len(data) > MAX_FRAME:
        raise ValueError("Invalid sidecar frame")
    sock.sendall(kind + struct.pack("!I", len(data)))
    sock.sendall(data)


def expect(sock, kind, limit=MAX_FRAME):
    actual, payload = receive(sock, limit)
    if actual == b"X":
        raise RuntimeError(payload.decode("utf-8", errors="replace"))
    if actual != kind:
        raise ValueError(f"Expected sidecar frame {kind!r}, got {actual!r}")
    return payload


def connect(path, timeout=60):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(str(path))
    except BaseException:
        sock.close()
        raise
    return sock
