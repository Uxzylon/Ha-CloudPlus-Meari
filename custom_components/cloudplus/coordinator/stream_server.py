"""Small TCP MPEG-TS stream server."""

from __future__ import annotations

import collections
import socket
import threading
from typing import Deque

from .stream_bootstrap import StreamBootstrap

CLIENT_QUEUE_CHUNKS = 4096


class StreamServer:
    def __init__(self) -> None:
        self._sock: socket.socket | None = None
        self._port = 0
        self._running = False
        self._clients: list[tuple[socket.socket, Deque[bytes], threading.Event]] = []
        self._lock = threading.Lock()
        self._accept_thread: threading.Thread | None = None
        self._bootstrap = StreamBootstrap()

    @property
    def port(self) -> int:
        return self._port

    @property
    def client_count(self) -> int:
        with self._lock:
            return len(self._clients)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", 0))
        sock.listen(8)
        sock.settimeout(1.0)
        self._sock = sock
        self._port = sock.getsockname()[1]
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def stop(self) -> None:
        self._running = False
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2)
            self._accept_thread = None
        with self._lock:
            for client, _queue, event in self._clients:
                event.set()
                try:
                    client.close()
                except Exception:
                    pass
            self._clients.clear()
            self._bootstrap.reset()
        self._port = 0

    def bootstrap_snapshot(self) -> bytes:
        with self._lock:
            return self._bootstrap.snapshot()

    def bootstrap_state(self) -> dict[str, int | bool]:
        with self._lock:
            return self._bootstrap.state()

    def broadcast(self, data: bytes) -> None:
        if not data:
            return
        with self._lock:
            self._bootstrap.update(data)
            for _client, queue, event in self._clients:
                queue.append(data)
                event.set()

    def _accept_loop(self) -> None:
        while self._running and self._sock is not None:
            try:
                client, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            queue: Deque[bytes] = collections.deque(maxlen=CLIENT_QUEUE_CHUNKS)
            event = threading.Event()
            with self._lock:
                bootstrap = self._bootstrap.snapshot()
                if bootstrap:
                    queue.append(bootstrap)
                    event.set()
                self._clients.append((client, queue, event))
            threading.Thread(
                target=self._client_writer,
                args=(client, queue, event),
                daemon=True,
            ).start()

    def _client_writer(
        self,
        client: socket.socket,
        queue: Deque[bytes],
        event: threading.Event,
    ) -> None:
        try:
            while self._running:
                event.wait(timeout=0.5)
                event.clear()
                while queue:
                    chunk = queue.popleft()
                    client.sendall(chunk)
        except Exception:
            pass
        finally:
            with self._lock:
                self._clients = [
                    (c, q, e) for c, q, e in self._clients if c is not client
                ]
            try:
                client.close()
            except Exception:
                pass
