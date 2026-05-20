"""Small TCP MPEG-TS stream server."""

from __future__ import annotations

import collections
import socket
import threading
import time
from dataclasses import dataclass
from typing import Callable, Deque

from .stream_bootstrap import StreamBootstrap

CLIENT_QUEUE_CHUNKS = 4096
CLIENT_SEND_TIMEOUT_S = 10.0
FRESH_BOOTSTRAP_WAIT_S = 3.0


@dataclass
class _Client:
    sock: socket.socket
    queue: Deque[bytes]
    event: threading.Event
    wait_generation: int = 0
    wait_deadline: float = 0.0


class StreamServer:
    def __init__(self, on_missing_bootstrap: Callable[[], bool] | None = None) -> None:
        self._sock: socket.socket | None = None
        self._port = 0
        self._running = False
        self._clients: list[_Client] = []
        self._lock = threading.Lock()
        self._accept_thread: threading.Thread | None = None
        self._bootstrap = StreamBootstrap()
        self._on_missing_bootstrap = on_missing_bootstrap

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
            for client in self._clients:
                client.event.set()
                try:
                    client.sock.close()
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

    def reset_bootstrap(self) -> None:
        with self._lock:
            self._bootstrap.reset()

    def broadcast(self, data: bytes) -> None:
        if not data:
            return
        with self._lock:
            self._bootstrap.update(data)
            generation = int(self._bootstrap.state().get("generation", 0) or 0)
            now = time.monotonic()
            for client in self._clients:
                released = False
                if client.wait_generation and (
                    generation >= client.wait_generation or now >= client.wait_deadline
                ):
                    bootstrap = self._bootstrap.snapshot()
                    if bootstrap:
                        client.queue.clear()
                        client.queue.append(bootstrap)
                        client.event.set()
                    client.wait_generation = 0
                    client.wait_deadline = 0.0
                    released = True
                if not client.wait_generation and not released:
                    client.queue.append(data)
                    client.event.set()

    def _accept_loop(self) -> None:
        while self._running and self._sock is not None:
            try:
                client, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            client.settimeout(CLIENT_SEND_TIMEOUT_S)
            queue: Deque[bytes] = collections.deque(maxlen=CLIENT_QUEUE_CHUNKS)
            event = threading.Event()
            wait_generation = 0
            wait_deadline = 0.0
            with self._lock:
                bootstrap = self._bootstrap.snapshot()
                target_generation = self._bootstrap_generation_unlocked() + 1
            if not bootstrap and self._on_missing_bootstrap is not None:
                try:
                    if self._on_missing_bootstrap():
                        wait_generation = target_generation
                        wait_deadline = time.monotonic() + FRESH_BOOTSTRAP_WAIT_S
                except Exception:
                    pass
            with self._lock:
                if wait_generation:
                    if self._bootstrap_generation_unlocked() >= wait_generation:
                        bootstrap = self._bootstrap.snapshot()
                        wait_generation = 0
                        wait_deadline = 0.0
                    else:
                        bootstrap = b""
                elif not bootstrap:
                    bootstrap = self._bootstrap.snapshot()
                if bootstrap:
                    queue.append(bootstrap)
                    event.set()
                self._clients.append(
                    _Client(client, queue, event, wait_generation, wait_deadline)
                )
            threading.Thread(
                target=self._client_writer,
                args=(client, queue, event),
                daemon=True,
            ).start()

    def _bootstrap_generation_unlocked(self) -> int:
        return int(self._bootstrap.state().get("generation", 0) or 0)

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
                    item for item in self._clients if item.sock is not client
                ]
            try:
                client.close()
            except Exception:
                pass
