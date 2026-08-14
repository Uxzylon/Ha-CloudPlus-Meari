"""Small TCP MPEG-TS stream server."""

from __future__ import annotations

import collections
import logging
import socket
import threading
from dataclasses import dataclass
from typing import Callable, Deque

from .stream_bootstrap import StreamBootstrap

_LOGGER = logging.getLogger(__name__)

CLIENT_QUEUE_CHUNKS = 4096
CLIENT_SEND_TIMEOUT_S = 10.0


@dataclass
class _Client:
    sock: socket.socket
    queue: Deque[bytes]
    event: threading.Event
    wait_generation: int = 0


class StreamServer:
    """Local TCP server that fans the muxed MPEG-TS stream out to clients."""

    def __init__(self, on_missing_bootstrap: Callable[[], bool] | None = None) -> None:
        self._sock: socket.socket | None = None
        self._port = 0
        self._running = False
        self._clients: list[_Client] = []
        self._lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
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
        with self._lifecycle_lock:
            if (
                self._running
                and self._sock is not None
                and self._accept_thread is not None
                and self._accept_thread.is_alive()
            ):
                return

            old_sock = self._sock
            if old_sock is not None:
                try:
                    old_sock.close()
                except OSError:
                    pass

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(("0.0.0.0", 0))
                sock.listen(8)
                sock.settimeout(1.0)
            except OSError:
                sock.close()
                self._running = False
                self._sock = None
                self._port = 0
                raise

            self._running = True
            self._sock = sock
            self._port = sock.getsockname()[1]
            self._accept_thread = threading.Thread(
                target=self._accept_loop,
                args=(sock,),
                daemon=True,
            )
            self._accept_thread.start()

    def ensure_running(self) -> int:
        """Restart a failed listener and return its current port."""
        self.start()
        return self._port

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._running = False
            sock = self._sock
            accept_thread = self._accept_thread
            self._sock = None
            self._accept_thread = None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        if accept_thread is not None:
            accept_thread.join(timeout=2)
        with self._lock:
            for client in self._clients:
                client.event.set()
                try:
                    client.sock.close()
                except OSError:
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
            for client in self._clients:
                released = False
                if client.wait_generation and generation >= client.wait_generation:
                    bootstrap = self._bootstrap.snapshot()
                    if bootstrap:
                        client.queue.clear()
                        client.queue.append(bootstrap)
                        client.event.set()
                    client.wait_generation = 0
                    released = True
                if not client.wait_generation and not released:
                    client.queue.append(data)
                    client.event.set()

    def _accept_loop(self, listener: socket.socket) -> None:
        while self._running and self._sock is listener:
            try:
                client, _addr = listener.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                if self._running and self._sock is listener:
                    _LOGGER.warning("Local stream listener stopped: %s", exc)
                break
            try:
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.settimeout(CLIENT_SEND_TIMEOUT_S)
            except OSError:
                client.close()
                continue
            queue: Deque[bytes] = collections.deque(maxlen=CLIENT_QUEUE_CHUNKS)
            event = threading.Event()
            wait_generation = 0
            with self._lock:
                bootstrap = self._bootstrap.snapshot()
                target_generation = self._bootstrap_generation_unlocked() + 1
            if self._on_missing_bootstrap is not None:
                try:
                    if self._on_missing_bootstrap():
                        wait_generation = target_generation
                except (OSError, RuntimeError):
                    pass
            with self._lock:
                if wait_generation:
                    if self._bootstrap_generation_unlocked() >= wait_generation:
                        bootstrap = self._bootstrap.snapshot()
                        wait_generation = 0
                    else:
                        bootstrap = b""
                elif not bootstrap:
                    bootstrap = self._bootstrap.snapshot()
                if bootstrap:
                    queue.append(bootstrap)
                    event.set()
                self._clients.append(_Client(client, queue, event, wait_generation))
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
        except OSError:
            pass
        finally:
            with self._lock:
                self._clients = [
                    item for item in self._clients if item.sock is not client
                ]
            try:
                client.close()
            except OSError:
                pass
