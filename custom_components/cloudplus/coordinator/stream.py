"""Stream server and bootstrap helpers for the coordinator."""

from __future__ import annotations

import collections
import logging
import socket
import statistics
import threading
import time
from typing import Any

from .mpegts import compare_stream_join_boundary, summarize_ts_window

_LOGGER = logging.getLogger(__name__)


class CoordinatorStreamMixin:
    """Owns MPEG-TS client serving, bootstrap backlog, and join diagnostics."""

    def _start_stream_server(self) -> None:
        """Start TCP server to serve MPEG-TS stream to clients."""
        try:
            self._stream_server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._stream_server_sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )
            self._stream_server_sock.bind(("0.0.0.0", 0))
            self._stream_port = self._stream_server_sock.getsockname()[1]
            self._stream_server_sock.listen(5)
            self._stream_server_sock.settimeout(2)
            self._stream_epoch = time.time()
            self._stream_accept_thread = threading.Thread(
                target=self._accept_stream_clients,
                daemon=True,
            )
            self._stream_accept_thread.start()
            _LOGGER.info(
                "Stream server started on port %d for %s",
                self._stream_port,
                self._sn_num,
            )
        except OSError as e:
            _LOGGER.error("Failed to start stream server: %s", e)

    def _stop_stream_server(self) -> None:
        """Stop the TCP stream server and disconnect all clients."""
        if self._stream_server_sock:
            try:
                self._stream_server_sock.close()
            except OSError:
                pass
            self._stream_server_sock = None
        if self._stream_accept_thread:
            self._stream_accept_thread.join(timeout=5)
            self._stream_accept_thread = None
        with self._stream_clients_lock:
            for client, _queue, event in self._stream_clients:
                event.set()
                try:
                    client.close()
                except OSError:
                    pass
            while self._pending_stream_clients:
                client, _queue, event, _connected_mono, _addr = (
                    self._pending_stream_clients.popleft()
                )
                event.set()
                try:
                    client.close()
                except OSError:
                    pass
            self._stream_clients.clear()
            self._pending_stream_clients.clear()
            self._stream_backlog.clear()
            self._stream_backlog_has_audio = False
            self._stream_idr_seed = b""
            self._stream_idr_seed_pts = 0
            self._stream_idr_seed_mono = 0.0
            self._stream_idr_seed_p2p_frame_index = 0
            self._stream_idr_seed_video_bytes = 0
            self._stream_idr_seed_is_strong = False
            self._stream_idr_seed_strength_reason = ""
            self._stream_idr_seed_generation = 0
            self._startup_safe_min_seed_generation = 0
            self._stream_idr_collecting = False
            self._stream_idr_buf.clear()
            self._stream_broadcast_audio_pts = 0
            self._stream_pid_last_cc.clear()
            self._gap_skip_seq_to_event_id.clear()
            with self._gap_skip_events_lock:
                self._gap_skip_events.clear()
                self._gap_skip_event_counter = 0
                self._last_gap_skip_event_id = 0
                self._last_gap_skip_diag = None
            self._recent_video_rai_ts_sizes.clear()
            self._seed_probe_reject_streak = 0
            self._seed_probe_first_reject_mono = 0.0
            self._stream_join_event_counter = 0
            self._stream_join_events.clear()
            self._stream_join_events_by_client.clear()

    def _get_startup_safe_seed_locked(
        self,
        pid_last_cc: dict[int, int] | None = None,
    ) -> tuple[bytes, str]:
        state = self.get_startup_bootstrap_state()
        block_reason = str(state.get("block_reason", "not-ready"))
        startup_safe = bool(state.get("startup_safe", False))
        frames_since_seed = int(state.get("frames_since_seed", 0) or 0)
        backlog_follow_target = int(
            state.get(
                "backlog_follow_video_pusi_target",
                self._preferred_backlog_follow_video_pusi_target(),
            )
            or 0
        )
        backlog_window_safe = block_reason in {
            "ready",
            "seed-not-fresh",
            "seed-awaiting-follow-frames",
        }
        if (
            backlog_window_safe
            and not self._stream_idr_collecting
            and self._video_codec != "hevc"
        ):
            backlog_bootstrap = self._extract_recent_backlog_bootstrap_locked(
                min_follow_video_pusi=backlog_follow_target,
            )
            if backlog_bootstrap:
                return (backlog_bootstrap, "ready-backlog")
            if frames_since_seed < backlog_follow_target:
                return (b"", "backlog-building")
        if startup_safe and not self._stream_idr_collecting:
            seed = self._rebase_idr_seed(pid_last_cc=pid_last_cc)
            if seed:
                return (seed, "ready")
        return (b"", block_reason)

    def _preferred_backlog_follow_video_pusi_target(self) -> int:
        """Return the desired live-runway depth before using backlog bootstrap."""
        reference_bytes = int(self._stream_idr_seed_video_bytes or 0)
        if self._recent_video_rai_ts_sizes:
            try:
                reference_bytes = max(
                    reference_bytes,
                    int(statistics.median(self._recent_video_rai_ts_sizes)),
                )
            except statistics.StatisticsError:
                pass

        if reference_bytes >= 160 * 1024:
            return 4
        if reference_bytes >= 96 * 1024:
            return 5
        if reference_bytes >= 64 * 1024:
            return 6
        return 8

    def _extract_recent_backlog_bootstrap_locked(
        self,
        *,
        min_follow_video_pusi: int,
    ) -> bytes:
        """Return a recent live-output bootstrap slice starting at a safe video RA point."""
        packet_size = 188
        pat_pid = 0x0000
        video_pid = 0x0100
        backlog = self._stream_backlog
        if len(backlog) < packet_size * 4:
            return b""

        last_pat_off = -1
        total_video_pusi = 0
        candidates: list[tuple[int, int]] = []
        n_packets = len(backlog) // packet_size
        for index in range(n_packets):
            off = index * packet_size
            if backlog[off] != 0x47:
                continue
            pid = ((backlog[off + 1] & 0x1F) << 8) | backlog[off + 2]
            pusi = bool(backlog[off + 1] & 0x40)
            if pid == pat_pid and pusi:
                last_pat_off = off
                continue
            if pid != video_pid or not pusi:
                continue

            afc = (backlog[off + 3] >> 4) & 0x03
            is_rai = False
            if (afc & 0x02) and backlog[off + 4] >= 1:
                is_rai = bool(backlog[off + 5] & 0x40)

            if is_rai and last_pat_off >= 0:
                candidates.append((last_pat_off, total_video_pusi))
            total_video_pusi += 1

        if total_video_pusi <= 0:
            return b""

        for start_off, pusi_index in reversed(candidates):
            follow_pusi = total_video_pusi - pusi_index - 1
            if follow_pusi < min_follow_video_pusi:
                continue
            bootstrap = bytes(backlog[start_off:])
            if len(bootstrap) >= packet_size * 8:
                return bootstrap
        return b""

    def _promote_pending_stream_clients_locked(
        self,
        pid_last_cc: dict[int, int] | None = None,
        seed: bytes | None = None,
        reason: str | None = None,
    ) -> tuple[int, set[int]]:
        if not self._pending_stream_clients:
            return (0, set())
        if seed is None:
            seed, reason = self._get_startup_safe_seed_locked(pid_last_cc=pid_last_cc)
        if reason is None:
            reason = "ready"
        if not self._stream_clients and reason == "ready":
            return (0, set())
        if not seed:
            return (0, set())

        promoted = 0
        skip_current_chunk_clients: set[int] = set()
        seed_bytes = len(seed)
        now_mono = time.monotonic()
        while self._pending_stream_clients:
            client, queue, event, connected_mono, addr = (
                self._pending_stream_clients.popleft()
            )
            self._register_stream_join_event_locked(
                client=client,
                addr=addr,
                seed=seed,
                reason=reason,
                connected_mono=connected_mono,
                released_mono=now_mono,
                mode="pending",
            )
            queue.append(seed)
            self._stream_clients.append((client, queue, event))
            if reason in {"ready", "ready-backlog"}:
                skip_current_chunk_clients.add(id(client))
            event.set()
            promoted += 1
            _LOGGER.info(
                "Released pending stream client %s after %.2fs with startup-safe bootstrap (%d bytes, reason=%s)",
                addr,
                max(0.0, now_mono - connected_mono),
                seed_bytes,
                reason,
            )
        return (promoted, skip_current_chunk_clients)

    def _accept_stream_clients(self) -> None:
        """Accept loop for TCP stream clients (runs in thread)."""
        while self._running and self._stream_server_sock:
            try:
                client, addr = self._stream_server_sock.accept()
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
                client.settimeout(10.0)
                seed_wait_deadline = time.monotonic() + 0.75
                while (
                    self._running
                    and self._stream_idr_collecting
                    and time.monotonic() < seed_wait_deadline
                ):
                    time.sleep(0.02)
                queue: collections.deque = collections.deque(
                    maxlen=self._stream_client_queue_limit,
                )
                event = threading.Event()
                with self._stream_clients_lock:
                    seed, seed_reason = self._get_startup_safe_seed_locked()
                    prefer_backlog_first_client = bool(
                        seed
                        and seed_reason == "ready"
                        and not self._stream_clients
                        and not self._pending_stream_clients
                    )
                    waiting_for_seed = not bool(seed) or prefer_backlog_first_client
                    if seed and not prefer_backlog_first_client:
                        now_mono = time.monotonic()
                        self._register_stream_join_event_locked(
                            client=client,
                            addr=str(addr),
                            seed=seed,
                            reason=seed_reason,
                            connected_mono=now_mono,
                            released_mono=now_mono,
                            mode="immediate",
                        )
                        queue.append(seed)
                        seed_bytes = len(seed)
                        self._stream_clients.append((client, queue, event))
                    else:
                        self._pending_stream_clients.append(
                            (client, queue, event, time.monotonic(), str(addr))
                        )
                        seed_bytes = 0
                threading.Thread(
                    target=self._client_writer,
                    args=(client, queue, event),
                    daemon=True,
                ).start()
                if waiting_for_seed:
                    _LOGGER.info(
                        "Stream client connected from %s (waiting for startup-safe bootstrap: %s)",
                        addr,
                        seed_reason,
                    )
                else:
                    _LOGGER.info(
                        "Stream client connected from %s (bootstrap=%d bytes, startup-safe)",
                        addr,
                        seed_bytes,
                    )
            except socket.timeout:
                continue
            except OSError:
                break

    def _client_writer(
        self,
        client: socket.socket,
        queue: collections.deque,
        event: threading.Event,
    ) -> None:
        """Dedicated writer thread per TCP client — blocking sendall."""
        try:
            while self._running:
                event.wait(timeout=0.5)
                event.clear()
                while queue:
                    chunk = queue.popleft()
                    client.sendall(chunk)
        except (BrokenPipeError, ConnectionError, OSError, TimeoutError) as exc:
            _LOGGER.warning("Stream client writer error: %s", exc)
        finally:
            with self._stream_clients_lock:
                self._mark_stream_join_disconnected_locked(client)
                self._stream_clients = [
                    (current_client, current_queue, current_event)
                    for current_client, current_queue, current_event in self._stream_clients
                    if current_client is not client
                ]
                self._pending_stream_clients = collections.deque(
                    (current_client, current_queue, current_event, connected_mono, addr)
                    for current_client, current_queue, current_event, connected_mono, addr in self._pending_stream_clients
                    if current_client is not client
                )
            try:
                client.close()
            except OSError:
                pass
            _LOGGER.info("Stream client disconnected")

    def _append_stream_backlog(self, data: bytes) -> None:
        """Keep an MPEG-TS backlog for new client bootstrap."""
        if not data:
            return
        packet_size = 188
        self._stream_backlog.extend(data)

        if not self._stream_backlog_has_audio:
            scan_start = max(0, len(self._stream_backlog) - len(data))
            scan_start = (scan_start // packet_size) * packet_size
            n_total = len(self._stream_backlog) // packet_size
            for index in range(scan_start // packet_size, n_total):
                off = index * packet_size
                if off + packet_size > len(self._stream_backlog):
                    break
                if self._stream_backlog[off] != 0x47:
                    continue
                pid = (
                    (self._stream_backlog[off + 1] & 0x1F) << 8
                ) | self._stream_backlog[off + 2]
                if pid == 0x101:
                    self._stream_backlog_has_audio = True
                    _LOGGER.info(
                        "Backlog: audio detected at %.2f MB, switching to rolling mode",
                        len(self._stream_backlog) / 1048576,
                    )
                    break

        if not self._stream_backlog_has_audio:
            if len(self._stream_backlog) > 64 * 1024 * 1024:
                _LOGGER.warning("Backlog: 64MB cap reached without audio, trimming")
                self._stream_backlog_has_audio = True
            else:
                return

        if len(self._stream_backlog) > self._stream_backlog_max_bytes:
            overflow = len(self._stream_backlog) - self._stream_backlog_max_bytes
            trim_at = overflow
            search_end = min(trim_at + 256 * packet_size, len(self._stream_backlog))
            for index in range(trim_at // packet_size, search_end // packet_size):
                off = index * packet_size
                if off + packet_size > len(self._stream_backlog):
                    break
                if self._stream_backlog[off] != 0x47:
                    continue
                pid = (
                    (self._stream_backlog[off + 1] & 0x1F) << 8
                ) | self._stream_backlog[off + 2]
                if pid == 0x0000:
                    trim_at = off
                    break
            del self._stream_backlog[:trim_at]

        if self._stream_backlog:
            first_sync = self._stream_backlog.find(b"\x47")
            if first_sync < 0:
                self._stream_backlog.clear()
                return
            if first_sync > 0:
                del self._stream_backlog[:first_sync]
            remainder = len(self._stream_backlog) % packet_size
            if remainder:
                del self._stream_backlog[-remainder:]

    def _broadcast_stream(self, data: bytes) -> None:
        """Send MPEG-TS data to all connected stream clients."""
        if not data:
            return
        chunk = bytes(data)
        pending_live_fallback_timeout_s = 1.5
        with self._stream_clients_lock:
            self._append_stream_backlog(chunk)
            previous_pid_last_cc = dict(self._stream_pid_last_cc)
            release_seed, release_reason = self._get_startup_safe_seed_locked(
                pid_last_cc=previous_pid_last_cc,
            )
            self._update_idr_seed(chunk)
            self._update_stream_pid_cc(chunk)
            skip_current_chunk_clients: set[int] = set()
            if self._pending_stream_clients and release_reason != "ready-backlog":
                post_update_seed, post_update_reason = (
                    self._get_startup_safe_seed_locked(
                        pid_last_cc=previous_pid_last_cc,
                    )
                )
                if post_update_seed and (
                    not release_seed or post_update_reason == "ready-backlog"
                ):
                    release_seed = post_update_seed
                    release_reason = post_update_reason
            if release_seed:
                _, skip_current_chunk_clients = (
                    self._promote_pending_stream_clients_locked(
                        seed=release_seed,
                        reason=release_reason,
                    )
                )
            elif self._pending_stream_clients:
                now_mono = time.monotonic()
                remaining_pending: collections.deque[
                    tuple[socket.socket, collections.deque, threading.Event, float, str]
                ] = collections.deque()
                while self._pending_stream_clients:
                    client, queue, event, connected_mono, addr = (
                        self._pending_stream_clients.popleft()
                    )
                    waited_s = max(0.0, now_mono - connected_mono)
                    if waited_s < pending_live_fallback_timeout_s:
                        remaining_pending.append(
                            (client, queue, event, connected_mono, addr)
                        )
                        continue
                    self._register_stream_join_event_locked(
                        client=client,
                        addr=addr,
                        seed=b"",
                        reason="live-fallback-timeout",
                        connected_mono=connected_mono,
                        released_mono=now_mono,
                        mode="pending-fallback",
                    )
                    self._stream_clients.append((client, queue, event))
                    event.set()
                    _LOGGER.info(
                        "Released pending stream client %s after %.2fs with live fallback (no startup-safe bootstrap available)",
                        addr,
                        waited_s,
                    )
                self._pending_stream_clients = remaining_pending
            for client, queue, event in self._stream_clients:
                if id(client) in skip_current_chunk_clients:
                    continue
                self._capture_stream_join_live_chunk_locked(client, chunk)
                queue.append(chunk)
                event.set()

    def _register_stream_join_event_locked(
        self,
        client: socket.socket,
        addr: str,
        seed: bytes,
        reason: str,
        connected_mono: float,
        released_mono: float,
        mode: str,
    ) -> None:
        self._stream_join_event_counter += 1
        seed_summary = summarize_ts_window(seed)
        event = {
            "event_id": self._stream_join_event_counter,
            "addr": addr,
            "mode": mode,
            "reason": reason,
            "connected_mono": float(connected_mono),
            "released_mono": float(released_mono),
            "wait_s": max(0.0, float(released_mono) - float(connected_mono)),
            "seed_generation": int(self._stream_idr_seed_generation),
            "required_seed_generation": int(self._startup_safe_min_seed_generation),
            "seed_bytes": len(seed),
            "seed_offset_s": (
                (self._stream_broadcast_video_pts - self._stream_idr_seed_pts) / 90000.0
                if self._stream_idr_seed_pts > 0
                else None
            ),
            "seed_summary": seed_summary,
            "live_chunk_count": 0,
            "live_capture": bytearray(),
        }
        self._stream_join_events.append(event)
        self._stream_join_events_by_client[id(client)] = event

    def _capture_stream_join_live_chunk_locked(
        self,
        client: socket.socket,
        chunk: bytes,
    ) -> None:
        event = self._stream_join_events_by_client.get(id(client))
        if event is None or not chunk:
            return
        capture = event.get("live_capture")
        if not isinstance(capture, bytearray):
            return
        if len(capture) >= 32 * 188:
            return
        event["live_chunk_count"] = int(event.get("live_chunk_count", 0) or 0) + 1
        remaining = (32 * 188) - len(capture)
        capture.extend(chunk[:remaining])

    def _mark_stream_join_disconnected_locked(self, client: socket.socket) -> None:
        event = self._stream_join_events_by_client.pop(id(client), None)
        if event is not None:
            event["disconnected_mono"] = time.monotonic()

    def get_stream_join_diagnostics_snapshot(self) -> list[dict[str, Any]]:
        with self._stream_clients_lock:
            snapshot: list[dict[str, Any]] = []
            for event in list(self._stream_join_events):
                item = {
                    key: value for key, value in event.items() if key != "live_capture"
                }
                seed_summary = dict(event.get("seed_summary", {}))
                item["seed_summary"] = seed_summary
                live_capture = bytes(event.get("live_capture", b""))
                live_summary = summarize_ts_window(live_capture) if live_capture else {}
                item["live_summary"] = live_summary
                item["boundary"] = (
                    compare_stream_join_boundary(seed_summary, live_summary)
                    if seed_summary and live_summary
                    else {}
                )
                snapshot.append(item)
            return snapshot

    def _update_stream_pid_cc(self, data: bytes) -> None:
        """Track the latest MPEG-TS continuity counter seen per PID."""
        packet_size = 188
        for index in range(len(data) // packet_size):
            off = index * packet_size
            if data[off] != 0x47:
                continue
            pid = ((data[off + 1] & 0x1F) << 8) | data[off + 2]
            self._stream_pid_last_cc[pid] = data[off + 3] & 0x0F
