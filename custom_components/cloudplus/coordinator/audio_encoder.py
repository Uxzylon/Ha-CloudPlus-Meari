"""Non-blocking G.711 mu-law to AAC/ADTS encoder."""

from __future__ import annotations

import collections
import os
import queue
import select
import subprocess
import threading
import time

AUDIO_GAIN_DB = 0.0
G711_SAMPLE_RATE = 8000
SILENCE_CHUNK = b"\xff" * (G711_SAMPLE_RATE // 25)
SILENCE_AFTER_S = 0.12
AAC_LOW_WATER_FRAMES = 16


class AacAudioEncoder:
    """Encode camera mu-law audio into ADTS frames for MPEG-TS injection."""

    def __init__(self, *, gain_db: float = AUDIO_GAIN_DB) -> None:
        self._gain_db = gain_db
        self._proc: subprocess.Popen | None = None
        self._queue: queue.Queue[bytes] = queue.Queue(maxsize=120)
        self._frames: collections.deque[bytes] = collections.deque(maxlen=120)
        self._running = False
        self._writer: threading.Thread | None = None
        self._reader: threading.Thread | None = None
        self._last_real_audio_at = 0.0

    def start(self) -> None:
        if self._proc is not None:
            return
        audio_filter = (
            ["-filter:a", f"volume={self._gain_db:.1f}dB"]
            if abs(self._gain_db) > 0.01
            else []
        )
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-f",
            "mulaw",
            "-ar",
            "8000",
            "-ac",
            "1",
            "-i",
            "pipe:0",
            *audio_filter,
            "-c:a",
            "aac",
            "-profile:a",
            "aac_low",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-b:a",
            "64k",
            "-flush_packets",
            "1",
            "-f",
            "adts",
            "pipe:1",
        ]
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        self._running = True
        proc = self._proc
        self._writer = threading.Thread(
            target=self._write_loop, args=(proc,), daemon=True
        )
        self._reader = threading.Thread(
            target=self._read_loop, args=(proc,), daemon=True
        )
        self._writer.start()
        self._reader.start()

    def stop(self) -> None:
        self._running = False
        proc = self._proc
        self._proc = None
        if proc is not None:
            try:
                if proc.stdin is not None:
                    proc.stdin.close()
            except Exception:
                pass
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                    proc.wait(timeout=1)
                except Exception:
                    pass
        self._drain_queue()
        self._frames.clear()
        self._last_real_audio_at = 0.0

    def clear(self) -> None:
        self._drain_queue()
        self._frames.clear()
        self._last_real_audio_at = 0.0

    def write_mulaw(self, payload: bytes) -> None:
        if not payload:
            return
        self._last_real_audio_at = time.monotonic()
        try:
            self._queue.put_nowait(payload)
            return
        except queue.Full:
            pass
        try:
            self._queue.get_nowait()
        except queue.Empty:
            return
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            pass

    def pop_frame(self) -> bytes | None:
        try:
            return self._frames.popleft()
        except IndexError:
            return None

    def frame_count(self) -> int:
        return len(self._frames)

    def silence_allowed(self) -> bool:
        last_real = self._last_real_audio_at
        return last_real <= 0.0 or time.monotonic() - last_real > SILENCE_AFTER_S

    def _drain_queue(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                return

    def _write_loop(self, proc: subprocess.Popen) -> None:
        if proc.stdin is None:
            return
        try:
            fd = proc.stdin.fileno()
        except Exception:
            return

        while self._running and self._proc is proc and proc.poll() is None:
            try:
                payload = self._queue.get(timeout=0.04)
            except queue.Empty:
                payload = self._silence_payload()
                if payload is None:
                    continue

            if not self._write_fd(fd, payload, proc):
                return

    def _silence_payload(self) -> bytes | None:
        if not self.silence_allowed():
            return None
        if len(self._frames) >= AAC_LOW_WATER_FRAMES or self._queue.qsize() >= 4:
            return None
        return SILENCE_CHUNK

    def _read_loop(self, proc: subprocess.Popen) -> None:
        if proc.stdout is None:
            return
        buf = bytearray()
        while self._running and self._proc is proc and proc.poll() is None:
            try:
                data = proc.stdout.read1(4096)
            except Exception:
                return
            if not data:
                return
            buf.extend(data)
            self._pull_adts_frames(buf)

    def _pull_adts_frames(self, buf: bytearray) -> None:
        while len(buf) >= 7:
            sync = -1
            for idx in range(len(buf) - 1):
                if buf[idx] == 0xFF and (buf[idx + 1] & 0xF0) == 0xF0:
                    sync = idx
                    break
            if sync < 0:
                buf.clear()
                return
            if sync:
                del buf[:sync]
            if len(buf) < 7:
                return
            frame_len = ((buf[3] & 0x03) << 11) | (buf[4] << 3) | ((buf[5] >> 5) & 0x07)
            if frame_len < 7:
                del buf[:1]
                continue
            if len(buf) < frame_len:
                return
            self._frames.append(bytes(buf[:frame_len]))
            del buf[:frame_len]

    def _write_fd(self, fd: int, payload: bytes, proc: subprocess.Popen) -> bool:
        view = memoryview(payload)
        while view and self._running and self._proc is proc and proc.poll() is None:
            try:
                _, writable, _ = select.select([], [fd], [], 0.5)
            except Exception:
                return False
            if not writable:
                continue
            try:
                written = os.write(fd, view)
            except Exception:
                return False
            if written <= 0:
                return False
            view = view[written:]
        return not view
