"""Compact ffmpeg muxer: Annex-B video plus AAC audio into MPEG-TS."""

from __future__ import annotations

import logging
import os
import queue
import select
import subprocess
import threading
import time
from typing import Callable

from .audio_encoder import AacAudioEncoder
from .mpegts import (
    AAC_FRAME_TICKS,
    AAC_SILENCE_FRAME,
    PAT_PID,
    PMT_PID,
    TS_PACKET_SIZE,
    VIDEO_PID,
    build_pat_packet,
    build_pmt_packet,
    packet_has_random_access,
    make_audio_ts,
    packet_pid,
    rewrite_video_timing,
)

_LOGGER = logging.getLogger(__name__)


class FfmpegMuxer:
    """Mux raw H.264/HEVC video and camera mu-law audio without blocking P2P."""

    def __init__(self, on_ts: Callable[[bytes], None]) -> None:
        self._on_ts = on_ts
        self._proc: subprocess.Popen | None = None
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._writer_thread: threading.Thread | None = None
        self._video_queue: queue.Queue[tuple[bytes, float, float | None]] = queue.Queue(
            maxsize=120
        )
        self._video_time_queue: queue.Queue[tuple[float, float | None]] = queue.Queue(
            maxsize=120
        )
        self._audio = AacAudioEncoder()
        self._running = False
        self._codec = "hevc"
        self._input_fps = 15.0
        self._audio_cc = 0
        self._next_audio_pts = -AAC_FRAME_TICKS
        self._audio_started = False
        self._last_video_pts: int | None = None
        self._last_input_frame_mono = 0.0
        self._video_pts_step_ticks = 6000
        self._current_video_pts = 0
        self._audio_gate_open = False

    @property
    def codec(self) -> str:
        return self._codec

    def start(self, codec: str, advertised_fps: float = 15.0) -> None:
        codec_name = (codec or "hevc").lower()
        if codec_name == "h265":
            codec_name = "hevc"
        if codec_name not in {"h264", "hevc"}:
            codec_name = "hevc"
        input_fps = max(1.0, min(60.0, float(advertised_fps or 15.0)))
        if (
            self._proc is not None
            and self._codec == codec_name
            and abs(self._input_fps - input_fps) < 0.01
        ):
            return

        self.stop()
        self._codec = codec_name
        self._input_fps = input_fps
        self._audio_cc = 0
        self._next_audio_pts = -AAC_FRAME_TICKS
        self._audio_started = False
        self._last_video_pts = None
        self._last_input_frame_mono = 0.0
        self._video_pts_step_ticks = int(90000 / input_fps)
        self._current_video_pts = 0
        self._audio_gate_open = False
        self._drain_video_queue()

        fps_text = f"{input_fps:.3f}"
        setts = f"setts=pts=N/({fps_text}*TB):dts=N/({fps_text}*TB)"
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-fflags",
            "+genpts+igndts+discardcorrupt",
            "-flags",
            "low_delay",
            "-probesize",
            "32768",
            "-analyzeduration",
            "0",
            "-framerate",
            fps_text,
            "-thread_queue_size",
            "128",
            "-f",
            codec_name,
            "-i",
            "pipe:0",
            "-map",
            "0:v:0",
            "-c:v",
            "copy",
            "-bsf:v",
            setts,
            "-muxdelay",
            "0",
            "-muxpreload",
            "0",
            "-flush_packets",
            "1",
            "-f",
            "mpegts",
            "-mpegts_flags",
            "+resend_headers+pat_pmt_at_frames",
            "pipe:1",
        ]
        self._audio.start()
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except Exception:
            self._audio.stop()
            raise
        self._running = True

        proc = self._proc
        self._stdout_thread = threading.Thread(
            target=self._read_stdout, args=(proc,), daemon=True
        )
        self._stderr_thread = threading.Thread(
            target=self._read_stderr, args=(proc,), daemon=True
        )
        self._writer_thread = threading.Thread(
            target=self._write_video, args=(proc,), daemon=True
        )
        self._stdout_thread.start()
        self._stderr_thread.start()
        self._writer_thread.start()

    def stop(self) -> None:
        self._running = False
        proc = self._proc
        self._proc = None
        self._audio.stop()
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
        self._drain_video_queue()

    def write_video(
        self,
        payload: bytes,
        *,
        pts_interval_s: float | None = None,
    ) -> None:
        if not payload:
            return
        item = (payload, time.monotonic(), pts_interval_s)
        self._enqueue_video(item)

    def replace_video(
        self,
        payload: bytes,
        *,
        pts_interval_s: float | None = None,
    ) -> None:
        if not payload:
            return
        self._drain_video_queue()
        self._enqueue_video((payload, time.monotonic(), pts_interval_s))

    def _enqueue_video(self, item: tuple[bytes, float, float | None]) -> None:
        try:
            self._video_queue.put_nowait(item)
            return
        except queue.Full:
            pass
        try:
            self._video_queue.get_nowait()
        except queue.Empty:
            return
        try:
            self._video_queue.put_nowait(item)
        except queue.Full:
            pass

    def write_audio(self, payload: bytes) -> None:
        self._audio.write_mulaw(payload)

    def _drain_video_queue(self) -> None:
        for item_queue in (self._video_queue, self._video_time_queue):
            while True:
                try:
                    item_queue.get_nowait()
                except queue.Empty:
                    break

    def _read_stdout(self, proc: subprocess.Popen) -> None:
        if proc.stdout is None:
            return
        pending = bytearray()
        while self._running and self._proc is proc and proc.poll() is None:
            try:
                ready, _, _ = select.select([proc.stdout], [], [], 0.03)
            except Exception:
                break
            if ready:
                try:
                    data = proc.stdout.read1(65536)
                except Exception:
                    break
                if not data:
                    break
                pending.extend(data)

            out, saw_video = self._consume_ts(pending)
            if out:
                audio = (
                    self._due_audio_ts() if saw_video and self._audio_gate_open else b""
                )
                if audio:
                    out.extend(audio)
                self._on_ts(bytes(out))

        out, _ = self._consume_ts(pending)
        if out:
            self._on_ts(bytes(out))

    def _consume_ts(self, pending: bytearray) -> tuple[bytearray, bool]:
        usable = (len(pending) // TS_PACKET_SIZE) * TS_PACKET_SIZE
        if usable <= 0:
            return bytearray(), False

        out = bytearray()
        saw_video = False
        chunk = bytes(pending[:usable])
        del pending[:usable]
        for off in range(0, len(chunk), TS_PACKET_SIZE):
            packet = chunk[off : off + TS_PACKET_SIZE]
            if packet[0] != 0x47:
                continue
            pid = packet_pid(packet)
            cc = packet[3] & 0x0F
            if pid == PAT_PID:
                out.extend(build_pat_packet(cc))
            elif pid == PMT_PID:
                out.extend(build_pmt_packet(self._codec, cc))
            else:
                if pid == VIDEO_PID:
                    saw_video = True
                    random_access = packet_has_random_access(packet)
                    if packet[1] & 0x40:
                        self._next_video_pts()
                        self._audio_gate_open = True
                    if self._audio_gate_open or random_access:
                        self._last_video_pts = self._current_video_pts
                        self._audio_gate_open = True
                    packet = bytearray(packet)
                    rewrite_video_timing(packet, self._current_video_pts)
                out.extend(packet)
        return out, saw_video

    def _next_video_pts(self) -> int:
        try:
            frame_mono, pts_interval_s = self._video_time_queue.get_nowait()
        except queue.Empty:
            frame_mono = time.monotonic()
            pts_interval_s = None
        if pts_interval_s is not None:
            step = max(1, int(max(0.001, pts_interval_s) * 90000))
            pts = 0 if self._last_video_pts is None else self._last_video_pts + step
            self._current_video_pts = pts
            return pts

        self._observe_video_cadence(frame_mono)
        pts = (
            0
            if self._last_video_pts is None
            else (self._last_video_pts + self._video_pts_step_ticks)
        )
        self._current_video_pts = pts
        return pts

    def _observe_video_cadence(self, frame_mono: float) -> None:
        prev = self._last_input_frame_mono
        self._last_input_frame_mono = frame_mono
        if prev <= 0.0:
            return
        dt = frame_mono - prev
        if dt < 0.02 or dt > 0.35:
            return
        target = max(1500, min(18000, int(dt * 90000)))
        self._video_pts_step_ticks = int(
            (self._video_pts_step_ticks * 0.82) + (target * 0.18)
        )

    def _due_audio_ts(self) -> bytes:
        if self._last_video_pts is None:
            return b""
        if not self._audio_started:
            self._next_audio_pts = self._last_video_pts - AAC_FRAME_TICKS
            self._audio_started = True
        out = bytearray()
        emitted = 0
        while (
            self._next_audio_pts + AAC_FRAME_TICKS <= self._last_video_pts
            and emitted < 32
        ):
            self._next_audio_pts += AAC_FRAME_TICKS
            frame = self._audio.pop_frame() or AAC_SILENCE_FRAME
            audio_ts, self._audio_cc = make_audio_ts(
                frame,
                self._next_audio_pts,
                self._audio_cc,
            )
            out.extend(audio_ts)
            emitted += 1
        return bytes(out)

    def _read_stderr(self, proc: subprocess.Popen) -> None:
        if proc.stderr is None:
            return
        try:
            for raw in proc.stderr:
                if not raw:
                    break
                line = raw.decode(errors="replace").strip()
                if line:
                    _LOGGER.debug("ffmpeg[%s]: %s", self._codec, line)
        except Exception:
            pass

    def _write_video(self, proc: subprocess.Popen) -> None:
        if proc.stdin is None:
            return
        try:
            stdin_fd = proc.stdin.fileno()
        except Exception:
            return

        while self._running and self._proc is proc and proc.poll() is None:
            try:
                payload, frame_mono, pts_interval_s = self._video_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            view = memoryview(payload)
            while view and self._running and self._proc is proc and proc.poll() is None:
                try:
                    _, writable, _ = select.select([], [stdin_fd], [], 0.5)
                except Exception:
                    return
                if not writable:
                    continue
                try:
                    written = os.write(stdin_fd, view)
                except Exception:
                    return
                if written <= 0:
                    return
                view = view[written:]
            try:
                self._video_time_queue.put_nowait((frame_mono, pts_interval_s))
            except queue.Full:
                try:
                    self._video_time_queue.get_nowait()
                    self._video_time_queue.put_nowait((frame_mono, pts_interval_s))
                except queue.Empty:
                    pass
