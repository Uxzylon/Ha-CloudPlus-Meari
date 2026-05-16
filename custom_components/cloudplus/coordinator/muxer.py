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

from ..p2p_streamer.codecs import demuxer_for
from ..p2p_streamer.codecs.base import CodecName
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

VIDEO_QUEUE_FRAMES = 120
AUDIO_START_BUFFER_FRAMES = 4
AUDIO_EMIT_LIMIT = 3
AUDIO_MAX_LEAD_TICKS = AAC_FRAME_TICKS * 3


class FfmpegMuxer:
    """Mux raw H.264/HEVC video and camera mu-law audio without blocking P2P."""

    def __init__(self, on_ts: Callable[[bytes], None]) -> None:
        self._on_ts = on_ts
        self._proc: subprocess.Popen | None = None
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._writer_thread: threading.Thread | None = None
        self._video_queue: queue.Queue[tuple[bytes, float, float | None]] = queue.Queue(
            maxsize=VIDEO_QUEUE_FRAMES
        )
        self._video_time_queue: queue.Queue[tuple[float, float | None]] = queue.Queue(
            maxsize=VIDEO_QUEUE_FRAMES
        )
        self._audio = AacAudioEncoder()
        self._running = False
        self._codec = CodecName.HEVC
        self._input_fps = 15.0
        self._pacing_buffer_s = 0.0
        self._audio_cc = 0
        self._next_audio_pts = -AAC_FRAME_TICKS
        self._audio_started = False
        self._last_video_pts: int | None = None
        self._video_pts_step_ticks = 6000
        self._current_video_pts = 0
        self._audio_gate_open = False
        self._audio_clock_started_at = 0.0
        self._audio_clock_base_pts = 0
        self._audio_emit_frames = 0
        self._audio_silence_frames = 0
        self._last_audio_emit_mono = 0.0
        self._audio_emit_max_gap_s = 0.0
        self._video_pacer_started = False
        self._next_video_write_mono = 0.0

    @property
    def codec(self) -> CodecName:
        return self._codec

    def start(
        self,
        codec: CodecName | str,
        advertised_fps: float = 15.0,
        pacing_buffer_s: float = 0.0,
    ) -> None:
        codec_name = CodecName.parse(codec)
        input_fps = max(1.0, min(60.0, float(advertised_fps or 15.0)))
        pacing_buffer = max(0.0, min(3.0, float(pacing_buffer_s or 0.0)))
        if (
            self._proc is not None
            and self._codec == codec_name
            and abs(self._input_fps - input_fps) < 0.01
            and abs(self._pacing_buffer_s - pacing_buffer) < 0.01
        ):
            return

        self.stop()
        self._codec = codec_name
        self._input_fps = input_fps
        self._pacing_buffer_s = pacing_buffer
        self._audio_cc = 0
        self._next_audio_pts = -AAC_FRAME_TICKS
        self._audio_started = False
        self._last_video_pts = None
        self._video_pts_step_ticks = int(90000 / input_fps)
        self._current_video_pts = 0
        self._audio_gate_open = False
        self._audio_clock_started_at = 0.0
        self._audio_clock_base_pts = 0
        self._audio_emit_frames = 0
        self._audio_silence_frames = 0
        self._last_audio_emit_mono = 0.0
        self._audio_emit_max_gap_s = 0.0
        self._video_pacer_started = False
        self._next_video_write_mono = 0.0
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
            "-probesize",
            "32768",
            "-analyzeduration",
            "0",
            "-framerate",
            fps_text,
            "-thread_queue_size",
            "128",
            "-f",
            demuxer_for(codec_name),
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

    def reset_live_timing(self) -> None:
        self._drain_video_queue()
        self._audio.clear()
        self._audio_started = False
        self._audio_gate_open = False
        self._audio_clock_started_at = 0.0
        self._audio_clock_base_pts = 0
        self._last_audio_emit_mono = 0.0
        self._video_pts_step_ticks = int(90000 / self._input_fps)
        self._video_pacer_started = False
        self._next_video_write_mono = 0.0

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

            out, _saw_video = self._consume_ts(pending)
            audio = self._due_audio_ts() if self._audio_gate_open else b""
            if out or audio:
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
            _frame_mono, pts_interval_s = self._video_time_queue.get_nowait()
        except queue.Empty:
            pts_interval_s = None
        if pts_interval_s is not None:
            step = max(1, int(max(0.001, pts_interval_s) * 90000))
            pts = 0 if self._last_video_pts is None else self._last_video_pts + step
            self._current_video_pts = pts
            return pts

        pts = (
            0
            if self._last_video_pts is None
            else (self._last_video_pts + self._video_pts_step_ticks)
        )
        self._current_video_pts = pts
        return pts

    def _due_audio_ts(self) -> bytes:
        target_pts = self._audio_target_pts()
        if target_pts is None:
            return b""
        out = bytearray()
        emitted = 0
        while self._next_audio_pts + AAC_FRAME_TICKS <= target_pts:
            if emitted >= AUDIO_EMIT_LIMIT:
                break
            frame = self._audio.pop_frame()
            if frame is None:
                frame = AAC_SILENCE_FRAME
                self._audio_silence_frames += 1
            self._next_audio_pts += AAC_FRAME_TICKS
            audio_ts, self._audio_cc = make_audio_ts(
                frame,
                self._next_audio_pts,
                self._audio_cc,
            )
            out.extend(audio_ts)
            emitted += 1
        if emitted:
            self._note_audio_emit(emitted)
        return bytes(out)

    def _audio_target_pts(self) -> int | None:
        if self._last_video_pts is None:
            return None
        now = time.monotonic()
        if not self._audio_started:
            if (
                not self._audio.silence_allowed()
                and self._audio.frame_count() < AUDIO_START_BUFFER_FRAMES
            ):
                return None
            self._next_audio_pts = self._last_video_pts - AAC_FRAME_TICKS
            self._audio_clock_started_at = now
            self._audio_clock_base_pts = self._last_video_pts
            self._audio_started = True
        elapsed_ticks = int(max(0.0, now - self._audio_clock_started_at) * 90000)
        wall_target = self._audio_clock_base_pts + elapsed_ticks
        max_lead_target = self._last_video_pts + AUDIO_MAX_LEAD_TICKS
        return min(max(wall_target, self._last_video_pts), max_lead_target)

    def _note_audio_emit(self, frames: int) -> None:
        now = time.monotonic()
        if self._last_audio_emit_mono > 0:
            self._audio_emit_max_gap_s = max(
                self._audio_emit_max_gap_s,
                now - self._last_audio_emit_mono,
            )
        self._last_audio_emit_mono = now
        self._audio_emit_frames += frames

    def audio_debug_snapshot(self) -> dict[str, int | float]:
        return {
            "mux_audio_frames": self._audio_emit_frames,
            "mux_audio_silence_frames": self._audio_silence_frames,
            "mux_audio_max_emit_gap_s": round(self._audio_emit_max_gap_s, 3),
            "mux_audio_next_pts": self._next_audio_pts,
            "mux_video_last_pts": self._last_video_pts or 0,
        }

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
            if not self._video_pacer_started:
                self._prime_video_pacer()
            try:
                payload, frame_mono, pts_interval_s = self._video_queue.get(timeout=0.5)
            except queue.Empty:
                if self._pacing_buffer_s > 0.0:
                    self._video_pacer_started = False
                    self._next_video_write_mono = 0.0
                continue

            self._pace_video_write(pts_interval_s)
            self._queue_video_time(frame_mono, pts_interval_s)
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

    def _prime_video_pacer(self) -> None:
        self._video_pacer_started = True
        if self._pacing_buffer_s <= 0.0:
            return
        target = max(1, int(self._input_fps * self._pacing_buffer_s))
        deadline = time.monotonic() + min(1.6, self._pacing_buffer_s + 0.4)
        while self._running and self._video_queue.qsize() < target:
            if time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        self._next_video_write_mono = time.monotonic()

    def _pace_video_write(self, pts_interval_s: float | None) -> None:
        if self._pacing_buffer_s <= 0.0:
            return
        target = max(1, int(self._input_fps * self._pacing_buffer_s))
        if self._video_queue.qsize() > target:
            self._next_video_write_mono = time.monotonic()
            return
        interval_s = (
            max(0.001, float(pts_interval_s))
            if pts_interval_s is not None
            else 1.0 / self._input_fps
        )
        now = time.monotonic()
        if self._next_video_write_mono <= 0.0:
            self._next_video_write_mono = now + interval_s
            return
        delay = self._next_video_write_mono - now
        if delay > 0.0:
            time.sleep(min(delay, 0.5))
            now = time.monotonic()
        self._next_video_write_mono = max(now, self._next_video_write_mono) + interval_s

    def _queue_video_time(
        self, frame_mono: float, pts_interval_s: float | None
    ) -> None:
        try:
            self._video_time_queue.put_nowait((frame_mono, pts_interval_s))
        except queue.Full:
            try:
                self._video_time_queue.get_nowait()
                self._video_time_queue.put_nowait((frame_mono, pts_interval_s))
            except queue.Empty:
                pass
