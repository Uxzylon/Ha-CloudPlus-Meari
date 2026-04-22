"""ffmpeg media pipeline and gap-recovery helpers for the coordinator."""

from __future__ import annotations

import collections
import fcntl
import hashlib
import logging
import os
import queue
import select
import subprocess
import threading
import time
from typing import Any

from .mpegts import (
    AAC_FRAME_TICKS,
    build_pat_packet,
    build_pmt_packet,
    extract_video_packets_from_seed,
    is_valid_idr_seed,
    make_audio_ts,
    make_silence_audio_ts,
    reinterleave_ts,
    rewrite_video_ts_timing,
)

_LOGGER = logging.getLogger(__name__)


class CoordinatorMediaMixin:
    """Owns ffmpeg muxing, audio pipeline, and gap-skip recovery plumbing."""

    def _start_ffmpeg_muxer(self) -> None:
        """Start ffmpeg to mux camera video into MPEG-TS."""
        if self._ffmpeg_proc is not None:
            return

        audio_r, audio_w = os.pipe()
        self._audio_write_fd = audio_w
        video_fmt = "h264" if self._video_codec == "h264" else "hevc"
        video_input_fps = max(5.0, min(60.0, float(self._video_mux_target_fps)))
        video_setts = (
            f"setts=pts=N/({video_input_fps:.3f}*TB):"
            f"dts=N/({video_input_fps:.3f}*TB)"
        )
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "info",
            "-fflags",
            "+genpts+igndts+discardcorrupt",
            "-err_detect",
            "ignore_err",
            "-probesize",
            "32768",
            "-analyzeduration",
            "0",
            "-framerate",
            f"{video_input_fps:.3f}",
            "-thread_queue_size",
            "128",
            "-f",
            video_fmt,
            "-i",
            "pipe:0",
            "-map",
            "0:v",
            "-c:v",
            "copy",
            "-bsf:v",
            video_setts,
            "-max_delay",
            "0",
            "-muxpreload",
            "0",
            "-muxdelay",
            "0",
            "-flush_packets",
            "1",
            "-f",
            "mpegts",
            "-mpegts_flags",
            "+resend_headers+pat_pmt_at_frames",
            "pipe:1",
        ]

        audio_enc_cmd = [
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
            f"pipe:{audio_r}",
            "-filter:a",
            f"volume={self._audio_gain_db:.1f}dB,alimiter=limit=0.92",
            "-c:a",
            "aac",
            "-profile:a",
            "aac_low",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-b:a",
            "32k",
            "-flush_packets",
            "1",
            "-f",
            "adts",
            "pipe:1",
        ]

        try:
            self._ffmpeg_proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            self._audio_enc_proc = subprocess.Popen(
                audio_enc_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                pass_fds=(audio_r,),
            )
            os.close(audio_r)

            try:
                flags = fcntl.fcntl(self._audio_write_fd, fcntl.F_GETFL)
                fcntl.fcntl(self._audio_write_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
            except Exception:
                pass

            try:
                fcntl.fcntl(self._audio_write_fd, 1031, 16384)
            except Exception:
                pass

            while True:
                try:
                    self._audio_queue.get_nowait()
                except queue.Empty:
                    break
            self._audio_queue_drops = 0
            self._audio_throttle_drops = 0
            self._video_queue_drops = 0
            self._audio_real_started = False
            self._audio_realtime_next_ts = 0.0
            self._audio_aac_deque.clear()
            self._muxer_video_started.clear()
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
            self._recovery_decode_probe_cache.clear()
            self._stream_join_event_counter = 0
            self._stream_join_events.clear()
            self._stream_join_events_by_client.clear()

            proc_ref = self._ffmpeg_proc
            self._audio_writer_thread = threading.Thread(
                target=self._audio_writer,
                args=(proc_ref,),
                daemon=True,
            )
            self._audio_writer_thread.start()

            threading.Thread(
                target=self._audio_aac_reader,
                daemon=True,
            ).start()

            try:
                fcntl.fcntl(self._ffmpeg_proc.stdin.fileno(), 1031, 1048576)
            except Exception:
                pass

            try:
                fcntl.fcntl(self._ffmpeg_proc.stdout.fileno(), 1031, 1048576)
            except Exception:
                pass

            self._audio_primed.clear()
            self._silence_feeder_gen += 1
            self._silence_feeder_thread = threading.Thread(
                target=self._silence_feeder,
                args=(self._silence_feeder_gen,),
                daemon=True,
            )
            self._silence_feeder_thread.start()

            self._ffmpeg_reader_thread = threading.Thread(
                target=self._ffmpeg_stdout_reader,
                daemon=True,
            )
            self._ffmpeg_reader_thread.start()
            proc_ref = self._ffmpeg_proc
            threading.Thread(
                target=self._video_pacer,
                args=(proc_ref,),
                daemon=True,
            ).start()
            if self._idle_video_keepalive:
                threading.Thread(
                    target=self._video_keepalive,
                    args=(proc_ref,),
                    daemon=True,
                ).start()
            threading.Thread(
                target=self._log_ffmpeg_stderr,
                args=(self._ffmpeg_proc, "muxer"),
                daemon=True,
            ).start()
            _LOGGER.debug("ffmpeg muxer started")

            if self._latest_video_kf:
                self._feed_video(self._latest_video_kf)
            self._last_video_time = time.monotonic()

        except FileNotFoundError:
            _LOGGER.error("ffmpeg not found — streaming will not work")
            os.close(audio_r)
            self._close_audio_fd()
            self._ffmpeg_proc = None
        except Exception as exc:
            _LOGGER.error("Failed to start ffmpeg muxer: %s", exc)
            os.close(audio_r)
            self._close_audio_fd()
            self._ffmpeg_proc = None

    def _stop_ffmpeg_muxer(self) -> None:
        """Stop the ffmpeg muxer and audio encoder."""
        self._audio_primed.set()
        if self._ffmpeg_proc:
            self._close_audio_fd()
            try:
                self._ffmpeg_proc.terminate()
            except OSError:
                pass
            try:
                self._ffmpeg_proc.stdin.raw.closefd = False
                os.close(self._ffmpeg_proc.stdin.fileno())
            except (OSError, ValueError, AttributeError):
                pass
            try:
                self._ffmpeg_proc.wait(timeout=5)
            except Exception:
                try:
                    self._ffmpeg_proc.kill()
                    self._ffmpeg_proc.wait(timeout=2)
                except Exception:
                    pass
            self._ffmpeg_proc = None

        if self._audio_enc_proc:
            try:
                self._audio_enc_proc.terminate()
                self._audio_enc_proc.wait(timeout=3)
            except Exception:
                try:
                    self._audio_enc_proc.kill()
                    self._audio_enc_proc.wait(timeout=1)
                except Exception:
                    pass
            self._audio_enc_proc = None

        if self._audio_writer_thread:
            self._audio_writer_thread.join(timeout=0.8)
            if self._audio_writer_thread.is_alive():
                _LOGGER.debug("Audio writer thread still alive after stop timeout")
            self._audio_writer_thread = None

        while True:
            try:
                self._audio_queue.get_nowait()
            except queue.Empty:
                break

        if self._ffmpeg_reader_thread:
            self._ffmpeg_reader_thread.join(timeout=1.5)
            if self._ffmpeg_reader_thread.is_alive():
                _LOGGER.debug("ffmpeg reader thread still alive after stop timeout")
            self._ffmpeg_reader_thread = None

    def _close_audio_fd(self) -> None:
        if self._audio_write_fd >= 0:
            try:
                os.close(self._audio_write_fd)
            except OSError:
                pass
            self._audio_write_fd = -1

    def _update_idr_seed(self, data: bytes) -> None:
        PKT = 188
        VIDEO_PID = 0x100
        n_pkts = len(data) // PKT

        for i in range(n_pkts):
            off = i * PKT
            if data[off] != 0x47:
                continue
            pid = ((data[off + 1] & 0x1F) << 8) | data[off + 2]
            is_video_pusi = (pid == VIDEO_PID) and bool(data[off + 1] & 0x40)

            if is_video_pusi:
                afc = (data[off + 3] >> 4) & 0x03
                is_rai = False
                if (afc & 0x02) and data[off + 4] >= 1:
                    is_rai = bool(data[off + 5] & 0x40)

                if is_rai:
                    if self._stream_idr_collecting and self._stream_idr_buf:
                        self._commit_idr_seed(
                            bytes(self._stream_idr_buf),
                            self._stream_broadcast_video_pts,
                        )
                    self._stream_idr_collecting = True
                    self._stream_idr_buf = bytearray()
                    self._stream_idr_buf.extend(build_pat_packet())
                    self._stream_idr_buf.extend(build_pmt_packet(0, self._video_codec))
                elif self._stream_idr_collecting:
                    self._commit_idr_seed(
                        bytes(self._stream_idr_buf),
                        self._stream_broadcast_video_pts,
                    )
                    self._stream_idr_collecting = False

            if self._stream_idr_collecting:
                self._stream_idr_buf.extend(data[off : off + PKT])

    def _rebase_idr_seed(
        self,
        pid_last_cc: dict[int, int] | None = None,
    ) -> bytes:
        seed = self._stream_idr_seed
        if not seed:
            return seed
        current_pts = self._stream_broadcast_video_pts
        current_audio_pts = self._stream_broadcast_audio_pts
        if current_audio_pts <= 0:
            current_audio_pts = max(0, current_pts - (3 * AAC_FRAME_TICKS))
        seed_pts = self._stream_idr_seed_pts
        offset = current_pts - seed_pts
        PKT = 188
        VIDEO_PID = 0x100
        AUDIO_PID = 0x101
        result = bytearray(seed)
        pid_packets: dict[int, list[int]] = {}
        for i in range(len(result) // PKT):
            off = i * PKT
            if result[off] != 0x47:
                continue
            pid = ((result[off + 1] & 0x1F) << 8) | result[off + 2]
            pid_packets.setdefault(pid, []).append(off)
            if pid == VIDEO_PID:
                if offset != 0:
                    rewrite_video_ts_timing(result, off, current_pts)
            elif pid == AUDIO_PID and (result[off + 1] & 0x40):
                afc = (result[off + 3] >> 4) & 0x03
                payload_off = off + 4
                if afc & 0x02:
                    payload_off = off + 5 + result[off + 4]
                if (
                    payload_off + 14 <= off + PKT
                    and result[payload_off] == 0
                    and result[payload_off + 1] == 0
                    and result[payload_off + 2] == 1
                ):
                    pts_dts_flags = (result[payload_off + 7] >> 6) & 0x03
                    if pts_dts_flags >= 2 and (
                        offset != 0 or current_audio_pts != seed_pts
                    ):
                        p = payload_off + 9
                        if p + 5 <= off + PKT:
                            marker = 0x02
                            result[p] = (
                                (marker << 4)
                                | ((current_audio_pts >> 29) & 0x0E)
                                | 0x01
                            )
                            result[p + 1] = (current_audio_pts >> 22) & 0xFF
                            result[p + 2] = ((current_audio_pts >> 14) & 0xFE) | 0x01
                            result[p + 3] = (current_audio_pts >> 7) & 0xFF
                            result[p + 4] = ((current_audio_pts << 1) & 0xFE) | 0x01

        cc_source = self._stream_pid_last_cc if pid_last_cc is None else pid_last_cc
        for pid, offsets in pid_packets.items():
            last_live_cc = cc_source.get(pid)
            if last_live_cc is None:
                continue
            cc = (last_live_cc - len(offsets) + 1) & 0x0F
            for off in offsets:
                result[off + 3] = (result[off + 3] & 0xF0) | cc
                cc = (cc + 1) & 0x0F
        _LOGGER.debug(
            "IDR seed rebased: video pts %d → %d (offset +%d, %.1fs), audio target=%d",
            seed_pts,
            current_pts,
            offset,
            offset / 90000,
            current_audio_pts,
        )
        return bytes(result)

    def _ffmpeg_stdout_reader(self) -> None:
        """Read video-only MPEG-TS from ffmpeg, add audio, broadcast."""
        PKT = 188
        AUDIO_PID = 0x101
        VIDEO_PID = 0x100
        FRAME_TICKS = AAC_FRAME_TICKS
        FLUSH_SIZE = 8 * 1024
        FLUSH_INTERVAL = 0.03

        buf = bytearray()
        last_flush = time.monotonic()
        audio_cc = 0
        next_audio_pts = -FRAME_TICKS
        stream_start: float = 0.0
        last_video_pts = 0
        _flush_count = 0
        pat_out_cc = 0
        pmt_out_cc = 0

        _jitter_q: collections.deque[tuple[bytearray, bool]] = collections.deque()
        _frame_acc = bytearray()
        _frame_acc_is_rai = False
        _jitter_primed = False
        _first_video_time = 0.0
        _next_release = 0.0
        _seen_gap_skip_reset_seq = self._gap_skip_reset_seq
        _seen_session_gen = self._p2p_session_generation
        _active_gap_event_id = 0
        _active_gap_severity = "unknown"
        _drop_output_until_video_pusi = False
        _post_gap_quarantine = False
        _post_gap_quarantine_drops = 0
        _post_gap_strict_release = False
        _post_gap_strict_rejections = 0
        _post_gap_quarantine_since: float = 0.0
        _post_gap_hold_release_rollovers = 0
        _post_gap_held_recovery_rollovers = 0
        _post_gap_release_buffer: collections.deque[tuple[bytearray, bool]] = (
            collections.deque()
        )
        _post_gap_release_follow_frames = 0
        _post_gap_release_follow_frames_needed = 0
        _post_gap_release_candidate_reason = ""
        _post_gap_release_candidate_mono: float = 0.0
        JITTER_DEPTH_S = 0.5
        RELEASE_INTERVAL_S = 1.0 / 15.0
        PMT_PID = 0x1000
        PAT_PID = 0x0000

        def _trim_jitter_to_latest_rai() -> int:
            if len(_jitter_q) < 2:
                return 0
            frames = list(_jitter_q)
            keep_from = None
            for idx in range(len(frames) - 1, -1, -1):
                if frames[idx][1]:
                    keep_from = idx
                    break
            if keep_from is None:
                keep_from = max(len(frames) - 2, 0)
            dropped = keep_from
            if dropped > 0:
                _jitter_q.clear()
                _jitter_q.extend(frames[keep_from:])
            return dropped

        def _release_post_gap_buffer(now_mono: float) -> None:
            nonlocal _post_gap_quarantine, _post_gap_release_follow_frames
            nonlocal _post_gap_release_follow_frames_needed
            nonlocal _post_gap_release_candidate_reason

            released_frames = list(_post_gap_release_buffer)
            if not released_frames:
                return

            _post_gap_release_buffer.clear()
            released_follow_frames = _post_gap_release_follow_frames
            _post_gap_release_follow_frames = 0
            _post_gap_release_follow_frames_needed = 0
            candidate_reason = _post_gap_release_candidate_reason or "clean"
            _post_gap_release_candidate_reason = ""

            for frame_bytes, is_rai in released_frames:
                if is_rai:
                    self._record_video_rai_ts_size(len(frame_bytes))
                _jitter_q.append((bytearray(frame_bytes), is_rai))

            _post_gap_quarantine = False
            min_seed_generation = self._startup_safe_min_seed_generation
            if _post_gap_strict_release:
                min_seed_generation = max(
                    self._startup_safe_min_seed_generation,
                    self._stream_idr_seed_generation + 1,
                )
                self._startup_safe_min_seed_generation = min_seed_generation
            self._update_gap_skip_event(
                _active_gap_event_id,
                quarantine_release_mono=now_mono,
                quarantine_drops=_post_gap_quarantine_drops,
                strict_rejections=_post_gap_strict_rejections,
                released_frame_bytes=len(released_frames[0][0]),
                released_buffered_frames=len(released_frames),
                released_follow_frames=released_follow_frames,
                release_reason=candidate_reason,
                startup_safe_min_seed_generation=min_seed_generation,
                status="released",
            )
            _LOGGER.info(
                "Gap-skip quarantine released #%d (%s, %s) after %d dropped frames (%d buffered frames, %d follow frames, next safe seed generation >= %d)",
                _active_gap_event_id,
                _active_gap_severity,
                candidate_reason,
                _post_gap_quarantine_drops,
                len(released_frames),
                released_follow_frames,
                min_seed_generation,
            )

        def _flush() -> None:
            nonlocal buf, last_flush, audio_cc
            nonlocal next_audio_pts, stream_start, last_video_pts, _flush_count
            nonlocal _frame_acc, _jitter_primed, _next_release
            nonlocal pat_out_cc, pmt_out_cc, _first_video_time
            nonlocal _frame_acc_is_rai
            nonlocal _seen_gap_skip_reset_seq, _seen_session_gen
            nonlocal _active_gap_event_id, _active_gap_severity
            nonlocal _drop_output_until_video_pusi
            nonlocal _post_gap_quarantine, _post_gap_quarantine_drops
            nonlocal _post_gap_strict_release, _post_gap_strict_rejections
            nonlocal _post_gap_quarantine_since
            nonlocal _post_gap_hold_release_rollovers, _post_gap_held_recovery_rollovers
            nonlocal _post_gap_release_follow_frames, _post_gap_release_follow_frames_needed
            nonlocal _post_gap_release_candidate_reason, _post_gap_release_candidate_mono
            now = time.monotonic()

            if _seen_gap_skip_reset_seq != self._gap_skip_reset_seq:
                stale_frames = len(_jitter_q)
                stale_partial = len(_frame_acc)
                _jitter_q.clear()
                _frame_acc = bytearray()
                _first_video_time = now
                _jitter_primed = True
                _next_release = now
                if stream_start <= 0.0:
                    stream_start = now
                _drop_output_until_video_pusi = True
                _post_gap_quarantine = True
                _post_gap_quarantine_drops = 0
                _post_gap_strict_rejections = 0
                _post_gap_quarantine_since = now
                _post_gap_hold_release_rollovers = 0
                _post_gap_held_recovery_rollovers = 0
                _post_gap_release_buffer.clear()
                _post_gap_release_follow_frames = 0
                _post_gap_release_follow_frames_needed = 0
                _post_gap_release_candidate_reason = ""
                _frame_acc_is_rai = False
                _seen_gap_skip_reset_seq = self._gap_skip_reset_seq
                _active_gap_event_id = self._gap_skip_seq_to_event_id.get(
                    _seen_gap_skip_reset_seq, 0
                )
                gap_event = (
                    self._get_gap_skip_event_snapshot(_active_gap_event_id) or {}
                )
                _active_gap_severity = str(gap_event.get("severity", "unknown"))
                _post_gap_strict_release = bool(gap_event.get("strict_release", False))
                _post_gap_hold_release_rollovers = (
                    1
                    if (_post_gap_strict_release and self._video_codec == "hevc")
                    else 0
                )
                self._update_gap_skip_event(
                    _active_gap_event_id,
                    output_reset_mono=now,
                    stale_jitter_frames=stale_frames,
                    stale_partial_ts_bytes=stale_partial,
                    status="quarantine",
                )
                _LOGGER.info(
                    "Gap-skip output reset #%d (%s, strict_release=%s): cleared %d buffered frames and %d partial TS bytes",
                    _active_gap_event_id,
                    _active_gap_severity,
                    _post_gap_strict_release,
                    stale_frames,
                    stale_partial,
                )

            if _seen_session_gen != self._p2p_session_generation:
                if _post_gap_quarantine or _post_gap_release_buffer:
                    _LOGGER.info(
                        "Gap-skip quarantine abandoned: P2P session restarted (was event #%d, %s)",
                        _active_gap_event_id,
                        _active_gap_severity,
                    )
                    _post_gap_quarantine = False
                    _post_gap_release_buffer.clear()
                    _post_gap_release_follow_frames = 0
                    _post_gap_release_follow_frames_needed = 0
                    _post_gap_release_candidate_mono = 0.0
                    _drop_output_until_video_pusi = True
                _seen_session_gen = self._p2p_session_generation

            if buf:
                n_complete = (len(buf) // PKT) * PKT
                if n_complete > 0:
                    chunk = bytes(buf[:n_complete])
                    buf = bytearray(buf[n_complete:])
                    for i in range(n_complete // PKT):
                        off = i * PKT
                        if chunk[off] != 0x47:
                            continue
                        pid = ((chunk[off + 1] & 0x1F) << 8) | chunk[off + 2]
                        if pid in (PMT_PID, PAT_PID):
                            continue
                        if pid == VIDEO_PID:
                            is_pusi = bool(chunk[off + 1] & 0x40)
                            afc = (chunk[off + 3] >> 4) & 0x03
                            is_rai = False
                            if (afc & 0x02) and (off + 5 < len(chunk)):
                                af_len = chunk[off + 4]
                                if af_len >= 1:
                                    is_rai = bool(chunk[off + 5] & 0x40)
                            if _drop_output_until_video_pusi:
                                if not is_pusi or not is_rai:
                                    continue
                                _drop_output_until_video_pusi = False
                            if is_pusi:
                                if _frame_acc:
                                    if _post_gap_quarantine:
                                        accepted = False
                                        release_reason = "not-clean"
                                        if _frame_acc_is_rai:
                                            accepted, release_reason = (
                                                self._evaluate_recovery_frame(
                                                    bytes(_frame_acc),
                                                    strict=_post_gap_strict_release,
                                                )
                                            )
                                        if accepted:
                                            if _post_gap_hold_release_rollovers > 0:
                                                self._record_video_rai_ts_size(
                                                    len(_frame_acc)
                                                )
                                                _post_gap_hold_release_rollovers -= 1
                                                _post_gap_held_recovery_rollovers += 1
                                                self._update_gap_skip_event(
                                                    _active_gap_event_id,
                                                    held_recovery_reason=release_reason,
                                                    held_recovery_frame_bytes=len(
                                                        _frame_acc
                                                    ),
                                                    held_recovery_rollovers=_post_gap_held_recovery_rollovers,
                                                    remaining_recovery_rollovers=_post_gap_hold_release_rollovers,
                                                    status="awaiting-rollover",
                                                )
                                                _LOGGER.info(
                                                    "Gap-skip quarantine held #%d (%s, %s): accepted recovery frame (%d bytes) but waiting for %d more recovery IDR rollover%s",
                                                    _active_gap_event_id,
                                                    _active_gap_severity,
                                                    release_reason,
                                                    len(_frame_acc),
                                                    _post_gap_hold_release_rollovers,
                                                    (
                                                        ""
                                                        if _post_gap_hold_release_rollovers
                                                        == 1
                                                        else "s"
                                                    ),
                                                )
                                            elif _post_gap_strict_release:
                                                if _post_gap_release_buffer:
                                                    # Keep progress when repeated valid IDRs arrive.
                                                    # Resetting the candidate each time can deadlock
                                                    # strict release on unstable links.
                                                    _post_gap_release_buffer.append(
                                                        (
                                                            bytearray(_frame_acc),
                                                            _frame_acc_is_rai,
                                                        )
                                                    )
                                                    _post_gap_release_follow_frames += 1
                                                    self._update_gap_skip_event(
                                                        _active_gap_event_id,
                                                        release_candidate_rollovers=_post_gap_release_follow_frames,
                                                        status="awaiting-follow-frames",
                                                    )
                                                    if (
                                                        _post_gap_release_follow_frames_needed
                                                        > 0
                                                        and _post_gap_release_follow_frames
                                                        >= _post_gap_release_follow_frames_needed
                                                    ):
                                                        _release_post_gap_buffer(now)
                                                else:
                                                    _post_gap_release_buffer.clear()
                                                    _post_gap_release_buffer.append(
                                                        (
                                                            bytearray(_frame_acc),
                                                            _frame_acc_is_rai,
                                                        )
                                                    )
                                                    _post_gap_release_follow_frames = 0
                                                    # Under repeated severe gap bursts, require a slightly
                                                    # deeper post-IDR runway before release to reduce
                                                    # HEVC reference-chain decode faults on weak links.
                                                    severe_recent = (
                                                        self._count_recent_gap_events(
                                                            severity="severe",
                                                            within_s=12.0,
                                                        )
                                                    )
                                                    _post_gap_release_follow_frames_needed = (
                                                        2
                                                    )
                                                    if (
                                                        _post_gap_strict_release
                                                        and self._video_codec == "hevc"
                                                    ):
                                                        # Without byte-size floors, HEVC benefits
                                                        # from a deeper post-IDR runway before release.
                                                        _post_gap_release_follow_frames_needed = (
                                                            3
                                                        )
                                                    _post_gap_release_candidate_mono = (
                                                        now
                                                    )
                                                    _post_gap_release_candidate_reason = (
                                                        release_reason
                                                    )
                                                    self._update_gap_skip_event(
                                                        _active_gap_event_id,
                                                        release_candidate_reason=release_reason,
                                                        release_candidate_frame_bytes=len(
                                                            _frame_acc
                                                        ),
                                                        severe_recent_events=severe_recent,
                                                        release_candidate_follow_frames_needed=_post_gap_release_follow_frames_needed,
                                                        status="awaiting-follow-frames",
                                                    )
                                                    _LOGGER.info(
                                                        "Gap-skip quarantine candidate #%d (%s, %s): buffered recovery IDR (%d bytes), waiting for %d follow frames (recent severe=%d)",
                                                        _active_gap_event_id,
                                                        _active_gap_severity,
                                                        release_reason,
                                                        len(_frame_acc),
                                                        _post_gap_release_follow_frames_needed,
                                                        severe_recent,
                                                    )
                                            else:
                                                self._record_video_rai_ts_size(
                                                    len(_frame_acc)
                                                )
                                                _jitter_q.append(
                                                    (
                                                        bytearray(_frame_acc),
                                                        _frame_acc_is_rai,
                                                    )
                                                )
                                                _post_gap_quarantine = False
                                                min_seed_generation = (
                                                    self._startup_safe_min_seed_generation
                                                )
                                                if _post_gap_strict_release:
                                                    min_seed_generation = max(
                                                        self._startup_safe_min_seed_generation,
                                                        self._stream_idr_seed_generation
                                                        + 1,
                                                    )
                                                    self._startup_safe_min_seed_generation = (
                                                        min_seed_generation
                                                    )
                                                self._update_gap_skip_event(
                                                    _active_gap_event_id,
                                                    quarantine_release_mono=now,
                                                    quarantine_drops=_post_gap_quarantine_drops,
                                                    strict_rejections=_post_gap_strict_rejections,
                                                    released_frame_bytes=len(
                                                        _frame_acc
                                                    ),
                                                    release_reason=release_reason,
                                                    startup_safe_min_seed_generation=min_seed_generation,
                                                    status="released",
                                                )
                                                _LOGGER.info(
                                                    "Gap-skip quarantine released #%d (%s, %s) after %d dropped frames (next safe seed generation >= %d)",
                                                    _active_gap_event_id,
                                                    _active_gap_severity,
                                                    release_reason,
                                                    _post_gap_quarantine_drops,
                                                    min_seed_generation,
                                                )
                                        else:
                                            if (
                                                _post_gap_release_buffer
                                                and not _frame_acc_is_rai
                                            ):
                                                _post_gap_release_buffer.append(
                                                    (
                                                        bytearray(_frame_acc),
                                                        _frame_acc_is_rai,
                                                    )
                                                )
                                                _post_gap_release_follow_frames += 1
                                                if (
                                                    _post_gap_release_follow_frames_needed
                                                    > 0
                                                    and _post_gap_release_follow_frames
                                                    >= _post_gap_release_follow_frames_needed
                                                ):
                                                    _release_post_gap_buffer(now)
                                            elif _frame_acc_is_rai:
                                                _post_gap_quarantine_drops += 1
                                                _post_gap_strict_rejections += 1
                                                self._update_gap_skip_event(
                                                    _active_gap_event_id,
                                                    quarantine_drops=_post_gap_quarantine_drops,
                                                    strict_rejections=_post_gap_strict_rejections,
                                                    last_rejection_reason=release_reason,
                                                    last_rejected_frame_bytes=len(
                                                        _frame_acc
                                                    ),
                                                )
                                                if (
                                                    _post_gap_strict_release
                                                    or _post_gap_quarantine_drops <= 3
                                                ):
                                                    _LOGGER.info(
                                                        "Gap-skip quarantine held #%d (%s): rejected recovery frame (%s, %d bytes)",
                                                        _active_gap_event_id,
                                                        _active_gap_severity,
                                                        release_reason,
                                                        len(_frame_acc),
                                                    )
                                    else:
                                        if _frame_acc_is_rai:
                                            self._record_video_rai_ts_size(
                                                len(_frame_acc)
                                            )
                                        _jitter_q.append(
                                            (bytearray(_frame_acc), _frame_acc_is_rai)
                                        )
                                _frame_acc = bytearray()
                                _frame_acc_is_rai = is_rai
                                if _first_video_time == 0.0:
                                    _first_video_time = now
                            _frame_acc.extend(chunk[off : off + PKT])
                        elif _drop_output_until_video_pusi:
                            continue

            if not _jitter_primed and _first_video_time > 0:
                if now - _first_video_time >= JITTER_DEPTH_S:
                    _jitter_primed = True
                    _next_release = now
                    stream_start = now

            severe_recent = self._count_recent_gap_events(
                severity="severe",
                within_s=12.0,
            )
            timeout_wait_s = 5.5 if severe_recent >= 5 else 6.5
            timeout_follow_needed = 1 if severe_recent >= 5 else 2
            if (
                _post_gap_quarantine
                and _post_gap_release_buffer
                and _post_gap_release_candidate_mono > 0.0
                and now - _post_gap_release_candidate_mono > timeout_wait_s
            ):
                waited = now - _post_gap_release_candidate_mono
                if _post_gap_release_follow_frames >= timeout_follow_needed:
                    # Prefer a degraded-but-live fallback over a hard freeze.
                    _post_gap_release_candidate_reason = (
                        _post_gap_release_candidate_reason or "clean"
                    ) + "+timeout-fallback"
                    _LOGGER.warning(
                        "Gap-skip quarantine candidate #%d timed out after %.1fs, forcing fallback release with %d/%d buffered follow frames (recent severe=%d)",
                        _active_gap_event_id,
                        waited,
                        _post_gap_release_follow_frames,
                        timeout_follow_needed,
                        severe_recent,
                    )
                    _release_post_gap_buffer(now)
                else:
                    _LOGGER.warning(
                        "Gap-skip quarantine candidate #%d timed out after %.1fs, abandoning (network not recovering, follow=%d/%d, recent severe=%d)",
                        _active_gap_event_id,
                        waited,
                        _post_gap_release_follow_frames,
                        timeout_follow_needed,
                        severe_recent,
                    )
                    _post_gap_quarantine = False
                    _post_gap_release_buffer.clear()
                    _post_gap_release_follow_frames = 0
                    _post_gap_release_follow_frames_needed = 0
                    _post_gap_release_candidate_mono = 0.0
                    _drop_output_until_video_pusi = True

            # No-candidate deadlock: strict mode has rejected every IDR for too long.
            # Downgrade to non-strict so the next acceptable IDR can unblock recovery.
            if (
                _post_gap_quarantine
                and _post_gap_strict_release
                and not _post_gap_release_buffer
                and _post_gap_quarantine_since > 0.0
                and now - _post_gap_quarantine_since > 7.0
                and _post_gap_strict_rejections >= 3
            ):
                _post_gap_strict_release = False
                _LOGGER.warning(
                    "Gap-skip quarantine #%d (%s): strict-release downgraded after %.1fs with no accepted candidate "
                    "(%d rejections) — lowering bar to avoid freeze",
                    _active_gap_event_id,
                    _active_gap_severity,
                    now - _post_gap_quarantine_since,
                    _post_gap_strict_rejections,
                )

            kept = bytearray()
            if _jitter_primed:
                SNAP_THRESHOLD = 0.75
                late_by = max(0.0, now - _next_release)
                if _next_release < now - SNAP_THRESHOLD:
                    queued_frames = len(_jitter_q)
                    dropped = _trim_jitter_to_latest_rai()
                    if dropped > 0:
                        _LOGGER.info(
                            "Jitter catch-up dropped %d stale queued frames (late=%.3fs, queued=%d)",
                            dropped,
                            late_by,
                            queued_frames,
                        )
                    _next_release = now
                if _jitter_q and now >= _next_release:
                    frame, _ = _jitter_q.popleft()
                    if stream_start <= 0.0:
                        stream_start = now
                    release_pts = int((_next_release - stream_start) * 90000)
                    current_pts = max(last_video_pts + 900, release_pts)
                    last_video_pts = current_pts
                    self._stream_broadcast_video_pts = current_pts
                    frame_ba = bytearray(frame)
                    for j in range(len(frame_ba) // PKT):
                        poff = j * PKT
                        rewrite_video_ts_timing(frame_ba, poff, current_pts)
                    pat_out_cc = (pat_out_cc + 1) & 0x0F
                    pmt_out_cc = (pmt_out_cc + 1) & 0x0F
                    kept.extend(build_pat_packet(pat_out_cc))
                    kept.extend(build_pmt_packet(pmt_out_cc, self._video_codec))
                    kept.extend(frame_ba)
                    _next_release += RELEASE_INTERVAL_S

            if _jitter_primed and stream_start > 0.0:
                wall_pts = int((now - stream_start) * 90000)
            else:
                wall_pts = 0

            AUDIO_PTS_DELAY = 3 * FRAME_TICKS
            audio_wall_pts = max(0, wall_pts - AUDIO_PTS_DELAY)
            self._stream_broadcast_audio_pts = audio_wall_pts

            audio_ts = bytearray()
            injected = 0
            live_audio = self._camera_awake and self._audio_real_started
            if live_audio:
                MAX_CATCHUP = 50
                earliest = audio_wall_pts - MAX_CATCHUP * FRAME_TICKS
                if next_audio_pts + FRAME_TICKS < earliest:
                    next_audio_pts = earliest - FRAME_TICKS
            while next_audio_pts + FRAME_TICKS <= audio_wall_pts:
                try:
                    real_frame = self._audio_aac_deque.popleft()
                except IndexError:
                    real_frame = None
                next_audio_pts += FRAME_TICKS
                if real_frame:
                    frame_bytes, audio_cc = make_audio_ts(
                        real_frame,
                        next_audio_pts,
                        audio_cc,
                        AUDIO_PID,
                    )
                    self._audio_flush_real += 1
                else:
                    frame_bytes, audio_cc = make_silence_audio_ts(
                        next_audio_pts,
                        audio_cc,
                        AUDIO_PID,
                    )
                    self._audio_flush_silence += 1
                audio_ts.extend(frame_bytes)
                injected += 1
            if audio_ts:
                kept.extend(audio_ts)

            if not kept:
                last_flush = now
                return

            result = reinterleave_ts(bytearray(kept), AUDIO_PID)

            result_ba = bytearray(result)
            pid_cc_state = {
                pid: cc
                for pid, cc in self._stream_pid_last_cc.items()
                if pid in (PAT_PID, PMT_PID, VIDEO_PID, AUDIO_PID)
            }
            for i in range(len(result_ba) // PKT):
                off = i * PKT
                if result_ba[off] != 0x47:
                    continue
                pid = ((result_ba[off + 1] & 0x1F) << 8) | result_ba[off + 2]
                if pid not in (PAT_PID, PMT_PID, VIDEO_PID, AUDIO_PID):
                    continue
                afc = (result_ba[off + 3] >> 4) & 0x03
                has_payload = bool(afc & 0x01)
                if not has_payload:
                    continue
                next_cc = (pid_cc_state.get(pid, -1) + 1) & 0x0F
                result_ba[off + 3] = (result_ba[off + 3] & 0xF0) | next_cc
                pid_cc_state[pid] = next_cc
            result = bytes(result_ba)

            _flush_count += 1
            if _flush_count <= 5 or _flush_count % 1000 == 0:
                _LOGGER.info(
                    "TS flush #%d: %dKB, audio_injected=%d, wall_pts=%d, jitter_buf=%d",
                    _flush_count,
                    len(result) // 1024,
                    injected,
                    wall_pts,
                    len(_jitter_q),
                )

            self._broadcast_stream(result)
            last_flush = now

        last_data_time = time.monotonic()
        flush_errors = 0
        try:
            stdout_fd = self._ffmpeg_proc.stdout
            while self._ffmpeg_proc and self._ffmpeg_proc.poll() is None:
                ready, _, _ = select.select([stdout_fd], [], [], FLUSH_INTERVAL)
                if ready:
                    data = stdout_fd.read1(65536)
                    if not data:
                        break
                    buf.extend(data)
                    last_data_time = time.monotonic()
                now = time.monotonic()
                video_flowing_secs = now - last_data_time
                if (
                    video_flowing_secs > 8.0
                    and self._last_video_time > 0
                    and now - self._last_video_time < 3.0
                    and self._last_p2p_video_time > 0
                    and now - self._last_p2p_video_time < 5.0
                ):
                    _LOGGER.warning(
                        "Muxer output stalled %.1fs while video flows, restarting",
                        now - last_data_time,
                    )
                    try:
                        self._ffmpeg_proc.kill()
                        self._ffmpeg_proc.wait(timeout=2)
                    except Exception:
                        pass
                    break
                if len(buf) >= FLUSH_SIZE or (now - last_flush) >= FLUSH_INTERVAL:
                    try:
                        _flush()
                    except Exception as exc:
                        flush_errors += 1
                        if flush_errors <= 5:
                            _LOGGER.warning(
                                "TS flush error (%d): %r",
                                flush_errors,
                                exc,
                                exc_info=True,
                            )
                        buf.clear()
                        last_flush = time.monotonic()
            try:
                _flush()
            except Exception:
                pass
        except Exception as exc:
            _LOGGER.warning("ffmpeg reader stopped: %s", exc)
        if self._running:
            if self._ffmpeg_proc and self._ffmpeg_proc.poll() is None:
                _LOGGER.warning("Reader exited while ffmpeg alive, killing muxer")
                try:
                    self._ffmpeg_proc.kill()
                    self._ffmpeg_proc.wait(timeout=2)
                except Exception:
                    pass
            if self._ffmpeg_proc is None or self._ffmpeg_proc.poll() is not None:
                _LOGGER.warning("ffmpeg muxer exited, restarting")
                self._ffmpeg_reader_thread = None
                self._stop_ffmpeg_muxer()
                while not self._video_queue.empty():
                    try:
                        self._video_queue.get_nowait()
                    except queue.Empty:
                        break
                time.sleep(1)
                self._start_ffmpeg_muxer()

    @staticmethod
    def _log_ffmpeg_stderr(proc: subprocess.Popen, label: str) -> None:
        try:
            for raw_line in proc.stderr:
                line = raw_line.decode(errors="replace").rstrip()
                if line:
                    _LOGGER.info("ffmpeg[%s]: %s", label, line)
        except Exception:
            pass

    def _feed_video(self, data: bytes) -> None:
        try:
            self._video_queue.put_nowait(data)
        except queue.Full:
            self._video_queue_drops += 1
            try:
                self._video_queue.get_nowait()
            except queue.Empty:
                return
            try:
                self._video_queue.put_nowait(data)
            except queue.Full:
                self._video_queue_drops += 1

    def _handle_gap_skip_reset(self, diag: dict[str, Any] | None = None) -> None:
        dropped = 0
        while not self._video_queue.empty():
            try:
                self._video_queue.get_nowait()
                dropped += 1
            except queue.Empty:
                break
        self._gap_skip_reset_seq += 1
        self._force_param_sets_on_next_keyframe = True
        event_id, severity, _ = self._register_gap_skip_event(diag, dropped)
        _LOGGER.info(
            "Gap-skip recovery #%d (%s): dropped %d queued video frames and armed parameter-set bootstrap",
            event_id,
            severity,
            dropped,
        )

    def _register_gap_skip_event(
        self, diag: dict[str, Any] | None, dropped: int
    ) -> tuple[int, str, bool]:
        severity = str((diag or {}).get("severity", "unknown"))
        strict_release = severity == "severe"
        event_id = int((diag or {}).get("event_id", 0) or 0)
        now_mono = time.monotonic()
        event_diag = dict(diag or {})
        with self._gap_skip_events_lock:
            if event_id <= 0:
                self._gap_skip_event_counter += 1
                event_id = self._gap_skip_event_counter
            else:
                self._gap_skip_event_counter = max(
                    self._gap_skip_event_counter, event_id
                )
            event_diag.update(
                {
                    "event_id": event_id,
                    "severity": severity,
                    "started_mono": float(
                        event_diag.get("started_mono", now_mono) or now_mono
                    ),
                    "recovery_seq": self._gap_skip_reset_seq,
                    "dropped_video_queue_frames": dropped,
                    "strict_release": strict_release,
                    "status": "armed",
                }
            )
            self._gap_skip_events.append(event_diag)
            self._gap_skip_seq_to_event_id[self._gap_skip_reset_seq] = event_id
            self._last_gap_skip_event_id = event_id
            self._last_gap_skip_diag = event_diag
        return (event_id, severity, strict_release)

    def _update_gap_skip_event(self, event_id: int, **updates: Any) -> None:
        if event_id <= 0:
            return
        with self._gap_skip_events_lock:
            for event in reversed(self._gap_skip_events):
                if int(event.get("event_id", 0) or 0) == event_id:
                    event.update(updates)
                    if event_id == self._last_gap_skip_event_id:
                        self._last_gap_skip_diag = event
                    break

    def _get_gap_skip_event_snapshot(self, event_id: int) -> dict[str, Any] | None:
        if event_id <= 0:
            return None
        with self._gap_skip_events_lock:
            for event in reversed(self._gap_skip_events):
                if int(event.get("event_id", 0) or 0) == event_id:
                    return dict(event)
        return None

    def get_gap_skip_events_snapshot(self) -> list[dict[str, Any]]:
        with self._gap_skip_events_lock:
            return [dict(event) for event in self._gap_skip_events]

    def _count_recent_gap_events(self, *, severity: str, within_s: float) -> int:
        now_mono = time.monotonic()
        cutoff = now_mono - max(0.0, float(within_s))
        count = 0
        with self._gap_skip_events_lock:
            for event in self._gap_skip_events:
                if str(event.get("severity", "")) != severity:
                    continue
                started_mono = float(event.get("started_mono", 0.0) or 0.0)
                if started_mono >= cutoff:
                    count += 1
        return count

    def _evaluate_bootstrap_seed(
        self, seed: bytes, *, strict: bool = False
    ) -> tuple[bool, str, int]:
        if not seed:
            return (False, "empty", 0)
        if not is_valid_idr_seed(seed):
            return (False, "not-valid", 0)
        frame_ts = extract_video_packets_from_seed(seed)
        if not frame_ts:
            return (False, "no-video", 0)
        accepted, reason = self._evaluate_recovery_frame(frame_ts, strict=strict)
        return (accepted, reason, len(frame_ts))

    def get_startup_bootstrap_state(self) -> dict[str, Any]:
        now_mono = time.monotonic()
        seed = self._stream_idr_seed
        collecting = bool(self._stream_idr_collecting)
        last_video = float(self._last_p2p_video_time or 0.0)
        video_age_s = (now_mono - last_video) if last_video > 0 else float("inf")
        seed_age_s = (
            (now_mono - self._stream_idr_seed_mono)
            if self._stream_idr_seed_mono > 0
            else float("inf")
        )
        frames_since_seed = max(
            0,
            int(self._p2p_video_frames) - int(self._stream_idr_seed_p2p_frame_index),
        )
        seed_valid = bool(seed) and is_valid_idr_seed(seed)
        seed_strong = bool(seed_valid and self._stream_idr_seed_is_strong)
        seed_generation = int(self._stream_idr_seed_generation)
        required_seed_generation = int(self._startup_safe_min_seed_generation)
        latest_severe = None
        for event in self.get_gap_skip_events_snapshot():
            if str(event.get("severity", "")) != "severe":
                continue
            if latest_severe is None or int(event.get("event_id", 0) or 0) > int(
                latest_severe.get("event_id", 0) or 0
            ):
                latest_severe = event

        block_reason = "ready"
        startup_safe = bool(
            seed_valid
            and seed_strong
            and seed_generation >= required_seed_generation
            and seed_age_s < 0.75
            and frames_since_seed >= 3
            and not collecting
            and video_age_s < 1.0
        )
        if not seed_valid:
            block_reason = "seed-invalid"
        elif not seed_strong:
            block_reason = self._stream_idr_seed_strength_reason or "seed-not-strong"
        elif seed_generation < required_seed_generation:
            block_reason = "seed-awaiting-post-gap-rollover"
        elif seed_age_s >= 0.75:
            block_reason = "seed-not-fresh"
        elif frames_since_seed < 3:
            block_reason = "seed-awaiting-follow-frames"
        elif collecting:
            block_reason = "seed-collecting"
        elif video_age_s >= 1.0:
            block_reason = "video-not-recent"

        if startup_safe and latest_severe is not None:
            severe_status = str(latest_severe.get("status", "armed"))
            severe_release_mono = float(
                latest_severe.get("quarantine_release_mono", 0.0) or 0.0
            )
            if severe_status != "released":
                startup_safe = False
                block_reason = f"severe-gap-{severe_status}"
            elif self._stream_idr_seed_mono <= severe_release_mono:
                startup_safe = False
                block_reason = "seed-older-than-last-severe-gap"

        backlog_follow_target = self._preferred_backlog_follow_video_pusi_target()
        backlog_candidate = b""
        if seed_valid and not collecting:
            backlog_candidate = self._extract_recent_backlog_bootstrap_locked(
                min_follow_video_pusi=backlog_follow_target,
            )
        backlog_ready = bool(backlog_candidate)
        backlog_generation_safe = bool(
            backlog_ready and seed_generation >= required_seed_generation
        )
        preferred_join_mode = (
            "ready-backlog"
            if backlog_generation_safe
            else ("ready" if startup_safe else "pending")
        )

        return {
            "startup_safe": startup_safe,
            "block_reason": block_reason,
            "seed_valid": seed_valid,
            "seed_strong": seed_strong,
            "seed_video_bytes": int(self._stream_idr_seed_video_bytes),
            "seed_strength_reason": self._stream_idr_seed_strength_reason,
            "seed_mono": float(self._stream_idr_seed_mono),
            "seed_generation": seed_generation,
            "required_seed_generation": required_seed_generation,
            "seed_age_s": float(seed_age_s),
            "frames_since_seed": frames_since_seed,
            "collecting": collecting,
            "video_age_s": float(video_age_s),
            "backlog_follow_video_pusi_target": int(backlog_follow_target),
            "backlog_ready": backlog_ready,
            "backlog_generation_safe": backlog_generation_safe,
            "backlog_candidate_bytes": len(backlog_candidate),
            "preferred_join_mode": preferred_join_mode,
            "latest_severe_gap_event": latest_severe,
        }

    def _record_video_rai_ts_size(self, size_bytes: int) -> None:
        if size_bytes > 0:
            self._recent_video_rai_ts_sizes.append(int(size_bytes))

    def _probe_recovery_frame_decode(self, frame_ts: bytes) -> tuple[bool, str]:
        """Decode-probe a recovery frame and cache verdicts.

        This validates real decoder acceptance (not only TS/NAL structure)
        while keeping repeated probes cheap via an LRU-style cache.
        """
        if not frame_ts:
            return (False, "decode-probe-empty")

        digest = hashlib.blake2b(frame_ts, digest_size=12).hexdigest()
        cache_key = f"{self._video_codec}:{len(frame_ts)}:{digest}"
        cached = self._recovery_decode_probe_cache.get(cache_key)
        if cached is not None:
            self._recovery_decode_probe_cache.move_to_end(cache_key)
            return cached

        probe_seed = bytearray()
        probe_seed.extend(build_pat_packet())
        probe_seed.extend(build_pmt_packet(0, self._video_codec))
        probe_seed.extend(frame_ts)

        verdict: tuple[bool, str]
        try:
            proc = subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-xerror",
                    "-err_detect",
                    "explode",
                    "-fflags",
                    "+discardcorrupt",
                    "-f",
                    "mpegts",
                    "-i",
                    "pipe:0",
                    "-map",
                    "0:v:0",
                    "-an",
                    "-frames:v",
                    "1",
                    "-f",
                    "null",
                    "-",
                ],
                input=bytes(probe_seed),
                capture_output=True,
                timeout=1.8,
            )
            if proc.returncode == 0:
                verdict = (True, "clean+decoded")
            else:
                stderr = (proc.stderr or b"").decode(errors="replace").strip()
                first_line = (
                    stderr.splitlines()[0].strip() if stderr else "decode-error"
                )
                verdict = (False, f"decode-probe-fail:{first_line[:120]}")
        except FileNotFoundError:
            _LOGGER.warning(
                "ffmpeg not found for recovery decode probe; accepting clean frame"
            )
            verdict = (True, "clean+probe-unavailable")
        except subprocess.TimeoutExpired:
            verdict = (False, "decode-probe-timeout")
        except Exception as exc:
            _LOGGER.debug("Recovery decode probe error: %s", exc)
            verdict = (False, "decode-probe-error")

        self._recovery_decode_probe_cache[cache_key] = verdict
        max_items = max(16, int(self._recovery_decode_probe_cache_max))
        while len(self._recovery_decode_probe_cache) > max_items:
            self._recovery_decode_probe_cache.popitem(last=False)
        return verdict

    def _probe_bootstrap_seed_decode(
        self, seed: bytes, *, max_frames: int = 6
    ) -> tuple[bool, str]:
        """Decode-probe a bootstrap seed over multiple frames.

        This catches seeds that decode for a single keyframe but quickly fail
        on the immediate follow-up prediction chain.
        """
        if not seed:
            return (False, "seed-probe-empty")

        frames = max(1, int(max_frames))
        digest = hashlib.blake2b(seed, digest_size=12).hexdigest()
        cache_key = f"seed:{self._video_codec}:{len(seed)}:{frames}:{digest}"
        cached = self._recovery_decode_probe_cache.get(cache_key)
        if cached is not None:
            self._recovery_decode_probe_cache.move_to_end(cache_key)
            return cached

        verdict: tuple[bool, str]
        try:
            proc = subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-xerror",
                    "-err_detect",
                    "explode",
                    "-fflags",
                    "+discardcorrupt",
                    "-f",
                    "mpegts",
                    "-i",
                    "pipe:0",
                    "-map",
                    "0:v:0",
                    "-an",
                    "-frames:v",
                    str(frames),
                    "-f",
                    "null",
                    "-",
                ],
                input=seed,
                capture_output=True,
                timeout=2.2,
            )
            if proc.returncode == 0:
                verdict = (True, "seed-decode-ok")
            else:
                stderr = (proc.stderr or b"").decode(errors="replace").strip()
                first_line = (
                    stderr.splitlines()[0].strip() if stderr else "decode-error"
                )
                verdict = (False, f"seed-decode-fail:{first_line[:120]}")
        except FileNotFoundError:
            _LOGGER.warning(
                "ffmpeg not found for bootstrap decode probe; accepting seed"
            )
            verdict = (True, "seed-probe-unavailable")
        except subprocess.TimeoutExpired:
            verdict = (False, "seed-decode-timeout")
        except Exception as exc:
            _LOGGER.debug("Bootstrap decode probe error: %s", exc)
            verdict = (False, "seed-decode-error")

        self._recovery_decode_probe_cache[cache_key] = verdict
        max_items = max(16, int(self._recovery_decode_probe_cache_max))
        while len(self._recovery_decode_probe_cache) > max_items:
            self._recovery_decode_probe_cache.popitem(last=False)
        return verdict

    def _evaluate_recovery_frame(
        self, frame_ts: bytes, *, strict: bool = False
    ) -> tuple[bool, str]:
        if not self._is_clean_recovery_frame(frame_ts):
            return (False, "not-clean")
        if strict:
            return self._probe_recovery_frame_decode(frame_ts)
        return (True, "clean")

    def _video_pacer(self, proc_ref: subprocess.Popen) -> None:
        try:
            stdin_fd = proc_ref.stdin.fileno()
        except (ValueError, OSError):
            return

        while self._ffmpeg_proc is proc_ref and proc_ref.poll() is None:
            try:
                data = self._video_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            pending = memoryview(data)
            stall_start: float | None = None
            while pending:
                if self._ffmpeg_proc is not proc_ref or proc_ref.poll() is not None:
                    return
                try:
                    _, writable, _ = select.select([], [stdin_fd], [], 0.5)
                except (ValueError, OSError):
                    return
                if writable:
                    try:
                        written = os.write(stdin_fd, pending)
                        if written <= 0:
                            return
                        pending = pending[written:]
                        stall_start = None
                    except (BrokenPipeError, OSError):
                        return
                else:
                    if stall_start is None:
                        stall_start = time.monotonic()
                    elif time.monotonic() - stall_start > 3.0:
                        _LOGGER.debug(
                            "Video pacer: stdin pipe full for 3s, dropping frame"
                        )
                        break
            self._last_video_time = time.monotonic()
            self._muxer_video_started.set()

    def _video_keepalive(self, proc_ref: subprocess.Popen) -> None:
        while self._ffmpeg_proc is proc_ref and proc_ref.poll() is None:
            if self._camera_awake:
                p2p_age = (
                    time.monotonic() - self._last_p2p_video_time
                    if self._last_p2p_video_time > 0
                    else 999.0
                )
                if p2p_age < 10.0:
                    time.sleep(0.2)
                    continue

            now_mono = time.monotonic()
            idle_elapsed = (
                (now_mono - self._idle_since) if self._idle_since > 0.0 else 0.0
            )
            fps = self._idle_keepalive_fps_initial
            if idle_elapsed >= self._idle_keepalive_settle_seconds:
                fps = self._idle_keepalive_fps_steady
            with self._stream_clients_lock:
                has_clients = bool(self._stream_clients or self._pending_stream_clients)
            if has_clients:
                fps = max(fps, self._idle_keepalive_fps_with_clients)
            interval = 1.0 / max(1.0, fps)
            time.sleep(interval / 2.0)
            now_mono = time.monotonic()
            source = self._latest_video_kf or self._idle_video_kf
            if (
                (self._idle_scene_kf or self._latest_video_kf or self._idle_video_kf)
                and self._idle_since > 0.0
                and (now_mono - self._idle_since) >= self._idle_scene_hold_seconds
            ):
                source = (
                    self._idle_scene_kf or self._latest_video_kf or self._idle_video_kf
                )
            if source and now_mono - self._last_video_time >= interval:
                self._feed_video(source)

    def _silence_feeder(self, gen: int) -> None:
        _LOGGER.info("Silence feeder: waiting for video start")
        self._muxer_video_started.wait()
        if gen != self._silence_feeder_gen:
            _LOGGER.info("Silence feeder: superseded by newer feeder, exiting")
            return
        _LOGGER.info("Silence feeder: video started, priming ffmpeg audio")

        primer_chunks = 3
        silence_chunk = b"\xff" * 320
        audio_dry_threshold = 0.06
        deque_low = 12

        for i in range(primer_chunks):
            if self._ffmpeg_proc is None or self._ffmpeg_proc.poll() is not None:
                break
            self._queue_audio(silence_chunk)
            if i == 0:
                _LOGGER.info("Silence feeder: primed %d/%d", i + 1, primer_chunks)
            time.sleep(0.1)

        _LOGGER.info("Silence feeder: primer done, entering gap-fill mode")

        while (
            gen == self._silence_feeder_gen
            and self._ffmpeg_proc is not None
            and self._ffmpeg_proc.poll() is None
        ):
            time.sleep(0.04)
            now = time.monotonic()
            audio_age = (
                now - self._last_p2p_audio_time
                if self._last_p2p_audio_time > 0
                else 999.0
            )
            if (
                audio_age > audio_dry_threshold
                and len(self._audio_aac_deque) < deque_low
                and self._audio_queue.qsize() < 4
            ):
                self._queue_audio(silence_chunk)

        _LOGGER.info("Silence feeder: exiting (gen=%d)", gen)

    def _queue_audio(self, data: bytes) -> None:
        if self._audio_write_fd < 0 or not data:
            return
        is_silence = all(b == 0xFF for b in data)

        if self._camera_awake and not is_silence:
            now_mono = time.monotonic()
            if self._audio_realtime_next_ts <= 0.0:
                self._audio_realtime_next_ts = now_mono
            chunk_secs = max(0.005, len(data) / 8000.0)
            queued = self._audio_queue.qsize()
            if queued >= max(10, self._audio_soft_queue_limit * 2 // 3) and (
                now_mono + 1.5 < self._audio_realtime_next_ts
            ):
                self._audio_throttle_drops += 1
                self._audio_realtime_next_ts = (
                    max(now_mono, self._audio_realtime_next_ts) + chunk_secs
                )
                return
            self._audio_realtime_next_ts = (
                max(now_mono, self._audio_realtime_next_ts) + chunk_secs
            )

        while self._audio_queue.qsize() > self._audio_soft_queue_limit:
            try:
                self._audio_queue.get_nowait()
                self._audio_throttle_drops += 1
            except queue.Empty:
                break

        while True:
            try:
                self._audio_queue.put_nowait(data)
                return
            except queue.Full:
                if is_silence:
                    return
                self._audio_queue_drops += 1
                try:
                    self._audio_queue.get_nowait()
                except queue.Empty:
                    return

    def _audio_writer(self, proc_ref: subprocess.Popen) -> None:
        pending = memoryview(b"")
        total_written = 0
        write_count = 0
        while self._ffmpeg_proc is proc_ref and proc_ref.poll() is None:
            if not pending:
                try:
                    chunk = self._audio_queue.get(timeout=0.2)
                except queue.Empty:
                    continue
                if not chunk:
                    continue
                pending = memoryview(chunk)
            try:
                written = os.write(self._audio_write_fd, pending)
                if written <= 0:
                    raise OSError("audio pipe write returned no bytes")
                pending = pending[written:]
                total_written += written
                write_count += 1
                if write_count in (1, 10, 50, 100, 500):
                    _LOGGER.info(
                        "Audio writer: %d writes, %d bytes total",
                        write_count,
                        total_written,
                    )
            except BlockingIOError:
                time.sleep(0.002)
            except (BrokenPipeError, OSError) as exc:
                _LOGGER.info(
                    "Audio writer: pipe error after %d bytes: %s", total_written, exc
                )
                break
        _LOGGER.info("Audio writer: exited, total %d bytes written", total_written)

    def _audio_aac_reader(self) -> None:
        adts_header_size = 7
        proc = self._audio_enc_proc
        if proc is None:
            return
        buf = bytearray()
        frame_count = 0
        try:
            while proc.poll() is None:
                data = proc.stdout.read1(4096)
                if not data:
                    break
                buf.extend(data)
                while len(buf) >= adts_header_size:
                    sync_pos = -1
                    for j in range(len(buf) - 1):
                        if buf[j] == 0xFF and (buf[j + 1] & 0xF0) == 0xF0:
                            sync_pos = j
                            break
                    if sync_pos < 0:
                        buf.clear()
                        break
                    if sync_pos > 0:
                        del buf[:sync_pos]
                    if len(buf) < adts_header_size:
                        break
                    frame_len = (
                        ((buf[3] & 0x03) << 11) | (buf[4] << 3) | ((buf[5] >> 5) & 0x07)
                    )
                    if frame_len < adts_header_size:
                        del buf[:1]
                        continue
                    if len(buf) < frame_len:
                        break
                    frame = bytes(buf[:frame_len])
                    del buf[:frame_len]
                    aac_deque_max = 200
                    while len(self._audio_aac_deque) >= aac_deque_max:
                        self._audio_aac_deque.popleft()
                        self._audio_aac_overflow += 1
                    self._audio_aac_deque.append(frame)
                    frame_count += 1
                    if frame_count in (1, 10, 100):
                        _LOGGER.info("Audio encoder: %d AAC frames queued", frame_count)
        except Exception as exc:
            _LOGGER.debug("Audio AAC reader stopped: %s", exc)
        _LOGGER.info("Audio AAC reader exited, %d frames total", frame_count)

    def _feed_audio(self, data: bytes) -> None:
        if self._audio_write_fd >= 0:
            self._audio_primed.set()
            if not self._audio_real_started:
                self._audio_real_started = True
                self._audio_realtime_next_ts = time.monotonic()
            self._queue_audio(data)
