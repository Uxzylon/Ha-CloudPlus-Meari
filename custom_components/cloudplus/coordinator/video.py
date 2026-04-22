"""Video, snapshot, and bootstrap-seed helpers for the coordinator."""

from __future__ import annotations

import logging
import queue
import re
import subprocess
import threading
import time

from .mpegts import build_pat_packet, build_pmt_packet, extract_video_bytestream_from_ts
from .nal import (
    collect_nal_types,
    detect_video_codec,
    is_video_keyframe,
    iter_annexb_nal_units,
)

_LOGGER = logging.getLogger(__name__)


class CoordinatorVideoMixin:
    """Owns video cadence, snapshots, codec state, and IDR seed helpers."""

    def _generate_black_keyframe(self, codec: str | None = None) -> None:
        """Generate a black keyframe for initial keepalive."""
        if not self._black_keyframe_lock.acquire(blocking=False):
            return
        try:
            size = self._video_size_hint
            if not size or size[0] <= 0 or size[1] <= 0:
                self._idle_video_kf = None
                _LOGGER.debug(
                    "Skipping synthetic black keyframe: source resolution unknown for %s",
                    self._sn_num,
                )
                return

            width, height = size
            fmt = (codec or self._video_codec or "hevc").lower()
            if fmt == "h264":
                video_args = [
                    "-c:v",
                    "libx264",
                    "-preset",
                    "ultrafast",
                    "-tune",
                    "zerolatency",
                    "-pix_fmt",
                    "yuv420p",
                    "-x264-params",
                    "keyint=1:min-keyint=1:scenecut=0",
                    "-f",
                    "h264",
                    "pipe:1",
                ]
            else:
                fmt = "hevc"
                video_args = [
                    "-c:v",
                    "libx265",
                    "-x265-params",
                    "log-level=error",
                    "-f",
                    "hevc",
                    "pipe:1",
                ]

            try:
                result = subprocess.run(
                    [
                        "ffmpeg",
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-f",
                        "lavfi",
                        "-i",
                        f"color=c=black:s={width}x{height}:r=1:d=0.1",
                        "-frames:v",
                        "1",
                        *video_args,
                    ],
                    capture_output=True,
                    timeout=10,
                )
                if result.returncode == 0 and result.stdout:
                    self._idle_video_kf = result.stdout
                    _LOGGER.debug(
                        "Generated black %s keyframe (%dx%d, %d bytes)",
                        fmt,
                        width,
                        height,
                        len(result.stdout),
                    )
                else:
                    _LOGGER.warning("Failed to generate black %s keyframe", fmt)
            except Exception as exc:
                _LOGGER.warning("Failed to generate black %s keyframe: %s", fmt, exc)
        finally:
            self._black_keyframe_lock.release()

    def _reset_video_cadence_state(self) -> None:
        self._video_fps_samples = 0
        self._video_fps_last_frame_time = 0.0
        self._video_fps_ema = max(5.0, min(60.0, float(self._video_mux_target_fps)))

    @staticmethod
    def _default_video_fps_for_codec(codec: str) -> float:
        return 15.0

    def _observe_video_cadence(self, frame_ts: float) -> None:
        prev = self._video_fps_last_frame_time
        self._video_fps_last_frame_time = frame_ts
        if prev <= 0.0:
            return

        dt = frame_ts - prev
        if dt <= 0.0 or dt > 0.5 or dt < 0.012:
            return

        inst_fps = max(5.0, min(60.0, 1.0 / dt))
        if self._video_fps_samples <= 0:
            self._video_fps_ema = inst_fps
        else:
            self._video_fps_ema = (self._video_fps_ema * 0.94) + (inst_fps * 0.06)
        self._video_fps_samples += 1

        min_fps, max_fps = 5.0, 60.0
        target = max(min_fps, min(max_fps, self._video_fps_ema))
        alpha = 0.25 if self._video_fps_samples <= 15 else 0.08
        blended = (self._video_mux_target_fps * (1.0 - alpha)) + (target * alpha)
        self._video_mux_target_fps = max(min_fps, min(max_fps, blended))

    def _probe_keyframe_resolution(
        self, frame: bytes, codec: str
    ) -> tuple[int, int] | None:
        fmt = "h264" if codec == "h264" else "hevc"
        try:
            proc = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-f",
                    fmt,
                    "-show_entries",
                    "stream=width,height",
                    "-of",
                    "csv=p=0:s=x",
                    "pipe:0",
                ],
                input=frame,
                capture_output=True,
                timeout=3,
            )
        except Exception:
            return None

        if proc.returncode != 0 or not proc.stdout:
            return None

        out = proc.stdout.decode(errors="replace").strip().splitlines()
        if not out:
            return None

        match = re.search(r"(\d+)x(\d+)", out[0])
        if not match:
            return None

        width = int(match.group(1))
        height = int(match.group(2))
        if width <= 0 or height <= 0:
            return None
        return (width, height)

    def _refresh_video_resolution_hint(self, frame: bytes, codec: str) -> None:
        now_mono = time.monotonic()
        if now_mono < self._video_size_probe_after:
            return
        if self._video_size_hint and self._video_size_codec == codec:
            return

        with self._video_size_probe_lock:
            if self._video_size_probe_inflight:
                return
            self._video_size_probe_inflight = True

        frame_copy = bytes(frame)

        def _probe_worker() -> None:
            try:
                size = self._probe_keyframe_resolution(frame_copy, codec)
                now_probe = time.monotonic()
                if not size:
                    self._video_size_probe_after = now_probe + 10.0
                    return

                prev = self._video_size_hint
                self._video_size_hint = size
                self._video_size_codec = codec
                self._video_size_probe_after = now_probe + 30.0
                if prev != size:
                    _LOGGER.info(
                        "Detected %s stream geometry for %s: %dx%d",
                        codec.upper(),
                        self._sn_num,
                        size[0],
                        size[1],
                    )
                    self._generate_black_keyframe(codec)
                    if (
                        self._camera_awake
                        and self._idle_video_kf
                        and self._ffmpeg_proc is not None
                        and self._ffmpeg_proc.poll() is None
                        and not self._muxer_video_started.is_set()
                    ):
                        self._feed_video(self._idle_video_kf)
            finally:
                with self._video_size_probe_lock:
                    self._video_size_probe_inflight = False

        threading.Thread(target=_probe_worker, daemon=True).start()

    def _update_parameter_set_cache(self, data: bytes, codec: str) -> None:
        """Cache latest parameter-set NAL units for decoder recovery."""
        codec = (codec or "hevc").lower()
        if codec == "h264" and self._h264_sps_nal and self._h264_pps_nal:
            return
        if (
            codec != "h264"
            and self._hevc_vps_nal
            and self._hevc_sps_nal
            and self._hevc_pps_nal
        ):
            return

        for off, unit in iter_annexb_nal_units(data):
            b0 = data[off]
            if codec == "h264":
                nal_type = b0 & 0x1F
                if nal_type not in (7, 8):
                    continue
                if nal_type == 7:
                    self._h264_sps_nal = unit
                else:
                    self._h264_pps_nal = unit
                continue

            if off + 1 >= len(data):
                continue
            b1 = data[off + 1]
            if (b1 & 0x07) == 0:
                continue
            nal_type = (b0 >> 1) & 0x3F
            if nal_type == 32:
                self._hevc_vps_nal = unit
            elif nal_type == 33:
                self._hevc_sps_nal = unit
            elif nal_type == 34:
                self._hevc_pps_nal = unit

    def _prepend_parameter_sets_if_needed(
        self,
        data: bytes,
        codec: str,
        nal_types: set[int],
        force: bool = False,
    ) -> bytes:
        """Prepend cached VPS/SPS/PPS when a keyframe arrives without them."""
        codec = (codec or "hevc").lower()
        if codec == "h264":
            prefix: list[bytes] = []
            if (force or 7 not in nal_types) and self._h264_sps_nal:
                prefix.append(self._h264_sps_nal)
            if (force or 8 not in nal_types) and self._h264_pps_nal:
                prefix.append(self._h264_pps_nal)
            if prefix:
                return b"".join(prefix) + data
            return data

        prefix: list[bytes] = []
        if (force or 32 not in nal_types) and self._hevc_vps_nal:
            prefix.append(self._hevc_vps_nal)
        if (force or 33 not in nal_types) and self._hevc_sps_nal:
            prefix.append(self._hevc_sps_nal)
        if (force or 34 not in nal_types) and self._hevc_pps_nal:
            prefix.append(self._hevc_pps_nal)
        if prefix:
            return b"".join(prefix) + data
        return data

    def _build_idle_scene_keyframe(self) -> None:
        """Cache an idle keyframe from the latest real camera scene."""
        if not self._idle_scene_convert_lock.acquire(blocking=False):
            return
        try:
            src = self._latest_video_kf
            if not src:
                return
            self._idle_scene_kf = bytes(src)
        except Exception as exc:
            _LOGGER.debug("Idle scene keyframe update failed: %s", exc)
        finally:
            self._idle_scene_convert_lock.release()

    def _convert_latest_kf(self) -> None:
        """Convert the saved video keyframe to JPEG in a background thread."""
        if not self._snapshot_conversion_enabled:
            return
        if not self._snapshot_convert_lock.acquire(blocking=False):
            return
        try:
            data = self._latest_video_kf
            if not data:
                return
            self._last_snapshot_convert_time = time.monotonic()
            jpeg = self._video_to_jpeg(data)
            if jpeg:
                self._latest_image = jpeg
                self._fire_update()
        finally:
            self._snapshot_convert_lock.release()

    def _video_to_jpeg(self, video_data: bytes) -> bytes | None:
        """Convert raw H264/HEVC frame data to JPEG using ffmpeg."""
        video_fmt = "h264" if self._video_codec == "h264" else "hevc"
        try:
            proc = subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    video_fmt,
                    "-probesize",
                    "32768",
                    "-analyzeduration",
                    "500000",
                    "-i",
                    "pipe:0",
                    "-vframes",
                    "1",
                    "-f",
                    "image2pipe",
                    "-vcodec",
                    "mjpeg",
                    "-q:v",
                    "5",
                    "pipe:1",
                ],
                input=video_data,
                capture_output=True,
                timeout=10,
            )
            if proc.returncode == 0 and proc.stdout:
                return proc.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as exc:
            _LOGGER.debug("ffmpeg conversion failed: %s", exc)
        return None

    def _is_strict_idr_seed(self, seed: bytes) -> bool:
        """Return True when a captured TS seed reconstructs to a decoder reset point."""
        bytestream = extract_video_bytestream_from_ts(seed)
        if not bytestream:
            return False
        codec = detect_video_codec(bytestream) or (self._video_codec or "hevc")
        nal_types = collect_nal_types(bytestream, codec)
        if codec == "h264":
            return 5 in nal_types and (
                (7 in nal_types or self._h264_sps_nal is not None)
                and (8 in nal_types or self._h264_pps_nal is not None)
            )
        return any(nal in nal_types for nal in (19, 20)) and (
            (32 in nal_types or self._hevc_vps_nal is not None)
            and (33 in nal_types or self._hevc_sps_nal is not None)
            and (34 in nal_types or self._hevc_pps_nal is not None)
        )

    def _trim_incomplete_audio_from_seed(self, seed: bytes) -> bytes:
        """Drop only a trailing incomplete AAC PES from a bootstrap seed."""
        packet_size = 188
        audio_pid = 0x101
        if not seed or len(seed) < packet_size:
            return seed

        last_audio_start = -1
        n_packets = len(seed) // packet_size
        for index in range(n_packets):
            off = index * packet_size
            if seed[off] != 0x47:
                continue
            pid = ((seed[off + 1] & 0x1F) << 8) | seed[off + 2]
            if pid == audio_pid and (seed[off + 1] & 0x40):
                last_audio_start = index
        if last_audio_start < 0:
            return seed

        off = last_audio_start * packet_size
        afc = (seed[off + 3] >> 4) & 0x03
        if not (afc & 0x01):
            return seed
        payload_off = off + 4
        if afc & 0x02:
            payload_off = off + 5 + seed[off + 4]
        if payload_off + 6 > off + packet_size:
            return seed
        if (
            seed[payload_off] != 0
            or seed[payload_off + 1] != 0
            or seed[payload_off + 2] != 1
        ):
            return seed

        pes_len = (seed[payload_off + 4] << 8) | seed[payload_off + 5]
        if pes_len <= 0:
            return seed
        expected_total = 6 + pes_len
        available_total = 0
        for index in range(last_audio_start, n_packets):
            off = index * packet_size
            if seed[off] != 0x47:
                continue
            pid = ((seed[off + 1] & 0x1F) << 8) | seed[off + 2]
            if pid != audio_pid:
                continue
            afc = (seed[off + 3] >> 4) & 0x03
            if not (afc & 0x01):
                continue
            payload_off = off + 4
            if afc & 0x02:
                payload_off = off + 5 + seed[off + 4]
            if payload_off >= off + packet_size:
                continue
            available_total += (off + packet_size) - payload_off

        if available_total >= expected_total:
            return seed

        trimmed = bytearray()
        dropped_packets = 0
        for index in range(n_packets):
            off = index * packet_size
            if seed[off] != 0x47:
                continue
            pid = ((seed[off + 1] & 0x1F) << 8) | seed[off + 2]
            if index >= last_audio_start and pid == audio_pid:
                dropped_packets += 1
                continue
            trimmed.extend(seed[off : off + packet_size])

        if dropped_packets > 0:
            _LOGGER.debug(
                "Trimmed trailing incomplete audio PES from IDR seed: dropped %d audio TS packets",
                dropped_packets,
            )
        return bytes(trimmed)

    def _commit_idr_seed(self, seed: bytes, pts: int) -> None:
        """Store a new client bootstrap seed only when it validates as IDR-safe."""
        packet_size = 188
        if not seed:
            return
        seed = self._trim_incomplete_audio_from_seed(seed)
        if not self._is_strict_idr_seed(seed):
            _LOGGER.debug(
                "Rejected TS seed lacking strict IDR reset semantics: %d bytes (%d TS pkts)",
                len(seed),
                len(seed) // packet_size,
            )
            return
        seed_is_strong, strength_reason, video_bytes = self._evaluate_bootstrap_seed(
            seed,
            strict=True,
        )
        self._stream_idr_seed_generation += 1
        self._stream_idr_seed = seed
        self._stream_idr_seed_pts = pts
        self._stream_idr_seed_mono = time.monotonic()
        self._stream_idr_seed_p2p_frame_index = int(self._p2p_video_frames)
        self._stream_idr_seed_video_bytes = video_bytes
        self._stream_idr_seed_is_strong = seed_is_strong
        self._stream_idr_seed_strength_reason = strength_reason
        _LOGGER.debug(
            "IDR seed captured: %d bytes (%d TS pkts), pts=%d, generation=%d, frame_index=%d, strong=%s (%s, video=%d bytes, min_safe_generation=%d)",
            len(self._stream_idr_seed),
            len(self._stream_idr_seed) // packet_size,
            self._stream_idr_seed_pts,
            self._stream_idr_seed_generation,
            self._stream_idr_seed_p2p_frame_index,
            seed_is_strong,
            strength_reason,
            video_bytes,
            self._startup_safe_min_seed_generation,
        )

    def _is_clean_recovery_frame(self, frame_ts: bytes) -> bool:
        """Return True if a post-gap frame is a strict decoder reset keyframe."""
        if not frame_ts:
            return False
        seed = bytearray()
        seed.extend(build_pat_packet())
        seed.extend(build_pmt_packet(0, self._video_codec))
        seed.extend(frame_ts)
        return self._is_strict_idr_seed(bytes(seed))

    def _switch_video_codec(self, codec: str) -> None:
        """Switch muxer input codec when camera stream codec changes."""
        new_codec = (codec or "").lower()
        if new_codec not in {"hevc", "h264"}:
            return
        if new_codec == self._video_codec:
            return

        old_codec = self._video_codec
        self._video_codec = new_codec
        _LOGGER.info(
            "Detected %s stream for %s, switching muxer from %s to %s",
            new_codec.upper(),
            self._sn_num,
            old_codec,
            new_codec,
        )

        while not self._video_queue.empty():
            try:
                self._video_queue.get_nowait()
            except queue.Empty:
                break

        self._video_mux_target_fps = self._default_video_fps_for_codec(new_codec)
        self._video_size_codec = None
        self._reset_video_cadence_state()
        self._h264_sps_nal = None
        self._h264_pps_nal = None
        self._hevc_vps_nal = None
        self._hevc_sps_nal = None
        self._hevc_pps_nal = None
        self._idle_scene_kf = None
        self._generate_black_keyframe(new_codec)
        self._stop_ffmpeg_muxer()
        self._start_ffmpeg_muxer()
