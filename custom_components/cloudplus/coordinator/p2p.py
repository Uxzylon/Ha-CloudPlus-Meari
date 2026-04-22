"""P2P streaming session helpers for the coordinator."""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any

from ..api import MeariApiClient
from ..p2p_streamer import P2PStreamer
from .nal import collect_nal_types, detect_video_codec, is_video_keyframe

_LOGGER = logging.getLogger(__name__)


class CoordinatorP2PMixin:
    """Owns P2P session start/stop and session restart handling."""

    def _begin_streaming(self, api: MeariApiClient, grab_only: bool = False) -> None:
        """Start P2P streaming in a background thread."""
        if self._stream_thread and self._stream_thread.is_alive():
            return

        if not grab_only:
            self._live_stream_requested = True
            self._audio_real_started = False
            self._audio_realtime_next_ts = 0.0
            while True:
                try:
                    self._audio_queue.get_nowait()
                except queue.Empty:
                    break

        grab_start: float | None = None
        grab_duration = 5.0
        grab_hard_timeout = 20.0

        got_keyframe = False
        self._p2p_video_frames = 0
        self._p2p_audio_frames = 0
        self._p2p_audio_bytes = 0
        self._p2p_audio_non_ff_bytes = 0
        self._p2p_audio_all_ff_frames = 0
        self._last_p2p_audio_time = 0.0
        self._video_mux_target_fps = self._default_video_fps_for_codec(
            self._video_codec
        )
        self._reset_video_cadence_state()
        self._h264_sps_nal = None
        self._h264_pps_nal = None
        self._hevc_vps_nal = None
        self._hevc_sps_nal = None
        self._hevc_pps_nal = None
        self._force_param_sets_on_next_keyframe = False
        have_h264_sps = False
        have_h264_pps = False
        have_hevc_vps = False
        have_hevc_sps = False
        have_hevc_pps = False
        bootstrap_injected = False

        def on_video(data: bytes) -> None:
            nonlocal grab_start
            nonlocal got_keyframe
            nonlocal have_h264_sps
            nonlocal have_h264_pps
            nonlocal have_hevc_vps
            nonlocal have_hevc_sps
            nonlocal have_hevc_pps
            nonlocal bootstrap_injected

            now_mono = time.monotonic()
            self._last_p2p_video_time = now_mono
            self._p2p_video_frames += 1
            self._observe_video_cadence(now_mono)

            detected_codec = detect_video_codec(data)
            if detected_codec and detected_codec != self._video_codec:
                self._switch_video_codec(detected_codec)
                got_keyframe = False
                have_h264_sps = False
                have_h264_pps = False
                have_hevc_vps = False
                have_hevc_sps = False
                have_hevc_pps = False
                bootstrap_injected = False

            codec = self._video_codec
            self._update_parameter_set_cache(data, codec)
            nal_types = collect_nal_types(data, codec)
            has_param_payload = False
            params_ready = False
            if codec == "h264":
                have_h264_sps = (
                    have_h264_sps
                    or (self._h264_sps_nal is not None)
                    or (7 in nal_types)
                )
                have_h264_pps = (
                    have_h264_pps
                    or (self._h264_pps_nal is not None)
                    or (8 in nal_types)
                )
                has_param_payload = (7 in nal_types) or (8 in nal_types)
                params_ready = have_h264_sps and have_h264_pps
            else:
                have_hevc_vps = (
                    have_hevc_vps
                    or (self._hevc_vps_nal is not None)
                    or (32 in nal_types)
                )
                have_hevc_sps = (
                    have_hevc_sps
                    or (self._hevc_sps_nal is not None)
                    or (33 in nal_types)
                )
                have_hevc_pps = (
                    have_hevc_pps
                    or (self._hevc_pps_nal is not None)
                    or (34 in nal_types)
                )
                has_param_payload = (
                    (32 in nal_types) or (33 in nal_types) or (34 in nal_types)
                )
                params_ready = have_hevc_vps and have_hevc_sps and have_hevc_pps

            is_kf = is_video_keyframe(data, codec)
            if is_kf or has_param_payload:
                self._refresh_video_resolution_hint(data, codec)

            if not got_keyframe:
                if not params_ready:
                    return
                if not bootstrap_injected and self._idle_video_kf:
                    if not self._muxer_video_started.is_set():
                        self._feed_video(self._idle_video_kf)
                    bootstrap_injected = True
                if has_param_payload and not is_kf:
                    self._feed_video(data)
                    return
                if is_kf:
                    got_keyframe = True
                    _LOGGER.debug(
                        "First %s keyframe after parameter sets, feeding muxer",
                        codec.upper(),
                    )
                    if (
                        self._ffmpeg_proc is None
                        or self._ffmpeg_proc.poll() is not None
                    ):
                        kf_data = self._prepend_parameter_sets_if_needed(
                            data,
                            codec,
                            collect_nal_types(data, codec),
                        )
                        self._latest_video_kf = bytes(kf_data)
                        self._generate_black_keyframe(codec)
                        self._start_ffmpeg_muxer()
                else:
                    return

            force_param_sets = self._force_param_sets_on_next_keyframe and is_kf
            feed_data = (
                self._prepend_parameter_sets_if_needed(
                    data,
                    codec,
                    nal_types,
                    force=force_param_sets,
                )
                if is_kf
                else data
            )

            if force_param_sets:
                self._force_param_sets_on_next_keyframe = False
                gap_event_id = self._last_gap_skip_event_id
                if gap_event_id > 0:
                    self._update_gap_skip_event(
                        gap_event_id,
                        param_sets_prepended_mono=time.monotonic(),
                        recovered_codec=codec.upper(),
                        recovered_keyframe_bytes=len(feed_data),
                    )
                _LOGGER.info(
                    "Gap recovery bootstrap #%d: prepended cached parameter sets to recovered %s keyframe",
                    gap_event_id,
                    codec.upper(),
                )

            self._feed_video(feed_data)

            if is_kf:
                self._latest_video_kf = bytes(feed_data)
                self._latest_hevc_kf = self._latest_video_kf
                if self._snapshot_conversion_enabled:
                    now_mono = time.monotonic()
                    should_convert = grab_only or (
                        (now_mono - self._last_snapshot_convert_time)
                        >= self._snapshot_convert_interval
                        and not self._snapshot_convert_lock.locked()
                    )
                    if should_convert:
                        threading.Thread(
                            target=self._convert_latest_kf,
                            daemon=True,
                        ).start()
                if grab_only:
                    if grab_start is None:
                        grab_start = time.time()
                        _LOGGER.debug(
                            "Grab: first keyframe, streaming for %.0fs", grab_duration
                        )
                    elif time.time() - grab_start >= grab_duration:
                        _LOGGER.debug("Grab: %.0fs elapsed, stopping", grab_duration)
                        streamer.request_stop()

        def on_audio(data: bytes) -> None:
            self._last_p2p_audio_time = time.monotonic()
            self._p2p_audio_frames += 1
            self._p2p_audio_bytes += len(data)
            non_ff = sum(1 for b in data if b != 0xFF)
            self._p2p_audio_non_ff_bytes += non_ff
            if data and non_ff == 0:
                self._p2p_audio_all_ff_frames += 1
            self._feed_audio(data)

        def on_login() -> None:
            _LOGGER.info("VVP login OK for %s", self._sn_num)

        def on_disconnect() -> None:
            _LOGGER.info("P2P stream ended for %s", self._sn_num)

        def on_gap_skip(diag: dict[str, Any]) -> None:
            self._handle_gap_skip_reset(diag)

        streamer = P2PStreamer(
            api=api,
            device=self._device,
            on_video=on_video,
            on_audio=on_audio,
            on_login=on_login,
            on_disconnect=on_disconnect,
            on_gap_skip=on_gap_skip,
            allow_lossy_gap_skip=bool(self._p2p_allow_lossy_gap_skip),
            adaptive_lossy_gap_skip=bool(self._p2p_adaptive_lossy_gap_skip),
            vvp_quality=self._vvp_quality,
        )
        self._p2p_streamer = streamer
        self._stream_grab_only = grab_only

        if grab_only:
            grab_deadline = time.time() + grab_hard_timeout

            def _grab_watcher() -> None:
                while self._running and self._p2p_streamer is streamer:
                    now = time.time()
                    if grab_start is not None and now - grab_start >= grab_duration:
                        _LOGGER.debug(
                            "Grab watcher: %.0fs elapsed, stopping", grab_duration
                        )
                        streamer.request_stop()
                        return
                    if now >= grab_deadline:
                        _LOGGER.debug(
                            "Grab watcher: hard timeout %.0fs reached, stopping",
                            grab_hard_timeout,
                        )
                        streamer.request_stop()
                        return
                    time.sleep(0.5)

            threading.Thread(
                target=_grab_watcher,
                name=f"cloudplus_grab_watch_{self._sn_num}",
                daemon=True,
            ).start()

        def _stream_worker() -> None:
            restart_live = False
            session_started_mono = time.monotonic()
            forced_rebootstrap = False
            try:
                video_frames, video_bytes = streamer.run_session()
                _LOGGER.info(
                    "P2P session done for %s: %d video frames, %d bytes, %d audio frames, %d audio bytes (non_ff=%d, all_ff_frames=%d, audio_q_drops=%d, audio_throttle_drops=%d, audio_silence_drops=%d, aac_overflow=%d, flush_real=%d, flush_silence=%d, video_q_drops=%d)",
                    self._sn_num,
                    video_frames,
                    video_bytes,
                    self._p2p_audio_frames,
                    self._p2p_audio_bytes,
                    self._p2p_audio_non_ff_bytes,
                    self._p2p_audio_all_ff_frames,
                    self._audio_queue_drops,
                    self._audio_throttle_drops,
                    self._audio_silence_drops,
                    self._audio_aac_overflow,
                    self._audio_flush_real,
                    self._audio_flush_silence,
                    self._video_queue_drops,
                )
                if self._audio_queue_drops:
                    _LOGGER.warning(
                        "Audio queue dropped %d chunks for %s (writer backpressure)",
                        self._audio_queue_drops,
                        self._sn_num,
                    )
                if self._audio_throttle_drops:
                    _LOGGER.info(
                        "Audio burst shaper dropped %d stale chunks for %s",
                        self._audio_throttle_drops,
                        self._sn_num,
                    )
                if self._audio_aac_overflow:
                    _LOGGER.warning(
                        "AAC deque overflow: %d frames trimmed for %s (encoder→flush lag)",
                        self._audio_aac_overflow,
                        self._sn_num,
                    )
                if self._audio_silence_drops:
                    _LOGGER.debug(
                        "Audio silence drops: %d for %s",
                        self._audio_silence_drops,
                        self._sn_num,
                    )
                if (
                    self._p2p_audio_frames >= 40
                    and self._p2p_audio_non_ff_bytes
                    <= max(8, self._p2p_audio_bytes // 200)
                ):
                    _LOGGER.warning(
                        "Audio payload appears silent for %s (mostly 0xFF frames)",
                        self._sn_num,
                    )
            except Exception as exc:
                _LOGGER.error("P2P stream error for %s: %s", self._sn_num, exc)
                video_frames = 0
                video_bytes = 0
            finally:
                self._p2p_streamer = None
                self._stream_grab_only = False
                if grab_only and not self._startup_ready.is_set():
                    self._startup_ready.set()

                restart_live = bool(
                    self._running
                    and not grab_only
                    and self._live_stream_requested
                    and self._api is api
                )

                while not self._video_queue.empty():
                    try:
                        self._video_queue.get_nowait()
                    except queue.Empty:
                        break

                if restart_live:
                    session_age = max(0.0, time.monotonic() - session_started_mono)
                    bootstrap_failed = (video_frames == 0) or (
                        video_frames < 30 and session_age >= 12.0
                    )
                    if bootstrap_failed:
                        self._consecutive_live_bootstrap_failures += 1
                    else:
                        self._consecutive_live_bootstrap_failures = 0

                    now_mono = time.monotonic()
                    if (
                        self._consecutive_live_bootstrap_failures >= 3
                        and (now_mono - self._last_rebootstrap_mono) > 30.0
                    ):
                        self._last_rebootstrap_mono = now_mono
                        self._session_rebootstrap_requested = True
                        self._consecutive_live_bootstrap_failures = 0
                        _LOGGER.warning(
                            "Repeated live bootstrap failures for %s; forcing full session rebootstrap",
                            self._sn_num,
                        )
                        self._stream_thread = None
                        self._camera_awake = False
                        self._idle_since = time.monotonic()
                        self._fire_update()
                        forced_rebootstrap = True

                    if not forced_rebootstrap:
                        _LOGGER.info(
                            "Live stream dropped, restarting for %s", self._sn_num
                        )
                        self._stream_thread = None
                        self._camera_awake = True
                        self._idle_since = 0.0
                        self._idle_scene_kf = None
                        self._fire_update()
                        self._audio_real_started = False
                        self._audio_primed.clear()
                        self._silence_feeder_gen += 1
                        threading.Thread(
                            target=self._silence_feeder,
                            args=(self._silence_feeder_gen,),
                            daemon=True,
                        ).start()
                        self._p2p_session_generation += 1
                        time.sleep(0.8)
                        self._begin_streaming(api, grab_only=False)
                elif not forced_rebootstrap:
                    threading.Thread(
                        target=self._build_idle_scene_keyframe,
                        daemon=True,
                    ).start()
                    self._audio_primed.clear()
                    self._silence_feeder_gen += 1
                    threading.Thread(
                        target=self._silence_feeder,
                        args=(self._silence_feeder_gen,),
                        daemon=True,
                    ).start()
                    self._camera_awake = False
                    self._idle_since = time.monotonic()
                    self._fire_update()

        self._stream_thread = threading.Thread(
            target=_stream_worker,
            name=f"cloudplus_p2p_{self._sn_num}",
            daemon=True,
        )
        self._stream_thread.start()
        if not grab_only:
            self._camera_awake = True
            self._idle_since = 0.0
            self._idle_scene_kf = None
        self._fire_update()

    def _end_streaming(self) -> None:
        """Stop the running P2P stream if any."""
        self._live_stream_requested = False
        if self._p2p_streamer:
            self._p2p_streamer.request_stop()
