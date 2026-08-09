"""``debug.py stream`` command: drive a live stream through ffplay with recording."""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import tempfile
import threading
import time
from typing import Any

from .audio import (
    RawAudioMonitor,
    _analyze_pcm_audio,
    _muxer_audio_snapshot,
    _print_audio_crackle_diagnostics,
    start_player_audio_diag,
)
from .capture import PacketCapture
from .codec_helpers import _coord_codec, _codec_policy, _codec_text
from .coordinator import (
    _await_live_stream,
    _camera_stream_source,
    _create_coordinator,
    _maybe_restart_stalled_stream,
)
from .correlation import (
    _print_player_decode_correlation,
    _print_stream_join_diagnostics,
    _print_ts_decode_correlation,
)
from .health import StreamHealthTracker
from .list_cmd import _parse_quality_arg
from .processes import (
    _build_pcm_recorder_cmd,
    _build_stream_player_cmd,
    _build_stream_recorder_cmd,
    _stop_player_process,
)
from .startup import (
    _await_adaptive_player_launch_gate,
    _await_clean_startup_seed,
    _await_startup_safe_bootstrap,
)
from .stream_info import (
    _print_live_stream_details,
    _print_stream_media_summary,
    _print_stream_request,
)
from .ts_analysis import (
    _analyze_player_log,
    _analyze_recorded_ts,
    _analyze_recorder_log,
)
from .telemetry import _write_gap_telemetry
from .visual import (
    _monitor_player_decode_correlation,
    _monitor_player_visual_state,
    _summarize_player_visual_state,
)


def _print_ts_metrics(ts_metrics: dict[str, Any]) -> None:
    """Print recorded-TS analysis metrics with per-key formatting."""
    for k, v in ts_metrics.items():
        if k == "decode_errors_sample":
            print(f"  {k}:")
            for line in v:
                print(f"    {line}")
        elif isinstance(v, float):
            print(f"  {k}: {v:.2f}")
        else:
            print(f"  {k}: {v}")


async def cmd_stream(args) -> int:
    # Stream mode is always interactive: wake camera and launch ffplay.
    setattr(args, "wake", True)
    setattr(args, "play", True)

    log = logging.getLogger(__name__)
    run_started_mono = time.monotonic()
    coord = None
    capture: PacketCapture | None = None
    player_proc = None
    player_log_fh = None
    recorder_proc: subprocess.Popen | None = None
    recorder_log_fh = None
    pcm_recorder_proc: subprocess.Popen | None = None
    raw_audio_monitor = RawAudioMonitor()
    recorder_started_mono: float | None = None
    player_started_mono: float | None = None
    live_ready_mono: float | None = None
    player_decode_corr_stop = threading.Event()
    player_decode_corr_thread: threading.Thread | None = None
    player_visual_stop = threading.Event()
    player_visual_thread: threading.Thread | None = None
    player_decode_corr_state: dict[str, Any] = {
        "run_started_mono": run_started_mono,
        "player_started_mono": 0.0,
        "startup_count": 0,
        "startup_samples": [],
        "by_event": {},
    }
    player_visual_state: dict[str, Any] = {
        "run_started_mono": run_started_mono,
        "live_ready_mono": 0.0,
        "launch_gate_started_mono": 0.0,
        "player_started_mono": 0.0,
        "texture_created_mono": 0.0,
        "first_buffer_mono": 0.0,
        "first_stats_mono": 0.0,
        "last_stats_mono": 0.0,
        "stats_count": 0,
        "first_showinfo_mono": 0.0,
        "last_showinfo_mono": 0.0,
        "showinfo_lines": 0,
        "last_showinfo_frame_n": -1,
        "showinfo_frame_jump_count": 0,
        "last_showinfo_pts_time": -1.0,
        "max_showinfo_pts_gap_s": 0.0,
        "showinfo_pts_gaps_over_300ms": 0,
        "max_showinfo_render_gap_s": 0.0,
        "showinfo_render_freezes_over_1s": 0,
        "showinfo_render_gaps_s": [],
        "showinfo_pts_gaps_s": [],
        "showinfo_startup_render_gaps_s": [],
        "showinfo_steady_render_gaps_s": [],
        "showinfo_startup_pts_gaps_s": [],
        "showinfo_steady_pts_gaps_s": [],
        "showinfo_timeline": [],
        "av_sync_samples": [],
        "texture_lines": 0,
        "buffer_lines": 0,
        "late_texture_warned": False,
        "late_stats_warned": False,
    }
    output_file_arg = str(getattr(args, "output_file", "") or "").strip()
    if output_file_arg:
        artifact_base = os.path.abspath(os.path.expanduser(output_file_arg))
        os.makedirs(os.path.dirname(artifact_base) or ".", exist_ok=True)
    else:
        artifact_base = os.path.join(
            tempfile.gettempdir(),
            f"cloudedge_stream_{os.getpid()}_{int(time.time() * 1000)}",
        )
    ts_record_path = f"{artifact_base}.ts"
    pcm_record_path = f"{artifact_base}.wav"
    player_log_path = f"{artifact_base}_player.log"
    recorder_log_path = f"{artifact_base}_recorder.log"
    try:
        capture_arg = getattr(args, "capture", None)
        if capture_arg is not None:
            capture_path = (
                os.path.abspath(os.path.expanduser(capture_arg))
                if capture_arg
                else f"{artifact_base}.pcap"
            )
            capture = PacketCapture(
                capture_path,
                filter_expr=getattr(args, "capture_filter", "udp"),
                iface=getattr(args, "capture_iface", "any"),
            )
            if not capture.start():
                capture = None

        setattr(args, "skip_initial_grab", bool(args.wake and args.play))
        if args.wake and args.play:
            logging.getLogger(__name__).info(
                "Auto-disabling startup frame grab for player wake mode"
            )

        coord, dev, mods = await _create_coordinator(args)
        muxer = getattr(coord, "_muxer", None)
        write_audio = getattr(muxer, "write_audio", None)
        if callable(write_audio):
            original_write_audio = write_audio

            def _debug_write_audio(payload: bytes) -> None:
                raw_audio_monitor.add(payload)
                original_write_audio(payload)

            setattr(muxer, "write_audio", _debug_write_audio)

        # Apply quality override from CLI
        quality_arg = getattr(args, "quality", None)
        if quality_arg is not None:
            coord.set_vvp_quality(
                _parse_quality_arg(quality_arg, coord.quality_profiles)
            )
        adaptive_recovery_for_player = bool(args.wake and args.play)
        setattr(coord, "_p2p_allow_lossy_gap_skip", False)
        setattr(coord, "_p2p_adaptive_lossy_gap_skip", adaptive_recovery_for_player)
        if adaptive_recovery_for_player:
            logging.getLogger(__name__).info(
                "Auto-enabling adaptive gap recovery for wake+play stability"
            )
        url = await _camera_stream_source(mods, coord)

        print("=" * 78)
        print(f"Device: {dev.get('deviceName')} ({dev.get('snNum')})")
        print(f"Stream URL: {url}")
        _print_stream_request(coord, dev, mods)
        print("=" * 78)

        baseline_video_time = float(
            getattr(
                coord, "_last_p2p_video_time", getattr(coord, "_last_video_time", 0.0)
            )
        )
        health_source = StreamHealthTracker(
            logging.getLogger(__name__),
            label="Source video",
        )
        # When a local player is requested, wait for live frames before launch
        # to avoid opening a persistent black window.
        effective_wait_live = bool(args.play)
        effective_stall_timeout = 0
        effective_wake_retry_interval = 0
        if args.wake and args.play:
            # In player mode, a first keyframe may appear before a stable live
            # session is established. Keep self-healing enabled by default so
            # playback does not freeze on a static frame.
            # Keep conservative defaults here. During active playback the
            # codec policy supplies the recovery window.
            # New daytime signaling paths can take >15s before first stable
            # keyframe; avoid preempting bootstrap too aggressively.
            effective_stall_timeout = 30
            effective_wake_retry_interval = 8
            logging.getLogger(__name__).info(
                "Player wake recovery enabled: stall_timeout=%ss wake_retry=%ss",
                effective_stall_timeout,
                effective_wake_retry_interval,
            )

        if args.wake:
            coord.wake_camera()
            if effective_wait_live:
                print("Waiting for live video...")
                live_ok = await _await_live_stream(
                    coord,
                    timeout=args.wake_timeout,
                    stall_timeout=effective_stall_timeout,
                    wake_retry_interval=effective_wake_retry_interval,
                )
                print(f"live_ready={live_ok}")
                if live_ok:
                    _print_live_stream_details(coord, dev, mods)
                if not live_ok:
                    raise RuntimeError(
                        "No live video frames received after wake attempts. "
                        "Aborting to avoid a black player window."
                    )
                live_ready_mono = time.monotonic()

        if args.play:
            active_codec = _coord_codec(coord)
            active_codec_text = _codec_text(active_codec)
            active_policy = _codec_policy(active_codec)
            player_cmd = _build_stream_player_cmd(
                url,
                duration=0,
            )
            start_frames = int(getattr(coord, "_p2p_video_frames", 0))
            if live_ready_mono is None:
                live_ready_mono = time.monotonic()
            launch_gate_started_mono = time.monotonic()
            player_visual_state["live_ready_mono"] = live_ready_mono
            player_visual_state["launch_gate_started_mono"] = launch_gate_started_mono
            gate_ready, gate_state = await _await_adaptive_player_launch_gate(
                coord,
                start_frames=start_frames,
                timeout=active_policy.launch_gate_timeout_s,
            )
            logging.getLogger(__name__).info(
                "Player launch gate %s after %.2fs (reason=%s, preferred=%s, backlog_ready=%s, frames=%s, stable_for=%.2fs, video_age=%.2fs, generation=%s/%s, budget=%.2fs)",
                "ready" if gate_ready else "expired; launching anyway",
                float(gate_state.get("launch_gate_wait_s", 0.0) or 0.0),
                gate_state.get("launch_gate_reason", "unknown"),
                gate_state.get("preferred_join_mode", "unknown"),
                gate_state.get("backlog_ready", False),
                gate_state.get("launch_gate_frames_since_start", 0),
                float(gate_state.get("stable_for_s", 0.0) or 0.0),
                float(gate_state.get("video_age_s", 999.0) or 999.0),
                gate_state.get("seed_generation", 0),
                gate_state.get("required_seed_generation", 0),
                float(gate_state.get("launch_gate_budget_s", 0.0) or 0.0),
            )
            if active_policy.clean_startup_seed and gate_ready:
                need_clean_seed = bool(
                    str(gate_state.get("launch_gate_reason", "") or "")
                    == "ready-backlog"
                    and not bool(gate_state.get("seed_decode_probed", False))
                )
                if need_clean_seed:
                    bounded_seed_wait_s = max(
                        0.9,
                        min(
                            2.4,
                            0.16
                            * max(
                                4,
                                int(
                                    gate_state.get(
                                        "backlog_follow_video_pusi_target",
                                        0,
                                    )
                                    or 0
                                ),
                            )
                            + 0.55,
                        ),
                    )
                    logging.getLogger(__name__).info(
                        "%s fast backlog launch is ready but not decode-probed yet; waiting up to %.2fs for a cleaner startup seed",
                        active_codec_text.upper(),
                        bounded_seed_wait_s,
                    )
                    clean_seed_ready, clean_seed_state = (
                        await _await_clean_startup_seed(
                            coord,
                            timeout=bounded_seed_wait_s,
                        )
                    )
                    gate_state = {**gate_state, **clean_seed_state}
                    if clean_seed_ready:
                        gate_state["launch_gate_reason"] = "ready-backlog-clean-seed"
                    logging.getLogger(__name__).info(
                        "%s fast backlog cleanup %s (reason=%s, probe=%s, startup_safe=%s, video_age=%.2fs)",
                        active_codec_text.upper(),
                        "ready" if clean_seed_ready else "expired; keeping fast launch",
                        gate_state.get("seed_strength_reason", ""),
                        gate_state.get("clean_seed_probe_reason", ""),
                        gate_state.get("startup_safe", False),
                        float(gate_state.get("video_age_s", 999.0) or 999.0),
                    )
            if active_policy.clean_startup_seed and not gate_ready:
                bounded_seed_wait_s = max(
                    1.4,
                    min(
                        4.0,
                        0.22
                        * max(
                            4,
                            int(
                                gate_state.get(
                                    "backlog_follow_video_pusi_target",
                                    0,
                                )
                                or 0
                            ),
                        )
                        + 0.9
                        + 0.35
                        * int(gate_state.get("recent_moderate_gap_count", 0) or 0),
                    ),
                )
                try_short_clean_seed_wait = bool(
                    float(gate_state.get("video_age_s", 999.0) or 999.0) < 1.2
                    and int(gate_state.get("recent_severe_gap_count", 0) or 0) == 0
                    and (
                        bool(gate_state.get("startup_safe", False))
                        or bool(gate_state.get("seed_decode_probed", False))
                        or str(gate_state.get("preferred_join_mode", "") or "")
                        == "ready-backlog"
                    )
                )
                if try_short_clean_seed_wait:
                    logging.getLogger(__name__).info(
                        "%s launch gate missed its fast budget; waiting up to %.2fs more for a cleaner startup seed",
                        active_codec_text.upper(),
                        bounded_seed_wait_s,
                    )
                    clean_seed_ready, clean_seed_state = (
                        await _await_clean_startup_seed(
                            coord,
                            timeout=bounded_seed_wait_s,
                        )
                    )
                    gate_state = {**gate_state, **clean_seed_state}
                    gate_ready = gate_ready or clean_seed_ready
                    logging.getLogger(__name__).info(
                        "%s bounded clean-seed wait %s (reason=%s, probe=%s, startup_safe=%s, video_age=%.2fs)",
                        active_codec_text.upper(),
                        "ready" if clean_seed_ready else "expired; launching anyway",
                        gate_state.get("seed_strength_reason", ""),
                        gate_state.get("clean_seed_probe_reason", ""),
                        gate_state.get("startup_safe", False),
                        float(gate_state.get("video_age_s", 999.0) or 999.0),
                    )
                else:
                    logging.getLogger(__name__).warning(
                        "%s launch gate expired and the source is already too stale or unstable; launching immediately",
                        active_codec_text.upper(),
                    )
            player_visual_state["player_launch_gate_reason"] = str(
                gate_state.get("launch_gate_reason", "unknown")
            )
            player_visual_state["player_launch_gate_budget_s"] = float(
                gate_state.get("launch_gate_budget_s", 0.0) or 0.0
            )
            player_visual_state["player_launch_gate_frames_since_start"] = int(
                gate_state.get("launch_gate_frames_since_start", 0) or 0
            )
            play_env = os.environ.copy()
            player_log_fh = open(player_log_path, "wb")

            logging.getLogger(__name__).info("Launching player: %s", player_cmd[0])
            player_proc = subprocess.Popen(
                player_cmd,
                stdin=subprocess.DEVNULL,
                stdout=player_log_fh,
                stderr=subprocess.STDOUT,
                env=play_env,
            )
            player_started_mono = time.monotonic()
            player_decode_corr_state["player_started_mono"] = player_started_mono
            player_visual_state["player_started_mono"] = player_started_mono
            player_decode_corr_stop.clear()
            player_decode_corr_thread = threading.Thread(
                target=_monitor_player_decode_correlation,
                args=(
                    coord,
                    player_log_path,
                    player_decode_corr_stop,
                    player_decode_corr_state,
                ),
                daemon=True,
            )
            player_decode_corr_thread.start()
            player_visual_stop.clear()
            player_visual_thread = threading.Thread(
                target=_monitor_player_visual_state,
                args=(player_log_path, player_visual_stop, player_visual_state),
                daemon=True,
            )
            player_visual_thread.start()

            # Give ffplay a short head start to lock to video.
            await asyncio.sleep(0.6)

            # Sidecar ffmpeg clients are stricter than ffplay about early
            # stream probing. Wait for post-launch frame progression so they
            # don't fail with "could not find codec parameters".
            sidecar_start_frames = int(getattr(coord, "_p2p_video_frames", 0))
            sidecar_deadline = time.monotonic() + 6.0
            while time.monotonic() < sidecar_deadline:
                await asyncio.sleep(0.2)
                frames = int(getattr(coord, "_p2p_video_frames", 0))
                last_video = float(getattr(coord, "_last_p2p_video_time", 0.0))
                age = (time.monotonic() - last_video) if last_video > 0 else 999.0
                if (frames - sidecar_start_frames) >= 8 and age < 1.0:
                    break
            await _await_startup_safe_bootstrap(coord, timeout=4.0)

            # Launch a separate ffmpeg recorder as a second TCP client
            recorder_cmd = _build_stream_recorder_cmd(
                url,
                ts_record_path,
                duration=0,
            )
            logging.getLogger(__name__).info(
                "Launching stream recorder: ffmpeg → %s",
                ts_record_path,
            )
            recorder_log_fh = open(  # pylint: disable=consider-using-with
                recorder_log_path, "wb"
            )
            recorder_proc = subprocess.Popen(  # pylint: disable=consider-using-with
                recorder_cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=recorder_log_fh,
                env=play_env,
            )
            recorder_started_mono = time.monotonic()

            # Launch PCM audio recorder as a third TCP client
            pcm_cmd = _build_pcm_recorder_cmd(
                url,
                pcm_record_path,
                duration=0,
            )
            logging.getLogger(__name__).info(
                "Launching PCM audio recorder: ffmpeg → %s",
                pcm_record_path,
            )
            pcm_recorder_proc = subprocess.Popen(  # pylint: disable=consider-using-with
                pcm_cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=play_env,
            )

            # Surface failures quickly instead of silently continuing.
            await asyncio.sleep(1)
            if recorder_proc.poll() is not None:
                logging.getLogger(__name__).warning(
                    "Recorder exited immediately with rc=%s, retrying once",
                    recorder_proc.returncode,
                )
                if recorder_log_fh is not None:
                    recorder_log_fh.flush()
                    recorder_log_fh.close()
                recorder_log_fh = open(recorder_log_path, "ab")
                recorder_proc = subprocess.Popen(
                    recorder_cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=recorder_log_fh,
                    env=play_env,
                )
                recorder_started_mono = time.monotonic()
                await asyncio.sleep(1.2)
            if pcm_recorder_proc.poll() is not None:
                logging.getLogger(__name__).warning(
                    "PCM recorder exited immediately with rc=%s, retrying once",
                    pcm_recorder_proc.returncode,
                )
                pcm_recorder_proc = subprocess.Popen(
                    pcm_cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    env=play_env,
                )
                await asyncio.sleep(1.2)
            if player_proc.poll() is not None:
                raise RuntimeError(
                    f"player exited early with rc={player_proc.returncode}"
                )

            start_player_audio_diag(player_proc)

        end_at = None
        if not args.play and args.duration > 0:
            end_at = time.time() + args.duration
        stall_started_at: float | None = None
        runtime_stall_timeout = effective_stall_timeout
        runtime_stall_timeout_logged = False
        while True:
            if end_at is not None and time.time() >= end_at:
                break
            if args.play and player_proc is not None and args.duration > 0:
                # Duration is measured as ffplay wall-clock open time.
                if player_started_mono is not None and (
                    time.monotonic() - player_started_mono
                ) >= float(args.duration):
                    break
                if player_proc.poll() is not None:
                    break
            health_source.tick(coord)

            if effective_stall_timeout > 0:
                if args.play:
                    runtime_codec = _coord_codec(coord)
                    runtime_codec_text = _codec_text(runtime_codec)
                    runtime_policy = _codec_policy(runtime_codec)
                    runtime_stall_timeout = runtime_policy.runtime_stall_timeout_s
                    if not runtime_stall_timeout_logged:
                        logging.getLogger(__name__).info(
                            "Runtime %s recovery timeout active: stall_timeout=%ss",
                            runtime_codec_text.upper(),
                            runtime_stall_timeout,
                        )
                        runtime_stall_timeout_logged = True
                stall_started_at = _maybe_restart_stalled_stream(
                    coord,
                    baseline_video_time,
                    stall_started_at,
                    runtime_stall_timeout,
                )
            await asyncio.sleep(1)

        _t0 = time.time()
        if player_proc is not None and player_proc.poll() is None:
            _stop_player_process(player_proc)
        # Stop recorder too
        _stop_player_process(recorder_proc)
        _stop_player_process(pcm_recorder_proc)
        if player_log_fh is not None:
            player_log_fh.flush()
            player_log_fh.close()
            player_log_fh = None
        if player_decode_corr_thread is not None:
            player_decode_corr_stop.set()
            player_decode_corr_thread.join(timeout=1.5)
            player_decode_corr_thread = None
        if player_visual_thread is not None:
            player_visual_stop.set()
            player_visual_thread.join(timeout=1.5)
            player_visual_thread = None
        if recorder_log_fh is not None:
            recorder_log_fh.flush()
            recorder_log_fh.close()
            recorder_log_fh = None
        logging.getLogger(__name__).info(
            "Player/recorder stopped in %.1fs",
            time.time() - _t0,
        )

        source_summary = health_source.summary(coord)
        if args.play:
            _print_stream_media_summary(coord, dev, mods, source_summary)
        analysis_mode = str(getattr(args, "analysis_mode", "ffplay") or "ffplay")
        full_mode = analysis_mode == "full"

        if (not args.play) or full_mode:
            print("\nStream health (source ingress)")
            print("-" * 78)
            print(f"video_frames: {int(source_summary['video_frames'])}")
            print(f"avg_fps: {float(source_summary['avg_fps']):.2f}")
            print(f"max_video_gap_s: {float(source_summary['max_gap_s']):.2f}")
            print(f"recovered_stalls: {int(source_summary['recovered_stalls'])}")
            print(
                f"recovered_stalls_over_1s: {int(source_summary['recovered_stalls_over_1s'])}"
            )
            print(
                f"recovered_stalls_over_3s: {int(source_summary['recovered_stalls_over_3s'])}"
            )
            unresolved = float(source_summary["unresolved_stall_s"])
            if unresolved > 0.0:
                print(f"unresolved_stall_s: {unresolved:.2f}")
            source_stalls = source_summary.get("stall_events", []) or []
            if source_stalls:
                print("stall_events:")
                for event in source_stalls[:8]:
                    print(
                        "  "
                        f"+{float(event['start_s']):.2f}s..+{float(event['end_s']):.2f}s "
                        f"duration={float(event['duration_s']):.2f}s "
                        f"frames={int(event['video_frames'])}"
                    )

        # --- TS recording analysis ---
        if args.play:
            log = logging.getLogger(__name__)
            run_failed = False
            _t_player = time.time()
            player_metrics = _analyze_player_log(player_log_path, log)
            mux_audio_metrics = _muxer_audio_snapshot(coord)
            player_metrics.update(
                _summarize_player_visual_state(
                    player_visual_state,
                    mux_audio_metrics,
                )
            )
            player_metrics["player_has_issues"] = bool(
                player_metrics.get("player_has_issues", False)
                or player_metrics.get("player_visual_has_issues", False)
            )
            log.info("Player log analysis completed in %.1fs", time.time() - _t_player)
            if player_metrics:
                print("\nFFplay observed stats (authoritative)")
                print("-" * 78)
                for k, v in player_metrics.items():
                    print(f"  {k}: {v}")
                run_failed = run_failed or bool(
                    player_metrics.get("player_has_issues", False)
                )
                if full_mode:
                    _print_player_decode_correlation(coord, player_decode_corr_state)
                    _print_stream_join_diagnostics(coord)

            _t_pcm = time.time()
            pcm_metrics = _analyze_pcm_audio(pcm_record_path, log)
            log.info(
                "PCM audio crackle analysis completed in %.1fs",
                time.time() - _t_pcm,
            )
            _print_audio_crackle_diagnostics(
                raw_audio_monitor.summary(),
                pcm_metrics,
                mux_audio_metrics,
            )

            recorder_only_timestamp_discontinuities = False
            ts_minor_glitch = False
            if full_mode:
                _t_rec = time.time()
                recorder_metrics = _analyze_recorder_log(recorder_log_path, log)
                log.info(
                    "Recorder log analysis completed in %.1fs", time.time() - _t_rec
                )
                if recorder_metrics:
                    print("\nRecorder log analysis (what ffmpeg recorder reported)")
                    print("-" * 78)
                    for k, v in recorder_metrics.items():
                        print(f"  {k}: {v}")
                    recorder_only_timestamp_discontinuities = bool(
                        int(recorder_metrics.get("recorder_decode_error_lines", 0) or 0)
                        == 0
                        and int(
                            recorder_metrics.get("recorder_continuity_error_lines", 0)
                            or 0
                        )
                        > 0
                        and int(
                            recorder_metrics.get("recorder_continuity_error_lines", 0)
                            or 0
                        )
                        == int(
                            recorder_metrics.get(
                                "recorder_timestamp_discontinuity_lines", 0
                            )
                            or 0
                        )
                    )
                    run_failed = run_failed or bool(
                        recorder_metrics.get("recorder_has_issues", False)
                        and not recorder_only_timestamp_discontinuities
                    )
                else:
                    print("\nRecorder log analysis (what ffmpeg recorder reported)")
                    print("-" * 78)
                    print("  recorder stderr was empty")

                _t1 = time.time()
                ts_metrics = _analyze_recorded_ts(ts_record_path, log)
                log.info("TS analysis completed in %.1fs", time.time() - _t1)
                if ts_metrics:
                    print("\nTS recording analysis (what the player received)")
                    print("-" * 78)
                    _print_ts_metrics(ts_metrics)
                    _print_ts_decode_correlation(
                        coord, recorder_started_mono, ts_metrics
                    )
                    video_skip_durations = [
                        float(v)
                        for v in (ts_metrics.get("video_skip_durations_s", []) or [])
                    ]
                    max_video_skip_s = (
                        max(video_skip_durations) if video_skip_durations else 0.0
                    )
                    ts_minor_glitch = bool(
                        int(ts_metrics.get("decode_error_lines", 0) or 0) == 0
                        and int(ts_metrics.get("audio_gap_count", 0) or 0) == 0
                        and int(ts_metrics.get("video_skip_count", 0) or 0) <= 1
                        and max_video_skip_s < 1.0
                    )
                    run_failed = run_failed or bool(
                        int(ts_metrics.get("decode_error_lines", 0) or 0) > 0
                        or int(ts_metrics.get("audio_gap_count", 0) or 0) > 0
                        or (
                            int(ts_metrics.get("video_skip_count", 0) or 0) > 0
                            and not ts_minor_glitch
                        )
                    )
                else:
                    run_failed = True

                if player_metrics and ts_metrics:
                    player_has_issues = bool(
                        player_metrics.get("player_has_issues", False)
                    )
                    ts_looks_clean = (
                        int(ts_metrics.get("decode_error_lines", 0) or 0) == 0
                        and int(ts_metrics.get("video_skip_count", 0) or 0) == 0
                    )
                    if player_has_issues and ts_looks_clean:
                        print("\nResult consistency warning")
                        print("-" * 78)
                        print(
                            "  ffplay reported player-visible decode/continuity issues even though"
                        )
                        print(
                            "  the recorded TS analysis looked clean. Treat the run as failed or"
                        )
                        print(
                            "  degraded; the player log is authoritative for viewer experience."
                        )
                        run_failed = True

                # --- PCM audio content analysis ---
                if pcm_metrics:
                    print("\nPCM audio analysis (actual audible content)")
                    print("-" * 78)
                    for k, v in pcm_metrics.items():
                        if isinstance(v, list):
                            print(f"  {k}: {v}")
                        elif isinstance(v, float):
                            print(f"  {k}: {v:.2f}")
                        else:
                            print(f"  {k}: {v}")
                else:
                    run_failed = True

            print("\nOverall test verdict")
            print("-" * 78)
            if run_failed:
                print("  degraded/fail")
                if full_mode:
                    print(
                        "  One or more of: player log, recorder log, or TS analysis reported"
                    )
                    print("  viewer-visible issues. Exit status will be non-zero.")
                else:
                    print(
                        "  FFplay observed issues (visual cadence/decode/continuity) are present."
                    )
                    print("  Exit status will be non-zero.")
                return 3
            if full_mode and (
                ts_minor_glitch or recorder_only_timestamp_discontinuities
            ):
                print("  acceptable")
                print(
                    "  ffplay stayed clean; only a short isolated TS skip and/or recorder"
                )
                print("  timestamp-offset warnings remained. Exit status will be zero.")
                return 0
            print("  clean")
            print("  Player log, recorder log, and TS analysis all look clean.")

        return 0
    finally:
        if capture is not None:
            capture.stop()
        _stop_player_process(player_proc)
        _stop_player_process(recorder_proc)
        _stop_player_process(pcm_recorder_proc)
        _write_gap_telemetry(coord, artifact_base, log)
        if player_log_fh is not None:
            try:
                player_log_fh.flush()
                player_log_fh.close()
            except (OSError, ValueError):
                pass
        if player_decode_corr_thread is not None:
            player_decode_corr_stop.set()
            player_decode_corr_thread.join(timeout=1.0)
        if recorder_log_fh is not None:
            try:
                recorder_log_fh.flush()
                recorder_log_fh.close()
            except (OSError, ValueError):
                pass
        if coord:
            await coord.async_stop()
