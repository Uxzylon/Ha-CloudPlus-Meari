from __future__ import annotations

import logging
import os
import re
import threading
import time
from typing import Any

from .stats import _percentile

_PLAYER_DECODE_MARKERS = (
    "Could not find ref with POC",
    "Error constructing the frame RPS",
    "Skipping invalid undecodable NALU",
    "Invalid NAL unit",
    "decode_slice_header error",
    "Error while decoding stream",
)
_SHOWINFO_PTS_TIME_RE = re.compile(r"pts_time:([\-\d\.]+)")
_SHOWINFO_FRAME_N_RE = re.compile(r"\bn:\s*(\d+)")
_FFPLAY_TEXTURE_LINE_RE = re.compile(r"Created\s+\d+x\d+\s+texture")
_FFPLAY_AV_DRIFT_RE = re.compile(r"A-V:\s*([\-\d\.]+)")


def _select_gap_event_for_error_time(
    events: list[dict[str, Any]],
    when_mono: float,
) -> dict[str, Any] | None:
    candidate: dict[str, Any] | None = None
    candidate_start = -1.0
    for event in events:
        started = float(event.get("started_mono", 0.0) or 0.0)
        if started <= 0.0 or when_mono < started:
            continue
        release_mono = float(
            event.get(
                "quarantine_release_mono",
                event.get("output_reset_mono", started),
            )
            or started
        )
        horizon = max(started + 8.0, release_mono + 4.0)
        if when_mono > horizon:
            continue
        if started >= candidate_start:
            candidate = event
            candidate_start = started
    return candidate


def _monitor_player_decode_correlation(
    coord: Any,
    log_path: str,
    stop_event: threading.Event,
    state: dict[str, Any],
) -> None:
    log = logging.getLogger(__name__)
    offset = 0
    partial = ""

    def _drain_decode_log_once() -> None:
        nonlocal offset, partial
        if not os.path.isfile(log_path):
            return
        try:
            with open(log_path, encoding="utf-8", errors="replace") as fh:
                fh.seek(offset)
                chunk = fh.read()
                offset = fh.tell()
        except Exception:
            return

        if not chunk:
            return

        partial += chunk.replace("\r", "\n")
        lines = partial.split("\n")
        partial = lines.pop() if lines else ""
        for raw_line in lines:
            line = raw_line.strip()
            if not line or not any(marker in line for marker in _PLAYER_DECODE_MARKERS):
                continue

            when_mono = time.monotonic()
            getter = getattr(coord, "get_gap_skip_events_snapshot", None)
            events = getter() if callable(getter) else []
            event = _select_gap_event_for_error_time(events, when_mono)
            if event is None:
                state["startup_count"] = int(state.get("startup_count", 0)) + 1
                startup_samples = state.setdefault("startup_samples", [])
                if len(startup_samples) < 3:
                    startup_samples.append(line)
                if int(state["startup_count"]) <= 3:
                    player_started_mono = float(
                        state.get("player_started_mono", when_mono) or when_mono
                    )
                    log.warning(
                        "Player decode error before any correlated gap event (+%.2fs): %s",
                        when_mono - player_started_mono,
                        line,
                    )
                continue

            event_id = int(event.get("event_id", 0) or 0)
            severity = str(event.get("severity", "unknown"))
            bucket = state.setdefault("by_event", {}).setdefault(
                event_id,
                {
                    "count": 0,
                    "severity": severity,
                    "first_after_s": None,
                    "last_after_s": None,
                    "samples": [],
                },
            )
            delta = when_mono - float(event.get("started_mono", when_mono) or when_mono)
            bucket["count"] += 1
            bucket["severity"] = severity
            if bucket["first_after_s"] is None:
                bucket["first_after_s"] = delta
            bucket["last_after_s"] = delta
            if len(bucket["samples"]) < 2:
                bucket["samples"].append(line)
            if int(bucket["count"]) <= 3:
                log.warning(
                    "Player decode error linked to gap event #%d (%s, +%.2fs): %s",
                    event_id,
                    severity,
                    delta,
                    line,
                )

    while not stop_event.is_set():
        _drain_decode_log_once()
        stop_event.wait(0.2)
    _drain_decode_log_once()


def _monitor_player_visual_state(
    log_path: str,
    stop_event: threading.Event,
    state: dict[str, Any],
) -> None:
    log = logging.getLogger(__name__)
    offset = 0
    partial = ""

    def _drain_visual_log_once() -> None:
        nonlocal offset, partial
        if not os.path.isfile(log_path):
            return
        try:
            with open(log_path, encoding="utf-8", errors="replace") as fh:
                fh.seek(offset)
                chunk = fh.read()
                offset = fh.tell()
        except Exception:
            return

        if not chunk:
            return

        partial += chunk.replace("\r", "\n")
        lines = partial.split("\n")
        partial = lines.pop() if lines else ""
        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue
            when_mono = time.monotonic()

            if "[ffplay_buffer" in line and " w:" in line and " h:" in line:
                state["buffer_lines"] = int(state.get("buffer_lines", 0) or 0) + 1
                if float(state.get("first_buffer_mono", 0.0) or 0.0) <= 0.0:
                    state["first_buffer_mono"] = when_mono

            if _FFPLAY_TEXTURE_LINE_RE.search(line):
                state["texture_lines"] = int(state.get("texture_lines", 0) or 0) + 1
                if float(state.get("texture_created_mono", 0.0) or 0.0) <= 0.0:
                    state["texture_created_mono"] = when_mono
                    player_started_mono = float(
                        state.get("player_started_mono", when_mono) or when_mono
                    )
                    run_started_mono = float(state.get("run_started_mono", 0.0) or 0.0)
                    if run_started_mono > 0.0:
                        log.info(
                            "Player visual ready: ffplay created its video texture after %.2fs from player launch / %.2fs from debug.py start",
                            when_mono - player_started_mono,
                            when_mono - run_started_mono,
                        )
                    else:
                        log.info(
                            "Player visual ready: ffplay created its video texture after %.2fs",
                            when_mono - player_started_mono,
                        )

            if "Parsed_showinfo" in line and "pts_time:" in line:
                state["showinfo_lines"] = int(state.get("showinfo_lines", 0) or 0) + 1
                if float(state.get("first_showinfo_mono", 0.0) or 0.0) <= 0.0:
                    state["first_showinfo_mono"] = when_mono

                prev_showinfo_mono = float(state.get("last_showinfo_mono", 0.0) or 0.0)
                if prev_showinfo_mono > 0.0:
                    render_gap_s = max(0.0, when_mono - prev_showinfo_mono)
                    render_gaps = state.setdefault("showinfo_render_gaps_s", [])
                    if len(render_gaps) < 10000:
                        render_gaps.append(render_gap_s)
                    player_started_at = float(
                        state.get("player_started_mono", when_mono) or when_mono
                    )
                    startup_cutoff_s = 5.0
                    target_bucket = (
                        "showinfo_startup_render_gaps_s"
                        if (when_mono - player_started_at) <= startup_cutoff_s
                        else "showinfo_steady_render_gaps_s"
                    )
                    phase_render_gaps = state.setdefault(target_bucket, [])
                    if len(phase_render_gaps) < 5000:
                        phase_render_gaps.append(render_gap_s)
                    state["max_showinfo_render_gap_s"] = max(
                        float(state.get("max_showinfo_render_gap_s", 0.0) or 0.0),
                        render_gap_s,
                    )
                    if render_gap_s > 1.0:
                        state["showinfo_render_freezes_over_1s"] = (
                            int(state.get("showinfo_render_freezes_over_1s", 0) or 0)
                            + 1
                        )
                    last_warned_gap_s = float(
                        state.get("last_warned_showinfo_gap_s", 0.0) or 0.0
                    )
                    if (
                        render_gap_s >= 2.0
                        and abs(render_gap_s - last_warned_gap_s) > 0.20
                    ):
                        player_started_mono = float(
                            state.get("player_started_mono", when_mono) or when_mono
                        )
                        log.warning(
                            "ffplay visible freeze: showinfo gap %.2fs at +%.2fs from player start",
                            render_gap_s,
                            when_mono - player_started_mono,
                        )
                        state["last_warned_showinfo_gap_s"] = render_gap_s
                state["last_showinfo_mono"] = when_mono

                n_match = _SHOWINFO_FRAME_N_RE.search(line)
                if n_match:
                    try:
                        frame_n = int(n_match.group(1))
                    except ValueError:
                        frame_n = -1
                    if frame_n >= 0:
                        prev_frame_n = int(state.get("last_showinfo_frame_n", -1) or -1)
                        if prev_frame_n >= 0 and frame_n > prev_frame_n + 1:
                            state["showinfo_frame_jump_count"] = int(
                                state.get("showinfo_frame_jump_count", 0) or 0
                            ) + (frame_n - prev_frame_n - 1)
                        state["last_showinfo_frame_n"] = frame_n

                pts_match = _SHOWINFO_PTS_TIME_RE.search(line)
                if pts_match:
                    try:
                        pts_time = float(pts_match.group(1))
                    except ValueError:
                        pts_time = -1.0
                    if pts_time >= 0.0:
                        prev_pts_time = float(
                            state.get("last_showinfo_pts_time", -1.0) or -1.0
                        )
                        if prev_pts_time >= 0.0 and pts_time > prev_pts_time:
                            pts_gap_s = pts_time - prev_pts_time
                            pts_gaps = state.setdefault("showinfo_pts_gaps_s", [])
                            if len(pts_gaps) < 10000:
                                pts_gaps.append(pts_gap_s)
                            player_started_at = float(
                                state.get("player_started_mono", when_mono) or when_mono
                            )
                            startup_cutoff_s = 5.0
                            target_bucket = (
                                "showinfo_startup_pts_gaps_s"
                                if (when_mono - player_started_at) <= startup_cutoff_s
                                else "showinfo_steady_pts_gaps_s"
                            )
                            phase_pts_gaps = state.setdefault(target_bucket, [])
                            if len(phase_pts_gaps) < 5000:
                                phase_pts_gaps.append(pts_gap_s)
                            state["max_showinfo_pts_gap_s"] = max(
                                float(state.get("max_showinfo_pts_gap_s", 0.0) or 0.0),
                                pts_gap_s,
                            )
                            if pts_gap_s > 0.30:
                                state["showinfo_pts_gaps_over_300ms"] = (
                                    int(
                                        state.get("showinfo_pts_gaps_over_300ms", 0)
                                        or 0
                                    )
                                    + 1
                                )
                        state["last_showinfo_pts_time"] = pts_time
                        timeline = state.setdefault("showinfo_timeline", [])
                        if len(timeline) < 20000:
                            timeline.append((when_mono, pts_time))

            if "A-V:" in line and "aq=" in line and "vq=" in line:
                state["stats_count"] = int(state.get("stats_count", 0) or 0) + 1
                prev_stats_mono = float(state.get("last_stats_mono", 0.0) or 0.0)
                if prev_stats_mono > 0.0:
                    stats_gap_s = max(0.0, when_mono - prev_stats_mono)
                    state["max_stats_gap_s"] = max(
                        float(state.get("max_stats_gap_s", 0.0) or 0.0),
                        stats_gap_s,
                    )
                    if stats_gap_s > 1.0:
                        state["stats_gaps_over_1s"] = (
                            int(state.get("stats_gaps_over_1s", 0) or 0) + 1
                        )
                state["last_stats_mono"] = when_mono
                if float(state.get("first_stats_mono", 0.0) or 0.0) <= 0.0:
                    state["first_stats_mono"] = when_mono
                av_match = _FFPLAY_AV_DRIFT_RE.search(line)
                if av_match:
                    try:
                        av_drift = float(av_match.group(1))
                    except ValueError:
                        av_drift = 0.0
                    av_samples = state.setdefault("av_sync_samples", [])
                    if len(av_samples) < 5000:
                        av_samples.append(av_drift)

    while not stop_event.is_set():
        _drain_visual_log_once()
        now_mono = time.monotonic()
        player_started_mono = float(state.get("player_started_mono", 0.0) or 0.0)
        if player_started_mono > 0.0:
            texture_created_mono = float(state.get("texture_created_mono", 0.0) or 0.0)
            if texture_created_mono <= 0.0 and not bool(
                state.get("late_texture_warned", False)
            ):
                if now_mono - player_started_mono > 8.0:
                    log.warning(
                        "Player visual readiness lagging after %.2fs: ffplay still has no video texture",
                        now_mono - player_started_mono,
                    )
                    state["late_texture_warned"] = True
            first_stats_mono = float(state.get("first_stats_mono", 0.0) or 0.0)
            if (
                texture_created_mono > 0.0
                and first_stats_mono <= 0.0
                and not bool(state.get("late_stats_warned", False))
            ):
                if now_mono - texture_created_mono > 6.0:
                    log.warning(
                        "Player visual progress lagging after texture creation: no ffplay stats activity for %.2fs",
                        now_mono - texture_created_mono,
                    )
                    state["late_stats_warned"] = True
        stop_event.wait(0.2)
        # Track visible inactivity gaps even when ffplay output arrives in bursts.
        texture_created_mono = float(state.get("texture_created_mono", 0.0) or 0.0)
        if texture_created_mono > 0.0:
            last_showinfo_mono = float(state.get("last_showinfo_mono", 0.0) or 0.0)
            if last_showinfo_mono > 0.0:
                silent_showinfo_gap_s = max(0.0, now_mono - last_showinfo_mono)
                state["max_silent_showinfo_gap_s"] = max(
                    float(state.get("max_silent_showinfo_gap_s", 0.0) or 0.0),
                    silent_showinfo_gap_s,
                )
            last_stats_mono = float(state.get("last_stats_mono", 0.0) or 0.0)
            if last_stats_mono > 0.0:
                silent_stats_gap_s = max(0.0, now_mono - last_stats_mono)
                state["max_silent_stats_gap_s"] = max(
                    float(state.get("max_silent_stats_gap_s", 0.0) or 0.0),
                    silent_stats_gap_s,
                )
    _drain_visual_log_once()


def _mux_av_delta_s(mux_metrics: dict[str, Any] | None) -> float | None:
    if not mux_metrics:
        return None
    try:
        audio_pts = int(mux_metrics.get("mux_audio_next_pts", 0) or 0)
        video_pts = int(mux_metrics.get("mux_video_last_pts", 0) or 0)
    except (TypeError, ValueError):
        return None
    if audio_pts <= 0 or video_pts <= 0:
        return None
    return (audio_pts - video_pts) / 90000.0


def _summarize_player_visual_state(
    state: dict[str, Any],
    mux_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    run_started_mono = float(state.get("run_started_mono", 0.0) or 0.0)
    live_ready_mono = float(state.get("live_ready_mono", 0.0) or 0.0)
    launch_gate_started_mono = float(state.get("launch_gate_started_mono", 0.0) or 0.0)
    player_started_mono = float(state.get("player_started_mono", 0.0) or 0.0)
    texture_created_mono = float(state.get("texture_created_mono", 0.0) or 0.0)
    first_buffer_mono = float(state.get("first_buffer_mono", 0.0) or 0.0)
    first_stats_mono = float(state.get("first_stats_mono", 0.0) or 0.0)
    stats_count = int(state.get("stats_count", 0) or 0)
    showinfo_lines = int(state.get("showinfo_lines", 0) or 0)

    result["player_texture_created"] = bool(texture_created_mono > 0.0)
    result["player_texture_lines"] = int(state.get("texture_lines", 0) or 0)
    result["player_buffer_lines"] = int(state.get("buffer_lines", 0) or 0)
    result["player_visual_stats_count"] = stats_count
    result["player_showinfo_lines"] = showinfo_lines
    if run_started_mono > 0.0 and live_ready_mono > 0.0:
        result["command_to_live_ready_latency_s"] = round(
            live_ready_mono - run_started_mono,
            3,
        )
    if run_started_mono > 0.0 and player_started_mono > 0.0:
        result["command_to_player_launch_latency_s"] = round(
            player_started_mono - run_started_mono,
            3,
        )
    if live_ready_mono > 0.0 and player_started_mono > 0.0:
        result["live_ready_to_player_launch_latency_s"] = round(
            player_started_mono - live_ready_mono,
            3,
        )
    if launch_gate_started_mono > 0.0 and player_started_mono > 0.0:
        result["player_prelaunch_gate_latency_s"] = round(
            player_started_mono - launch_gate_started_mono,
            3,
        )
    if state.get("player_launch_gate_reason"):
        result["player_launch_gate_reason"] = str(
            state.get("player_launch_gate_reason")
        )
    if float(state.get("player_launch_gate_budget_s", 0.0) or 0.0) > 0.0:
        result["player_launch_gate_budget_s"] = round(
            float(state.get("player_launch_gate_budget_s", 0.0) or 0.0),
            3,
        )
    if int(state.get("player_launch_gate_frames_since_start", 0) or 0) > 0:
        result["player_launch_gate_frames_since_start"] = int(
            state.get("player_launch_gate_frames_since_start", 0) or 0
        )
    if player_started_mono > 0.0 and texture_created_mono > 0.0:
        result["player_texture_open_latency_s"] = round(
            texture_created_mono - player_started_mono, 3
        )
    if run_started_mono > 0.0 and texture_created_mono > 0.0:
        result["command_to_player_texture_latency_s"] = round(
            texture_created_mono - run_started_mono,
            3,
        )
    if player_started_mono > 0.0 and first_buffer_mono > 0.0:
        result["player_first_buffer_latency_s"] = round(
            first_buffer_mono - player_started_mono, 3
        )
    if run_started_mono > 0.0 and first_buffer_mono > 0.0:
        result["command_to_player_first_buffer_latency_s"] = round(
            first_buffer_mono - run_started_mono,
            3,
        )
    if player_started_mono > 0.0 and first_stats_mono > 0.0:
        result["player_first_stats_latency_s"] = round(
            first_stats_mono - player_started_mono, 3
        )
    if run_started_mono > 0.0 and first_stats_mono > 0.0:
        result["command_to_player_first_stats_latency_s"] = round(
            first_stats_mono - run_started_mono,
            3,
        )
    first_showinfo_mono = float(state.get("first_showinfo_mono", 0.0) or 0.0)
    last_showinfo_mono = float(state.get("last_showinfo_mono", 0.0) or 0.0)
    if player_started_mono > 0.0 and first_showinfo_mono > 0.0:
        result["player_first_showinfo_latency_s"] = round(
            first_showinfo_mono - player_started_mono, 3
        )
    if run_started_mono > 0.0 and first_showinfo_mono > 0.0:
        result["command_to_player_first_showinfo_latency_s"] = round(
            first_showinfo_mono - run_started_mono,
            3,
        )
    if (
        showinfo_lines > 1
        and first_showinfo_mono > 0.0
        and last_showinfo_mono > first_showinfo_mono
    ):
        showinfo_span = max(0.001, last_showinfo_mono - first_showinfo_mono)
        result["player_showinfo_estimated_fps"] = round(
            (showinfo_lines - 1) / showinfo_span,
            2,
        )
    result["player_showinfo_frame_jump_count"] = int(
        state.get("showinfo_frame_jump_count", 0) or 0
    )
    result["player_showinfo_render_freezes_over_1s"] = int(
        state.get("showinfo_render_freezes_over_1s", 0) or 0
    )
    result["player_showinfo_pts_gaps_over_300ms"] = int(
        state.get("showinfo_pts_gaps_over_300ms", 0) or 0
    )
    result["player_showinfo_max_render_gap_s"] = round(
        float(state.get("max_showinfo_render_gap_s", 0.0) or 0.0),
        3,
    )
    result["player_showinfo_max_pts_gap_s"] = round(
        float(state.get("max_showinfo_pts_gap_s", 0.0) or 0.0),
        3,
    )
    result["player_stats_gaps_over_1s"] = int(state.get("stats_gaps_over_1s", 0) or 0)
    result["player_stats_max_gap_s"] = round(
        float(state.get("max_stats_gap_s", 0.0) or 0.0),
        3,
    )
    result["player_silent_showinfo_max_gap_s"] = round(
        float(state.get("max_silent_showinfo_gap_s", 0.0) or 0.0),
        3,
    )
    result["player_silent_stats_max_gap_s"] = round(
        float(state.get("max_silent_stats_gap_s", 0.0) or 0.0),
        3,
    )
    steady_render_gaps = [
        float(v)
        for v in (state.get("showinfo_steady_render_gaps_s", []) or [])
        if isinstance(v, (int, float)) and float(v) > 0.0
    ]
    steady_pts_gaps = [
        float(v)
        for v in (state.get("showinfo_steady_pts_gaps_s", []) or [])
        if isinstance(v, (int, float)) and float(v) > 0.0
    ]

    render_median = _percentile(steady_render_gaps, 0.5)
    render_p95 = _percentile(steady_render_gaps, 0.95)
    pts_median = _percentile(steady_pts_gaps, 0.5)
    pts_p95 = _percentile(steady_pts_gaps, 0.95)
    result["player_showinfo_steady_render_gap_median_s"] = round(render_median, 4)
    result["player_showinfo_steady_render_gap_p95_s"] = round(render_p95, 4)
    result["player_showinfo_steady_pts_gap_median_s"] = round(pts_median, 4)
    result["player_showinfo_steady_pts_gap_p95_s"] = round(pts_p95, 4)

    startup_render_gaps = [
        float(v)
        for v in (state.get("showinfo_startup_render_gaps_s", []) or [])
        if isinstance(v, (int, float)) and float(v) > 0.0
    ]
    startup_pts_gaps = [
        float(v)
        for v in (state.get("showinfo_startup_pts_gaps_s", []) or [])
        if isinstance(v, (int, float)) and float(v) > 0.0
    ]
    short_render_stalls = sum(1 for g in startup_render_gaps if g >= 0.35)
    short_pts_stalls = sum(1 for g in startup_pts_gaps if g >= 0.22)
    result["player_startup_short_render_stalls_over_350ms"] = int(short_render_stalls)
    result["player_startup_short_pts_stalls_over_220ms"] = int(short_pts_stalls)

    steady_observed_duration_s = max(
        0.0,
        sum(steady_pts_gaps) if steady_pts_gaps else sum(steady_render_gaps),
    )
    startup_observed_duration_s = max(
        0.0,
        sum(startup_pts_gaps) if startup_pts_gaps else sum(startup_render_gaps),
    )
    visible_render_stall_threshold = (
        max(0.28, min(0.95, render_median * 4.5)) if render_median > 0.0 else 0.45
    )
    visible_pts_stall_threshold = (
        max(0.18, min(0.95, pts_median * 4.0)) if pts_median > 0.0 else 0.30
    )
    startup_visible_render_threshold = max(0.24, visible_render_stall_threshold * 0.85)
    startup_visible_pts_threshold = max(0.14, visible_pts_stall_threshold * 0.85)
    steady_visible_render_stalls = [
        g for g in steady_render_gaps if g >= visible_render_stall_threshold
    ]
    steady_visible_pts_stalls = [
        g for g in steady_pts_gaps if g >= visible_pts_stall_threshold
    ]
    startup_visible_render_stalls = [
        g for g in startup_render_gaps if g >= startup_visible_render_threshold
    ]
    startup_visible_pts_stalls = [
        g for g in startup_pts_gaps if g >= startup_visible_pts_threshold
    ]
    steady_visible_render_stall_excess_s = sum(
        max(0.0, g - visible_render_stall_threshold)
        for g in steady_visible_render_stalls
    )
    steady_visible_pts_stall_excess_s = sum(
        max(0.0, g - visible_pts_stall_threshold) for g in steady_visible_pts_stalls
    )
    startup_visible_render_stall_excess_s = sum(
        max(0.0, g - startup_visible_render_threshold)
        for g in startup_visible_render_stalls
    )
    startup_visible_pts_stall_excess_s = sum(
        max(0.0, g - startup_visible_pts_threshold) for g in startup_visible_pts_stalls
    )
    visible_stall_budget_count = max(
        1,
        min(4, int(round(max(steady_observed_duration_s, 1.0) / 45.0))),
    )
    visible_stall_budget_excess_s = max(
        0.35,
        min(2.5, max(steady_observed_duration_s, 1.0) * 0.015),
    )
    startup_visible_stall_budget_count = 1 if startup_observed_duration_s >= 2.0 else 0
    startup_visible_stall_budget_excess_s = max(
        0.20,
        min(1.2, max(startup_observed_duration_s, 1.0) * 0.08),
    )
    result["player_visible_render_stall_threshold_s"] = round(
        visible_render_stall_threshold, 3
    )
    result["player_visible_pts_stall_threshold_s"] = round(
        visible_pts_stall_threshold, 3
    )
    result["player_visible_render_stall_count"] = int(len(steady_visible_render_stalls))
    result["player_visible_pts_stall_count"] = int(len(steady_visible_pts_stalls))
    result["player_visible_render_stall_excess_s"] = round(
        steady_visible_render_stall_excess_s,
        3,
    )
    result["player_visible_pts_stall_excess_s"] = round(
        steady_visible_pts_stall_excess_s,
        3,
    )
    result["player_visible_stall_budget_count"] = int(visible_stall_budget_count)
    result["player_visible_stall_budget_excess_s"] = round(
        visible_stall_budget_excess_s,
        3,
    )
    result["player_startup_visible_render_stall_count"] = int(
        len(startup_visible_render_stalls)
    )
    result["player_startup_visible_pts_stall_count"] = int(
        len(startup_visible_pts_stalls)
    )
    result["player_startup_visible_render_stall_excess_s"] = round(
        startup_visible_render_stall_excess_s,
        3,
    )
    result["player_startup_visible_pts_stall_excess_s"] = round(
        startup_visible_pts_stall_excess_s,
        3,
    )

    startup_ratio = 1.0
    startup_speed_spikes = 0
    startup_fast_speed_spikes = 0
    startup_slow_speed_spikes = 0
    timeline_raw = state.get("showinfo_timeline", []) or []
    timeline: list[tuple[float, float]] = []
    for item in timeline_raw:
        if (
            isinstance(item, (tuple, list))
            and len(item) == 2
            and isinstance(item[0], (int, float))
            and isinstance(item[1], (int, float))
        ):
            timeline.append((float(item[0]), float(item[1])))
    if player_started_mono > 0.0 and timeline:
        startup_window = [
            (mono, pts)
            for mono, pts in timeline
            if 0.0 <= (mono - player_started_mono) <= 8.0
        ]
        if len(startup_window) >= 4:
            wall_span = max(0.001, startup_window[-1][0] - startup_window[0][0])
            pts_span = max(0.0, startup_window[-1][1] - startup_window[0][1])
            startup_ratio = pts_span / wall_span
            for idx in range(1, len(startup_window)):
                wall_dt = startup_window[idx][0] - startup_window[idx - 1][0]
                pts_dt = startup_window[idx][1] - startup_window[idx - 1][1]
                if wall_dt < 0.02 or pts_dt < 0.02:
                    continue
                local_ratio = pts_dt / wall_dt
                if local_ratio > 1.35:
                    startup_fast_speed_spikes += 1
                elif local_ratio < 0.70:
                    startup_slow_speed_spikes += 1
                if local_ratio > 1.35 or local_ratio < 0.70:
                    startup_speed_spikes += 1
    result["player_startup_playback_speed_ratio"] = round(startup_ratio, 3)
    result["player_startup_speed_spike_count"] = int(startup_speed_spikes)
    result["player_startup_fast_speed_spike_count"] = int(startup_fast_speed_spikes)
    result["player_startup_slow_speed_spike_count"] = int(startup_slow_speed_spikes)

    av_samples_raw = state.get("av_sync_samples", []) or []
    av_samples = [float(v) for v in av_samples_raw if isinstance(v, (int, float))]
    av_abs_max = max((abs(v) for v in av_samples), default=0.0)
    av_large_count = sum(1 for v in av_samples if abs(v) >= 0.20)
    av_baseline = _percentile(av_samples, 0.5)
    av_residuals = [v - av_baseline for v in av_samples]
    av_jitter_abs_max = max((abs(v) for v in av_residuals), default=0.0)
    av_jitter_large_count = sum(1 for v in av_residuals if abs(v) >= 0.20)
    result["player_av_drift_abs_max_s"] = round(av_abs_max, 3)
    result["player_av_drift_over_200ms_count"] = int(av_large_count)
    result["player_av_drift_baseline_s"] = round(av_baseline, 3)
    result["player_av_drift_jitter_abs_max_s"] = round(av_jitter_abs_max, 3)
    result["player_av_drift_jitter_over_200ms_count"] = int(av_jitter_large_count)

    dynamic_render_threshold = max(0.25, min(1.2, render_median * 4.2))
    dynamic_pts_threshold = max(0.12, min(0.8, pts_median * 3.8))
    if render_median <= 0.0:
        dynamic_render_threshold = 0.55
    if pts_median <= 0.0:
        dynamic_pts_threshold = 0.30
    render_outliers = sum(1 for g in steady_render_gaps if g > dynamic_render_threshold)
    pts_outliers = sum(1 for g in steady_pts_gaps if g > dynamic_pts_threshold)

    steady_frames = max(
        1,
        len(steady_render_gaps),
        len(steady_pts_gaps),
    )
    frame_jumps = int(state.get("showinfo_frame_jump_count", 0) or 0)
    artifact_score = (
        (render_outliers * 1.0) + (pts_outliers * 0.8) + (frame_jumps * 0.12)
    ) / max(1.0, steady_frames / 120.0)

    result["player_dynamic_render_gap_threshold_s"] = round(dynamic_render_threshold, 3)
    result["player_dynamic_pts_gap_threshold_s"] = round(dynamic_pts_threshold, 3)
    result["player_render_gap_outlier_count"] = int(render_outliers)
    result["player_pts_gap_outlier_count"] = int(pts_outliers)
    result["player_motion_artifact_score"] = round(artifact_score, 2)

    visual_issues: list[str] = []
    if not result["player_texture_created"]:
        visual_issues.append("no-video-texture")
    elif float(result.get("player_texture_open_latency_s", 0.0) or 0.0) > 8.0:
        visual_issues.append(
            f"late-video-texture:{float(result['player_texture_open_latency_s']):.2f}s"
        )
    if stats_count <= 0 and showinfo_lines <= 0:
        visual_issues.append("no-ffplay-stats")
    elif float(result.get("player_first_stats_latency_s", 0.0) or 0.0) > 10.0:
        visual_issues.append(
            f"late-ffplay-stats:{float(result['player_first_stats_latency_s']):.2f}s"
        )
    if showinfo_lines <= 0:
        visual_issues.append("no-showinfo-frames")
    if float(result.get("player_showinfo_max_render_gap_s", 0.0) or 0.0) > 1.0:
        visual_issues.append(
            f"render-freeze:{float(result['player_showinfo_max_render_gap_s']):.2f}s"
        )
    if float(result.get("player_stats_max_gap_s", 0.0) or 0.0) > 1.0:
        visual_issues.append(
            f"stats-freeze:{float(result['player_stats_max_gap_s']):.2f}s"
        )
    if float(result.get("player_silent_showinfo_max_gap_s", 0.0) or 0.0) > 1.2:
        visual_issues.append(
            f"showinfo-silence:{float(result['player_silent_showinfo_max_gap_s']):.2f}s"
        )
    render_outlier_budget = max(3, int(steady_frames * 0.018))
    pts_outlier_budget = max(3, int(steady_frames * 0.022))
    jump_budget = max(10, int(steady_frames * 0.06))
    if render_outliers > render_outlier_budget:
        visual_issues.append(
            f"render-cadence-instability:{render_outliers}>{render_outlier_budget}"
        )
    if pts_outliers > pts_outlier_budget:
        visual_issues.append(
            f"pts-cadence-instability:{pts_outliers}>{pts_outlier_budget}"
        )
    if frame_jumps > jump_budget:
        visual_issues.append(f"frame-jumps:{frame_jumps}>{jump_budget}")
    if artifact_score > 4.0:
        visual_issues.append(f"motion-artifact-score:{artifact_score:.2f}")
    if startup_ratio > 1.18:
        visual_issues.append(f"startup-fast-play:{startup_ratio:.2f}x")
    startup_oscillation_pairs = min(
        startup_fast_speed_spikes, startup_slow_speed_spikes
    )
    if startup_oscillation_pairs >= 2:
        visual_issues.append(
            "startup-speed-oscillation:"
            f"pairs={startup_oscillation_pairs},fast={startup_fast_speed_spikes},slow={startup_slow_speed_spikes}"
        )
    if short_render_stalls >= 2:
        visual_issues.append(f"startup-render-stalls:{short_render_stalls}")
    if short_pts_stalls >= 2:
        visual_issues.append(f"startup-pts-stalls:{short_pts_stalls}")
    if len(startup_visible_render_stalls) > startup_visible_stall_budget_count:
        visual_issues.append(
            "startup-visible-render-stalls:"
            f"{len(startup_visible_render_stalls)}>{startup_visible_stall_budget_count}"
        )
    if len(startup_visible_pts_stalls) > startup_visible_stall_budget_count:
        visual_issues.append(
            "startup-visible-pts-stalls:"
            f"{len(startup_visible_pts_stalls)}>{startup_visible_stall_budget_count}"
        )
    if startup_visible_render_stall_excess_s > startup_visible_stall_budget_excess_s:
        visual_issues.append(
            "startup-render-stall-excess:"
            f"{startup_visible_render_stall_excess_s:.2f}s>{startup_visible_stall_budget_excess_s:.2f}s"
        )
    if startup_visible_pts_stall_excess_s > startup_visible_stall_budget_excess_s:
        visual_issues.append(
            "startup-pts-stall-excess:"
            f"{startup_visible_pts_stall_excess_s:.2f}s>{startup_visible_stall_budget_excess_s:.2f}s"
        )
    if len(steady_visible_render_stalls) > visible_stall_budget_count:
        visual_issues.append(
            "visible-render-stalls:"
            f"{len(steady_visible_render_stalls)}>{visible_stall_budget_count}"
        )
    if len(steady_visible_pts_stalls) > visible_stall_budget_count:
        visual_issues.append(
            "visible-pts-stalls:"
            f"{len(steady_visible_pts_stalls)}>{visible_stall_budget_count}"
        )
    if steady_visible_render_stall_excess_s > visible_stall_budget_excess_s:
        visual_issues.append(
            "visible-render-stall-excess:"
            f"{steady_visible_render_stall_excess_s:.2f}s>{visible_stall_budget_excess_s:.2f}s"
        )
    if steady_visible_pts_stall_excess_s > visible_stall_budget_excess_s:
        visual_issues.append(
            "visible-pts-stall-excess:"
            f"{steady_visible_pts_stall_excess_s:.2f}s>{visible_stall_budget_excess_s:.2f}s"
        )
    mux_av_delta = _mux_av_delta_s(mux_metrics)
    if mux_av_delta is not None:
        result["mux_av_delta_s"] = round(mux_av_delta, 3)
    mux_av_synced = mux_av_delta is not None and abs(mux_av_delta) <= 0.35
    av_jitter_issue = av_jitter_abs_max >= 0.45 or (
        av_jitter_abs_max >= 0.35 and av_jitter_large_count >= 25
    )
    if av_jitter_issue and not mux_av_synced:
        visual_issues.append(
            "audio-sync-instability:"
            f"jitter={av_jitter_abs_max:.3f}s,count={av_jitter_large_count},"
            f"baseline={av_baseline:.3f}s"
        )
    result["player_visual_has_issues"] = bool(visual_issues)
    if visual_issues:
        result["player_visual_issues"] = visual_issues
    return result
