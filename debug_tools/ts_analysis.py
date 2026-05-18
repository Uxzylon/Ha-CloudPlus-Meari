from __future__ import annotations

import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from .visual import _PLAYER_DECODE_MARKERS, _SHOWINFO_PTS_TIME_RE


def _collect_ts_decode_error_timeline(
    ts_path: str, log: logging.Logger
) -> dict[str, Any]:
    """Approximate recorded-TS decode-error timestamps using ffmpeg showinfo."""
    result: dict[str, Any] = {}
    proc = subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "info",
            "-i",
            ts_path,
            "-map",
            "0:v:0",
            "-an",
            "-vf",
            "showinfo",
            "-f",
            "null",
            "-",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=45,
        check=False,
    )
    raw = (proc.stdout or b"").decode(errors="replace")
    if not raw:
        return result

    last_pts_time = 0.0
    error_times: list[float] = []
    error_samples: list[str] = []
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        pts_match = _SHOWINFO_PTS_TIME_RE.search(line)
        if pts_match:
            try:
                last_pts_time = float(pts_match.group(1))
            except ValueError:
                pass
        if any(marker in line for marker in _PLAYER_DECODE_MARKERS):
            error_time = max(0.0, last_pts_time)
            error_times.append(error_time)
            if len(error_samples) < 5:
                error_samples.append(f"{error_time:.3f}s {line}")

    if error_times:
        result["first_decode_error_s"] = round(error_times[0], 3)
        result["decode_error_timestamps_s"] = [
            round(ts_s, 3) for ts_s in error_times[:12]
        ]
        result["decode_error_timeline_sample"] = error_samples
    return result


def _analyze_recorded_ts(ts_path: str, log: logging.Logger) -> dict[str, Any]:
    """Analyse a recorded MPEG-TS file for video/audio quality metrics.

    Uses ffprobe to extract per-frame timing, then computes:
    - total decoded frames (video + audio)
    - average video FPS
    - frame-to-frame gap histogram (detects skips/freezes)
    - duplicate DTS count (indicates frozen output)
    - decode errors counted by ffmpeg second-pass
    """
    result: dict[str, Any] = {}
    if not os.path.isfile(ts_path) or os.path.getsize(ts_path) < 1000:
        log.warning("TS recording missing or too small: %s", ts_path)
        return result

    # --- ffprobe: extract per-frame timestamps for video stream ---
    # Use best_effort_timestamp_time which is always populated (pkt_pts_time
    # can be N/A for copy-mode TS recordings).
    # Output csv: key_frame,best_effort_timestamp_time
    try:
        raw = subprocess.check_output(
            [
                "ffprobe",
                "-hide_banner",
                "-loglevel",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "frame=key_frame,best_effort_timestamp_time",
                "-of",
                "csv=p=0",
                ts_path,
            ],
            timeout=30,
            stderr=subprocess.DEVNULL,
        ).decode(errors="replace")
    except Exception as e:
        log.warning("ffprobe frame extraction failed: %s", e)
        return result

    pts_list: list[float] = []
    n_keyframes = 0
    for line in raw.strip().splitlines():
        parts = line.split(",")
        if len(parts) < 2:
            continue
        # csv order: best_effort_timestamp_time, key_frame
        # Find the float value (timestamp) and the 0/1 (key_frame)
        ts_val = None
        kf_val = None
        for p in parts:
            p = p.strip()
            if p in ("0", "1") and kf_val is None:
                kf_val = p
            else:
                try:
                    ts_val = float(p)
                except (ValueError, TypeError):
                    pass
        if ts_val is not None:
            pts_list.append(ts_val)
            if kf_val == "1":
                n_keyframes += 1

    result["video_frames_decoded"] = len(pts_list)
    result["video_keyframes"] = n_keyframes

    if len(pts_list) >= 2:
        pts_list.sort()
        span = pts_list[-1] - pts_list[0]
        result["video_span_s"] = round(span, 3)
        result["video_avg_fps"] = (
            round((len(pts_list) - 1) / span, 2) if span > 0 else 0
        )

        # Frame gaps
        gaps = [pts_list[i + 1] - pts_list[i] for i in range(len(pts_list) - 1)]
        if gaps:
            median_gap = sorted(gaps)[len(gaps) // 2]
            result["video_median_frame_gap_ms"] = round(median_gap * 1000, 1)
            result["video_max_frame_gap_ms"] = round(max(gaps) * 1000, 1)
            # Count gaps that exceed 3x median (likely skips/stalls)
            skip_threshold = max(median_gap * 3, 0.15)
            skips = [g for g in gaps if g > skip_threshold]
            result["video_skip_count"] = len(skips)
            if skips:
                result["video_skip_durations_s"] = [
                    round(g, 3) for g in sorted(skips, reverse=True)[:10]
                ]
            # Duplicate PTS (frozen frames)
            n_dup = sum(1 for g in gaps if g < 0.001)
            result["video_duplicate_pts"] = n_dup

    # --- ffprobe: extract audio frame timestamps ---
    try:
        raw_audio = subprocess.check_output(
            [
                "ffprobe",
                "-hide_banner",
                "-loglevel",
                "error",
                "-select_streams",
                "a:0",
                "-show_entries",
                "frame=best_effort_timestamp_time",
                "-of",
                "csv=p=0",
                ts_path,
            ],
            timeout=15,
            stderr=subprocess.DEVNULL,
        ).decode(errors="replace")
    except Exception as e:
        log.warning("ffprobe audio extraction failed: %s", e)
        raw_audio = ""

    audio_pts: list[float] = []
    for line in raw_audio.strip().splitlines():
        try:
            audio_pts.append(float(line.strip()))
        except (ValueError, TypeError):
            continue

    result["audio_frames_decoded"] = len(audio_pts)
    if len(audio_pts) >= 2:
        audio_pts.sort()
        a_span = audio_pts[-1] - audio_pts[0]
        result["audio_span_s"] = round(a_span, 3)
        a_gaps = [audio_pts[i + 1] - audio_pts[i] for i in range(len(audio_pts) - 1)]
        if a_gaps:
            a_median = sorted(a_gaps)[len(a_gaps) // 2]
            result["audio_median_gap_ms"] = round(a_median * 1000, 1)
            result["audio_max_gap_ms"] = round(max(a_gaps) * 1000, 1)
            a_skip_threshold = max(a_median * 3, 0.15)
            a_skips = [g for g in a_gaps if g > a_skip_threshold]
            result["audio_gap_count"] = len(a_skips)
            if a_skips:
                result["audio_gap_durations_s"] = [
                    round(g, 3) for g in sorted(a_skips, reverse=True)[:10]
                ]

    # --- ffmpeg decode pass: count errors ---
    try:
        err_raw = subprocess.check_output(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                ts_path,
                "-f",
                "null",
                "-",
            ],
            timeout=30,
            stderr=subprocess.STDOUT,
        ).decode(errors="replace")
        error_lines = [
            l
            for l in err_raw.splitlines()
            if l.strip() and "non monotonically" not in l
        ]
        result["decode_error_lines"] = len(error_lines)
        if error_lines:
            result["decode_errors_sample"] = error_lines[:5]
    except subprocess.CalledProcessError as e:
        err_out = (e.output or b"").decode(errors="replace")
        error_lines = [l for l in err_out.splitlines() if l.strip()]
        result["decode_error_lines"] = len(error_lines)
        if error_lines:
            result["decode_errors_sample"] = error_lines[:5]
    except Exception as e:
        log.warning("TS decode error check failed: %s", e)

    if int(result.get("decode_error_lines", 0) or 0) > 0:
        try:
            result.update(_collect_ts_decode_error_timeline(ts_path, log))
        except Exception as e:
            log.warning("TS decode timeline correlation failed: %s", e)

    return result


def _analyze_media_client_log(
    log_path: str,
    log: logging.Logger,
    *,
    prefix: str,
    benign_patterns: tuple[str, ...] = (),
    treat_significant_as_issue: bool = False,
) -> dict[str, Any]:
    """Analyse ffmpeg/ffplay client logs for decode and continuity problems."""
    result: dict[str, Any] = {}
    if not os.path.isfile(log_path) or os.path.getsize(log_path) < 1:
        log.warning("%s log missing or empty: %s", prefix, log_path)
        return result

    try:
        raw = Path(log_path).read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        log.warning("%s log read failed: %s", prefix, e)
        return result

    lines = [
        line.strip()
        for chunk in raw.replace("\r", "\n").splitlines()
        for line in [chunk.strip()]
        if line
    ]
    if not lines:
        return result

    shared_benign_patterns = (
        "max_analyze_duration",
        "Could not find codec parameters for stream 0",
        "Successfully connected to",
        "Created 2304x1296 texture",
        "auto-inserting filter",
        "tb:1/16000",
        "w:2304 h:1296 pixfmt:",
    ) + tuple(benign_patterns)
    decode_markers = (
        "Could not find ref with POC",
        "Error constructing the frame RPS",
        "Skipping invalid undecodable NALU",
        "Invalid NAL unit",
        "decode_slice_header error",
        "Error while decoding stream",
        "concealing",
        "corrupt",
    )
    continuity_markers = (
        "Continuity check failed",
        "Packet corrupt",
        "non monotonically increasing dts",
        "invalid dropping",
        "Past duration",
        "timestamp discontinuity",
    )

    benign_lines = [
        line for line in lines if any(p in line for p in shared_benign_patterns)
    ]
    decode_lines = [
        line
        for line in lines
        if any(marker in line for marker in decode_markers)
        and not any(p in line for p in shared_benign_patterns)
    ]
    continuity_lines = [
        line
        for line in lines
        if any(marker in line for marker in continuity_markers)
        and not any(p in line for p in shared_benign_patterns)
    ]
    timestamp_discontinuity_lines = [
        line for line in continuity_lines if "timestamp discontinuity" in line
    ]
    significant_lines = [
        line
        for line in lines
        if any(
            token in line.lower()
            for token in ("error", "invalid", "corrupt", "continuity", "dropping")
        )
        and not any(p in line for p in shared_benign_patterns)
    ]

    poc_values: list[int] = []
    for line in decode_lines:
        match = re.search(r"POC\s+(\d+)", line)
        if match:
            try:
                poc_values.append(int(match.group(1)))
            except ValueError:
                pass

    result[f"{prefix}_log_lines"] = len(lines)
    result[f"{prefix}_benign_lines"] = len(benign_lines)
    result[f"{prefix}_decode_error_lines"] = len(decode_lines)
    result[f"{prefix}_continuity_error_lines"] = len(continuity_lines)
    result[f"{prefix}_timestamp_discontinuity_lines"] = len(
        timestamp_discontinuity_lines
    )
    result[f"{prefix}_significant_error_lines"] = len(significant_lines)
    result[f"{prefix}_has_issues"] = bool(
        decode_lines
        or continuity_lines
        or (treat_significant_as_issue and significant_lines)
    )
    if poc_values:
        result[f"{prefix}_error_poc_first"] = min(poc_values)
        result[f"{prefix}_error_poc_last"] = max(poc_values)
    if decode_lines:
        result[f"{prefix}_decode_errors_sample"] = decode_lines[:8]
    if continuity_lines:
        result[f"{prefix}_continuity_errors_sample"] = continuity_lines[:8]
    elif significant_lines and not decode_lines:
        result[f"{prefix}_significant_errors_sample"] = significant_lines[:8]
    return result


def _analyze_player_log(log_path: str, log: logging.Logger) -> dict[str, Any]:
    """Analyse ffplay output for player-visible decode and continuity issues."""
    return _analyze_media_client_log(log_path, log, prefix="player")


def _analyze_recorder_log(log_path: str, log: logging.Logger) -> dict[str, Any]:
    """Analyse recorder ffmpeg stderr for transport/decode warnings."""
    return _analyze_media_client_log(
        log_path,
        log,
        prefix="recorder",
        benign_patterns=("frame=", "size=", "video:", "audio:"),
        treat_significant_as_issue=True,
    )
