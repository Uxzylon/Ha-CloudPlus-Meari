from __future__ import annotations

import collections
import logging
import math
import os
import statistics
import struct
import subprocess
import threading
import time
from typing import Any

from .stats import _percentile


def _mulaw_to_pcm16(byte: int) -> int:
    uval = (~byte) & 0xFF
    sign = uval & 0x80
    exponent = (uval >> 4) & 0x07
    mantissa = uval & 0x0F
    sample = (((mantissa << 3) + 0x84) << exponent) - 0x84
    return -sample if sign else sample


def _pcm_quality_metrics(
    samples: list[int],
    sample_rate: int,
    prefix: str,
) -> dict[str, Any]:
    if not samples:
        return {}
    duration = len(samples) / max(1, sample_rate)
    rms = math.sqrt(sum(sample * sample for sample in samples) / len(samples))
    peak = max(abs(sample) for sample in samples)
    deltas = [abs(samples[idx] - samples[idx - 1]) for idx in range(1, len(samples))]
    delta_rms = (
        math.sqrt(sum(delta * delta for delta in deltas) / len(deltas))
        if deltas
        else 0.0
    )
    zero_crossings = sum(
        1
        for idx in range(1, len(samples))
        if (samples[idx] < 0) != (samples[idx - 1] < 0)
    )
    hard_jumps = sum(1 for delta in deltas if delta >= 18000)
    clipped = sum(1 for sample in samples if abs(sample) >= 32760)
    return {
        f"{prefix}_sample_rate": sample_rate,
        f"{prefix}_duration_s": round(duration, 2),
        f"{prefix}_rms": round(rms, 1),
        f"{prefix}_peak": peak,
        f"{prefix}_clip_pct": round(100.0 * clipped / len(samples), 4),
        f"{prefix}_hard_jumps_per_s": round(hard_jumps / max(duration, 0.001), 3),
        f"{prefix}_delta_rms": round(delta_rms, 1),
        f"{prefix}_delta_rms_ratio": round(delta_rms / max(rms, 1.0), 3),
        f"{prefix}_zero_crossings_per_s": round(
            zero_crossings / max(duration, 0.001),
            1,
        ),
    }


class RawAudioMonitor:
    """Small debug-only monitor for camera G.711 payload health."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._frames = 0
        self._bytes = 0
        self._lengths: collections.Counter[int] = collections.Counter()
        self._arrival_gaps: list[float] = []
        self._last_at = 0.0
        self._samples: list[int] = []

    def add(self, payload: bytes) -> None:
        if not payload:
            return
        now = time.monotonic()
        samples = [_mulaw_to_pcm16(byte) for byte in payload]
        with self._lock:
            if self._last_at > 0:
                self._arrival_gaps.append(now - self._last_at)
            self._last_at = now
            self._frames += 1
            self._bytes += len(payload)
            self._lengths[len(payload)] += 1
            self._samples.extend(samples)

    def summary(self) -> dict[str, Any]:
        with self._lock:
            gaps = list(self._arrival_gaps)
            samples = list(self._samples)
            result: dict[str, Any] = {
                "raw_audio_frames": self._frames,
                "raw_audio_bytes": self._bytes,
                "raw_audio_payload_lengths": self._lengths.most_common(6),
            }
        if gaps:
            gap_ms = [gap * 1000.0 for gap in gaps]
            result.update(
                {
                    "raw_audio_arrival_median_ms": round(statistics.median(gap_ms), 1),
                    "raw_audio_arrival_p95_ms": round(_percentile(gap_ms, 0.95), 1),
                    "raw_audio_arrival_max_ms": round(max(gap_ms), 1),
                    "raw_audio_arrival_gaps_over_80ms": sum(
                        1 for gap in gaps if gap > 0.08
                    ),
                }
            )
        result.update(_pcm_quality_metrics(samples, 8000, "raw_ulaw"))
        return result


def _muxer_audio_snapshot(coord: Any) -> dict[str, Any]:
    muxer = getattr(coord, "_muxer", None)
    getter = getattr(muxer, "audio_debug_snapshot", None)
    if not callable(getter):
        return {}
    try:
        return dict(getter())
    except Exception:
        return {}


def start_player_audio_diag(player_proc: subprocess.Popen) -> threading.Thread:
    thread = threading.Thread(
        target=_poll_player_audio_diag,
        args=(player_proc,),
        daemon=True,
    )
    thread.start()
    return thread


def _poll_player_audio_diag(player_proc: subprocess.Popen) -> None:
    """Poll PipeWire/PulseAudio to verify ffplay audio output."""
    log = logging.getLogger(__name__)
    pid = player_proc.pid
    checked = 0
    found_stream = False
    while player_proc.poll() is None:
        time.sleep(2)
        checked += 1
        try:
            raw = subprocess.check_output(
                ["pactl", "list", "sink-inputs"],
                timeout=3,
                stderr=subprocess.DEVNULL,
            ).decode(errors="replace")
        except Exception:
            if checked <= 2:
                log.warning("Audio diag: pactl not available")
            break

        blocks = raw.split("Sink Input #")
        ffplay_idx, ffplay_block = _find_player_sink_input(blocks, pid)
        if ffplay_block is None:
            if not found_stream and checked <= 5:
                log.warning(
                    "Audio diag: no sink-input for PID %d (%d inputs: %s)",
                    pid,
                    len(blocks) - 1,
                    "; ".join(_sink_input_apps(blocks)[:10]),
                )
            continue

        details = _sink_input_details(ffplay_block)
        if not found_stream:
            log.info(
                "Audio diag: FOUND sink-input #%s — %s | %s | %s | %s | corked=%s",
                ffplay_idx,
                details["app_name"],
                _resolve_sink_name(details["sink_id"]),
                details["muted"],
                details["volume"],
                details["corked"],
            )
            found_stream = True
        elif checked % 5 == 0:
            log.info(
                "Audio diag: sink-input #%s — %s | %s | %s | corked=%s",
                ffplay_idx,
                _resolve_sink_name(details["sink_id"]),
                details["muted"],
                details["volume"],
                details["corked"],
            )
    if not found_stream:
        log.warning(
            "Audio diag: ffplay NEVER registered an audio stream with PipeWire/PulseAudio (PID %d)",
            pid,
        )


def _find_player_sink_input(
    blocks: list[str], pid: int
) -> tuple[str | None, str | None]:
    for block in blocks[1:]:
        is_pid = f'pid = "{pid}"' in block or f"pid = {pid}" in block
        is_sdl = (
            "SDL Application" in block
            or "ffplay" in block.lower()
            or "mpv" in block.lower()
        )
        if is_pid or is_sdl:
            return block.split("\n")[0].strip(), block
    return None, None


def _sink_input_apps(blocks: list[str]) -> list[str]:
    apps = []
    for block in blocks[1:]:
        for line in block.splitlines():
            if "application.name" in line or "application.process.id" in line:
                apps.append(line.strip())
    return apps


def _sink_input_details(block: str) -> dict[str, Any]:
    details: dict[str, Any] = {
        "muted": "UNKNOWN",
        "volume": "UNKNOWN",
        "corked": False,
        "app_name": "",
        "sink_id": "UNKNOWN",
    }
    for line in block.splitlines():
        ls = line.strip()
        if ls.startswith("Mute:"):
            details["muted"] = ls
        elif ls.startswith("Volume:") and "Base" not in ls:
            details["volume"] = ls
        elif ls.startswith("Corked:"):
            details["corked"] = "yes" in ls.lower()
        elif "application.name" in ls:
            details["app_name"] = ls
        elif ls.startswith("Sink:"):
            details["sink_id"] = ls
    return details


def _resolve_sink_name(sink_id: str) -> str:
    if sink_id == "UNKNOWN":
        return sink_id
    try:
        sinks_raw = subprocess.check_output(
            ["pactl", "list", "sinks", "short"],
            timeout=2,
            stderr=subprocess.DEVNULL,
        ).decode(errors="replace")
    except Exception:
        return sink_id
    sid = sink_id.replace("Sink:", "").strip()
    for line in sinks_raw.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0].strip() == sid:
            return f"Sink: {sid} ({parts[1]})"
    return sink_id


def _print_compact_metrics(
    title: str, metrics: dict[str, Any], keys: list[str]
) -> None:
    if not metrics:
        return
    print(f"  {title}:")
    for key in keys:
        if key in metrics:
            print(f"    {key}: {metrics[key]}")


def _print_audio_crackle_diagnostics(
    raw_metrics: dict[str, Any],
    pcm_metrics: dict[str, Any],
    mux_metrics: dict[str, Any],
) -> None:
    print("\nAudio crackle diagnostics")
    print("-" * 78)
    _print_compact_metrics(
        "camera raw mu-law",
        raw_metrics,
        [
            "raw_audio_frames",
            "raw_audio_payload_lengths",
            "raw_audio_arrival_median_ms",
            "raw_audio_arrival_p95_ms",
            "raw_audio_arrival_max_ms",
            "raw_audio_arrival_gaps_over_80ms",
            "raw_ulaw_rms",
            "raw_ulaw_peak",
            "raw_ulaw_clip_pct",
            "raw_ulaw_hard_jumps_per_s",
            "raw_ulaw_delta_rms_ratio",
        ],
    )
    _print_compact_metrics(
        "decoded player audio",
        pcm_metrics,
        [
            "pcm_duration_s",
            "pcm_audible_pct",
            "pcm_max_silence_gap_s",
            "pcm_short_gaps_under_200ms",
            "pcm_rms",
            "pcm_peak",
            "pcm_clip_pct",
            "pcm_hard_jumps_per_s",
            "pcm_delta_rms_ratio",
            "pcm_zero_crossings_per_s",
        ],
    )
    _print_compact_metrics(
        "mux audio pacing",
        mux_metrics,
        [
            "mux_audio_frames",
            "mux_audio_silence_frames",
            "mux_audio_max_emit_gap_s",
            "mux_audio_next_pts",
            "mux_video_last_pts",
        ],
    )
    flags = []
    if float(raw_metrics.get("raw_ulaw_clip_pct", 0.0) or 0.0) > 0.05:
        flags.append("raw-clipping")
    if float(raw_metrics.get("raw_ulaw_hard_jumps_per_s", 0.0) or 0.0) > 0.5:
        flags.append("raw-hard-jumps")
    if float(pcm_metrics.get("pcm_clip_pct", 0.0) or 0.0) > 0.05:
        flags.append("decoded-clipping")
    if float(pcm_metrics.get("pcm_hard_jumps_per_s", 0.0) or 0.0) > 0.5:
        flags.append("decoded-hard-jumps")
    if float(mux_metrics.get("mux_audio_max_emit_gap_s", 0.0) or 0.0) > 0.25:
        flags.append("mux-pacing-gap")
    if int(raw_metrics.get("raw_audio_arrival_gaps_over_80ms", 0) or 0) > 20:
        flags.append("raw-arrival-jitter")
    print(f"  audio_artifact_flags: {flags}")


def _analyze_pcm_audio(
    wav_path: str,
    log: logging.Logger,
    chunk_ms: int = 64,
    silence_threshold_rms: float = 50.0,
) -> dict[str, Any]:
    """Analyse a raw PCM WAV file for silence gaps and audible content.

    Splits the audio into *chunk_ms*-length windows and classifies each
    as silence (RMS < *silence_threshold_rms*) or audible.

    Returns a dict with:
      pcm_duration_s, pcm_audible_pct, pcm_silence_pct,
      pcm_silence_gap_count, pcm_silence_gap_durations_s (top 10),
      pcm_max_silence_gap_s, pcm_avg_rms, pcm_max_rms,
      pcm_audible_segments (count of continuous audible runs),
    """
    result: dict[str, Any] = {}

    if not os.path.isfile(wav_path) or os.path.getsize(wav_path) < 100:
        log.warning("PCM recording missing or too small: %s", wav_path)
        return result

    # Read WAV — we know it is 16-bit LE mono 16 kHz from our own ffmpeg cmd.
    with open(wav_path, "rb") as f:
        header = f.read(44)  # standard WAV header
        if len(header) < 44 or header[:4] != b"RIFF":
            log.warning("Not a valid WAV file: %s", wav_path)
            return result
        pcm_data = f.read()

    if len(pcm_data) < 2:
        log.warning("PCM recording has no audio data")
        return result

    sample_rate = 16000
    bytes_per_sample = 2  # s16le
    chunk_samples = int(sample_rate * chunk_ms / 1000)
    chunk_bytes = chunk_samples * bytes_per_sample
    total_samples = len(pcm_data) // bytes_per_sample
    total_duration = total_samples / sample_rate

    result["pcm_duration_s"] = round(total_duration, 2)
    all_samples = list(
        struct.unpack(f"<{total_samples}h", pcm_data[: total_samples * 2])
    )
    result.update(_pcm_quality_metrics(all_samples, sample_rate, "pcm"))

    # Classify each chunk
    chunk_rms_values: list[float] = []
    is_silence: list[bool] = []
    offset = 0
    while offset + chunk_bytes <= len(pcm_data):
        samples = struct.unpack(
            f"<{chunk_samples}h", pcm_data[offset : offset + chunk_bytes]
        )
        sum_sq = sum(s * s for s in samples)
        rms = math.sqrt(sum_sq / chunk_samples)
        chunk_rms_values.append(rms)
        is_silence.append(rms < silence_threshold_rms)
        offset += chunk_bytes

    if not chunk_rms_values:
        return result

    n_chunks = len(chunk_rms_values)
    n_silent = sum(is_silence)
    n_audible = n_chunks - n_silent
    chunk_dur = chunk_ms / 1000.0

    result["pcm_total_chunks"] = n_chunks
    result["pcm_audible_chunks"] = n_audible
    result["pcm_silence_chunks"] = n_silent
    result["pcm_audible_pct"] = round(100.0 * n_audible / n_chunks, 1)
    result["pcm_silence_pct"] = round(100.0 * n_silent / n_chunks, 1)

    # RMS stats for audible chunks
    audible_rms = [r for r, s in zip(chunk_rms_values, is_silence) if not s]
    if audible_rms:
        result["pcm_avg_rms_audible"] = round(sum(audible_rms) / len(audible_rms), 1)
        result["pcm_max_rms"] = round(max(audible_rms), 1)
        result["pcm_min_rms_audible"] = round(min(audible_rms), 1)
    result["pcm_avg_rms_all"] = round(sum(chunk_rms_values) / len(chunk_rms_values), 1)

    # Detect silence gaps (contiguous silent runs)
    silence_gaps: list[float] = []
    audible_segments = 0
    in_gap = False
    gap_start = 0
    for i, silent in enumerate(is_silence):
        if silent:
            if not in_gap:
                in_gap = True
                gap_start = i
        else:
            if in_gap:
                gap_dur = (i - gap_start) * chunk_dur
                silence_gaps.append(gap_dur)
                in_gap = False
            audible_segments += 1 if (i == 0 or is_silence[i - 1]) else 0
    # Close trailing gap
    if in_gap:
        silence_gaps.append((n_chunks - gap_start) * chunk_dur)

    # Count audible segments (contiguous audible runs) and their durations
    audible_seg_count = 0
    audible_seg_lengths: list[float] = []
    in_audible = False
    seg_start = 0
    for idx, silent in enumerate(is_silence):
        if not silent:
            if not in_audible:
                audible_seg_count += 1
                seg_start = idx
                in_audible = True
        else:
            if in_audible:
                audible_seg_lengths.append((idx - seg_start) * chunk_dur)
                in_audible = False
    if in_audible:
        audible_seg_lengths.append((len(is_silence) - seg_start) * chunk_dur)

    result["pcm_audible_segments"] = audible_seg_count
    result["pcm_silence_gap_count"] = len(silence_gaps)
    if silence_gaps:
        result["pcm_max_silence_gap_s"] = round(max(silence_gaps), 3)
        result["pcm_min_silence_gap_s"] = round(min(silence_gaps), 3)
        result["pcm_avg_silence_gap_s"] = round(
            sum(silence_gaps) / len(silence_gaps), 3
        )
        result["pcm_silence_gap_durations_s"] = [
            round(g, 3) for g in sorted(silence_gaps, reverse=True)[:10]
        ]
        # Count very short gaps that suggest pipeline fragmentation
        result["pcm_short_gaps_under_200ms"] = sum(1 for g in silence_gaps if g < 0.2)

    if audible_seg_lengths:
        result["pcm_max_audible_seg_s"] = round(max(audible_seg_lengths), 3)
        result["pcm_avg_audible_seg_s"] = round(
            sum(audible_seg_lengths) / len(audible_seg_lengths), 3
        )
        result["pcm_audible_seg_durations_s"] = [
            round(g, 3) for g in sorted(audible_seg_lengths, reverse=True)[:10]
        ]

    # Timeline summary: first/last audible chunk position
    audible_indices = [i for i, s in enumerate(is_silence) if not s]
    if audible_indices:
        result["pcm_first_audible_at_s"] = round(audible_indices[0] * chunk_dur, 2)
        result["pcm_last_audible_at_s"] = round(audible_indices[-1] * chunk_dur, 2)

    return result
