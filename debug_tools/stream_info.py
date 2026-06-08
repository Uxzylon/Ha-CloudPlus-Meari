"""Stream summary and diagnostics formatting for the debug harness."""

from __future__ import annotations

import time
from typing import Any


def _print_stream_request(
    coord: Any, dev: dict[str, Any], mods: dict[str, Any]
) -> None:
    """Print static stream settings before the player starts."""
    p2p = mods["p2p_streamer"]
    quality = _effective_quality(coord)
    stream_id = p2p.stream_id_for_quality(dev, quality)
    option = _quality_option(p2p.quality_options(dev), quality, stream_id)
    label = option.label if option is not None else str(quality)
    detail = f" ({option.detail})" if option is not None and option.detail else ""
    adaptive = "yes" if option is not None and option.is_auto else "no"

    print(f"Quality: {label}{detail} stream_id={stream_id} adaptive={adaptive}")
    print(f"Profiles: {_format_quality_options(p2p.quality_options(dev))}")
    print(
        "Device info: "
        f"id={dev.get('deviceID')} category={dev.get('_category')} "
        f"firmware={dev.get('deviceVersionID', 'unknown')}"
    )


def _print_live_stream_details(
    coord: Any,
    dev: dict[str, Any],
    mods: dict[str, Any],
) -> None:
    """Print live details once the coordinator has seen frames."""
    p2p = mods["p2p_streamer"]
    quality = _effective_quality(coord)
    stream_id = p2p.stream_id_for_quality(dev, quality)
    codec = _text(getattr(coord, "_video_codec", "unknown"))
    fps = float(getattr(coord, "_video_mux_target_fps", 0.0) or 0.0)
    stream_started_at = float(getattr(coord, "_stream_started_at", 0.0) or 0.0)
    first_video_at = float(getattr(coord, "_live_first_video_time", 0.0) or 0.0)
    now = time.monotonic()
    elapsed = max(0.0, now - (first_video_at or stream_started_at or now))
    video_frames = int(getattr(coord, "_p2p_video_frames", 0) or 0)
    audio_seen = float(getattr(coord, "_last_p2p_audio_time", 0.0) or 0.0) > 0.0

    diagnostics = _stream_diagnostics(coord)
    source_video_frames = int(diagnostics.get("source_video_frames", video_frames) or 0)
    video_bytes = int(diagnostics.get("video_bytes", 0) or 0)
    audio_frames = int(diagnostics.get("audio_frames", 0) or 0)
    audio_bytes = int(diagnostics.get("audio_bytes", 0) or 0)
    actual_stream_id = int(diagnostics.get("stream_id", stream_id) or stream_id)
    media_span = float(diagnostics.get("video_media_span_s", 0.0) or 0.0)
    sample_span = media_span or elapsed
    observed_fps = float(diagnostics.get("video_timestamp_fps", 0.0) or 0.0)
    if observed_fps <= 0.0 < elapsed:
        observed_fps = source_video_frames / elapsed
    video_kbps = video_bytes * 8.0 / 1000.0 / sample_span if sample_span > 0.0 else 0.0
    audio_kbps = audio_bytes * 8.0 / 1000.0 / sample_span if sample_span > 0.0 else 0.0

    print(
        "Live details: "
        f"codec={codec} stream_id={actual_stream_id} "
        f"advertised_fps={fps:.1f} media_fps={observed_fps:.1f} "
        f"video_bitrate={video_kbps:.0f}kbps audio_bitrate={audio_kbps:.0f}kbps"
    )
    print(
        "Media counters: "
        f"video_frames={video_frames} source_video_frames={source_video_frames} "
        f"video_bytes={video_bytes} audio_frames={audio_frames} "
        f"audio_bytes={audio_bytes} audio_seen={'yes' if audio_seen else 'no'} "
        f"video_decrypted={_yes_no(diagnostics.get('video_decrypted'))} "
        f"audio_decrypted={_yes_no(diagnostics.get('audio_decrypted'))}"
    )
    if diagnostics:
        print(
            "P2P details: "
            f"signaling={diagnostics.get('signaling_endpoint', 'unknown')} "
            f"turn={diagnostics.get('turn_endpoint', 'unknown')} "
            f"candidates={diagnostics.get('candidate_count', 0)} "
            f"stream_flag={diagnostics.get('stream_flag', 'unknown')}"
        )


def _print_stream_media_summary(
    coord: Any,
    dev: dict[str, Any],
    mods: dict[str, Any],
    source_summary: dict[str, Any],
) -> None:
    p2p = mods["p2p_streamer"]
    quality = _effective_quality(coord)
    stream_id = p2p.stream_id_for_quality(dev, quality)
    option = _quality_option(p2p.quality_options(dev), quality, stream_id)
    diagnostics = _stream_diagnostics(coord)

    elapsed = float(source_summary.get("active_span_s", 0.0) or 0.0)
    media_span = float(diagnostics.get("video_media_span_s", 0.0) or 0.0)
    rate_span = media_span or elapsed
    video_bytes = int(diagnostics.get("video_bytes", 0) or 0)
    audio_bytes = int(diagnostics.get("audio_bytes", 0) or 0)
    media_fps = float(diagnostics.get("video_timestamp_fps", 0.0) or 0.0)
    if media_fps <= 0.0:
        media_fps = float(source_summary.get("avg_fps", 0.0) or 0.0)
    video_kbps = video_bytes * 8.0 / 1000.0 / rate_span if rate_span > 0.0 else 0.0
    audio_kbps = audio_bytes * 8.0 / 1000.0 / rate_span if rate_span > 0.0 else 0.0

    print("\nStream media summary")
    print("-" * 78)
    print(
        "  "
        f"quality={option.label if option else _text(quality)} "
        f"stream_id={diagnostics.get('stream_id', stream_id)} "
        f"adaptive={'yes' if option is not None and option.is_auto else 'no'} "
        f"codec={diagnostics.get('codec', _text(getattr(coord, '_video_codec', 'unknown')))}"
    )
    print(
        "  "
        f"advertised_fps={float(getattr(coord, '_video_mux_target_fps', 0.0) or 0.0):.1f} "
        f"media_fps={media_fps:.2f} ingress_fps={float(source_summary.get('avg_fps', 0.0) or 0.0):.2f} "
        f"video_bitrate={video_kbps:.0f}kbps audio_bitrate={audio_kbps:.0f}kbps "
        f"media_duration={media_span:.2f}s ingress_duration={elapsed:.2f}s"
    )
    print(
        "  "
        f"video_frames={int(source_summary.get('video_frames', 0) or 0)} "
        f"source_video_frames={int(diagnostics.get('source_video_frames', 0) or 0)} "
        f"video_bytes={video_bytes} audio_frames={int(diagnostics.get('audio_frames', 0) or 0)} "
        f"audio_bytes={audio_bytes}"
    )
    print(
        "  "
        f"audio_path=G.711u-camera-to-AAC-ts "
        f"video_decrypted={_yes_no(diagnostics.get('video_decrypted'))} "
        f"audio_decrypted={_yes_no(diagnostics.get('audio_decrypted'))}"
    )


def _effective_quality(coord: Any) -> int | None:
    getter = getattr(coord, "_stream_quality", None)
    return getter() if callable(getter) else getattr(coord, "vvp_quality", None)


def _quality_option(options: list[Any], quality: int | None, stream_id: int) -> Any:
    for option in options:
        if option.quality == quality or option.stream_id == stream_id:
            return option
    return None


def _format_quality_options(options: list[Any]) -> str:
    if not options:
        return "none"
    return ", ".join(
        option.label if not option.detail else f"{option.label}={option.detail}"
        for option in options
    )


def _text(value: Any) -> str:
    return str(getattr(value, "value", value))


def _stream_diagnostics(coord: Any) -> dict[str, Any]:
    streamer = getattr(coord, "_p2p_streamer", None)
    diagnostics = (
        getattr(streamer, "diagnostics", None) if streamer is not None else None
    )
    return dict(diagnostics or getattr(coord, "_last_p2p_diagnostics", {}) or {})


def _yes_no(value: Any) -> str:
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "unknown"
