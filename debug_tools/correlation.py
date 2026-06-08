"""Correlate ffplay decode errors with coordinator gap/skip events."""

from __future__ import annotations

from typing import Any

from .visual import _select_gap_event_for_error_time


def _print_player_decode_correlation(coord: Any, state: dict[str, Any]) -> None:
    getter = getattr(coord, "get_gap_skip_events_snapshot", None)
    events = getter() if callable(getter) else []
    startup_count = int(state.get("startup_count", 0) or 0)
    by_event = state.get("by_event", {})
    if startup_count <= 0 and not events and not by_event:
        return

    print("\nGap recovery correlation")
    print("-" * 78)
    print(f"  startup_player_decode_errors: {startup_count}")
    startup_samples = state.get("startup_samples", [])
    if startup_samples:
        print(f"  startup_player_decode_errors_sample: {startup_samples[:3]}")

    for event in events:
        event_id = int(event.get("event_id", 0) or 0)
        severity = str(event.get("severity", "unknown"))
        strict_release = bool(event.get("strict_release", False))
        bucket = by_event.get(event_id, {})
        parts = [
            f"gap_event_{event_id}:",
            f"severity={severity}",
            f"strict_release={'yes' if strict_release else 'no'}",
            f"player_decode_errors={int(bucket.get('count', 0) or 0)}",
        ]
        if event.get("mode"):
            parts.append(f"mode={event['mode']}")
        if "gap_size" in event:
            parts.append(f"gap={int(event.get('gap_size', 0) or 0)}")
        if "stall_s" in event:
            parts.append(f"stall_s={float(event.get('stall_s', 0.0) or 0.0):.2f}")
        if "backlog_s" in event:
            parts.append(f"backlog_s={float(event.get('backlog_s', 0.0) or 0.0):.2f}")
        if "payload_idle_s" in event:
            parts.append(f"idle_s={float(event.get('payload_idle_s', 0.0) or 0.0):.2f}")
        if "quarantine_drops" in event:
            parts.append(
                f"quarantine_drops={int(event.get('quarantine_drops', 0) or 0)}"
            )
        if event.get("release_reason"):
            parts.append(f"release={event['release_reason']}")
        if bucket.get("first_after_s") is not None:
            parts.append(f"first_error_after_s={float(bucket['first_after_s']):.2f}")
        print("  " + " ".join(parts))
        if bucket.get("samples"):
            print(f"  gap_event_{event_id}_sample: {bucket['samples'][0]}")


def _print_stream_join_diagnostics(coord: Any) -> None:
    getter = getattr(coord, "get_stream_join_diagnostics_snapshot", None)
    events = getter() if callable(getter) else []
    if not events:
        return

    print("\nStream join diagnostics")
    print("-" * 78)
    for event in events:
        event_id = int(event.get("event_id", 0) or 0)
        seed_summary = dict(event.get("seed_summary") or {})
        live_summary = dict(event.get("live_summary") or {})
        boundary = dict(event.get("boundary") or {})
        print(
            "  "
            f"join_event_{event_id}: "
            f"mode={event.get('mode', 'unknown')} "
            f"reason={event.get('reason', 'unknown')} "
            f"wait_s={float(event.get('wait_s', 0.0) or 0.0):.2f} "
            f"seed_generation={int(event.get('seed_generation', 0) or 0)}/"
            f"{int(event.get('required_seed_generation', 0) or 0)} "
            f"seed_bytes={int(event.get('seed_bytes', 0) or 0)} "
            f"live_chunks={int(event.get('live_chunk_count', 0) or 0)} "
            f"seed_offset_s={float(event.get('seed_offset_s', 0.0) or 0.0):.3f}"
        )
        gap_tag = ""
        if int(event.get("active_gap_event_id", 0) or 0) > 0:
            gap_tag = f" gap_event_id={int(event['active_gap_event_id'])}"
        stale_tag = " STALE_SEED_GEN" if event.get("seed_gen_stale") else ""
        print(
            "  "
            f"join_event_{event_id}_extra:"
            f"{gap_tag}{stale_tag}"
            f" seed_gen={int(event.get('seed_generation', 0) or 0)}"
            f"/required={int(event.get('required_seed_generation', 0) or 0)}"
        )
        print(
            "  "
            f"seed_video_pts={seed_summary.get('video_first_pts')}->{seed_summary.get('video_last_pts')} "
            f"seed_audio_pts={seed_summary.get('audio_first_pts')}->{seed_summary.get('audio_last_pts')}"
        )
        live_first_pid = boundary.get("live_first_packet_pid")
        live_first_pid_text = (
            f"0x{int(live_first_pid):04x}" if isinstance(live_first_pid, int) else "n/a"
        )
        parts = [
            f"live_first_pid={live_first_pid_text}",
            f"live_video_pusi={boundary.get('live_first_video_packet_is_pusi')}",
            f"live_video_rai={boundary.get('live_first_video_packet_is_rai')}",
            f"live_video_pusi_index={boundary.get('live_first_video_pusi_index')}",
            f"live_audio_pusi={boundary.get('live_first_audio_packet_is_pusi')}",
            f"live_audio_pusi_index={boundary.get('live_first_audio_pusi_index')}",
        ]
        if "video_pts_gap_s" in boundary:
            parts.append(f"video_pts_gap_s={float(boundary['video_pts_gap_s']):.3f}")
        if "video_pcr_gap_s" in boundary:
            parts.append(f"video_pcr_gap_s={float(boundary['video_pcr_gap_s']):.3f}")
        if "video_first_live_cc" in boundary:
            parts.append(
                f"video_cc={int(boundary['video_first_live_cc'])} "
                f"exp={int(boundary.get('video_expected_next_cc', 0) or 0)} "
                f"delta={int(boundary.get('video_cc_delta', 0) or 0)}"
            )
        if "audio_pts_gap_s" in boundary:
            parts.append(f"audio_pts_gap_s={float(boundary['audio_pts_gap_s']):.3f}")
        if "audio_first_live_cc" in boundary:
            parts.append(
                f"audio_cc={int(boundary['audio_first_live_cc'])} "
                f"exp={int(boundary.get('audio_expected_next_cc', 0) or 0)} "
                f"delta={int(boundary.get('audio_cc_delta', 0) or 0)}"
            )
        print("  " + " ".join(parts))
        if seed_summary.get("sample_packets"):
            print(
                f"  join_event_{event_id}_seed_packets: {seed_summary['sample_packets']}"
            )
        if live_summary.get("sample_packets"):
            print(
                f"  join_event_{event_id}_live_packets: {live_summary['sample_packets']}"
            )


def _print_ts_decode_correlation(
    coord: Any,
    recorder_started_mono: float | None,
    ts_metrics: dict[str, Any],
) -> None:
    if recorder_started_mono is None:
        return
    error_times = ts_metrics.get("decode_error_timestamps_s") or []
    if not error_times:
        return

    getter = getattr(coord, "get_gap_skip_events_snapshot", None)
    events = getter() if callable(getter) else []
    if not events:
        return

    print("\nRecorded TS decode correlation")
    print("-" * 78)

    first_error_s = float(ts_metrics.get("first_decode_error_s", error_times[0]) or 0.0)
    first_when_mono = recorder_started_mono + first_error_s
    first_event = _select_gap_event_for_error_time(events, first_when_mono)
    print(f"  first_ts_decode_error_s: {first_error_s:.3f}")
    if first_event is None:
        print(
            "  first_ts_decode_error_gap_event: none (join/startup-side or uncorrelated)"
        )
    else:
        event_id = int(first_event.get("event_id", 0) or 0)
        severity = str(first_event.get("severity", "unknown"))
        started_mono = float(
            first_event.get("started_mono", first_when_mono) or first_when_mono
        )
        release_mono = float(
            first_event.get(
                "quarantine_release_mono",
                first_event.get("output_reset_mono", started_mono),
            )
            or started_mono
        )
        print(
            "  first_ts_decode_error_gap_event: "
            f"#{event_id} severity={severity} release={first_event.get('release_reason', 'unknown')}"
        )
        print(
            f"  first_ts_decode_error_after_gap_s: {max(0.0, first_when_mono - started_mono):.3f}"
        )
        print(
            f"  first_ts_decode_error_after_release_s: {max(0.0, first_when_mono - release_mono):.3f}"
        )

    sampled_counts: dict[int, int] = {}
    uncorrelated = 0
    event_lookup = {
        int(event.get("event_id", 0) or 0): event
        for event in events
        if int(event.get("event_id", 0) or 0) > 0
    }
    for ts_s in error_times[:12]:
        event = _select_gap_event_for_error_time(
            events, recorder_started_mono + float(ts_s)
        )
        if event is None:
            uncorrelated += 1
            continue
        event_id = int(event.get("event_id", 0) or 0)
        sampled_counts[event_id] = sampled_counts.get(event_id, 0) + 1

    if uncorrelated:
        print(
            f"  sampled_ts_decode_errors_without_gap: {uncorrelated}/{len(error_times[:12])}"
        )
    for event_id, count in sorted(sampled_counts.items()):
        severity = str(event_lookup.get(event_id, {}).get("severity", "unknown"))
        print(
            f"  gap_event_{event_id}_sampled_ts_decode_errors: {count}/{len(error_times[:12])} ({severity})"
        )
