"""Optional gap/join telemetry dump for the debug harness."""

from __future__ import annotations

import json
import logging
from typing import Any


def _write_gap_telemetry(coord: Any, artifact_base: str, log: logging.Logger) -> None:
    """Write optional gap/join telemetry from coordinator debug hooks."""
    try:
        gap_events = []
        join_events = []
        if coord is not None:
            getter_gap = getattr(coord, "get_gap_skip_events_snapshot", None)
            if callable(getter_gap):
                gap_events = getter_gap()
            getter_join = getattr(coord, "get_stream_join_diagnostics_snapshot", None)
            if callable(getter_join):
                join_events = getter_join()
        if not gap_events and not join_events:
            return

        path = f"{artifact_base}_gap_telemetry.jsonl"
        join_by_gap: dict[int, list[dict[str, Any]]] = {}
        for event in join_events:
            gap_id = int(event.get("active_gap_event_id", 0) or 0)
            join_by_gap.setdefault(gap_id, []).append(_compact_join_event(event))

        with open(path, "w", encoding="utf-8") as file:
            for event in gap_events:
                event_id = int(event.get("event_id", 0) or 0)
                record = {
                    "type": "gap_event",
                    "event_id": event_id,
                    "severity": event.get("severity"),
                    "gap_size": event.get("gap_size"),
                    "stall_s": event.get("stall_s"),
                    "backlog_s": event.get("backlog_s"),
                    "strict_release": event.get("strict_release"),
                    "release_reason": event.get("release_reason"),
                    "quarantine_drops": event.get("quarantine_drops"),
                    "released_frame_bytes": event.get("released_frame_bytes"),
                    "startup_safe_min_seed_generation": event.get(
                        "startup_safe_min_seed_generation"
                    ),
                    "status": event.get("status"),
                    "join_events": join_by_gap.get(event_id, []),
                }
                file.write(json.dumps(record) + "\n")

            for event in join_events:
                if int(event.get("active_gap_event_id", 0) or 0) == 0:
                    file.write(
                        json.dumps(
                            {
                                "type": "join_event_unlinked",
                                **_compact_join_event(event),
                            }
                        )
                        + "\n"
                    )
        log.info(
            "Gap telemetry written to %s (%d gap events, %d join events)",
            path,
            len(gap_events),
            len(join_events),
        )
    except (OSError, TypeError, ValueError):
        log.debug("Failed to write gap telemetry", exc_info=True)


def _compact_join_event(event: dict[str, Any]) -> dict[str, Any]:
    verbose_keys = {"live_capture", "seed_summary", "live_summary", "boundary"}
    return {key: value for key, value in event.items() if key not in verbose_keys}
