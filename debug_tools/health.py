"""Live video cadence tracking and freeze-window reporting."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable


class StreamHealthTracker:
    """Track live video cadence and report freeze windows."""

    def __init__(
        self,
        logger: logging.Logger,
        *,
        label: str = "Video",
        frame_ts_reader: Callable[[Any], float] | None = None,
        frame_count_reader: Callable[[Any], int] | None = None,
        emit_logs: bool = True,
    ):
        self._logger = logger
        self._label = label
        self._frame_ts_reader = frame_ts_reader or self._default_frame_ts_reader
        self._frame_count_reader = (
            frame_count_reader or self._default_frame_count_reader
        )
        self._emit_logs = bool(emit_logs)
        self._first_frame_ts: float = 0.0
        self._last_frame_ts: float = 0.0
        self._last_frame_count: int = 0
        self._stall_start_ts: float | None = None
        self._last_stall_log_mono: float = 0.0
        self._stalls_over_1s: int = 0
        self._stalls_over_3s: int = 0
        self._recovered_stalls: int = 0
        self._max_gap_s: float = 0.0
        self._stall_events: list[dict[str, float | int]] = []

    @staticmethod
    def _default_frame_ts_reader(coord: Any) -> float:
        return float(
            getattr(
                coord, "_last_p2p_video_time", getattr(coord, "_last_video_time", 0.0)
            )
        )

    @staticmethod
    def _default_frame_count_reader(coord: Any) -> int:
        return int(getattr(coord, "_p2p_video_frames", 0))

    def _record_stall(self, stall_s: float) -> None:
        self._max_gap_s = max(self._max_gap_s, stall_s)
        self._recovered_stalls += 1
        if stall_s >= 1.0:
            self._stalls_over_1s += 1
        if stall_s >= 3.0:
            self._stalls_over_3s += 1

    def _append_stall_event(
        self,
        *,
        start_ts: float,
        end_ts: float,
        frames: int,
    ) -> None:
        if self._first_frame_ts <= 0.0 or len(self._stall_events) >= 32:
            return
        self._stall_events.append(
            {
                "start_s": max(0.0, start_ts - self._first_frame_ts),
                "end_s": max(0.0, end_ts - self._first_frame_ts),
                "duration_s": max(0.0, end_ts - start_ts),
                "video_frames": frames,
            }
        )

    def tick(self, coord: Any) -> None:
        now_mono = time.monotonic()
        frame_ts = float(self._frame_ts_reader(coord))
        frame_count = int(self._frame_count_reader(coord))
        if frame_ts <= 0.0:
            return

        if self._first_frame_ts <= 0.0:
            self._first_frame_ts = frame_ts

        progressed = (frame_ts > self._last_frame_ts) or (
            frame_count > self._last_frame_count
        )
        if progressed:
            if self._last_frame_ts > 0.0:
                self._max_gap_s = max(
                    self._max_gap_s, 0.0, frame_ts - self._last_frame_ts
                )
            if self._stall_start_ts is not None:
                stall_s = max(0.0, frame_ts - self._stall_start_ts)
                self._record_stall(stall_s)
                self._append_stall_event(
                    start_ts=self._stall_start_ts,
                    end_ts=frame_ts,
                    frames=frame_count,
                )
                if self._emit_logs:
                    start_s = max(0.0, self._stall_start_ts - self._first_frame_ts)
                    end_s = max(0.0, frame_ts - self._first_frame_ts)
                    self._logger.warning(
                        "%s stall recovered after %.2fs at +%.2f..+%.2fs (video_frames=%d)",
                        self._label,
                        stall_s,
                        start_s,
                        end_s,
                        frame_count,
                    )
                self._stall_start_ts = None
                self._last_stall_log_mono = 0.0
            self._last_frame_ts = frame_ts
            self._last_frame_count = frame_count
            return

        if self._last_frame_ts <= 0.0:
            return
        if self._stall_start_ts is None:
            self._stall_start_ts = self._last_frame_ts

        stall_s = max(0.0, now_mono - self._stall_start_ts)
        if stall_s >= 1.0 and (
            self._last_stall_log_mono <= 0.0
            or (now_mono - self._last_stall_log_mono) >= 2.0
        ):
            if self._emit_logs:
                start_s = max(0.0, self._stall_start_ts - self._first_frame_ts)
                self._logger.warning(
                    "%s stall ongoing %.2fs from +%.2fs (video_frames=%d)",
                    self._label,
                    stall_s,
                    start_s,
                    frame_count,
                )
            self._last_stall_log_mono = now_mono

    def summary(self, coord: Any) -> dict[str, Any]:
        now_mono = time.monotonic()
        frame_count = int(self._frame_count_reader(coord))
        last_frame_ts = float(self._frame_ts_reader(coord))

        active_span = 0.0
        if self._first_frame_ts > 0.0 and last_frame_ts >= self._first_frame_ts:
            active_span = max(0.0, last_frame_ts - self._first_frame_ts)

        avg_fps = 0.0
        if active_span > 0.0 and frame_count > 1:
            avg_fps = (frame_count - 1) / active_span

        unresolved_stall_s = 0.0
        if self._stall_start_ts is not None:
            unresolved_stall_s = max(0.0, now_mono - self._stall_start_ts)

        max_gap_s = max(self._max_gap_s, unresolved_stall_s)

        return {
            "video_frames": frame_count,
            "avg_fps": avg_fps,
            "active_span_s": active_span,
            "max_gap_s": max_gap_s,
            "recovered_stalls": self._recovered_stalls,
            "recovered_stalls_over_1s": self._stalls_over_1s,
            "recovered_stalls_over_3s": self._stalls_over_3s,
            "unresolved_stall_s": unresolved_stall_s,
            "stall_events": list(self._stall_events),
        }
