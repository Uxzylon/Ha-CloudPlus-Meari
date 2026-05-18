from __future__ import annotations

import asyncio
import time
from typing import Any

from .codec_helpers import _coord_codec, _codec_policy, _codec_text


def _get_startup_bootstrap_state(coord: Any) -> dict[str, Any]:
    getter = getattr(coord, "get_startup_bootstrap_state", None)
    if callable(getter):
        try:
            return dict(getter())
        except Exception:
            pass

    validator = getattr(coord, "_is_valid_idr_seed", None)
    seed = getattr(coord, "_stream_idr_seed", b"")
    collecting = bool(getattr(coord, "_stream_idr_collecting", False))
    last_video = float(getattr(coord, "_last_p2p_video_time", 0.0))
    video_age_s = (time.monotonic() - last_video) if last_video > 0 else float("inf")
    seed_valid = bool(seed) and callable(validator) and bool(validator(seed))
    startup_safe = bool(seed_valid and not collecting and video_age_s < 1.0)
    return {
        "startup_safe": startup_safe,
        "block_reason": "ready" if startup_safe else "fallback-not-ready",
        "seed_valid": seed_valid,
        "seed_strong": seed_valid,
        "seed_video_bytes": 0,
        "seed_strength_reason": "",
        "collecting": collecting,
        "video_age_s": float(video_age_s),
        "latest_severe_gap_event": None,
    }


async def _await_startup_safe_bootstrap(
    coord: Any, timeout: float = 8.0
) -> tuple[bool, dict[str, Any]]:
    """Wait until bootstrap seed is safe for a fresh player join."""
    deadline = time.monotonic() + timeout
    last_state = _get_startup_bootstrap_state(coord)

    while time.monotonic() < deadline:
        last_state = _get_startup_bootstrap_state(coord)
        if bool(last_state.get("startup_safe", False)):
            return True, last_state
        await asyncio.sleep(0.1)

    return False, last_state


async def _await_clean_startup_seed(
    coord: Any,
    timeout: float = 12.0,
) -> tuple[bool, dict[str, Any]]:
    """Wait for a fully clean decode-probed startup seed before player launch."""
    deadline = time.monotonic() + timeout
    last_state = _get_startup_bootstrap_state(coord)

    while time.monotonic() < deadline:
        last_state = _get_startup_bootstrap_state(coord)
        startup_safe = bool(last_state.get("startup_safe", False))
        seed_strong = bool(last_state.get("seed_strong", False))
        reason = str(last_state.get("seed_strength_reason", "") or "")
        frames_since_seed = int(last_state.get("frames_since_seed", 0) or 0)
        preferred_join_mode = str(last_state.get("preferred_join_mode", "") or "")
        # strict path currently marks decode-probed frames as clean+decoded
        decode_probed = (
            "decoded" in reason or "validated" in reason or "params" in reason
        )
        if startup_safe and seed_strong and decode_probed and frames_since_seed >= 3:
            probe_fn = getattr(coord, "_probe_bootstrap_seed_decode", None)
            seed_bytes = bytes(getattr(coord, "_stream_idr_seed", b""))
            if callable(probe_fn) and seed_bytes:
                try:
                    ok_probe, probe_reason = probe_fn(seed_bytes, max_frames=6)
                except Exception:
                    ok_probe, probe_reason = False, "seed-probe-exception"
                last_state["clean_seed_probe_reason"] = probe_reason
                if ok_probe and preferred_join_mode in {"ready", "ready-backlog"}:
                    return True, last_state
            elif preferred_join_mode in {"ready", "ready-backlog"}:
                return True, last_state
        await asyncio.sleep(0.1)

    return False, last_state


def _count_recent_gap_events(
    coord: Any,
    *,
    severity: str,
    within_s: float,
) -> int:
    count_recent_fn = getattr(coord, "_count_recent_gap_events", None)
    if callable(count_recent_fn):
        try:
            return int(count_recent_fn(severity=severity, within_s=within_s) or 0)
        except Exception:
            return 0
    return 0


async def _await_adaptive_player_launch_gate(
    coord: Any,
    *,
    start_frames: int,
    timeout: float,
) -> tuple[bool, dict[str, Any]]:
    deadline = time.monotonic() + timeout
    gate_started_mono = time.monotonic()
    last_generation = int(getattr(coord, "_p2p_session_generation", 0) or 0)
    stable_since_mono = gate_started_mono
    last_state = _get_startup_bootstrap_state(coord)

    while time.monotonic() < deadline:
        now_mono = time.monotonic()
        generation = int(getattr(coord, "_p2p_session_generation", 0) or 0)
        if generation != last_generation:
            last_generation = generation
            stable_since_mono = now_mono

        last_state = _get_startup_bootstrap_state(coord)
        codec = _coord_codec(coord)
        codec_text = _codec_text(codec)
        policy = _codec_policy(codec)
        target_fps = float(getattr(coord, "_video_mux_target_fps", 15.0) or 15.0)
        target_fps = max(8.0, min(30.0, target_fps))
        frames = int(getattr(coord, "_p2p_video_frames", 0) or 0)
        frames_since_gate = max(0, frames - start_frames)
        video_age_s = float(last_state.get("video_age_s", 999.0) or 999.0)
        preferred_join_mode = str(last_state.get("preferred_join_mode", "") or "")
        adaptive_stream = bool(last_state.get("adaptive_stream", False))
        backlog_ready = bool(last_state.get("backlog_ready", False))
        backlog_target = max(
            2,
            int(last_state.get("backlog_follow_video_pusi_target", 0) or 0),
        )
        seed_generation = int(last_state.get("seed_generation", 0) or 0)
        required_generation = int(last_state.get("required_seed_generation", 0) or 0)
        startup_safe = bool(last_state.get("startup_safe", False))
        seed_strong = bool(last_state.get("seed_strong", False))
        seed_reason = str(last_state.get("seed_strength_reason", "") or "")

        recent_severe = _count_recent_gap_events(
            coord,
            severity="severe",
            within_s=policy.severe_gap_window_s,
        )
        recent_moderate = _count_recent_gap_events(
            coord,
            severity="moderate",
            within_s=4.2,
        )
        latest_severe = last_state.get("latest_severe_gap_event") or {}
        severe_status = str(latest_severe.get("status", "") or "")
        severe_started = float(latest_severe.get("started_mono", 0.0) or 0.0)
        severe_released = float(
            latest_severe.get("quarantine_release_mono", 0.0) or 0.0
        )
        severe_reference = max(severe_started, severe_released)
        severe_active = bool(latest_severe) and severe_status != "released"
        severe_recent = bool(severe_reference > 0.0) and (
            now_mono - severe_reference
        ) < max(0.7, min(2.2, 0.10 * backlog_target + 0.45))

        stable_for_s = max(0.0, now_mono - stable_since_mono)
        fast_frames = int(
            max(
                6,
                min(
                    18,
                    round(
                        max(
                            backlog_target * 2,
                            target_fps * policy.fast_frames_fps_factor,
                        )
                    ),
                ),
            )
        )
        stable_frames = int(
            max(
                fast_frames + 2,
                min(
                    28,
                    round(
                        max(
                            backlog_target * 3,
                            target_fps * policy.stable_frames_fps_factor,
                        )
                    ),
                ),
            )
        )
        fast_quiet_s = max(0.55, min(1.7, 0.08 * fast_frames + 0.15))
        stable_quiet_s = max(0.9, min(2.6, 0.09 * stable_frames + 0.25))
        wait_penalty_s = min(
            3.0,
            (0.9 * recent_severe)
            + (0.45 * recent_moderate)
            + (0.6 if severe_active else 0.0),
        )
        launch_budget_s = max(
            policy.launch_budget_min_s,
            min(
                policy.launch_budget_max_s,
                1.0 + stable_quiet_s + (0.12 * fast_frames) + wait_penalty_s,
            ),
        )
        waited_s = now_mono - gate_started_mono
        stale_source_s = max(1.4, min(3.1, 0.11 * fast_frames + 0.55))
        decode_probed = (
            "decoded" in seed_reason
            or "validated" in seed_reason
            or "params" in seed_reason
        )

        last_state.update(
            {
                "codec": codec_text,
                "session_generation": generation,
                "launch_gate_started_mono": gate_started_mono,
                "launch_gate_wait_s": waited_s,
                "launch_gate_budget_s": launch_budget_s,
                "launch_gate_frames_since_start": frames_since_gate,
                "launch_gate_fast_frames": fast_frames,
                "launch_gate_stable_frames": stable_frames,
                "launch_gate_fast_quiet_s": fast_quiet_s,
                "launch_gate_stable_quiet_s": stable_quiet_s,
                "stable_for_s": stable_for_s,
                "recent_severe_gap_count": recent_severe,
                "recent_moderate_gap_count": recent_moderate,
                "severe_gap_active": severe_active,
                "severe_gap_recent": severe_recent,
                "seed_decode_probed": decode_probed,
                "seed_strong": seed_strong,
            }
        )

        if (
            preferred_join_mode == "ready-backlog"
            and backlog_ready
            and seed_generation >= required_generation
            and frames_since_gate >= fast_frames
            and stable_for_s >= min(fast_quiet_s, policy.fast_quiet_cap_s)
            and video_age_s < 0.9
            and not severe_active
        ):
            last_state["launch_gate_reason"] = "ready-backlog"
            return True, last_state

        if (
            startup_safe
            and seed_generation >= required_generation
            and frames_since_gate >= stable_frames
            and stable_for_s >= stable_quiet_s
            and video_age_s < 0.85
            and recent_severe == 0
            and not severe_recent
        ):
            last_state["launch_gate_reason"] = "startup-safe"
            return True, last_state

        if (
            policy.allow_fast_live_flow
            and not adaptive_stream
            and frames_since_gate >= max(8, fast_frames - 2)
            and stable_for_s >= max(0.8, fast_quiet_s)
            and video_age_s < 0.7
            and seed_strong
            and seed_generation >= required_generation
            and recent_severe <= 1
            and not severe_active
        ):
            last_state["launch_gate_reason"] = "fast-live-flow"
            return True, last_state

        if video_age_s > stale_source_s and frames_since_gate >= max(
            4, fast_frames // 2
        ):
            last_state["launch_gate_reason"] = "source-stale"
            if not policy.clean_startup_seed:
                return False, last_state

        if waited_s >= launch_budget_s and not policy.clean_startup_seed:
            last_state["launch_gate_reason"] = "budget-expired"
            return False, last_state

        await asyncio.sleep(0.1)

    last_state["launch_gate_reason"] = "deadline-expired"
    return False, last_state
