"""Shared ffmpeg subprocess helpers for the audio encoder and muxer."""

from __future__ import annotations

import subprocess

# Common ffmpeg invocation prefix: quiet banner, warnings only.
FFMPEG_BASE_CMD = ["ffmpeg", "-hide_banner", "-loglevel", "warning"]


def terminate_process(proc: subprocess.Popen | None) -> None:
    """Best-effort teardown of an ffmpeg subprocess.

    Closes stdin, asks the process to terminate, then escalates to kill.
    Every step is guarded so teardown never raises.
    """
    if proc is None:
        return
    try:
        if proc.stdin is not None:
            proc.stdin.close()
    except (OSError, ValueError):
        pass
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except (OSError, subprocess.SubprocessError):
        try:
            proc.kill()
            proc.wait(timeout=1)
        except (OSError, subprocess.SubprocessError):
            pass
