from __future__ import annotations

import logging
import shutil
import signal
import subprocess
from typing import Callable


def _build_stream_player_cmd(
    url: str,
    duration: int = 0,
) -> list[str]:
    """Build ffplay command for visible live playback."""
    if not shutil.which("ffplay"):
        raise RuntimeError("ffplay not found")

    fflags = "+discardcorrupt"
    sync_mode = "audio"
    include_framedrop = False

    cmd = [
        "ffplay",
        "-hide_banner",
        "-stats",
        "-loglevel",
        "verbose",
        "-window_title",
        "CloudEdge live",
        "-fflags",
        fflags,
    ]
    if include_framedrop:
        cmd.extend(["-framedrop"])
    cmd.extend(
        [
            "-sync",
            sync_mode,
            "-analyzeduration",
            "1000000",
            "-probesize",
            "524288",
            "-vf",
            "showinfo",
            "-f",
            "mpegts",
        ]
    )
    if int(duration) > 0:
        cmd.extend(["-t", str(int(duration))])
    cmd.append(url)
    return cmd


def _build_stream_recorder_cmd(
    url: str,
    output_path: str,
    duration: int = 0,
) -> list[str]:
    """Build ffmpeg command to record the TCP stream to a .ts file.

    Connects as a second TCP client to the stream server, so it gets
    the same data the player sees without interfering with playback.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostdin",
        "-rw_timeout",
        "20000000",
        "-fflags",
        "+discardcorrupt+nobuffer",
        "-analyzeduration",
        "20000000",
        "-probesize",
        "8388608",
        "-f",
        "mpegts",
        "-i",
        url,
        "-c",
        "copy",
        "-f",
        "mpegts",
        "-y",
        output_path,
    ]
    if int(duration) > 0:
        cmd.insert(-4, "-t")
        cmd.insert(-4, str(int(duration)))
    return cmd


def _stop_player_process(proc: subprocess.Popen | None) -> None:
    """Terminate ffplay reliably so CLI exits without manual Ctrl+C."""
    if proc is None or proc.poll() is not None:
        return

    attempts: list[tuple[str, Callable[[], None], float]] = [
        ("SIGINT", lambda: proc.send_signal(signal.SIGINT), 1.0),
        ("terminate", proc.terminate, 1.5),
        ("kill", proc.kill, 1.0),
    ]
    for label, action, timeout_s in attempts:
        if proc.poll() is not None:
            return
        try:
            action()
        except Exception:
            continue
        try:
            proc.wait(timeout=timeout_s)
            return
        except subprocess.TimeoutExpired:
            logging.getLogger(__name__).debug(
                "ffplay did not exit after %s, escalating", label
            )
        except Exception:
            return


def _build_pcm_recorder_cmd(
    url: str,
    output_path: str,
    duration: int = 0,
) -> list[str]:
    """Build ffmpeg command to extract raw PCM audio from the stream.

    Connects as a third TCP client (same TS data) and decodes audio to
    16-bit signed-LE mono 16 kHz WAV for objective silence analysis.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostdin",
        "-rw_timeout",
        "20000000",
        "-fflags",
        "+discardcorrupt+nobuffer",
        "-analyzeduration",
        "20000000",
        "-probesize",
        "8388608",
        "-f",
        "mpegts",
        "-i",
        url,
        "-vn",  # drop video
        "-acodec",
        "pcm_s16le",  # raw PCM
        "-ar",
        "16000",  # 16 kHz (matches our AAC encoder)
        "-ac",
        "1",  # mono
        "-f",
        "wav",
        "-y",
        output_path,
    ]
    if int(duration) > 0:
        cmd.insert(-4, "-t")
        cmd.insert(-4, str(int(duration)))
    return cmd
