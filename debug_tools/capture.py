"""Optional host-side packet capture for `debug.py stream` sessions.

Runs ``tcpdump`` alongside a stream so a flaky / stalling session can be diffed
against the official-app captures. Defaults to ``-i any`` so VPN tunnel
interfaces are included (relevant when toggling a VPN changes the public IP the
relay maps), and to a ``udp`` filter — root discovery (9253), STUN/TURN (9100)
and the P2P media live here, which is where relay-path failures show up. HTTPS
API noise is skipped. ``-U`` flushes per packet, so the pcap stays valid even if
the process is killed abruptly.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import time

_LOGGER = logging.getLogger(__name__)

DEFAULT_FILTER = "udp"


class PacketCapture:
    """Start/stop a tcpdump sidecar writing a pcap for the stream's lifetime."""

    def __init__(
        self, path: str, *, filter_expr: str = DEFAULT_FILTER, iface: str = "any"
    ) -> None:
        self.path = path
        self.filter_expr = filter_expr or DEFAULT_FILTER
        self.iface = iface or "any"
        self._proc: subprocess.Popen | None = None

    def start(self) -> bool:
        if shutil.which("tcpdump") is None:
            _LOGGER.warning("Capture requested but tcpdump not on PATH; skipping")
            return False
        argv = ["tcpdump", "-i", self.iface, "-n", "-s", "0", "-U", "-w", self.path]
        argv += self.filter_expr.split()
        if os.geteuid() != 0:
            # Pre-authenticate so the sudo prompt happens cleanly up front rather
            # than racing the liveness poll below.
            try:
                subprocess.run(["sudo", "-v"], check=True)
            except (subprocess.CalledProcessError, OSError) as err:
                _LOGGER.warning("sudo auth failed; capture skipped: %s", err)
                return False
            argv = ["sudo", *argv]
        _LOGGER.info(
            "Starting packet capture: %s (filter: %s)", self.path, self.filter_expr
        )
        try:
            self._proc = subprocess.Popen(
                argv, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
            )
        except OSError as err:
            _LOGGER.warning("Could not start tcpdump: %s", err)
            self._proc = None
            return False
        time.sleep(0.6)
        if self._proc.poll() is not None:
            err = (self._proc.stderr.read() or b"").decode(errors="replace").strip()
            tail = err.splitlines()[-1] if err else "unknown error"
            _LOGGER.warning("tcpdump exited immediately; no capture (%s)", tail)
            self._proc = None
            return False
        return True

    def stop(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        try:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.kill()
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        try:
            _LOGGER.info(
                "Packet capture saved: %s (%d bytes)",
                self.path,
                os.path.getsize(self.path),
            )
        except OSError:
            _LOGGER.warning("Packet capture file missing: %s", self.path)
