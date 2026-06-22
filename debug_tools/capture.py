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
import tempfile
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
        self._stderr_path: str | None = None
        self._started_at = 0.0

    def start(self) -> bool:
        if shutil.which("tcpdump") is None:
            _LOGGER.warning("Capture requested but tcpdump not on PATH; skipping")
            return False
        argv = ["tcpdump", "-i", self.iface, "-n", "-s", "0", "-U", "-w", self.path]
        argv += self.filter_expr.split()
        if os.geteuid() != 0:
            if not self._authenticate_sudo():
                return False
            argv = ["sudo", "-n", *argv]
        _LOGGER.info(
            "Starting packet capture: %s (filter: %s)", self.path, self.filter_expr
        )
        self._started_at = time.time()
        try:
            os.unlink(self.path)
        except FileNotFoundError:
            pass
        except OSError as err:
            _LOGGER.warning("Could not remove old capture %s: %s", self.path, err)
        stderr_file = tempfile.NamedTemporaryFile(
            prefix="cloudedge_tcpdump_", suffix=".log", delete=False
        )
        self._stderr_path = stderr_file.name
        try:
            with stderr_file:
                self._proc = subprocess.Popen(  # pylint: disable=consider-using-with
                    argv,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=stderr_file,
                    process_group=0,
                )
        except OSError as err:
            _LOGGER.warning("Could not start tcpdump: %s", err)
            self._cleanup_stderr_log()
            self._proc = None
            return False
        time.sleep(0.6)
        if self._proc.poll() is not None:
            _LOGGER.warning(
                "tcpdump exited immediately; no capture (rc=%s, %s)",
                self._proc.returncode,
                self._stderr_tail(),
            )
            self._cleanup_stderr_log()
            self._proc = None
            return False
        return True

    def _authenticate_sudo(self) -> bool:
        prompt = "[sudo] password for %u : "
        tty_fd: int | None = None
        try:
            tty_fd = os.open("/dev/tty", os.O_RDWR)
            subprocess.run(
                ["sudo", "-p", prompt, "-v"],
                stdin=tty_fd,
                stdout=tty_fd,
                stderr=tty_fd,
                check=True,
            )
        except OSError as err:
            _LOGGER.warning("sudo auth failed; no interactive terminal: %s", err)
            return False
        except subprocess.CalledProcessError as err:
            _LOGGER.warning("sudo auth failed; capture skipped: %s", err)
            return False
        finally:
            if tty_fd is not None:
                os.close(tty_fd)
        try:
            subprocess.run(
                ["sudo", "-n", "-v"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                check=True,
            )
        except subprocess.CalledProcessError as err:
            tail = (err.stderr or b"").decode(errors="replace").strip()
            _LOGGER.warning(
                "sudo auth did not create a reusable ticket; capture skipped: %s",
                tail or err,
            )
            return False
        return True

    def stop(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        try:
            os.killpg(proc.pid, signal.SIGINT)
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
        except (OSError, subprocess.SubprocessError):
            try:
                proc.kill()
            except (OSError, subprocess.SubprocessError):
                pass
        rc = proc.poll()
        stderr_tail = self._stderr_tail()
        self._cleanup_stderr_log()
        try:
            stat = os.stat(self.path)
        except OSError:
            _LOGGER.warning(
                "Packet capture file missing: %s (tcpdump rc=%s, %s)",
                self.path,
                rc,
                stderr_tail,
            )
            return
        if stat.st_mtime < self._started_at or stat.st_size <= 24:
            _LOGGER.warning(
                "Packet capture not updated: %s (%d bytes, mtime %.0f < start %.0f, "
                "tcpdump rc=%s, %s)",
                self.path,
                stat.st_size,
                stat.st_mtime,
                self._started_at,
                rc,
                stderr_tail,
            )
            return
        _LOGGER.info("Packet capture saved: %s (%d bytes)", self.path, stat.st_size)

    def _stderr_tail(self) -> str:
        if not self._stderr_path:
            return "no tcpdump stderr"
        try:
            with open(self._stderr_path, "r", encoding="utf-8", errors="replace") as fh:
                lines = [line.strip() for line in fh.readlines() if line.strip()]
        except OSError as err:
            return f"tcpdump stderr unavailable: {err}"
        if not lines:
            return "no tcpdump stderr"
        return lines[-1]

    def _cleanup_stderr_log(self) -> None:
        if not self._stderr_path:
            return
        try:
            os.unlink(self._stderr_path)
        except OSError:
            pass
        self._stderr_path = None
