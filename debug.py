#!/usr/bin/env python3
"""CloudPlus camera local debug harness entry point."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import IO

from debug_tools.auth import _prepare_auth_args
from debug_tools.list_cmd import cmd_list
from debug_tools.stream_cmd import cmd_stream


class _Tee:
    def __init__(self, primary: IO[str], mirror: IO[str]) -> None:
        self._primary = primary
        self._mirror = mirror

    def write(self, data: str) -> int:
        self._primary.write(data)
        self._mirror.write(data)
        return len(data)

    def flush(self) -> None:
        self._primary.flush()
        self._mirror.flush()

    def __getattr__(self, name: str):
        return getattr(self._primary, name)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CloudPlus local debug harness")
    parser.add_argument("--email", help="Account email; defaults to .env")
    parser.add_argument("--password", help="Account password; defaults to .env")
    parser.add_argument("--country-code", help="Country code; defaults to .env or FR")
    parser.add_argument("--phone-code", help="Phone code; defaults to .env or 33")
    parser.add_argument(
        "--profile",
        choices=["cloudedge", "cloudplus", "iegeek"],
        help="App profile; defaults to .env or cloudedge",
    )
    parser.add_argument("--debug", action="store_true", help="Enable verbose logs")

    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="Login and list cameras")

    stream = commands.add_parser("stream", help="Open a camera stream in ffplay")
    stream.add_argument("--device-id", type=int, help="Camera deviceID")
    stream.add_argument("--sn", help="Camera serial number")
    stream.add_argument(
        "--duration",
        type=int,
        default=0,
        help="ffplay open time in seconds; 0 runs until Ctrl+C",
    )
    stream.add_argument(
        "--wake-timeout",
        type=int,
        default=90,
        help="Seconds to wait for live frames before launching playback",
    )
    stream.add_argument(
        "--quality",
        help="AUTO, SD, HD, QHD, or numeric profile id; defaults to integration setting",
    )
    stream.add_argument("--video-password", help="Camera video encryption password")
    stream.add_argument(
        "--output-file",
        default="",
        help="Optional artifact basename for .ts/.wav/player/recorder logs",
    )
    stream.add_argument(
        "--analysis-mode",
        choices=["ffplay", "full"],
        default="ffplay",
        help="Use ffplay-only verdicts, or include recorder/TS/PCM diagnostics",
    )
    stream.add_argument(
        "--capture",
        nargs="?",
        const="",
        default=None,
        metavar="PATH",
        help="Capture host traffic with tcpdump for the stream's lifetime "
        "(needs root/sudo). Bare flag auto-names a .pcap next to the artifacts; "
        "pass a PATH to override. Diff against the official-app captures.",
    )
    stream.add_argument(
        "--capture-filter",
        default="udp",
        help="tcpdump BPF filter for --capture (default: udp — root discovery, "
        "STUN/TURN and P2P; skips HTTPS API noise)",
    )
    stream.add_argument(
        "--capture-iface",
        default="any",
        metavar="IFACE",
        help="Interface for --capture (default: any). Set to the LAN interface "
        "facing the camera (e.g. wlan0) to capture the direct P2P path cleanly "
        "without unrelated docker-bridge/VPN noise.",
    )
    stream.add_argument(
        "--log-file",
        default="",
        metavar="PATH",
        help="Tee all CLI output (logs + analysis) to this file as well as the "
        "terminal. Pairs with --capture for a full text+pcap record of a run.",
    )
    return parser


async def async_main(args: argparse.Namespace) -> int:
    if args.command == "list":
        return await cmd_list(args)
    if args.command == "stream":
        return await cmd_stream(args)
    raise RuntimeError(f"Unknown command: {args.command}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    _prepare_auth_args(parser, args)
    log_file_arg = str(getattr(args, "log_file", "") or "").strip()
    log_fh = None
    if log_file_arg:
        log_path = os.path.abspath(os.path.expanduser(log_file_arg))
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        log_fh = open(log_path, "w", encoding="utf-8")  # pylint: disable=consider-using-with
        sys.stdout = _Tee(sys.stdout, log_fh)
        sys.stderr = _Tee(sys.stderr, log_fh)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        return asyncio.run(async_main(args))
    finally:
        if log_fh is not None:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            log_fh.flush()
            log_fh.close()


if __name__ == "__main__":
    raise SystemExit(main())
