#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging

from debug_tools.auth import _prepare_auth_args
from debug_tools.list_cmd import cmd_list
from debug_tools.stream_cmd import cmd_stream


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
        default=45,
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
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
