"""Codec lookup and formatting helpers bridging the harness to the integration."""

from __future__ import annotations

import sys

from typing import Any

from .bootstrap import _bootstrap_integration_modules


def _codec_module() -> Any:
    mod = sys.modules.get("custom_components.cloudplus.p2p_streamer.codecs")
    if mod is None:
        _bootstrap_integration_modules()
        mod = sys.modules["custom_components.cloudplus.p2p_streamer.codecs"]
    return mod


def _codec_name(value: Any) -> Any:
    return _codec_module().CodecName.parse(value)


def _coord_codec(coord: Any) -> Any:
    return _codec_name(getattr(coord, "_video_codec", None))


def _codec_text(codec: Any) -> str:
    return str(getattr(codec, "value", codec)).lower()


def _codec_policy(codec: Any) -> Any:
    return _codec_module().runtime_policy_for(codec)
