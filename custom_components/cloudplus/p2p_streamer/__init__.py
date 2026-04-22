"""P2P streaming engine for CloudEdge / Meari cameras.

This package implements signaling, TURN, ICE, KCP, VVP and stream
decryption. Sub-modules:

  protocol.py  — VVP constants, packet builders, quality profile utils
  codec.py     — Stream frame decryption, parsing, HEVC NAL utilities
  network.py   — Local IP detection, signaling resolution, ICE helpers
  _streamer.py — P2PStreamer class (the main public interface)
"""

from .protocol import (
    VVP_MAGIC,
    VVP_CMD_START_LIVE,
    VVP_CMD_STOP,
    VVP_CMD_HEARTBEAT,
    VVP_HEADER_SIZE,
    STREAM_TYPE_INFO,
    STREAM_TYPE_AUDIO,
    STREAM_TYPE_IFRAME,
    STREAM_TYPE_PFRAME,
    STREAM_TYPE_PHOTO,
    STREAM_ENCRYPT_KEY,
    format_licence_id,
    parse_quality_profiles,
    _best_quality_from_device,
    build_vvp_auth_md5,
    build_vvp_packet,
)
from .codec import (
    decrypt_stream_frame,
    parse_stream_frame,
    split_stream_frames,
    _is_idr_video_frame,
    _iter_annexb_nals,
    _find_annexb_start_code,
)
from .network import (
    _is_private_ip,
    _get_local_ips,
    _resolve_signaling_server,
    _build_ice_response,
    _send_direct_ice_binding,
)
from ._streamer import P2PStreamer

__all__ = [
    # Core class
    "P2PStreamer",
    # Protocol
    "VVP_MAGIC",
    "VVP_CMD_START_LIVE",
    "VVP_CMD_STOP",
    "VVP_CMD_HEARTBEAT",
    "VVP_HEADER_SIZE",
    "STREAM_TYPE_INFO",
    "STREAM_TYPE_AUDIO",
    "STREAM_TYPE_IFRAME",
    "STREAM_TYPE_PFRAME",
    "STREAM_TYPE_PHOTO",
    "STREAM_ENCRYPT_KEY",
    "format_licence_id",
    "parse_quality_profiles",
    "build_vvp_auth_md5",
    "build_vvp_packet",
    # Codec
    "decrypt_stream_frame",
    "parse_stream_frame",
    "split_stream_frames",
    "_is_idr_video_frame",
    # Network
    "_is_private_ip",
    "_get_local_ips",
    "_resolve_signaling_server",
    "_build_ice_response",
    "_send_direct_ice_binding",
]
