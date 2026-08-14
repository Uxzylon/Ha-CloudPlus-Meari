"""Codec registry for stream payload detection."""

from __future__ import annotations

from .av1 import detect_av1
from .base import (
    DEFAULT_GAP_RECOVERY,
    CodecName,
    CodecRuntimePolicy,
    CodecSpec,
    GapRecoveryPolicy,
)
from .h264 import (
    ARRIVAL_TIMED_MUX as H264_ARRIVAL_TIMED_MUX,
    GAP_RECOVERY as H264_GAP_RECOVERY,
    KEYFRAME_NAL_TYPES as H264_KEYFRAMES,
    MPEGTS_STREAM_TYPE as H264_MPEGTS,
    PARAM_NAL_TYPES as H264_PARAMS,
    RUNTIME_POLICY as H264_RUNTIME,
    STARTUP_BACKLOG_FRAMES as H264_BACKLOG,
    detect_h264,
    live_pacing_buffer as h264_live_pacing_buffer,
    nal_type as h264_nal_type,
)
from .hevc import (
    ADAPTIVE_ARRIVAL_TIMED_MUX as HEVC_ADAPTIVE_ARRIVAL_TIMED_MUX,
    ADAPTIVE_TIMESTAMP_TIMED_MUX as HEVC_ADAPTIVE_TIMESTAMP_TIMED_MUX,
    GAP_RECOVERY as HEVC_GAP_RECOVERY,
    KEYFRAME_NAL_TYPES as HEVC_KEYFRAMES,
    MPEGTS_STREAM_TYPE as HEVC_MPEGTS,
    PARAM_NAL_TYPES as HEVC_PARAMS,
    RUNTIME_POLICY as HEVC_RUNTIME,
    STARTUP_BACKLOG_FRAMES as HEVC_BACKLOG,
    TIMESTAMP_TIMED_MUX as HEVC_TIMESTAMP_TIMED_MUX,
    detect_hevc,
    live_pacing_buffer as hevc_live_pacing_buffer,
    nal_type as hevc_nal_type,
)

REGISTRY: tuple[CodecSpec, ...] = (
    CodecSpec(
        name=CodecName.H264,
        ffmpeg_demuxer=CodecName.H264.value,
        detect=detect_h264,
        recovery=H264_GAP_RECOVERY,
        mpegts_stream_type=H264_MPEGTS,
        param_nal_types=H264_PARAMS,
        keyframe_nal_types=H264_KEYFRAMES,
        nal_type=h264_nal_type,
        startup_backlog_frames=H264_BACKLOG,
        arrival_timed_mux=H264_ARRIVAL_TIMED_MUX,
        runtime=H264_RUNTIME,
    ),
    CodecSpec(
        name=CodecName.HEVC,
        ffmpeg_demuxer=CodecName.HEVC.value,
        detect=detect_hevc,
        recovery=HEVC_GAP_RECOVERY,
        mpegts_stream_type=HEVC_MPEGTS,
        param_nal_types=HEVC_PARAMS,
        keyframe_nal_types=HEVC_KEYFRAMES,
        nal_type=hevc_nal_type,
        startup_backlog_frames=HEVC_BACKLOG,
        arrival_timed_mux=False,
        adaptive_arrival_timed_mux=HEVC_ADAPTIVE_ARRIVAL_TIMED_MUX,
        timestamp_timed_mux=HEVC_TIMESTAMP_TIMED_MUX,
        adaptive_timestamp_timed_mux=HEVC_ADAPTIVE_TIMESTAMP_TIMED_MUX,
        runtime=HEVC_RUNTIME,
    ),
    CodecSpec(
        name=CodecName.AV1, ffmpeg_demuxer=CodecName.AV1.value, detect=detect_av1
    ),
)


def spec_for(codec_name: CodecName | str | None) -> CodecSpec:
    codec = CodecName.parse(codec_name)
    for spec in REGISTRY:
        if spec.name == codec:
            return spec
    return REGISTRY[1]


def identify_codec(payload: bytes) -> CodecName | None:
    for spec in REGISTRY:
        try:
            if spec.detect(payload):
                return spec.name
        except (IndexError, ValueError):
            continue
    return None


def detect_codec(
    payload: bytes,
    default: CodecName | str = CodecName.HEVC,
) -> CodecName:
    detected = identify_codec(payload)
    if detected is not None:
        return detected
    return CodecName.parse(default)


def demuxer_for(codec_name: CodecName | str) -> str:
    return spec_for(codec_name).ffmpeg_demuxer


def gap_recovery_for(codec_name: CodecName | str) -> GapRecoveryPolicy:
    codec = CodecName.parse(codec_name)
    for spec in REGISTRY:
        if spec.name == codec:
            return spec.recovery
    return DEFAULT_GAP_RECOVERY


def mpegts_stream_type_for(codec_name: CodecName | str) -> int:
    return spec_for(codec_name).mpegts_stream_type


def codec_from_mpegts_stream_type(stream_type: int) -> CodecName | None:
    for spec in REGISTRY:
        if spec.mpegts_stream_type == stream_type:
            return spec.name
    return None


def startup_backlog_frames(codec_name: CodecName | str) -> int:
    return spec_for(codec_name).startup_backlog_frames


def uses_arrival_timed_mux(
    codec_name: CodecName | str,
    *,
    adaptive: bool = False,
) -> bool:
    spec = spec_for(codec_name)
    if adaptive and spec.adaptive_arrival_timed_mux is not None:
        return spec.adaptive_arrival_timed_mux
    return spec.arrival_timed_mux


def uses_timestamp_timed_mux(
    codec_name: CodecName | str,
    *,
    adaptive: bool = False,
) -> bool:
    spec = spec_for(codec_name)
    if adaptive and spec.adaptive_timestamp_timed_mux is not None:
        return spec.adaptive_timestamp_timed_mux
    return spec.timestamp_timed_mux


def runtime_policy_for(codec_name: CodecName | str) -> CodecRuntimePolicy:
    return spec_for(codec_name).runtime


def codec_live_pacing_buffer(
    codec_name: CodecName | str,
    device: dict,
    profiles: list[int],
    *,
    adaptive: bool,
) -> float:
    """Return the live mux pacing buffer requested by a codec family."""
    codec = CodecName.parse(codec_name)
    if codec is CodecName.H264:
        return h264_live_pacing_buffer(device, profiles, adaptive=adaptive)
    if codec is CodecName.HEVC:
        return hevc_live_pacing_buffer(device, profiles, adaptive=adaptive)
    return 0.0


def annexb_units(payload: bytes) -> list[bytes]:
    starts: list[tuple[int, int]] = []
    i = 0
    while i + 3 < len(payload):
        if payload[i] == 0 and payload[i + 1] == 0:
            if payload[i + 2] == 1:
                starts.append((i, 3))
                i += 3
                continue
            if i + 3 < len(payload) and payload[i + 2] == 0 and payload[i + 3] == 1:
                starts.append((i, 4))
                i += 4
                continue
        i += 1
    return [
        payload[start : starts[idx + 1][0] if idx + 1 < len(starts) else len(payload)]
        for idx, (start, _sc_len) in enumerate(starts)
    ]


def nal_payload(unit: bytes) -> bytes:
    if unit.startswith(b"\x00\x00\x01"):
        return unit[3:]
    if unit.startswith(b"\x00\x00\x00\x01"):
        return unit[4:]
    return unit


def nal_types(codec_name: CodecName | str, payload: bytes) -> set[int]:
    spec = spec_for(codec_name)
    if spec.nal_type is None:
        return set()
    out: set[int] = set()
    for unit in annexb_units(payload):
        nal_type = spec.nal_type(nal_payload(unit))
        if nal_type is not None:
            out.add(nal_type)
    return out


def is_keyframe(codec_name: CodecName | str, payload: bytes) -> bool:
    spec = spec_for(codec_name)
    return bool(nal_types(spec.name, payload) & spec.keyframe_nal_types)


def is_recovery_keyframe(payload: bytes, *, require_param_sets: bool = True) -> bool:
    codec = detect_codec(payload)
    spec = spec_for(codec)
    types = nal_types(codec, payload)
    if not types & spec.keyframe_nal_types:
        return False
    if codec is CodecName.H264 or not require_param_sets:
        return True
    return bool(types & spec.param_nal_types)


class CodecParameterCache:
    """Cache of per-codec parameter-set NAL units (SPS/PPS/VPS)."""

    def __init__(self) -> None:
        self._units: dict[CodecName, dict[int, bytes]] = {}

    def clear(self, codec_name: CodecName | str | None = None) -> None:
        if codec_name is None:
            self._units.clear()
            return
        self._units.pop(CodecName.parse(codec_name), None)

    def update(self, codec_name: CodecName | str, payload: bytes) -> None:
        codec = CodecName.parse(codec_name)
        spec = spec_for(codec)
        if spec.nal_type is None:
            return
        bucket = self._units.setdefault(codec, {})
        for unit in annexb_units(payload):
            nal_type = spec.nal_type(nal_payload(unit))
            if nal_type in spec.param_nal_types:
                bucket[nal_type] = unit

    def ready(self, codec_name: CodecName | str) -> bool:
        codec = CodecName.parse(codec_name)
        spec = spec_for(codec)
        return spec.param_nal_types.issubset(self._units.get(codec, {}))

    def with_params(self, codec_name: CodecName | str, payload: bytes) -> bytes:
        codec = CodecName.parse(codec_name)
        spec = spec_for(codec)
        if not is_keyframe(codec, payload):
            return payload
        present = nal_types(codec, payload)
        cached = self._units.get(codec, {})
        prefix = [
            cached[nal_type]
            for nal_type in spec.param_nal_types
            if nal_type not in present and nal_type in cached
        ]
        return b"".join(prefix) + payload if prefix else payload
