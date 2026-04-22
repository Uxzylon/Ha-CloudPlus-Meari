"""NAL unit parsing and codec detection utilities"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Annex-B NAL unit iterators
# ---------------------------------------------------------------------------


def iter_nal_headers(data: bytes):
    """Yield Annex-B NAL header offsets from a bytestream."""
    i = 0
    n = len(data)
    while i < n - 3:
        if data[i] == 0 and data[i + 1] == 0:
            if data[i + 2] == 1:
                start = i + 3
            elif i + 3 < n and data[i + 2] == 0 and data[i + 3] == 1:
                start = i + 4
            else:
                i += 1
                continue
            if start < n:
                yield start
                i = start
                continue
        i += 1


def iter_annexb_nal_units(data: bytes):
    """Yield (nal_header_offset, full_nal_unit_with_start_code)."""
    marks: list[tuple[int, int]] = []
    i = 0
    n = len(data)
    while i < n - 3:
        if data[i] == 0 and data[i + 1] == 0:
            if data[i + 2] == 1:
                marks.append((i, i + 3))
                i += 3
                continue
            if i + 3 < n and data[i + 2] == 0 and data[i + 3] == 1:
                marks.append((i, i + 4))
                i += 4
                continue
        i += 1

    for idx, (unit_start, nal_start) in enumerate(marks):
        next_start = marks[idx + 1][0] if (idx + 1) < len(marks) else n
        if nal_start >= next_start:
            continue
        yield nal_start, bytes(data[unit_start:next_start])


# ---------------------------------------------------------------------------
# Codec detection
# ---------------------------------------------------------------------------


def detect_video_codec(data: bytes) -> str | None:
    """Detect camera payload codec from Annex-B NAL units."""
    h264_ps = 0
    hevc_ps = 0
    h264_score = 0
    hevc_score = 0

    for idx, off in enumerate(iter_nal_headers(data)):
        if idx >= 24:
            break
        b0 = data[off]
        h264_type = b0 & 0x1F
        if h264_type in (7, 8):
            h264_ps += 1
            h264_score += 3
        elif h264_type in (1, 5, 6, 9):
            h264_score += 1

        if off + 1 >= len(data):
            continue
        b1 = data[off + 1]
        hevc_type = (b0 >> 1) & 0x3F
        tid_plus1 = b1 & 0x07
        if tid_plus1 == 0:
            continue
        if hevc_type in (32, 33, 34):
            hevc_ps += 1
            hevc_score += 3
        elif hevc_type in (0, 1, 19, 20, 21, 39, 40):
            hevc_score += 1

    if h264_ps and not hevc_ps:
        return "h264"
    if hevc_ps and not h264_ps:
        return "hevc"
    if h264_score >= hevc_score + 2:
        return "h264"
    if hevc_score >= h264_score + 2:
        return "hevc"
    return None


def is_video_keyframe(data: bytes, codec: str) -> bool:
    """Return True if payload contains a keyframe for the given codec."""
    codec = (codec or "hevc").lower()
    for off in iter_nal_headers(data):
        b0 = data[off]
        if codec == "h264":
            nal_type = b0 & 0x1F
            if nal_type in (5, 7, 8):
                return True
        else:
            nal_type = (b0 >> 1) & 0x3F
            if nal_type in (32, 33, 34, 19, 20):
                return True
    return False


def collect_nal_types(data: bytes, codec: str) -> set[int]:
    """Return NAL unit types present in a bytestream payload."""
    codec = (codec or "hevc").lower()
    types: set[int] = set()
    for off in iter_nal_headers(data):
        b0 = data[off]
        if codec == "h264":
            types.add(b0 & 0x1F)
            continue
        if off + 1 >= len(data):
            continue
        b1 = data[off + 1]
        if (b1 & 0x07) == 0:
            continue
        types.add((b0 >> 1) & 0x3F)
    return types
