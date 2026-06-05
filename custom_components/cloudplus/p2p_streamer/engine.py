"""Minimal P2P streamer for CloudPlus / Meari cameras."""

from __future__ import annotations

import logging
import hashlib
import os
import socket
import threading
import time
from typing import Any, Callable

from ..api import format_sn
from ..meari_signaling import MsgSvrClient
from ..turn_client import BINDING_REQUEST, BINDING_RESPONSE, TurnClient
from ..kcp_tunnel import (
    KCP_CMD_PUSH,
    KCP_WND,
    KcpTunnel,
    parse_iva_frame,
    parse_kcp_segment,
    parse_kcp_segments,
)
from .network import (
    _build_ice_response,
    _cache_signaling_endpoint,
    _forget_signaling_endpoint,
    _get_local_ips,
    _resolve_signaling_server_candidates,
    _send_direct_ice_binding,
    recv_peer_packets,
)
from .lan import build_lan_connect_frame, host_candidate_endpoints
from .sdp import (
    add_candidate_once,
    candidates_from_response,
    collect_trickle_candidates,
    format_endpoint,
    parse_sdp_answer,
)
from .codecs import (
    CodecName,
    detect_codec,
    gap_recovery_for,
    identify_codec,
    is_recovery_keyframe,
    nal_types,
    runtime_policy_for,
)
from .protocol import (
    VVP_CMD_HEARTBEAT,
    VVP_CMD_STOP,
    VVP_CMD_START_LIVE,
    STREAM_TYPE_AUDIO,
    STREAM_TYPE_IFRAME,
    STREAM_TYPE_PFRAME,
    build_vvp_packet,
    format_licence_id,
)
from .relay_probe import probe_relay
from .codec import (
    FrameSequenceTracker,
    decrypt_stream_frame,
    parse_stream_frame,
    split_stream_frames,
)
from .keepalive import SnapDeviceKeepalive
from .quality import stream_id_for_quality

_LOGGER = logging.getLogger(__name__)

IVA_HEARTBEAT_S = 3.0
VVP_HEARTBEAT_S = 10.0
AUTH_FALLBACK_NO_VIDEO_S = 10.0
DORMANCY_WAKE_TIMEOUT_S = 45.0
WAKE_RETRY_S = 4.0
AUTH_FALLBACK_RESULT = (-1, -1)
SIGNALING_CONNECT_TIMEOUT_S = 5.0
CLIENT_KEYFRAME_REQUEST_DEBOUNCE_S = 4.0
_CLIENT_SESSION_LOCK = threading.Lock()


class SignalingClusterMiss(RuntimeError):
    """The MsgSvr endpoint is reachable but does not know this device."""


def _unwrap_iva_payload(data: bytes) -> bytes:
    if len(data) >= 20 and data[0:2] == b"\xff\x01":
        iva = parse_iva_frame(data)
        if iva:
            type_marker, _, _, payload = iva
            if type_marker == 0x7012:
                return b""
            return payload
    return data


def _client_uuid_for(api: Any, client_id: str | None = None) -> str:
    override = str(os.environ.get("CLOUDPLUS_CLIENT_UUID") or "").strip().lower()
    if len(override) == 16 and all(ch in "0123456789abcdef" for ch in override):
        return override
    identity = str(client_id or getattr(api, "user_id", "") or "").strip()
    cache = getattr(api, "_p2p_client_uuids", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(api, "_p2p_client_uuids", cache)
    existing = str(cache.get(identity) or "")
    if existing:
        return existing
    seed = "|".join(
        (
            "cloudplus-p2p",
            str(getattr(api, "app_profile", "") or ""),
            identity,
        )
    )
    value = hashlib.md5(seed.encode()).hexdigest()[:16]
    cache[identity] = value
    return value


def _next_session_index(api: Any) -> int:
    with _CLIENT_SESSION_LOCK:
        current = int(getattr(api, "_p2p_session_index", 0) or 0)
        index = current + 1 if current < 99 else 1
        setattr(api, "_p2p_session_index", index)
        return index


def _client_id_for(api: Any, device: dict[str, Any], override: Any = None) -> str:
    for value in (
        override,
        device.get("_iot_client_id"),
        device.get("iotClientId"),
        device.get("clientId"),
        getattr(api, "user_id", None),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return "0"


class P2PStreamer:
    """Runs a CloudPlus/Meari camera P2P stream using TURN + KCP + VVP."""

    def __init__(
        self,
        api: Any,
        device: dict[str, Any],
        *,
        on_video: Callable[[bytes, int | None], None] | None = None,
        on_audio: Callable[[bytes], None] | None = None,
        on_login: Callable[[], None] | None = None,
        on_disconnect: Callable[[], None] | None = None,
        remote: bool = False,
        vvp_quality: int | None = None,
        video_password: str | None = None,
        client_id: Any = None,
    ) -> None:
        self._api = api
        self._device = device
        self._sn_num = device["snNum"]
        self._device_uuid = format_sn(str(self._sn_num))
        self._is_snap = str(device.get("_category", "")).lower() == "snap"
        self._host_key = device.get("hostKey", "")
        self._video_password = (video_password or "").strip()
        self._remote = remote
        self._client_id = _client_id_for(api, device, client_id)
        self._client_uuid = _client_uuid_for(api, self._client_id)
        self._webrtc_session_index = _next_session_index(api)
        self._vvp_stream_id = stream_id_for_quality(device, vvp_quality)
        app_profile = str(getattr(api, "app_profile", "") or "").lower()
        self._vvp_stream_flag = 0 if app_profile == "cloudedge" else 1

        self.on_video = on_video
        self.on_audio = on_audio
        self.on_login = on_login
        self.on_disconnect = on_disconnect

        self._running = False
        self._video_count = 0
        self._source_video_count = 0
        self._total_bytes = 0
        self._audio_count = 0
        self._audio_bytes = 0
        self._video_decrypt: bool | None = None
        self._audio_decrypt: bool | None = None
        self._video_sequence = FrameSequenceTracker()
        self._video_codec = CodecName.HEVC
        self._first_video_timestamp_ms: int | None = None
        self._last_video_timestamp_ms: int | None = None
        self._active_sig: MsgSvrClient | None = None
        self._active_sock: socket.socket | None = None
        self._active_stop_live: Callable[[], None] | None = None
        self._last_signaling_endpoint: tuple[str, int] | None = None
        self._last_turn_endpoint: tuple[str, int] | None = None
        self._last_candidate_count = 0
        self._keyframe_request = threading.Event()

    def request_stop(self) -> None:
        self._running = False
        self._keyframe_request.set()
        stop_live = self._active_stop_live
        if stop_live is not None:
            try:
                stop_live()
            except Exception:
                pass
        if self._active_sig is not None:
            try:
                self._active_sig.close()
            except Exception:
                pass
        sock = self._active_sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def request_keyframe(self) -> None:
        """Ask the running live session to nudge the camera toward a fresh IDR."""
        self._keyframe_request.set()

    @property
    def video_count(self) -> int:
        return self._video_count

    @property
    def diagnostics(self) -> dict[str, Any]:
        return {
            "codec": self._video_codec.value,
            "stream_id": self._vvp_stream_id,
            "stream_flag": self._vvp_stream_flag,
            "video_frames": self._video_count,
            "source_video_frames": self._source_video_count,
            "video_bytes": self._total_bytes,
            "video_media_span_s": self._video_media_span_s(),
            "video_timestamp_fps": self._video_timestamp_fps(),
            "audio_frames": self._audio_count,
            "audio_bytes": self._audio_bytes,
            "video_decrypted": self._video_decrypt,
            "audio_decrypted": self._audio_decrypt,
            "signaling_endpoint": format_endpoint(self._last_signaling_endpoint),
            "turn_endpoint": format_endpoint(self._last_turn_endpoint),
            "candidate_count": self._last_candidate_count,
        }

    def _note_video_timestamp(self, timestamp_ms: int | None) -> None:
        if timestamp_ms is None:
            return
        timestamp = int(timestamp_ms) & 0xFFFFFFFF
        if self._first_video_timestamp_ms is None:
            self._first_video_timestamp_ms = timestamp
        self._last_video_timestamp_ms = timestamp

    def _video_media_span_s(self) -> float:
        first = self._first_video_timestamp_ms
        last = self._last_video_timestamp_ms
        if first is None or last is None:
            return 0.0
        delta_ms = (last - first) & 0xFFFFFFFF
        if delta_ms == 0 or delta_ms > 0x7FFFFFFF:
            return 0.0
        return round(delta_ms / 1000.0, 3)

    def _video_timestamp_fps(self) -> float:
        span_s = self._video_media_span_s()
        if span_s <= 0.0 or self._source_video_count <= 1:
            return 0.0
        return round((self._source_video_count - 1) / span_s, 3)

    def _auth_attempts(self) -> list[tuple[str, bool]]:
        host_key = str(self._host_key or "")
        if self._video_password and host_key:
            return [(f"{self._video_password}{host_key}", True), (host_key, False)]
        return [(host_key, False)]

    @staticmethod
    def _video_parse_score(parsed) -> int:
        if not parsed:
            return 0
        payload = parsed.payload
        if len(payload) < 5:
            return 0
        score = 1
        if payload.startswith(b"\x00\x00\x01") or payload.startswith(
            b"\x00\x00\x00\x01"
        ):
            score += 1
        if identify_codec(payload) is not None:
            score += 4
        return score

    def _parse_video_chunk(self, chunk: bytes):
        def _raw():
            return parse_stream_frame(chunk)

        def _decrypted():
            return parse_stream_frame(bytes(decrypt_stream_frame(bytearray(chunk))))

        raw = dec = None
        if self._video_decrypt is True:
            dec = _decrypted()
            if self._video_parse_score(dec) >= 5:
                return dec
            raw = _raw()
        elif self._video_decrypt is False:
            raw = _raw()
            if self._video_parse_score(raw) >= 5:
                return raw
            dec = _decrypted()
        else:
            raw = _raw()
            dec = _decrypted()

        raw_score = self._video_parse_score(raw)
        dec_score = self._video_parse_score(dec)
        if raw_score > dec_score:
            if self._video_decrypt is None:
                self._video_decrypt = False
            return raw
        if self._video_decrypt is None:
            self._video_decrypt = True
        return dec

    def _parse_stream_chunk(self, chunk: bytes):
        if len(chunk) < 4:
            return None
        frame_type = chunk[3]

        if frame_type in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
            return self._parse_video_chunk(chunk)

        if frame_type == STREAM_TYPE_AUDIO:
            if self._audio_decrypt is True:
                decrypted = decrypt_stream_frame(bytearray(chunk))
                return parse_stream_frame(bytes(decrypted))
            if self._audio_decrypt is False:
                return parse_stream_frame(chunk)

            if len(chunk) >= 0x34:
                remaining = len(chunk) - 0x34
                raw_dl = int.from_bytes(chunk[0x30:0x34], "little")
                if 0 < raw_dl <= remaining and raw_dl < 2000:
                    self._audio_decrypt = False
                    return parse_stream_frame(chunk)
                decrypted = decrypt_stream_frame(bytearray(chunk))
                dec_dl = int.from_bytes(bytes(decrypted[0x30:0x34]), "little")
                if 0 < dec_dl <= remaining and dec_dl < 2000:
                    self._audio_decrypt = True
                    return parse_stream_frame(bytes(decrypted))

            self._audio_decrypt = True
            decrypted = decrypt_stream_frame(bytearray(chunk))
            return parse_stream_frame(bytes(decrypted))

        return parse_stream_frame(chunk)

    def _handle_stream_payload(
        self, payload: bytes, *, wait_for_keyframe: bool = False
    ) -> bool:
        saw_keyframe = False
        for chunk in split_stream_frames(payload):
            if len(chunk) < 4 or chunk[0:3] != b"\x00\x00\x01":
                continue
            parsed = self._parse_stream_chunk(chunk)
            if not parsed:
                continue
            frame_type = parsed.frame_type
            media_data = parsed.payload
            if frame_type in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
                is_keyframe = frame_type == STREAM_TYPE_IFRAME
                is_recovery = is_keyframe and is_recovery_keyframe(
                    media_data,
                    require_param_sets=False,
                )
                self._source_video_count += 1
                self._note_video_timestamp(parsed.timestamp_ms)
                if wait_for_keyframe and not is_recovery:
                    self._video_sequence.require_keyframe()
                    continue
                if self._video_sequence.should_drop(
                    parsed.sequence,
                    recovery=is_recovery,
                ):
                    continue
                self._video_codec = detect_codec(media_data, default=self._video_codec)
                if (
                    _LOGGER.isEnabledFor(logging.DEBUG)
                    and self._source_video_count <= 5
                ):
                    _LOGGER.debug(
                        "Video frame #%d type=0x%02x bytes=%d ts=%s seq=%s "
                        "codec=%s nals=%s recovery=%s",
                        self._source_video_count,
                        frame_type,
                        len(media_data),
                        parsed.timestamp_ms,
                        parsed.sequence,
                        self._video_codec.value,
                        sorted(nal_types(self._video_codec, media_data)),
                        is_recovery,
                    )
                saw_keyframe = saw_keyframe or is_recovery
                self._video_count += 1
                self._total_bytes += len(media_data)
                if self.on_video:
                    self.on_video(media_data, parsed.timestamp_ms)
            elif frame_type == STREAM_TYPE_AUDIO:
                self._audio_count += 1
                self._audio_bytes += len(media_data)
                if self.on_audio:
                    self.on_audio(media_data)
        return saw_keyframe

    def run_session(self) -> tuple[int, int]:
        self._running = True
        try:
            for host_key, uses_video_password in self._auth_attempts():
                self._video_count = 0
                self._source_video_count = 0
                self._total_bytes = 0
                self._audio_count = 0
                self._audio_bytes = 0
                self._video_decrypt = None
                self._audio_decrypt = None
                self._video_sequence.reset()
                self._video_codec = CodecName.HEVC
                self._first_video_timestamp_ms = None
                self._last_video_timestamp_ms = None
                self._last_turn_endpoint = None
                self._last_candidate_count = 0
                result = self._run_session_once(
                    host_key,
                    allow_auth_fallback=uses_video_password,
                )
                if result == AUTH_FALLBACK_RESULT and self._running:
                    _LOGGER.info(
                        "No video with configured E2EE password; retrying without it"
                    )
                    continue
                return result
            return (self._video_count, self._total_bytes)
        finally:
            if self.on_disconnect:
                try:
                    self.on_disconnect()
                except Exception:
                    pass

    def _run_session_once(
        self,
        host_key: str,
        *,
        allow_auth_fallback: bool = False,
    ) -> tuple[int, int]:
        platform_hint = getattr(self._api, "platform_domain", None)
        openapi_hint = getattr(self._api, "openapi_server", None)
        api_hint = getattr(self._api, "api_server", None)
        client_id_hint = self._client_uuid
        candidates = _resolve_signaling_server_candidates(
            platform_domain_hint=platform_hint,
            openapi_server_hint=openapi_hint,
            api_server_hint=api_hint,
            client_id_hint=client_id_hint,
        )
        last_error: Exception | None = None
        for sig_ip, sig_port in candidates:
            sig = None
            self._last_signaling_endpoint = (sig_ip, sig_port)
            try:
                _LOGGER.info(
                    "Connecting to signaling %s:%d (profile=%s, stream_id=%s)",
                    sig_ip,
                    sig_port,
                    getattr(self._api, "app_profile", "unknown"),
                    self._vvp_stream_id,
                )
                sig = MsgSvrClient(
                    sig_ip,
                    sig_port,
                    session_index=self._webrtc_session_index,
                )
                self._active_sig = sig
                sig.connect(timeout_s=SIGNALING_CONNECT_TIMEOUT_S)
                return self._do_stream(
                    sig,
                    host_key,
                    allow_auth_fallback=allow_auth_fallback,
                )
            except SignalingClusterMiss as err:
                last_error = err
                _forget_signaling_endpoint(
                    self._last_signaling_endpoint,
                    platform_domain_hint=platform_hint,
                    openapi_server_hint=openapi_hint,
                    api_server_hint=api_hint,
                    client_id_hint=client_id_hint,
                )
                if not self._running:
                    break
                _LOGGER.info("%s; trying next signaling candidate", err)
            except Exception as err:
                last_error = err
                if not self._running:
                    _LOGGER.debug("P2P session interrupted during stop: %s", err)
                    break
                _forget_signaling_endpoint(
                    self._last_signaling_endpoint,
                    platform_domain_hint=platform_hint,
                    openapi_server_hint=openapi_hint,
                    api_server_hint=api_hint,
                    client_id_hint=client_id_hint,
                )
                _LOGGER.debug(
                    "Signaling candidate %s:%d failed: %s",
                    sig_ip,
                    sig_port,
                    err,
                )
            finally:
                self._log_session_done()
                self._active_sig = None
                self._active_sock = None
                self._active_stop_live = None
                if sig is not None:
                    try:
                        sig.send_logout(self._device_uuid)
                    except Exception:
                        pass
                    sig.close()

        if last_error is not None and self._running:
            _LOGGER.warning("All signaling candidates failed: %s", last_error)
        return (self._video_count, self._total_bytes)

    def _log_session_done(self) -> None:
        _LOGGER.info(
            "P2P session done: video_frames=%d source_frames=%d "
            "video_bytes=%d audio_frames=%d audio_bytes=%d codec=%s "
            "stream_id=%s candidates=%d signaling=%s turn=%s",
            self._video_count,
            self._source_video_count,
            self._total_bytes,
            self._audio_count,
            self._audio_bytes,
            self._video_codec.value,
            self._vvp_stream_id,
            self._last_candidate_count,
            format_endpoint(self._last_signaling_endpoint),
            format_endpoint(self._last_turn_endpoint),
        )

    def _do_stream(
        self,
        sig: MsgSvrClient,
        host_key: str,
        *,
        allow_auth_fallback: bool = False,
    ) -> tuple[int, int]:
        api = self._api
        device_uuid = self._device_uuid

        app_ver = str(getattr(api, "_app_ver", "5.9.2") or "5.9.2")
        sig.register(
            client_id=self._client_id,
            brand=str(getattr(api, "_source_app", "77") or "77"),
            app_ver=f"{app_ver}a16" if "a" not in app_ver else app_ver,
            country=api.country_code,
            client_uuid=self._client_uuid,
        )
        sig.webrtc_hello_full()

        status = sig.query_device_status(device_uuid)
        device_status = status.get("status", "unknown")
        if device_status == "dormancy":
            _LOGGER.info("Camera dormant, waking...")
            contact = status.get("contact", {})
            local_ips = _get_local_ips()

            def _fire_wake() -> None:
                try:
                    sig.send_wake_connect(
                        device_uuid,
                        contact.get("keepalive", contact),
                        local_ips,
                        16685,
                    )
                except Exception:
                    _LOGGER.debug("signaling wake_connect failed", exc_info=True)
                try:
                    self._api.wake_device(
                        self._sn_num, self._device.get("deviceID", 0)
                    )
                except Exception:
                    pass

            # Snap cameras can miss a single wake-connect when deeply dormant.
            # Re-send the signaling+HTTP wake every WAKE_RETRY_S until the camera
            # pushes `status=online` or the dormancy budget expires.
            deadline = time.time() + DORMANCY_WAKE_TIMEOUT_S
            awake = None
            while time.time() < deadline and self._running:
                _fire_wake()
                remaining = deadline - time.time()
                awake = sig.wait_for_status(
                    device_uuid, "online", timeout=min(WAKE_RETRY_S, remaining)
                )
                if awake:
                    break
            if not awake:
                _LOGGER.warning("Camera did not come online")
                return (0, 0)
            status = awake
            device_status = status.get("status", "unknown")

        if device_status != "online":
            message = (
                f"Camera not online on {format_endpoint(self._last_signaling_endpoint)} "
                f"(status={device_status})"
            )
            if str(device_status).lower() in {"offline", "unknown"}:
                raise SignalingClusterMiss(message)
            _LOGGER.warning("%s", message)
            return (0, 0)

        _cache_signaling_endpoint(
            self._last_signaling_endpoint,
            platform_domain_hint=getattr(api, "platform_domain", None),
            openapi_server_hint=getattr(api, "openapi_server", None),
            api_server_hint=getattr(api, "api_server", None),
            client_id_hint=self._client_uuid,
        )
        coturn = sig.request_coturn(device_uuid)
        turn = TurnClient(
            coturn.get("coturn_ip", ""),
            int(coturn.get("coturn_port", 9100)),
            coturn.get("username", ""),
            coturn.get("pwd", ""),
        )
        self._last_turn_endpoint = (turn.server_ip, turn.server_port)
        turn.connect()
        self._active_sock = turn.sock
        if not turn.allocate():
            _LOGGER.error("TURN allocation failed")
            turn.close()
            self._active_sock = None
            return (0, 0)

        try:
            return self._stream_with_turn(
                sig,
                turn,
                device_uuid,
                host_key,
                status.get("nat", {}),
                coturn.get("coturn_ip", ""),
                coturn.get("coturn_host", ""),
                allow_auth_fallback=allow_auth_fallback,
            )
        finally:
            self._active_sock = None
            turn.close()

    def _stream_with_turn(
        self,
        sig: MsgSvrClient,
        turn: TurnClient,
        device_uuid: str,
        host_key: str,
        device_nat: dict[str, Any],
        coturn_ip: str,
        coturn_host: str,
        *,
        allow_auth_fallback: bool = False,
    ) -> tuple[int, int]:
        local_ips = _get_local_ips()
        ice_ufrag = os.urandom(4).hex()
        ice_pwd = os.urandom(12).hex()

        sdp_lines = [
            "n=0 0 0 0 0",
            "a=transport:auto",
            f"a=ice-ufrag:{ice_ufrag}",
            f"a=ice-pwd:{ice_pwd}",
            f"m=audio {turn.relay_port} RTP / AVP 0",
            f"c=IN IP4 {turn.relay_ip}",
        ]
        if not self._remote:
            for ip in local_ips:
                ip_hex = socket.inet_aton(ip).hex()
                sdp_lines.append(
                    f"a=candidate:H{ip_hex} 1 UDP 1694498815 {ip} {turn.local_port} typ host"
                )
            if turn.mapped_ip:
                ip_hex = socket.inet_aton(local_ips[0]).hex()
                sdp_lines.append(
                    f"a=candidate:S{ip_hex} 1 UDP 1862270975 {turn.mapped_ip} {turn.mapped_port} typ srflx"
                )
        offer_tag = str((sig.webrtcsvr or {}).get("tag") or "") + "01"
        answer = sig.send_offer(device_uuid, "\n".join(sdp_lines) + "\n")
        camera_ufrag, camera_pwd, camera_candidates = parse_sdp_answer(
            answer.get("sdp", "")
        )
        collect_trickle_candidates(sig, camera_candidates)
        deduped_candidates: list[dict[str, Any]] = []
        for cand in camera_candidates:
            add_candidate_once(deduped_candidates, cand)
        camera_candidates = deduped_candidates

        complete = sig.send_candidate_complete(device_uuid)
        if isinstance(complete, dict):
            if complete.get("sdp"):
                complete_ufrag, complete_pwd, _ = parse_sdp_answer(complete["sdp"])
                camera_ufrag = camera_ufrag or complete_ufrag
                camera_pwd = camera_pwd or complete_pwd
            for cand in candidates_from_response(complete):
                add_candidate_once(camera_candidates, cand)

        if not camera_candidates:
            _LOGGER.error("No camera candidate found")
            return (0, 0)

        permission_ips = {coturn_ip}
        if device_nat.get("wan_ip"):
            permission_ips.add(device_nat["wan_ip"])
        for cand in camera_candidates:
            if cand.get("ip"):
                permission_ips.add(cand["ip"])

        for ip in permission_ips:
            if ip:
                turn.create_permission(ip)
        for cand in camera_candidates:
            if cand.get("ip") and cand.get("port"):
                turn.channel_bind(cand["ip"], cand["port"])

        probe_hosts = []
        for host in (coturn_ip, coturn_host):
            host = str(host or "").strip()
            if host and host not in probe_hosts:
                probe_hosts.append(host)
        probe_ok = False
        for probe_host in probe_hosts:
            probe_ok = probe_relay(probe_host, offer_tag)
            if probe_ok:
                break
        _LOGGER.debug(
            "TURN relay probe %s for %s tag=%s",
            "ok" if probe_ok else "failed",
            ", ".join(probe_hosts) or "unknown",
            offer_tag,
        )
        self._last_candidate_count = len(camera_candidates)
        lan_connect_frame = build_lan_connect_frame()
        lan_connect_endpoints = (
            [] if self._remote else host_candidate_endpoints(camera_candidates)
        )
        last_lan_connect = 0.0

        def _send_lan_connect(now_ts: float, *, force: bool = False) -> None:
            nonlocal last_lan_connect
            if not lan_connect_endpoints:
                return
            if not force and now_ts < last_lan_connect + 1.0:
                return
            for endpoint in lan_connect_endpoints:
                try:
                    turn.sock.sendto(lan_connect_frame, endpoint)
                except Exception:
                    pass
            last_lan_connect = now_ts

        _LOGGER.debug(
            "Camera ICE candidates: %s",
            ", ".join(
                f"{cand.get('ip')}:{cand.get('port')}:{cand.get('type', '')}"
                for cand in camera_candidates
            ),
        )
        try:
            turn.refresh()
        except Exception:
            pass

        confirmed_peer: list[tuple[str, int, bool] | None] = [None]
        candidate_fanout_until = time.time() + 8.0

        def _send_udp(data: bytes) -> None:
            fanout = time.time() < candidate_fanout_until
            if confirmed_peer[0] is not None:
                peer_ip, peer_port, direct = confirmed_peer[0]
                direct_sent = False
                if direct and not self._remote:
                    try:
                        turn.sock.sendto(data, (peer_ip, peer_port))
                        direct_sent = True
                    except Exception:
                        pass
                try:
                    turn.send_to_peer(peer_ip, peer_port, data)
                    if not fanout:
                        return
                except Exception:
                    pass
                if direct_sent and not fanout:
                    return
            for cand in camera_candidates:
                try:
                    turn.send_to_peer(cand["ip"], cand["port"], data)
                except Exception:
                    pass
                if not self._remote and cand.get("type") != "relay":
                    try:
                        turn.sock.sendto(data, (cand["ip"], cand["port"]))
                    except Exception:
                        pass

        kcp = KcpTunnel(_send_udp, recv_wnd=KCP_WND)
        licence_id = format_licence_id(str(self._sn_num))
        vvp_seq = 0
        last_heartbeat = 0.0
        last_iva_heartbeat = 0.0
        last_start_live = 0.0
        live_started = False
        last_ice = 0.0
        last_video_time = 0.0
        last_udp_time = 0.0
        last_kcp_push_time = 0.0
        last_kcp_payload_time = 0.0
        signal_heartbeat_s = max(
            5.0, float(getattr(sig, "heartbeat_interval_s", 30.0) or 30.0)
        )
        next_signal_heartbeat = time.time() + max(5.0, signal_heartbeat_s * 2.0 / 3.0)
        signal_heartbeat_pending = [False]
        last_ack_probe = 0.0
        last_gap_nudge = 0.0
        last_gap_skip = 0.0
        last_idle_start_live = 0.0
        live_started_at = 0.0
        last_client_keyframe_request = 0.0
        last_stall_debug = 0.0
        wait_for_keyframe_until = 0.0
        pending_payloads: list[bytes] = []
        reconnect_idle_source = False
        turn_refresh = time.time() + 60.0
        snap_keepalive = SnapDeviceKeepalive(
            self._api, self._sn_num, enabled=self._is_snap
        )
        auth_fallback_at = (
            time.time() + AUTH_FALLBACK_NO_VIDEO_S if allow_auth_fallback else 0.0
        )

        def _next_vvp(
            cmd: int,
            *,
            param: int = 8,
            stream_id: int | None = None,
            stream_flag: int | None = None,
        ) -> bytes:
            nonlocal vvp_seq
            packet = build_vvp_packet(
                cmd=cmd,
                seq=vvp_seq,
                host_key=host_key,
                param=param,
                licence_id=licence_id,
                stream_id=self._vvp_stream_id if stream_id is None else stream_id,
                stream_flag=(
                    self._vvp_stream_flag if stream_flag is None else stream_flag
                ),
            )
            vvp_seq += 1
            return packet

        def _send_start_live(
            reason: str,
            now_ts: float | None = None,
            *,
            min_interval: float = 0.0,
            wait_for_recovery_keyframe: bool = False,
        ) -> bool:
            nonlocal last_start_live, live_started, live_started_at, last_heartbeat
            nonlocal wait_for_keyframe_until
            now_ts = time.time() if now_ts is None else now_ts
            if now_ts < last_start_live + min_interval:
                return False
            kcp.send_iva_data(_next_vvp(VVP_CMD_START_LIVE))
            last_start_live = now_ts
            last_heartbeat = now_ts + VVP_HEARTBEAT_S
            if not live_started:
                live_started_at = now_ts
            live_started = True
            keyframe_wait = gap_recovery_for(self._video_codec).keyframe_wait_s
            if wait_for_recovery_keyframe and keyframe_wait > 0.0:
                wait_for_keyframe_until = max(
                    wait_for_keyframe_until,
                    now_ts + keyframe_wait,
                )
            _LOGGER.debug("Sent VVP START_LIVE (%s)", reason)
            return True

        def _send_stop_live() -> None:
            if not live_started:
                return
            kcp.send_iva_data(_next_vvp(VVP_CMD_STOP, stream_id=0, stream_flag=0))
            kcp.flush_acks()

        self._active_stop_live = _send_stop_live

        def _send_signal_heartbeat() -> None:
            try:
                sig.send_heartbeat(read_response=True)
            except Exception:
                _LOGGER.debug("Signaling heartbeat failed", exc_info=True)
            finally:
                signal_heartbeat_pending[0] = False

        def _send_ice_checks() -> None:
            for cand in camera_candidates:
                if not cand.get("ip") or not cand.get("port"):
                    continue
                if cand.get("type") == "relay":
                    continue
                try:
                    turn.send_ice_binding(
                        cand["ip"],
                        cand["port"],
                        ice_ufrag,
                        camera_ufrag,
                        camera_pwd,
                    )
                except Exception:
                    pass
                if not self._remote and cand.get("type") != "relay":
                    try:
                        _send_direct_ice_binding(
                            turn.sock,
                            cand["ip"],
                            cand["port"],
                            ice_ufrag,
                            camera_ufrag,
                            camera_pwd,
                        )
                    except Exception:
                        pass

        def _handle_kcp_payload(payload: bytes) -> bool:
            nonlocal wait_for_keyframe_until
            before_source = self._source_video_count
            payload = _unwrap_iva_payload(payload)
            if payload:
                now = time.time()
                wait_for_keyframe = wait_for_keyframe_until > now
                if wait_for_keyframe_until and not wait_for_keyframe:
                    wait_for_keyframe_until = 0.0
                saw_keyframe = self._handle_stream_payload(
                    payload,
                    wait_for_keyframe=wait_for_keyframe,
                )
                if wait_for_keyframe and (
                    saw_keyframe or time.time() >= wait_for_keyframe_until
                ):
                    wait_for_keyframe_until = 0.0
            return self._source_video_count > before_source

        def _queue_kcp_payload(payload: bytes) -> None:
            nonlocal last_kcp_payload_time
            last_kcp_payload_time = time.time()
            pending_payloads.append(payload)

        def _drain_kcp_queue() -> bool:
            queued_any = False
            while True:
                queued = kcp.poll_data()
                if queued is None:
                    break
                _queue_kcp_payload(queued)
                queued_any = True
            return queued_any

        def _process_pending_payloads() -> bool:
            nonlocal last_video_time
            saw_video = False
            payloads = pending_payloads[:]
            pending_payloads.clear()
            for payload in payloads:
                saw_video = _handle_kcp_payload(payload) or saw_video
            if saw_video:
                last_video_time = time.time()
            return saw_video

        def _kcp_gap_backlog() -> int:
            next_sn = int(getattr(kcp, "next_recv_sn", -1))
            recv_buf = getattr(kcp, "recv_buf", {})
            if next_sn < 0 or not recv_buf or next_sn in recv_buf:
                return 0
            above = [sn for sn in recv_buf if sn > next_sn]
            if not above:
                return 0
            return max(1, max(above) - next_sn + 1)

        def _video_wait_s(now_ts: float) -> float:
            if last_video_time > 0.0:
                return now_ts - last_video_time
            if last_kcp_push_time > 0.0:
                return now_ts - last_kcp_push_time
            if last_kcp_payload_time > 0.0:
                return now_ts - last_kcp_payload_time
            if live_started_at > 0.0:
                return now_ts - live_started_at
            return 0.0

        def _attempt_gap_recovery(now_ts: float) -> None:
            nonlocal reconnect_idle_source
            nonlocal candidate_fanout_until
            nonlocal last_ack_probe, last_gap_nudge, last_gap_skip
            nonlocal last_idle_start_live
            nonlocal last_stall_debug, wait_for_keyframe_until, last_video_time
            stall_time = _video_wait_s(now_ts)
            if stall_time <= 0.8:
                return

            gap_backlog = _kcp_gap_backlog()
            if not gap_backlog:
                udp_idle = now_ts - last_udp_time if last_udp_time > 0 else -1.0
                payload_idle = (
                    now_ts - last_kcp_payload_time
                    if last_kcp_payload_time > 0
                    else -1.0
                )
                if now_ts - last_ack_probe > 0.35 and kcp.send_ack_probe():
                    last_ack_probe = now_ts
                if stall_time > 1.2:
                    candidate_fanout_until = max(candidate_fanout_until, now_ts + 4.0)
                kcp_pending = bool(
                    getattr(kcp, "recv_buf", None)
                    or getattr(kcp, "recv_queue", None)
                    or getattr(kcp, "recv_frag_buf", None)
                )
                runtime_policy = runtime_policy_for(self._video_codec)
                idle_retry_s = runtime_policy.idle_start_live_retry_s
                if (
                    live_started
                    and idle_retry_s > 0.0
                    and stall_time >= idle_retry_s
                    and now_ts >= last_idle_start_live + idle_retry_s
                ):
                    if _send_start_live(
                        "idle",
                        now_ts,
                        min_interval=idle_retry_s,
                        wait_for_recovery_keyframe=True,
                    ):
                        last_idle_start_live = now_ts
                idle_reconnect_s = runtime_policy.source_idle_reconnect_s
                if stall_time >= idle_reconnect_s and not kcp_pending:
                    _LOGGER.warning(
                        "P2P source idle %.1fs; reconnecting session",
                        stall_time,
                    )
                    reconnect_idle_source = True
                if now_ts - last_stall_debug >= 2.0:
                    last_stall_debug = now_ts
                    rx = kcp.receive_snapshot()
                    _LOGGER.debug(
                        "Video stalled %.2fs without KCP gap: udp_idle=%.2fs "
                        "payload_idle=%.2fs recv_buf=%d queued=%d partial=%d "
                        "partial_bytes=%d first=%s last=%s",
                        stall_time,
                        udp_idle,
                        payload_idle,
                        rx["recv_buf"],
                        rx["queued"],
                        rx["partial"],
                        rx["partial_bytes"],
                        rx["partial_first"],
                        rx["partial_last"],
                    )
                return

            if now_ts - last_gap_nudge > 0.08 and kcp.send_gap_nudge():
                last_gap_nudge = now_ts
                candidate_fanout_until = max(candidate_fanout_until, now_ts + 4.0)

            recovery = gap_recovery_for(self._video_codec)
            skip_wait = recovery.skip_wait_s
            skip_interval = recovery.skip_interval_s
            max_gaps = recovery.max_gaps(gap_backlog, stall_time)
            keyframe_wait = recovery.keyframe_wait_s

            if stall_time <= skip_wait or now_ts - last_gap_skip <= skip_interval:
                return
            if kcp.skip_gap(
                max_gaps=max_gaps,
                require_iva_start=True,
                start_frame_types={STREAM_TYPE_IFRAME},
            ):
                last_gap_skip = now_ts
                self._video_sequence.require_keyframe()
                wait_for_keyframe_until = now_ts + keyframe_wait
                _drain_kcp_queue()
                _process_pending_payloads()

        def _handle_peer_packet(packet) -> None:
            nonlocal last_udp_time, last_kcp_push_time
            last_udp_time = time.time()
            peer = packet.peer
            if peer is None and not packet.via_turn:
                peer = packet.source

            parsed = packet.stun
            is_turn_server_stun = (
                bool(parsed)
                and not packet.via_turn
                and packet.source[0] == getattr(turn, "server_ip", None)
            )
            if parsed:
                msg_type = parsed.get("type")
                if msg_type == BINDING_REQUEST and peer is not None:
                    try:
                        response = _build_ice_response(
                            parsed, ice_pwd, peer[0], peer[1]
                        )
                        if not packet.via_turn and not self._remote:
                            turn.sock.sendto(response, peer)
                        else:
                            turn.send_to_peer(peer[0], peer[1], response)
                    except Exception:
                        pass
                    return
                if msg_type == BINDING_RESPONSE:
                    kcp.retransmit_unacked()
                    return

            segments = parse_kcp_segments(packet.data)
            first_segment = parse_kcp_segment(packet.data) if not segments else None
            is_iva = packet.data[0:2] == b"\xff\x01"
            is_kcp = bool(segments or first_segment or is_iva)
            if is_kcp:
                # via_turn=True packets are camera media unwrapped from a bound
                # channel; the camera's relay can share an IP with our TURN
                # server, so peer[0]==server_ip is not a rejection signal there.
                # Direct (non-TURN) packets from the TURN server itself are
                # already filtered via is_turn_server_stun.
                valid_peer = peer is not None and (
                    packet.via_turn
                    or (
                        not is_turn_server_stun
                        and peer[0] != getattr(turn, "server_ip", None)
                    )
                )
                if valid_peer:
                    direct = not packet.via_turn and not self._remote
                    current = confirmed_peer[0]
                    if (
                        current is None
                        or (direct and not current[2])
                    ):
                        confirmed_peer[0] = (peer[0], peer[1], direct)
                        _LOGGER.debug(
                            "Confirmed media peer %s via %s",
                            format_endpoint(peer),
                            "direct" if direct else "turn",
                        )
                if is_iva or (first_segment and first_segment["cmd"] == KCP_CMD_PUSH):
                    last_kcp_push_time = time.time()
                elif any(seg["cmd"] == KCP_CMD_PUSH for seg in segments):
                    last_kcp_push_time = time.time()
                processed = kcp.process_input(packet.data)
                if kcp.ack_flush_due():
                    kcp.flush_acks()
                if processed is not None:
                    typ, payload = processed
                    if typ in ("data", "iva_data", "iva") and payload:
                        _queue_kcp_payload(payload)
                    elif typ == "handshake":
                        kcp.flush_acks()
                        kcp.retransmit_unacked()
                _drain_kcp_queue()

        now = time.time()
        _send_lan_connect(now, force=True)
        kcp.send_handshake()
        # Official app sends START_LIVE in the very next KCP push (sn=1) without
        # waiting for the camera's handshake echo or any ACK. Mirroring that
        # avoids a class of post-dormancy stalls where the camera ACKs but never
        # echoes the IVA handshake, then ignores a delayed START_LIVE.
        _send_start_live("startup", now)
        last_iva_heartbeat = now + IVA_HEARTBEAT_S

        while self._running:
            now = time.time()
            _send_lan_connect(now)

            if auth_fallback_at and self._video_count <= 0 and now >= auth_fallback_at:
                return AUTH_FALLBACK_RESULT

            if now >= last_ice:
                _send_ice_checks()
                last_ice = now + 2.0
            snap_keepalive.tick()

            if now >= next_signal_heartbeat:
                if not signal_heartbeat_pending[0]:
                    signal_heartbeat_pending[0] = True
                    threading.Thread(
                        target=_send_signal_heartbeat,
                        name=f"cloudplus_signal_hb_{self._sn_num}",
                        daemon=True,
                    ).start()
                next_signal_heartbeat = now + signal_heartbeat_s

            if self._keyframe_request.is_set():
                self._keyframe_request.clear()
                if live_started and now >= (
                    last_client_keyframe_request + CLIENT_KEYFRAME_REQUEST_DEBOUNCE_S
                ):
                    _send_start_live("client-join", now, min_interval=0.5)
                    last_client_keyframe_request = now

            if now >= last_iva_heartbeat:
                kcp.send_handshake()
                last_iva_heartbeat = now + IVA_HEARTBEAT_S

            if live_started and now >= last_heartbeat:
                kcp.send_iva_data(
                    _next_vvp(VVP_CMD_HEARTBEAT, stream_id=0, stream_flag=0)
                )
                last_heartbeat = now + VVP_HEARTBEAT_S

            keepalive_s = runtime_policy_for(
                self._video_codec
            ).start_live_keepalive_s
            if (
                keepalive_s > 0.0
                and last_video_time > 0.0
                and now >= last_start_live + keepalive_s
            ):
                _send_start_live(
                    "keepalive",
                    now,
                    wait_for_recovery_keyframe=True,
                )

            if now >= turn_refresh:
                try:
                    turn.refresh()
                except Exception:
                    pass
                turn_refresh = now + 60.0

            packets = recv_peer_packets(turn, timeout=0.08, max_packets=2048)
            if not packets:
                _process_pending_payloads()
                _attempt_gap_recovery(now)
                kcp.flush_acks()
                if reconnect_idle_source:
                    return (self._video_count, self._total_bytes)
                continue

            for packet in packets:
                if not self._running:
                    break
                _handle_peer_packet(packet)

            _drain_kcp_queue()
            _process_pending_payloads()
            _attempt_gap_recovery(time.time())
            kcp.flush_acks()
            if reconnect_idle_source:
                return (self._video_count, self._total_bytes)

        try:
            _send_stop_live()
        except Exception:
            pass
        finally:
            if self._active_stop_live is _send_stop_live:
                self._active_stop_live = None

        return (self._video_count, self._total_bytes)
