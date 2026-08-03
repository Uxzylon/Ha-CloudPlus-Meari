"""P2P streamer for CloudPlus / Meari cameras: lifecycle + session orchestration.

The per-session streaming loop lives in :mod:`live_session`; shared constants and
identity helpers in :mod:`session_support`.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Any, Callable

from ..api import format_sn
from ..meari_signaling import MsgSvrClient
from ..turn_client import TurnClient
from .network import (
    _cache_signaling_endpoint,
    _forget_signaling_endpoint,
    _get_local_ips,
    _resolve_signaling_server_candidates,
)
from .sdp import (
    add_candidate_once,
    candidates_from_response,
    format_endpoint,
    parse_sdp_answer,
)
from .codecs import (
    CodecName,
    detect_codec,
    identify_codec,
    is_recovery_keyframe,
    nal_types,
)
from .protocol import (
    STREAM_TYPE_AUDIO,
    STREAM_TYPE_IFRAME,
    STREAM_TYPE_PFRAME,
)
from .codec import (
    FrameSequenceTracker,
    decrypt_stream_frame,
    parse_stream_frame,
    split_stream_frames,
)
from .quality import stream_id_for_quality
from .live_session import LiveSessionMixin
from .session_support import (
    AUTH_FALLBACK_RESULT,
    DORMANCY_WAKE_TIMEOUT_S,
    SIGNALING_CONNECT_TIMEOUT_S,
    WAKE_RETRY_S,
    SignalingClusterMiss,
    _client_id_for,
    _client_uuid_for,
    _next_session_index,
)

_LOGGER = logging.getLogger(__name__)


class P2PStreamer(LiveSessionMixin):
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
        default_stream_flag = 0 if app_profile == "cloudedge" else 1
        self._vvp_stream_flag = int(
            getattr(api, "vvp_stream_flag", default_stream_flag)
        )

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
        # True while a dormant camera is actively being woken — watchdogs must
        # not restart the session underneath the dormancy-wake budget.
        self.awaiting_wake = False
        # True once this session has confirmed a *direct* LAN media peer. The
        # direct path is precious on battery cameras (see
        # DIRECT_SOURCE_IDLE_RECONNECT_S), so while it holds, the engine and the
        # restart watchdogs stay patient through brief mid-stream silences
        # instead of abandoning it for the lossy relay.
        self.direct_confirmed = False

    def request_stop(self) -> None:
        self._running = False
        self._keyframe_request.set()
        stop_live = self._active_stop_live
        if stop_live is not None:
            try:
                stop_live()
            except OSError:
                pass
        if self._active_sig is not None:
            try:
                self._active_sig.close()
            except OSError:
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
                self.direct_confirmed = False
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
                except (OSError, RuntimeError):
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
            except (OSError, RuntimeError, ValueError, KeyError) as err:
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
                self.awaiting_wake = False
                self._active_sig = None
                self._active_sock = None
                self._active_stop_live = None
                if sig is not None:
                    try:
                        sig.send_logout(self._device_uuid)
                    except OSError:
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
        was_dormant = device_status == "dormancy"
        fire_wake: Callable[[], None] | None = None
        if was_dormant:
            # Do NOT block on the camera's `online` status push — for a deeply
            # dormant snap camera it lags 40-60 s. The official app instead sends
            # the live request *while the camera is still dormant*: the camera
            # wakes in response to the offer and answers in ~10-15 s. We mirror
            # that — drive coturn + offer below and treat the camera's SDP answer
            # as the wake confirmation (see `_negotiate_dormant_offer`).
            # `awaiting_wake` keeps watchdogs off our back until it arrives.
            _LOGGER.info("Camera dormant; waking via live request")
            contact = status.get("contact", {})
            local_ips = _get_local_ips()

            def _do_fire_wake() -> None:
                try:
                    sig.send_wake_connect(
                        device_uuid,
                        contact.get("keepalive", contact),
                        local_ips,
                        16685,
                    )
                except OSError:
                    _LOGGER.debug("signaling wake_connect failed", exc_info=True)
                try:
                    self._api.wake_device(self._sn_num, self._device.get("deviceID", 0))
                except (OSError, RuntimeError, ValueError, KeyError):
                    pass

            # Request coturn *before* any wake (the app's order) — firing the
            # wake first leaves stray signaling frames that desync the coturn
            # read. The actual wake is fired from _negotiate_dormant_offer.
            fire_wake = _do_fire_wake
            self.awaiting_wake = True
        elif device_status != "online":
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

        def _coturn_ok(value: Any) -> bool:
            return isinstance(value, dict) and bool(value.get("coturn_ip"))

        coturn = sig.request_coturn(device_uuid)
        if not _coturn_ok(coturn) and was_dormant:
            # A dormant camera's first coturn read races buffered signaling
            # frames and can come back empty for a second or two. Retry on the
            # same cluster rather than thrashing through every signaling
            # candidate (and re-running discovery) before one finally answers.
            coturn_deadline = time.time() + 8.0
            old_timeout = sig.sock.gettimeout() if sig.sock else None
            if sig.sock:
                sig.sock.settimeout(1.5)
            try:
                while (
                    not _coturn_ok(coturn)
                    and time.time() < coturn_deadline
                    and self._running
                ):
                    try:
                        coturn = sig.request_coturn(device_uuid)
                    except (socket.timeout, OSError):
                        coturn = {}
            finally:
                if sig.sock is not None and old_timeout is not None:
                    try:
                        sig.sock.settimeout(old_timeout)
                    except OSError:
                        pass
        if not _coturn_ok(coturn):
            # No relay creds on this cluster — fail fast to the next signaling
            # candidate instead of spending the allocate retries on an empty
            # server address.
            self.awaiting_wake = False
            raise SignalingClusterMiss(
                f"No coturn from {format_endpoint(self._last_signaling_endpoint)}"
            )
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
            self._active_sock = None
            turn.close()
            self.awaiting_wake = False
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
                was_dormant=was_dormant,
                fire_wake=fire_wake,
            )
        finally:
            self._active_sock = None
            turn.close()

    def _negotiate_dormant_offer(
        self,
        sig: MsgSvrClient,
        device_uuid: str,
        offer_sdp: str,
        fire_wake: Callable[[], None] | None,
    ) -> tuple[str, str, list[dict[str, Any]]]:
        """Offer-driven dormant wake, matching the official app.

        A dormant snap camera answers a live request in ~10-15 s — far sooner
        than its ``online`` status push. Re-issue the offer and re-fire the wake
        every few seconds while continuously polling the signaling socket for the
        camera's (delayed, pushed) SDP answer + trickle candidates, until it
        responds or ``DORMANCY_WAKE_TIMEOUT_S`` elapses.
        """
        ufrag = ""
        pwd = ""
        candidates: list[dict[str, Any]] = []

        def _ingest(resp: Any) -> None:
            nonlocal ufrag, pwd
            if not isinstance(resp, dict):
                return
            if resp.get("sdp"):
                a_ufrag, a_pwd, a_cands = parse_sdp_answer(str(resp["sdp"]))
                ufrag = ufrag or a_ufrag
                pwd = pwd or a_pwd
                for cand in a_cands:
                    add_candidate_once(candidates, cand)
            for cand in candidates_from_response(resp):
                add_candidate_once(candidates, cand)

        deadline = time.time() + DORMANCY_WAKE_TIMEOUT_S
        last_offer = 0.0
        last_wake = 0.0
        old_timeout = sig.sock.gettimeout() if sig.sock else None
        if sig.sock:
            sig.sock.settimeout(0.5)
        try:
            while self._running and time.time() < deadline:
                now = time.time()
                if now - last_offer >= 5.0:
                    if fire_wake is not None and now - last_wake >= WAKE_RETRY_S:
                        fire_wake()
                        last_wake = now
                    try:
                        _ingest(sig.send_offer(device_uuid, offer_sdp))
                    except (socket.timeout, OSError):
                        pass
                    last_offer = now
                try:
                    _ingest(sig.recv_webrtc_content())
                except (socket.timeout, OSError):
                    pass
                if ufrag and pwd and candidates:
                    break
        finally:
            if sig.sock is not None and old_timeout is not None:
                try:
                    sig.sock.settimeout(old_timeout)
                except OSError:
                    pass
        return ufrag, pwd, candidates
