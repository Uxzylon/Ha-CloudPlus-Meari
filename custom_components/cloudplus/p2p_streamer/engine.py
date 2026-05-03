"""Minimal P2P streamer for CloudPlus / Meari cameras."""

from __future__ import annotations

import logging
import os
import socket
import time
from typing import Any, Callable

from ..api import format_sn
from ..meari_signaling import MsgSvrClient
from ..turn_client import BINDING_REQUEST, BINDING_RESPONSE, TurnClient
from ..kcp_tunnel import KcpTunnel, parse_iva_frame, parse_kcp_segment
from .network import (
    _build_ice_response,
    _get_local_ips,
    _is_private_ip,
    _resolve_signaling_server,
    _send_direct_ice_binding,
    recv_peer_packets,
)
from .codecs import detect_codec
from .protocol import (
    VVP_CMD_STOP,
    VVP_CMD_HEARTBEAT,
    VVP_CMD_START_LIVE,
    STREAM_TYPE_AUDIO,
    STREAM_TYPE_IFRAME,
    STREAM_TYPE_PFRAME,
    build_vvp_packet,
    format_licence_id,
)
from .codec import (
    decrypt_stream_frame,
    is_idr_video_frame,
    parse_stream_frame,
    split_stream_frames,
)
from .quality import best_quality_profile

_LOGGER = logging.getLogger(__name__)

KEYFRAME_WAIT_AFTER_GAP_S = 0.8
START_LIVE_IDLE_NUDGE_S = 1.0
START_LIVE_RETRY_S = 1.5
AUTH_FALLBACK_NO_VIDEO_S = 10.0
AUTH_FALLBACK_RESULT = (-1, -1)


def _parse_sdp_answer(sdp: str) -> tuple[str, str, list[dict[str, Any]]]:
    camera_ufrag = ""
    camera_pwd = ""
    camera_candidates: list[dict[str, Any]] = []
    camera_sdp_ip = ""
    camera_sdp_port = 0

    for line in sdp.replace("\\n", "\n").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("a=ice-ufrag:"):
            camera_ufrag = line.split(":", 1)[1].strip()
        elif line.startswith("a=ice-pwd:"):
            camera_pwd = line.split(":", 1)[1].strip()
        elif line.startswith("c=IN IP4 "):
            camera_sdp_ip = line.split("c=IN IP4 ", 1)[1].strip()
        elif line.startswith("m=audio "):
            parts = line.split()
            if len(parts) > 1 and parts[1].isdigit():
                camera_sdp_port = int(parts[1])
        elif line.startswith("a=candidate:"):
            parts = line.split()
            if len(parts) >= 6:
                camera_candidates.append(
                    {
                        "ip": parts[4],
                        "port": int(parts[5]),
                        "type": parts[7] if len(parts) > 7 else "relay",
                    }
                )

    if not camera_candidates and camera_sdp_ip and camera_sdp_port:
        camera_candidates.append(
            {"ip": camera_sdp_ip, "port": camera_sdp_port, "type": "relay"}
        )

    return camera_ufrag, camera_pwd, camera_candidates


def _collect_trickle_candidates(
    sig: MsgSvrClient, camera_candidates: list[dict[str, Any]]
) -> None:
    old_timeout = sig.sock.gettimeout()
    try:
        sig.sock.settimeout(0.5)
        for _ in range(10):
            try:
                extra = sig._recv_webrtc_content()
            except socket.timeout:
                break
            except Exception:
                break
            if not isinstance(extra, dict):
                continue
            sdp = extra.get("sdp", "")
            if sdp:
                _, _, extra_candidates = _parse_sdp_answer(sdp)
                camera_candidates.extend(extra_candidates)
            candidate = extra.get("candidate")
            if isinstance(candidate, dict):
                cip = candidate.get("ip")
                cport = candidate.get("port")
                ctype = candidate.get("type", "relay")
                if cip and cport:
                    camera_candidates.append(
                        {"ip": cip, "port": int(cport), "type": ctype}
                    )
    finally:
        sig.sock.settimeout(old_timeout)


def _add_candidate_once(
    candidates: list[dict[str, Any]],
    candidate: dict[str, Any],
) -> bool:
    ip = candidate.get("ip")
    port = candidate.get("port")
    if not ip or not port:
        return False
    normalized = {
        "ip": str(ip),
        "port": int(port),
        "type": str(candidate.get("type") or "relay"),
    }
    for existing in candidates:
        if (
            existing.get("ip") == normalized["ip"]
            and int(existing.get("port", 0)) == normalized["port"]
        ):
            return False
    candidates.append(normalized)
    return True


def _unwrap_iva_payload(data: bytes) -> bytes:
    if len(data) >= 20 and data[0:2] == b"\xff\x01":
        iva = parse_iva_frame(data)
        if iva:
            type_marker, _, _, payload = iva
            if type_marker == 0x7012:
                return b""
            return payload
    return data


class P2PStreamer:
    """Runs a CloudPlus/Meari camera P2P stream using TURN + KCP + VVP."""

    def __init__(
        self,
        api: Any,
        device: dict[str, Any],
        *,
        on_video: Callable[[bytes], None] | None = None,
        on_audio: Callable[[bytes], None] | None = None,
        on_login: Callable[[], None] | None = None,
        on_disconnect: Callable[[], None] | None = None,
        remote: bool = False,
        vvp_quality: int | None = None,
        video_password: str | None = None,
    ) -> None:
        self._api = api
        self._device = device
        self._sn_num = device["snNum"]
        self._device_uuid = format_sn(str(self._sn_num))
        self._host_key = device.get("hostKey", "")
        self._video_password = (video_password or "").strip()
        self._remote = remote
        self._vvp_video_id = int(device.get("deviceID") or 0) & 0xFF
        # The app sets this live-stream flag for the normal viewer path.
        # Battery HEVC/E2EE cameras can stall if local sessions clear it.
        self._vvp_stream_flag = 1
        self._vvp_quality = (
            int(vvp_quality)
            if vvp_quality is not None
            else best_quality_profile(device)
        )

        self.on_video = on_video
        self.on_audio = on_audio
        self.on_login = on_login
        self.on_disconnect = on_disconnect

        self._running = False
        self._video_count = 0
        self._total_bytes = 0
        self._audio_decrypt: bool | None = None
        self._video_codec = "hevc"
        self._active_sig: MsgSvrClient | None = None
        self._active_sock: socket.socket | None = None

    def request_stop(self) -> None:
        self._running = False
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

    @property
    def video_count(self) -> int:
        return self._video_count

    def _auth_attempts(self) -> list[tuple[str, bool]]:
        host_key = str(self._host_key or "")
        if self._video_password and host_key:
            return [(f"{self._video_password}{host_key}", True), (host_key, False)]
        return [(host_key, False)]

    def _parse_stream_chunk(self, chunk: bytes):
        if len(chunk) < 4:
            return None
        frame_type = chunk[3]

        if frame_type in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
            decrypted = decrypt_stream_frame(bytearray(chunk))
            return parse_stream_frame(bytes(decrypted))

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
            frame_type, _, media_data = parsed
            if frame_type in (STREAM_TYPE_IFRAME, STREAM_TYPE_PFRAME):
                is_keyframe = frame_type == STREAM_TYPE_IFRAME
                is_recovery_keyframe = is_keyframe and is_idr_video_frame(
                    frame_type,
                    media_data,
                    require_param_sets=False,
                )
                if wait_for_keyframe and not is_recovery_keyframe:
                    continue
                self._video_codec = detect_codec(media_data, default=self._video_codec)
                saw_keyframe = saw_keyframe or is_recovery_keyframe
                self._video_count += 1
                self._total_bytes += len(media_data)
                if self.on_video:
                    self.on_video(media_data)
            elif frame_type == STREAM_TYPE_AUDIO:
                if self.on_audio:
                    self.on_audio(media_data)
        return saw_keyframe

    def run_session(self) -> tuple[int, int]:
        self._running = True
        try:
            for host_key, uses_video_password in self._auth_attempts():
                self._video_count = 0
                self._total_bytes = 0
                self._audio_decrypt = None
                self._video_codec = "hevc"
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
        sig = None
        try:
            sig_ip, sig_port = _resolve_signaling_server()
            sig = MsgSvrClient(sig_ip, sig_port)
            self._active_sig = sig
            sig.connect()
            return self._do_stream(
                sig,
                host_key,
                allow_auth_fallback=allow_auth_fallback,
            )
        except Exception as err:
            if not self._running:
                _LOGGER.debug("P2P session interrupted during stop: %s", err)
            else:
                _LOGGER.exception("P2P session failed")
            return (self._video_count, self._total_bytes)
        finally:
            self._active_sig = None
            self._active_sock = None
            if sig is not None:
                try:
                    sig.send_logout(self._device_uuid)
                except Exception:
                    pass
                sig.close()

    def _do_stream(
        self,
        sig: MsgSvrClient,
        host_key: str,
        *,
        allow_auth_fallback: bool = False,
    ) -> tuple[int, int]:
        api = self._api
        device_uuid = self._device_uuid

        sig.register(client_id=api.user_id, brand="77", country=api.country_code)
        sig.webrtc_hello_full()

        status = sig.query_device_status(device_uuid)
        device_status = status.get("status", "unknown")
        if device_status == "dormancy":
            _LOGGER.info("Camera dormant, waking...")
            contact = status.get("contact", {})
            local_ips = _get_local_ips()
            sig.send_wake_connect(
                device_uuid,
                contact.get("keepalive", contact),
                local_ips,
                16685,
            )
            try:
                self._api.wake_device(self._sn_num, self._device.get("deviceID", 0))
            except Exception:
                pass
            awake = sig.wait_for_status(device_uuid, "online", timeout=30)
            if not awake:
                _LOGGER.warning("Camera did not come online")
                return (0, 0)
            status = awake
            device_status = status.get("status", "unknown")

        if device_status != "online":
            _LOGGER.warning("Camera not online (status=%s)", device_status)
            return (0, 0)

        coturn = sig.request_coturn(device_uuid)
        turn = TurnClient(
            coturn.get("coturn_ip", ""),
            int(coturn.get("coturn_port", 9100)),
            coturn.get("username", ""),
            coturn.get("pwd", ""),
        )
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
        *,
        allow_auth_fallback: bool = False,
    ) -> tuple[int, int]:
        local_ips = _get_local_ips()
        ice_ufrag = os.urandom(4).hex()
        ice_pwd = os.urandom(12).hex()

        sdp_lines = [
            "v=0",
            f"o=- {int(time.time())} {int(time.time())} IN IP4 0.0.0.0",
            "s=ice",
            "t=0 0",
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
        if turn.relay_ip:
            relay_hex = socket.inet_aton(turn.relay_ip).hex()
            sdp_lines.append(
                f"a=candidate:R{relay_hex} 1 UDP 16777215 {turn.relay_ip} {turn.relay_port} typ srflx"
            )

        answer = sig.send_offer(device_uuid, "\n".join(sdp_lines) + "\n")
        camera_ufrag, camera_pwd, camera_candidates = _parse_sdp_answer(
            answer.get("sdp", "")
        )
        _collect_trickle_candidates(sig, camera_candidates)
        deduped_candidates: list[dict[str, Any]] = []
        for cand in camera_candidates:
            _add_candidate_once(deduped_candidates, cand)
        camera_candidates = deduped_candidates

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

        complete = sig.send_candidate_complete(device_uuid)
        if isinstance(complete, dict):
            _, _, complete_candidates = _parse_sdp_answer(complete.get("sdp", ""))
            candidate = complete.get("candidate")
            if isinstance(candidate, dict):
                cip = candidate.get("ip")
                cport = candidate.get("port")
                if cip and cport:
                    complete_candidates.append(
                        {
                            "ip": cip,
                            "port": int(cport),
                            "type": candidate.get("type", "relay"),
                        }
                    )
            for cand in complete_candidates:
                if _add_candidate_once(camera_candidates, cand):
                    turn.create_permission(cand["ip"])
                    turn.channel_bind(cand["ip"], cand["port"])
        try:
            turn.refresh()
        except Exception:
            pass

        confirmed_peer: list[tuple[str, int, bool] | None] = [None]
        candidate_fanout_until = 0.0

        def _send_udp(data: bytes) -> None:
            fanout = time.time() < candidate_fanout_until
            if confirmed_peer[0] is not None:
                peer_ip, peer_port, direct = confirmed_peer[0]
                if direct and not self._remote:
                    try:
                        turn.sock.sendto(data, (peer_ip, peer_port))
                        if not fanout:
                            return
                    except Exception:
                        pass
                try:
                    turn.send_to_peer(peer_ip, peer_port, data)
                    if not fanout:
                        return
                except Exception:
                    pass
            for cand in camera_candidates:
                try:
                    turn.send_to_peer(cand["ip"], cand["port"], data)
                except Exception:
                    pass
                if not self._remote and _is_private_ip(cand["ip"]):
                    try:
                        turn.sock.sendto(data, (cand["ip"], cand["port"]))
                    except Exception:
                        pass

        kcp = KcpTunnel(_send_udp)
        licence_id = format_licence_id(str(self._sn_num))
        vvp_seq = 0
        last_heartbeat = 0.0
        last_iva_heartbeat = 0.0
        last_start_live = 0.0
        last_ice = 0.0
        last_video_time = 0.0
        last_udp_time = 0.0
        last_kcp_payload_time = 0.0
        last_ack_probe = 0.0
        last_gap_nudge = 0.0
        last_gap_skip = 0.0
        last_stall_debug = 0.0
        wait_for_keyframe_until = 0.0
        turn_refresh = time.time() + 60.0
        auth_fallback_at = (
            time.time() + AUTH_FALLBACK_NO_VIDEO_S if allow_auth_fallback else 0.0
        )

        def _next_vvp(cmd: int, *, param: int = 8) -> bytes:
            nonlocal vvp_seq
            packet = build_vvp_packet(
                cmd=cmd,
                seq=vvp_seq,
                host_key=host_key,
                param=param,
                licence_id=licence_id,
                video_id=self._vvp_video_id,
                stream_flag=self._vvp_stream_flag,
                quality=self._vvp_quality,
            )
            vvp_seq += 1
            return packet

        def _send_start_live(
            reason: str,
            now_ts: float | None = None,
            *,
            min_interval: float = 0.0,
        ) -> bool:
            nonlocal last_start_live
            now_ts = time.time() if now_ts is None else now_ts
            if now_ts < last_start_live + min_interval:
                return False
            kcp.send_iva_data(_next_vvp(VVP_CMD_START_LIVE))
            last_start_live = now_ts
            _LOGGER.debug("Sent VVP START_LIVE (%s)", reason)
            return True

        def _send_ice_checks() -> None:
            for cand in camera_candidates:
                if not cand.get("ip") or not cand.get("port"):
                    continue
                turn.send_ice_binding(
                    cand["ip"],
                    cand["port"],
                    ice_ufrag,
                    camera_ufrag,
                    camera_pwd,
                )
                if not self._remote and _is_private_ip(cand["ip"]):
                    _send_direct_ice_binding(
                        turn.sock,
                        cand["ip"],
                        cand["port"],
                        ice_ufrag,
                        camera_ufrag,
                        camera_pwd,
                    )

        def _handle_kcp_payload(payload: bytes) -> bool:
            nonlocal wait_for_keyframe_until
            before = self._video_count
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
            return self._video_count > before

        def _drain_kcp_queue() -> bool:
            nonlocal last_kcp_payload_time
            saw_video = False
            while True:
                queued = kcp.poll_data()
                if queued is None:
                    break
                last_kcp_payload_time = time.time()
                saw_video = _handle_kcp_payload(queued) or saw_video
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

        def _attempt_gap_recovery(now_ts: float) -> None:
            nonlocal candidate_fanout_until
            nonlocal last_ack_probe, last_gap_nudge, last_gap_skip
            nonlocal last_stall_debug, wait_for_keyframe_until, last_video_time
            if last_video_time <= 0.0 or now_ts - last_video_time <= 0.8:
                return

            stall_time = now_ts - last_video_time
            gap_backlog = _kcp_gap_backlog()
            if not gap_backlog:
                udp_idle = now_ts - last_udp_time if last_udp_time > 0 else -1.0
                if now_ts - last_ack_probe > 0.35 and kcp.send_ack_probe():
                    last_ack_probe = now_ts
                if stall_time > 1.2 and (udp_idle < 0 or udp_idle > 0.8):
                    candidate_fanout_until = max(candidate_fanout_until, now_ts + 4.0)
                if stall_time > START_LIVE_IDLE_NUDGE_S:
                    _send_start_live(
                        "video-idle",
                        now_ts,
                        min_interval=START_LIVE_RETRY_S,
                    )
                if now_ts - last_stall_debug >= 2.0:
                    last_stall_debug = now_ts
                    _LOGGER.debug(
                        "Video stalled %.2fs without KCP gap: udp_idle=%.2fs "
                        "payload_idle=%.2fs recv_buf=%d queued=%d partial=%d",
                        stall_time,
                        udp_idle,
                        (
                            now_ts - last_kcp_payload_time
                            if last_kcp_payload_time > 0
                            else -1.0
                        ),
                        len(getattr(kcp, "recv_buf", {}) or {}),
                        len(getattr(kcp, "recv_queue", []) or []),
                        len(getattr(kcp, "recv_frag_buf", []) or []),
                    )
                return

            is_hevc = self._video_codec == "hevc"
            if now_ts - last_gap_nudge > 0.08 and kcp.send_gap_nudge():
                last_gap_nudge = now_ts

            if is_hevc:
                skip_wait = 1.6 if gap_backlog > 96 else 2.4
                skip_interval = 1.0
                max_gaps = None if gap_backlog > 256 or stall_time > 4.0 else 2
                keyframe_wait = 8.0
            else:
                skip_wait = 1.2 if gap_backlog > 96 else 2.0
                skip_interval = 0.45
                max_gaps = None if gap_backlog > 96 or stall_time > 2.5 else 2
                keyframe_wait = KEYFRAME_WAIT_AFTER_GAP_S

            if stall_time <= skip_wait or now_ts - last_gap_skip <= skip_interval:
                return
            if kcp.skip_gap(max_gaps=max_gaps, require_iva_start=True):
                last_gap_skip = now_ts
                wait_for_keyframe_until = now_ts + keyframe_wait
                if _drain_kcp_queue():
                    last_video_time = time.time()

        def _handle_peer_packet(packet) -> None:
            nonlocal last_kcp_payload_time, last_udp_time, last_video_time
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
            if (
                peer is not None
                and confirmed_peer[0] is None
                and not is_turn_server_stun
            ):
                direct = (
                    not packet.via_turn and not self._remote and _is_private_ip(peer[0])
                )
                confirmed_peer[0] = (peer[0], peer[1], direct)

            if parsed:
                msg_type = parsed.get("type")
                if msg_type == BINDING_REQUEST and peer is not None:
                    try:
                        response = _build_ice_response(
                            parsed, ice_pwd, peer[0], peer[1]
                        )
                        if (
                            not packet.via_turn
                            and not self._remote
                            and _is_private_ip(peer[0])
                        ):
                            turn.sock.sendto(response, peer)
                        else:
                            turn.send_to_peer(peer[0], peer[1], response)
                    except Exception:
                        pass
                    return
                if msg_type == BINDING_RESPONSE:
                    kcp.retransmit_unacked()
                    return

            if parse_kcp_segment(packet.data) or packet.data[0:2] == b"\xff\x01":
                processed = kcp.process_input(packet.data)
                if processed is not None:
                    typ, payload = processed
                    if typ in ("data", "iva_data", "iva") and payload:
                        last_kcp_payload_time = time.time()
                        if _handle_kcp_payload(payload):
                            last_video_time = time.time()
                    elif typ == "handshake":
                        kcp.retransmit_unacked()
                if _drain_kcp_queue():
                    last_video_time = time.time()

        kcp.send_handshake()
        _send_start_live("initial")

        while self._running:
            now = time.time()

            if auth_fallback_at and self._video_count <= 0 and now >= auth_fallback_at:
                return AUTH_FALLBACK_RESULT

            if now >= last_ice:
                _send_ice_checks()
                last_ice = now + 2.0

            if now >= last_heartbeat:
                kcp.send_iva_data(_next_vvp(VVP_CMD_HEARTBEAT))
                last_heartbeat = now + 10.0

            if now >= last_iva_heartbeat:
                kcp.send_handshake()
                last_iva_heartbeat = now + 3.0

            video_stale = last_video_time <= 0.0 or (
                now - last_video_time > START_LIVE_IDLE_NUDGE_S
            )
            if video_stale and now >= last_start_live + START_LIVE_RETRY_S:
                _send_start_live("retry", now)

            if now >= turn_refresh:
                try:
                    turn.refresh()
                except Exception:
                    pass
                turn_refresh = now + 60.0

            packets = recv_peer_packets(turn, timeout=0.08, max_packets=512)
            if not packets:
                _attempt_gap_recovery(now)
                kcp.flush_acks()
                continue

            for packet in packets:
                if not self._running:
                    break
                _handle_peer_packet(packet)

            _attempt_gap_recovery(time.time())
            kcp.flush_acks()

        try:
            kcp.send_iva_data(_next_vvp(VVP_CMD_STOP, param=0))
            kcp.flush_acks()
        except Exception:
            pass

        return (self._video_count, self._total_bytes)
