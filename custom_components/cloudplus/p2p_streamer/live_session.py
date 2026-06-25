"""The per-session live streaming loop (TURN + KCP + VVP).

Carved out of ``engine.py`` as a mixin to keep both files under the
1000-line limit; ``P2PStreamer`` inherits it. The method body is unchanged.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from typing import Any, Callable

from ..meari_signaling import MsgSvrClient
from ..turn_client import BINDING_REQUEST, BINDING_RESPONSE, TurnClient
from ..kcp_tunnel import (
    KCP_CMD_PUSH,
    KCP_WND,
    KcpTunnel,
    parse_kcp_segment,
    parse_kcp_segments,
)
from .network import (
    _build_ice_response,
    _get_local_ips,
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
from .codecs import gap_recovery_for, runtime_policy_for
from .protocol import (
    VVP_CMD_HEARTBEAT,
    VVP_CMD_STOP,
    VVP_CMD_START_LIVE,
    STREAM_TYPE_IFRAME,
    build_vvp_packet,
    format_licence_id,
)
from .relay_probe import probe_relay
from .keepalive import SnapDeviceKeepalive
from .session_support import (
    AUTH_FALLBACK_NO_VIDEO_S,
    AUTH_FALLBACK_RESULT,
    CLIENT_KEYFRAME_REQUEST_DEBOUNCE_S,
    DIRECT_ICE_CHECK_INTERVAL_S,
    DIRECT_ICE_SEEK_WINDOW_S,
    DIRECT_STARTUP_ICE_GRACE_S,
    DIRECT_SOURCE_IDLE_RECONNECT_S,
    ICE_KEEPALIVE_INTERVAL_S,
    IVA_HEARTBEAT_S,
    VVP_HEARTBEAT_S,
    _unwrap_iva_payload,
)

# Log under the engine logger so existing log filters and the grep-targets in
# docs/diagnosis.md keep working now that this loop was carved out of engine.py.
_LOGGER = logging.getLogger(f"{__package__}.engine")


class LiveSessionMixin:
    """Provides the live streaming loop (`_stream_with_turn`) for P2PStreamer."""

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
        was_dormant: bool = False,
        fire_wake: Callable[[], None] | None = None,
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
        offer_sdp = "\n".join(sdp_lines) + "\n"
        if was_dormant:
            camera_ufrag, camera_pwd, camera_candidates = self._negotiate_dormant_offer(
                sig, device_uuid, offer_sdp, fire_wake
            )
        else:
            answer = sig.send_offer(device_uuid, offer_sdp)
            camera_ufrag, camera_pwd, camera_candidates = parse_sdp_answer(
                answer.get("sdp", "")
            )
            collect_trickle_candidates(sig, camera_candidates)
        self.awaiting_wake = False
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
                    turn.send_direct(endpoint[0], endpoint[1], lan_connect_frame)
                except OSError:
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
        except OSError:
            pass

        confirmed_peer: list[tuple[str, int, bool] | None] = [None]
        direct_ice_peer: list[tuple[str, int] | None] = [None]
        candidate_fanout_until = time.time() + 8.0

        def _send_udp(data: bytes) -> None:
            if confirmed_peer[0] is None and direct_ice_peer[0] is not None:
                try:
                    peer_ip, peer_port = direct_ice_peer[0]
                    turn.send_direct(peer_ip, peer_port, data)
                    return
                except OSError:
                    pass
            fanout = time.time() < candidate_fanout_until
            if confirmed_peer[0] is not None:
                peer_ip, peer_port, direct = confirmed_peer[0]
                direct_sent = False
                if direct and not self._remote:
                    try:
                        turn.send_direct(peer_ip, peer_port, data)
                        direct_sent = True
                    except OSError:
                        pass
                try:
                    turn.send_to_peer(peer_ip, peer_port, data)
                    if not fanout:
                        return
                except OSError:
                    pass
                if direct_sent and not fanout:
                    return
            for cand in camera_candidates:
                try:
                    turn.send_to_peer(cand["ip"], cand["port"], data)
                except OSError:
                    pass
                if not self._remote and cand.get("type") != "relay":
                    try:
                        turn.send_direct(cand["ip"], cand["port"], data)
                    except OSError:
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
            except (OSError, ValueError):
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
                except OSError:
                    pass
                if not self._remote and cand.get("type") != "relay":
                    try:
                        _send_direct_ice_binding(
                            turn.open_socket(),
                            cand["ip"],
                            cand["port"],
                            ice_ufrag,
                            camera_ufrag,
                            camera_pwd,
                        )
                    except OSError:
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
                if stall_time > 1.2 and not self._remote:
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
                    and 0.0 < idle_retry_s <= stall_time
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
                if self.direct_confirmed:
                    # Direct LAN often survives brief camera radio silences;
                    # keep nudging START_LIVE before falling back to reconnect.
                    idle_reconnect_s = max(
                        idle_reconnect_s, DIRECT_SOURCE_IDLE_RECONNECT_S
                    )
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
            nonlocal candidate_fanout_until, last_udp_time, last_kcp_push_time
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
                    if not packet.via_turn and not self._remote:
                        direct_ice_peer[0] = peer
                    try:
                        response = _build_ice_response(
                            parsed, ice_pwd, peer[0], peer[1]
                        )
                        if not packet.via_turn and not self._remote:
                            turn.send_direct(peer[0], peer[1], response)
                        else:
                            turn.send_to_peer(peer[0], peer[1], response)
                    except OSError:
                        pass
                    return
                if msg_type == BINDING_RESPONSE:
                    if peer is not None and not packet.via_turn and not self._remote:
                        direct_ice_peer[0] = peer
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
                    if current is None or (direct and not current[2]):
                        confirmed_peer[0] = (peer[0], peer[1], direct)
                        if direct:
                            # Prefer confirmed LAN media and stop fallback fanout.
                            self.direct_confirmed = True
                            candidate_fanout_until = 0.0
                        elif self._remote and packet.via_turn:
                            # On WAN, the native app settles on the relay quickly.
                            # Continuing candidate fanout multiplies ACKs and
                            # recovery nudges across fallbacks that cannot carry
                            # media from here.
                            candidate_fanout_until = 0.0
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

        def _seek_direct_ice_before_startup() -> None:
            if self._remote or not lan_connect_endpoints:
                return
            deadline = time.time() + DIRECT_STARTUP_ICE_GRACE_S
            while (
                self._running and direct_ice_peer[0] is None and time.time() < deadline
            ):
                now_ts = time.time()
                _send_lan_connect(now_ts, force=True)
                _send_ice_checks()
                for packet in recv_peer_packets(
                    turn,
                    timeout=DIRECT_ICE_CHECK_INTERVAL_S,
                    max_packets=256,
                ):
                    _handle_peer_packet(packet)
                kcp.flush_acks()

        now = time.time()
        _seek_direct_ice_before_startup()
        now = time.time()
        _send_lan_connect(now, force=True)
        kcp.send_handshake()
        # Official app sends START_LIVE in the very next KCP push (sn=1) without
        # waiting for the camera's handshake echo or any ACK. Mirroring that
        # avoids a class of post-dormancy stalls where the camera ACKs but never
        # echoes the IVA handshake, then ignores a delayed START_LIVE.
        _send_start_live("startup", now)
        last_iva_heartbeat = now + IVA_HEARTBEAT_S
        ice_aggressive_deadline = now + DIRECT_ICE_SEEK_WINDOW_S

        while self._running:
            now = time.time()
            # Keep ICE rapid until a direct pair wins or the seek window expires.
            peer = confirmed_peer[0]
            ice_aggressive = peer is None or (
                not peer[2] and now < ice_aggressive_deadline
            )
            _send_lan_connect(now)

            if auth_fallback_at and self._video_count <= 0 and now >= auth_fallback_at:
                return AUTH_FALLBACK_RESULT

            if now >= last_ice:
                _send_ice_checks()
                # Match the official app's LAN cadence: rapid nominated checks
                # until direct media wins, then lightweight ICE keepalives.
                last_ice = now + (
                    DIRECT_ICE_CHECK_INTERVAL_S
                    if ice_aggressive
                    else ICE_KEEPALIVE_INTERVAL_S
                )
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
                client_join_stale = live_started and runtime_policy_for(
                    self._video_codec
                ).idle_start_live_retry_s <= _video_wait_s(now)
                if client_join_stale and now >= (
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

            keepalive_s = runtime_policy_for(self._video_codec).start_live_keepalive_s
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
                except OSError:
                    pass
                turn_refresh = now + 60.0

            # Keep the loop tight while seeking/streaming so ICE replies and
            # KCP ACKs stay close to native cadence.
            streaming_or_seeking = ice_aggressive or confirmed_peer[0] is not None
            packets = recv_peer_packets(
                turn,
                timeout=0.02 if streaming_or_seeking else 0.08,
                max_packets=2048,
            )
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
        except OSError:
            pass
        finally:
            if self._active_stop_live is _send_stop_live:
                self._active_stop_live = None

        return (self._video_count, self._total_bytes)
