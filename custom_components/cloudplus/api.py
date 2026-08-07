"""CloudEdge / Meari HTTP API client.

Handles authentication, device discovery, battery info, status queries,
and camera wake-up — extracted from main.py for use in Home Assistant.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import requests
from Crypto.Cipher import AES, DES
from Crypto.Util.Padding import pad, unpad

from .const import (
    BATTERY_CODES,
    DEFAULT_CA_KEY,
    DEFAULT_CA_SECRET,
    DES_IV,
    DES_KEY,
    IOT_CODE_PTZ_START,
    IOT_CODE_PTZ_STOP,
    IOT_CODE_PTZ2_START,
    IOT_CODE_PTZ2_STOP,
    PHONE_TYPE,
    PTZ_DIRECTIONS,
    REDIRECT_URL,
    TTID,
)
from .url_util import parse_host as _host

_LOGGER = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Linux; U; Android 14; en-us; Pixel Build/UP1A.231105.001) "
    "AppleWebKit/533.1 (KHTML, like Gecko) Version/5.0 Mobile Safari/533.1"
)
HTTP_TIMEOUT = (10.0, 30.0)


@dataclass(frozen=True, slots=True)
class AppProfileConfig:
    """Wire identity and behavior of one official Meari-family app."""

    source_app: str
    app_ver: str
    app_ver_code: str
    redirect_url: str
    partner_id: str
    ttid: str = TTID
    signaling_app_ver: str = ""
    encrypted_login: bool = False
    vvp_stream_flag: int = 1


APP_PROFILE_CONFIG: dict[str, AppProfileConfig] = {
    "cloudplus": AppProfileConfig("77", "6.0.1", "1029", REDIRECT_URL, "77"),
    "cloudedge": AppProfileConfig(
        "8",
        "6.1.4",
        "616",
        REDIRECT_URL,
        "8",
        signaling_app_ver="6.1.4a11",
        encrypted_login=True,
        vvp_stream_flag=0,
    ),
    "iegeek": AppProfileConfig("81", "5.5.2", "552", REDIRECT_URL, "81"),
    "arenti": AppProfileConfig(
        "39",
        "5.0.3",
        "168",
        "https://apis.arenti.net",
        "39",
        encrypted_login=True,
    ),
}

PLATFORM_REGIONS = {"eu", "us", "as", "cn"}
DEVICE_CATEGORIES = (
    "ipc",
    "snap",
    "doorbell",
    "voiceBell",
    "fourthGeneration",
    "cellular",
    "light",
    "pictureDoorBell",
    "nvr",
    "nvr-neutral",
    "base",
    "chime",
)
CAMERA_CATEGORIES = {
    "ipc",
    "snap",
    "doorbell",
    "voicebell",
    "fourthgeneration",
    "cellular",
    "light",
    "picturedoorbell",
}


# ---------------------------------------------------------------------------
# Crypto helpers
# ---------------------------------------------------------------------------


def _hmac_sha1_b64(message: str, key: str) -> str:
    sig = hmac.new(key.encode(), message.encode(), hashlib.sha1).digest()
    return base64.b64encode(sig).decode()


def _md5_hex(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def _des_encrypt(plaintext: str) -> str:
    """DES-CBC encrypt (Meari password encryption)."""
    cipher = DES.new(DES_KEY[:8], DES.MODE_CBC, DES_IV)
    ct = cipher.encrypt(pad(plaintext.encode("utf-8"), DES.block_size))
    return base64.b64encode(ct).decode()


def _password_variants(password: str) -> list[str]:
    """Generate minimal password variants for apostrophe normalization.

    Some accounts are configured with typographic apostrophes (U+2019) while
    users may type ASCII apostrophes ('). Try the alternate form only when
    apostrophe-like characters are present.
    """
    variants = [password]

    if "'" in password:
        variants.append(password.replace("'", "’"))
    if "’" in password:
        variants.append(password.replace("’", "'"))
    if "‘" in password:
        variants.append(password.replace("‘", "'"))

    out: list[str] = []
    for candidate in variants:
        if candidate not in out:
            out.append(candidate)
    return out


def _aes_encrypt(plaintext: str, key_str: str) -> str:
    """AES-CBC encrypt (Meari account encryption)."""
    key = key_str.encode("utf-8")
    cipher = AES.new(key, AES.MODE_CBC, key)
    ct = cipher.encrypt(pad(plaintext.encode("utf-8"), AES.block_size))
    return base64.b64encode(ct).decode().rstrip("\n")


def _aes_decrypt(ciphertext_b64: str, key_str: str) -> str:
    """AES-CBC decrypt (platform signature)."""
    key = key_str.encode("utf-8")
    p = 4 - len(ciphertext_b64) % 4 if len(ciphertext_b64) % 4 else 0
    ct = base64.b64decode(ciphertext_b64 + "=" * p)
    cipher = AES.new(key, AES.MODE_CBC, key)
    return unpad(cipher.decrypt(ct), AES.block_size).decode("utf-8")


def _encode_user_account(
    email: str,
    api_path: str,
    timestamp_ms: int,
    partner_id: str,
    ttid: str,
) -> str:
    raw_key = f"{api_path}{partner_id}{ttid}{timestamp_ms}"
    key_b64 = base64.b64encode(raw_key.encode()).decode()
    key16 = key_b64[:16]
    return _aes_encrypt(email, key16)


def format_sn(sn: str) -> str:
    """Convert snNum to IoT device UUID."""
    if not sn:
        return ""
    if len(sn) == 9:
        return "0000000" + sn
    return sn[4:]


def _region_code(*hints: str | None) -> str:
    for hint in hints:
        host = _host(hint).replace("-", ".")
        for part in host.split("."):
            if part in PLATFORM_REGIONS:
                return part
            if len(part) >= 4 and part[:2] in PLATFORM_REGIONS and part[2:] == "ce":
                return part[:2]
    return ""


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------


class MeariApiClient:
    """HTTP API client for CloudEdge / Meari."""

    def __init__(
        self,
        email: str,
        password: str,
        country_code: str = "FR",
        phone_code: str = "33",
        app_profile: str = "auto",
    ) -> None:
        self.email = email
        self.password = password
        self.country_code = (country_code or "FR").upper()
        self.phone_code = str(phone_code or "33").lstrip("+")
        self.app_profile = (app_profile or "cloudplus").lower()
        if self.app_profile == "auto":
            # Backward compatibility for existing config entries.
            self.app_profile = "cloudplus"

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._request_lock = threading.RLock()

        self.api_server: str = ""
        self.user_id: Optional[int] = None
        self.client_id: str = ""
        self.user_token: Optional[str] = None

        # OpenAPI / MQTT
        self.openapi_server: str = ""
        self.platform_domain: str = ""
        self.access_id: str = ""
        self.access_key: str = ""
        self.mqtt_host: str = ""
        self.mqtt_port: int = 1883
        self.mqtt_signature: str = ""

        # Devices
        self.devices: dict[Any, dict] = {}

        # Active app profile settings (selected during login)
        default_profile = APP_PROFILE_CONFIG["cloudplus"]
        self._apply_profile(default_profile)

    def _http_get(self, url: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", HTTP_TIMEOUT)
        with self._request_lock:
            return self.session.get(url, **kwargs)

    def _http_post(self, url: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", HTTP_TIMEOUT)
        with self._request_lock:
            return self.session.post(url, **kwargs)

    def _select_profile(self, profile: str) -> None:
        cfg = APP_PROFILE_CONFIG.get(profile)
        if not cfg:
            raise ValueError(f"Unknown app profile: {profile}")
        self._apply_profile(cfg)

    def _apply_profile(self, cfg: AppProfileConfig) -> None:
        self._source_app = cfg.source_app
        self._app_ver = cfg.app_ver
        self._app_ver_code = cfg.app_ver_code
        self._redirect_url = cfg.redirect_url
        self._partner_id = cfg.partner_id
        self._ttid = cfg.ttid
        self._signaling_app_ver = cfg.signaling_app_ver or f"{cfg.app_ver}a16"
        self._encrypted_login = cfg.encrypted_login
        self.vvp_stream_flag = cfg.vvp_stream_flag

    def _apply_platform_defaults(self) -> None:
        code = _region_code(self.api_server, self.openapi_server, self.platform_domain)
        if not code:
            return
        suffix = f"{code}ce"
        if not self.openapi_server:
            self.openapi_server = f"https://openapi-{suffix}.mearicloud.com"
        if not self.platform_domain:
            self.platform_domain = f"{suffix}.mearicloud.com"
        if not self.mqtt_host:
            self.mqtt_host = f"events-{suffix}.mearicloud.com"

    def _apply_login_iot_config(self, result: dict[str, Any]) -> None:
        iot = result.get("iot")
        if not isinstance(iot, dict):
            self._apply_platform_defaults()
            return

        pf_key = iot.get("pfKey")
        if isinstance(pf_key, dict):
            self.access_id = pf_key.get("accessid") or self.access_id
            self.access_key = pf_key.get("accesskey") or self.access_key
            self.openapi_server = pf_key.get("openapiDomain") or self.openapi_server
            self.platform_domain = pf_key.get("platformDomain") or self.platform_domain

        mqtt_cfg = iot.get("mqtt")
        if isinstance(mqtt_cfg, dict):
            self.mqtt_host = mqtt_cfg.get("host") or self.mqtt_host
            try:
                self.mqtt_port = int(mqtt_cfg.get("port") or self.mqtt_port)
            except (TypeError, ValueError):
                pass

        self._apply_platform_defaults()

    # ------------------------------------------------------------------
    # X-Ca-* header auth
    # ------------------------------------------------------------------

    def _ca_headers(self, api_path: str) -> dict:
        ts = str(int(time.time() * 1000))
        nonce = str(random.randint(100000, 999999))
        if self.user_token:
            ca_key = self.user_token
            sign_key = self.user_token
        else:
            ca_key = DEFAULT_CA_KEY
            sign_key = DEFAULT_CA_SECRET
        msg = (
            f"api=/ppstrongs/{api_path}"
            f"|X-Ca-Key={ca_key}"
            f"|X-Ca-Timestamp={ts}"
            f"|X-Ca-Nonce={nonce}"
        )
        sign = _hmac_sha1_b64(msg, sign_key)
        return {
            "X-Ca-Timestamp": ts,
            "X-Ca-Key": ca_key,
            "X-Ca-Nonce": nonce,
            "X-Ca-Sign": sign,
        }

    def _sign_params(self, params: dict) -> dict:
        params = dict(params)
        sorted_keys = sorted(params.keys())
        content = "&".join(f"{k}={params[k]}" for k in sorted_keys)
        params["signature"] = _hmac_sha1_b64(content, self.user_token)
        return params

    def _base_params(self) -> dict:
        ts = int(time.time() * 1000)
        tz_offset = time.timezone if time.daylight == 0 else time.altzone
        dt = datetime.fromtimestamp(ts / 1000)
        sign = f"{tz_offset // -3600:+03d}:00"
        ts_str = dt.strftime(f"%Y-%m-%dT%H:%M:%S.{ts % 1000:03d}GMT{sign}")
        params = {
            "phoneType": PHONE_TYPE,
            "sourceApp": self._source_app,
            "appVer": self._app_ver,
            "appVerCode": self._app_ver_code,
            "lngType": "en",
            "t": str(ts),
            "countryCode": self.country_code,
            "phoneCode": self.phone_code,
            "signatureMethod": "HMAC-SHA1",
            "signatureVersion": "1.0",
            "signatureNonce": str(ts),
            "timestamp": ts_str,
        }
        if self.user_id:
            params["userID"] = str(self.user_id)
        return params

    def _get(self, path: str, extra_params: dict | None = None) -> dict:
        params = self._base_params()
        if extra_params:
            params.update(extra_params)
        params = self._sign_params(params)
        url = self.api_server + path
        headers = self._ca_headers(path)
        r = self._http_get(url, params=params, headers=headers)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, extra_params: dict | None = None) -> dict:
        params = self._base_params()
        if extra_params:
            params.update(extra_params)
        params = self._sign_params(params)
        url = self.api_server + path
        headers = self._ca_headers(path)
        r = self._http_post(url, data=params, headers=headers)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------
    # OpenAPI auth
    # ------------------------------------------------------------------

    def _openapi_signature(self, path: str, action: str) -> tuple[str, str]:
        timeout = str(int(time.time()) + 60)
        msg = f"GET\n\n\n{timeout}\n{path}\n{action}"
        return _hmac_sha1_b64(msg, self.access_key), timeout

    def _openapi_get(self, path: str, params: dict) -> dict:
        self._apply_platform_defaults()
        if not self.openapi_server or not self.access_id or not self.access_key:
            raise RuntimeError("OpenAPI credentials unavailable")
        action = params.get("action", "get")
        sig, timeout = self._openapi_signature(path, action)
        params["accessid"] = self.access_id
        params["expires"] = timeout
        params["signature"] = sig
        url = self.openapi_server + path
        r = self._http_get(url, params=params)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------
    # Login flow
    # ------------------------------------------------------------------

    def login(self) -> None:
        """Full login: redirect → login → IoT config → device list."""
        with self._request_lock:
            self._select_profile(self.app_profile)
            self._redirect()
            self._do_login()
            self._get_iot_config()
            self._get_devices()

    def _redirect(self) -> None:
        ts = int(time.time() * 1000)
        nonce = "".join(str(random.randint(0, 9)) for _ in range(8))
        sign = _md5_hex(f"GET|/ppstrongs/redirect|{ts}|apis.meari.com.cn")
        params = {
            "t": str(ts),
            "localTime": str(ts),
            "nonce": nonce,
            "sign": sign,
            "partnerId": self._partner_id,
            "phoneType": PHONE_TYPE,
            "sourceApp": self._source_app,
            "appVer": self._app_ver,
            "appVerCode": self._app_ver_code,
            "countryCode": self.country_code,
            "phoneCode": self.phone_code,
            "lngType": "en",
            "userAccount": _encode_user_account(
                self.email,
                "/ppstrongs/redirect",
                ts,
                self._partner_id,
                self._ttid,
            ),
        }
        path = "/ppstrongs/redirect"
        headers = self._ca_headers(path)
        url = self._redirect_url + path
        r = self._http_get(url, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()
        if data.get("resultCode") != "1001":
            raise RuntimeError(f"Redirect failed: {data}")
        result = data["result"]
        self.api_server = result["apiServer"]
        self.country_code = result.get("countryCode", self.country_code)
        self._apply_platform_defaults()

    def _do_login(self) -> None:
        path = "/meari/app/login"
        url = self.api_server + path

        last_code: str = ""
        last_msg: str = ""

        for idx, password_candidate in enumerate(
            _password_variants(self.password), start=1
        ):
            ts = int(time.time() * 1000)
            params = {
                "phoneType": PHONE_TYPE,
                "sourceApp": self._source_app,
                "appVer": self._app_ver,
                "appVerCode": self._app_ver_code,
                "countryCode": self.country_code,
                "phoneCode": self.phone_code,
                "lngType": "en",
                "t": str(ts),
                "userAccount": _encode_user_account(
                    self.email,
                    path,
                    ts,
                    self._partner_id,
                    self._ttid,
                ),
                "localTime": str(ts),
                "password": _des_encrypt(password_candidate),
                "iotType": "4",
                "equipmentNo": " ",
            }
            if self._encrypted_login:
                params["encryStatus"] = "1"

            headers = self._ca_headers(path)
            r = self._http_post(url, data=params, headers=headers)
            r.raise_for_status()
            data = r.json()

            result_code = str(data.get("resultCode", ""))
            if result_code == "1001":
                if idx > 1:
                    _LOGGER.info(
                        "Login succeeded after password apostrophe normalization for account %s",
                        self.email,
                    )
                result = data["result"]
                self.user_id = result["userID"]
                self.client_id = str(result.get("clientId") or "").strip()
                self.user_token = result["userToken"]
                self._apply_login_iot_config(result)
                return

            last_code = result_code
            last_msg = str(data.get("resultMsg") or "")

        if last_msg:
            raise PermissionError(f"Login failed: {last_code} ({last_msg})")
        raise PermissionError(f"Login failed: {last_code}")

    def _get_iot_config(self) -> None:
        data = self._get("/v2/app/config/pf/init", {"iotType": "4"})
        result_code = str(data.get("resultCode", ""))
        if result_code != "1001":
            # Some accounts/devices intermittently return 1023 for this endpoint
            # even when auth is valid. Keep defaults and continue.
            if result_code == "1023":
                self._apply_platform_defaults()
                _LOGGER.info(
                    "IoT config returned 1023 for %s; continuing with default endpoints",
                    self.email,
                )
                return
            raise RuntimeError(f"IoT config failed: {data}")
        result = data["result"]
        pf = result.get("pfApi", {})
        openapi = pf.get("openapi", {})
        if openapi.get("domain"):
            self.openapi_server = openapi["domain"]
        mqtt_cfg = pf.get("mqtt", {})
        self.mqtt_host = mqtt_cfg.get("host") or self.mqtt_host
        try:
            self.mqtt_port = int(mqtt_cfg.get("port") or self.mqtt_port)
        except (TypeError, ValueError):
            pass
        self.mqtt_signature = pf.get("mqttSignature", "")

        # Decrypt platform signature for OpenAPI credentials
        platform = pf.get("platform", {})
        if platform.get("domain"):
            self.platform_domain = platform["domain"]
        plat_signature = platform.get("signature", "")
        expire_time = str(platform.get("expireTime", ""))
        if plat_signature and expire_time:
            key_temp = f"{self.user_id}{self._partner_id}{self._ttid}{expire_time}"
            key_b64 = base64.b64encode(key_temp.encode()).decode().rstrip("=")
            key16 = key_b64[:16]
            decrypted = _aes_decrypt(plat_signature, key16)
            parts = decrypted.split("-")
            info_b64 = parts[0]
            p = 4 - len(info_b64) % 4 if len(info_b64) % 4 else 0
            info_json = base64.b64decode(info_b64 + "=" * p).decode()
            info = json.loads(info_json)
            self.access_id = info["accessid"]
            self.access_key = info["accesskey"]
        self._apply_platform_defaults()

    @staticmethod
    def _collect_devices_from_payload(
        payload: dict[str, Any],
        target: dict[Any, dict],
        home_id: Optional[str] = None,
    ) -> None:
        category_by_key = {category.lower(): category for category in DEVICE_CATEGORIES}

        def infer_category(dev: dict[str, Any], current: str | None) -> str:
            if current:
                return current
            try:
                dev_type = int(dev.get("devTypeID", 0) or 0)
            except (TypeError, ValueError):
                dev_type = 0
            return "snap" if dev_type == 5 else "ipc"

        def add_device(dev: Any, category: str | None) -> None:
            if not isinstance(dev, dict):
                return
            dev_id = dev.get("deviceID")
            if dev_id is None or not dev.get("snNum"):
                return
            normalized = dict(dev)
            normalized["_category"] = infer_category(normalized, category)
            if home_id:
                normalized["_home_id"] = home_id
            target[dev_id] = normalized

        def walk(node: Any, category: str | None = None) -> None:
            if isinstance(node, list):
                for item in node:
                    add_device(item, category)
                    walk(item, category)
                return
            if not isinstance(node, dict):
                return
            for key, value in node.items():
                child_category = category_by_key.get(str(key).lower())
                if child_category and isinstance(value, list):
                    for item in value:
                        add_device(item, child_category)
                    continue
                if key == "deviceList" and isinstance(value, list):
                    for item in value:
                        add_device(item, category)
                    continue
                walk(value, child_category or category)

        walk(payload)

    def _get_homes(self) -> list[dict[str, Any]]:
        data = self._get("/v1/app/home/list")
        if data.get("resultCode") != "1001":
            raise RuntimeError(f"Home list failed: {data}")
        homes = data.get("result", {}).get("homes", [])
        return homes if isinstance(homes, list) else []

    def _get_home_devices(self, home_id: str) -> dict[Any, dict]:
        data = self._get(
            "/v1/app/home/join/device/list",
            {"homeID": home_id, "funSwitch": "1"},
        )
        result_code = str(data.get("resultCode", ""))
        if result_code not in {"1001", "1107"}:
            raise RuntimeError(f"Home device list failed for home {home_id}: {data}")
        devices: dict[Any, dict] = {}
        self._collect_devices_from_payload(data, devices, home_id=home_id)
        return devices

    def _get_devices(self) -> None:
        devices: dict[Any, dict] = {}
        default_result_code = ""
        home_error = ""

        # Default/home API (works for owner accounts and some shared setups).
        default_payload: Optional[dict[str, Any]] = None
        try:
            default_payload = self._post("/v1/app/device/info/get", {"funSwitch": "1"})
        except (OSError, ValueError, KeyError, RuntimeError) as err:
            _LOGGER.debug("Default device list failed: %s", err)

        if isinstance(default_payload, dict):
            default_result_code = str(default_payload.get("resultCode", ""))
            if default_result_code == "1001":
                self._collect_devices_from_payload(default_payload, devices)
            else:
                _LOGGER.debug("Default device list returned %s", default_result_code)

        # Multi-home fallback/augmentation for invited/family homes.
        try:
            homes = self._get_homes()
        except (OSError, ValueError, KeyError, RuntimeError) as err:
            home_error = str(err)
            _LOGGER.debug("Home list discovery failed: %s", err)
            homes = []

        for home in homes:
            home_id = home.get("homeID")
            if not home_id:
                continue
            try:
                home_devices = self._get_home_devices(str(home_id))
                devices.update(home_devices)
            except (OSError, ValueError, KeyError, RuntimeError) as err:
                _LOGGER.debug("Home device list failed for home %s: %s", home_id, err)

        if not devices:
            _LOGGER.debug(
                "Device discovery returned no cameras for %s "
                "(profile=%s, api_server=%s, openapi=%s, default_code=%s, home_error=%s)",
                self.email,
                self.app_profile,
                self.api_server or "unknown",
                self.openapi_server or "unknown",
                default_result_code or "none",
                home_error or "none",
            )

        self.devices = devices

    # ------------------------------------------------------------------
    # Device queries
    # ------------------------------------------------------------------

    def get_snap_devices(self) -> list[dict]:
        """Return only battery (snap) cameras."""
        return [d for d in self.devices.values() if d.get("_category") == "snap"]

    def get_camera_devices(self) -> list[dict]:
        """Return camera-capable devices (battery + wired cameras)."""
        return [
            d
            for d in self.devices.values()
            if str(d.get("_category", "")).lower() in CAMERA_CATEGORIES
        ]

    def get_device_events(
        self,
        device_id: int | str,
        day: str,
        *,
        index: int = 0,
        direction: int = 1,
    ) -> list[dict[str, Any]]:
        """Return the alarm-event log for one device on a given day.

        Mirrors the official app's Messages tab (``/v3/app/event/list``).
        Unlike ``/v3/app/event/new/get`` this is the real per-event log: each
        entry carries the actual ``eventType`` and a stable ``msgID``, and it is
        independent of notification read-state — so it stays reliable even when
        the user's phone app has already read (and cleared) the notifications,
        and when the live MQTT push is being evicted by a duplicate login.

        *day* is ``YYYYMMDD`` in the device's local time; *direction* ``1`` is
        newest-first, *index* ``0`` is the first (most recent) page.
        """
        data = self._get(
            "/v3/app/event/list",
            {
                "deviceID": str(device_id),
                "day": str(day),
                "direction": str(direction),
                "index": str(index),
            },
        )
        if str(data.get("resultCode", "")) != "1001":
            raise RuntimeError(f"Device event list failed: {data}")
        # The event log is returned at the top level (not under ``result``).
        events = data.get("alertMsg")
        return events if isinstance(events, list) else []

    def get_new_device_events(self) -> list[dict[str, Any]]:
        """Return all-device unread/new alarm summaries.

        CloudEdge home screen calls ``/v3/app/event/new/get`` with
        ``listAllDevice=1``. It is not as complete as the per-device event log
        because read state can clear it.
        """
        data = self._get("/v3/app/event/new/get", {"listAllDevice": "1"})
        if str(data.get("resultCode", "")) != "1001":
            raise RuntimeError(f"New-device event summary failed: {data}")
        devices = data.get("result", {}).get("device")
        if isinstance(devices, list):
            return [event for event in devices if isinstance(event, dict)]
        return []

    def get_device_status(self, sn_num: str) -> str:
        """Query device status via OpenAPI. Returns online/offline/dormancy."""
        device_id = format_sn(sn_num)
        params = {"action": "query", "deviceid": device_id}
        try:
            result = self._openapi_get("/openapi/device/status", params)
            return result.get("status", "unknown")
        except (OSError, ValueError, KeyError, RuntimeError) as e:
            _LOGGER.debug("Status query failed: %s", e)
            return "unknown"

    def get_battery_info(self, sn_num: str) -> dict[str, Any]:
        """Get battery info for a device. Returns {code: value} dict."""
        sn_map = {sn_num: BATTERY_CODES}
        data = self._get(
            "/v2/app/iot/model/get/batch",
            {
                "snIdentifier": json.dumps(sn_map, separators=(",", ":")),
            },
        )
        if str(data.get("resultCode", "")) != "1001":
            raise RuntimeError(f"Battery info failed: {data}")
        return data.get("result", {}).get(sn_num, {})

    # ------------------------------------------------------------------
    # Lamp / LED control via OpenAPI device config
    # ------------------------------------------------------------------

    def get_device_iot_config(self, sn_num: str) -> dict[str, Any]:
        """Fetch full IoT config for a device via OpenAPI.

        Returns the ``iot`` dict from the response (code→value mapping).
        """
        dev_uuid = format_sn(sn_num)
        params_payload = json.dumps(
            {"code": 100001, "action": "get", "name": "iot"},
            separators=(",", ":"),
        )
        params_b64 = base64.b64encode(params_payload.encode()).decode()
        resp = self._openapi_get(
            "/openapi/device/config",
            {
                "action": "get",
                "params": params_b64,
                "deviceid": dev_uuid,
                "target": "server",
            },
        )
        if "errid" in resp:
            raise RuntimeError(f"Device IoT config failed: {resp}")
        return resp.get("iot", {})

    def get_device_iot_values(
        self,
        sn_num: str,
        codes: list[str] | tuple[str, ...],
    ) -> dict[str, Any]:
        """Fetch selected IoT values for a device via OpenAPI."""
        dev_uuid = format_sn(sn_num)
        params_payload = json.dumps(
            {"code": 100001, "action": "get", "name": "iot", "iot": list(codes)},
            separators=(",", ":"),
        )
        params_b64 = base64.b64encode(params_payload.encode()).decode()
        resp = self._openapi_get(
            "/openapi/device/config",
            {
                "action": "get",
                "params": params_b64,
                "deviceid": dev_uuid,
            },
        )
        if "errid" in resp:
            raise RuntimeError(f"Device IoT values failed: {resp}")
        return resp.get("iot", {})

    def set_device_iot_value(self, sn_num: str, code: str, value: int) -> bool:
        """Set a single IoT config value on the device via OpenAPI."""
        dev_uuid = format_sn(sn_num)
        params_payload = json.dumps(
            {"code": 100001, "action": "set", "name": "iot", "iot": {code: value}},
            separators=(",", ":"),
        )
        params_b64 = base64.b64encode(params_payload.encode()).decode()
        resp = self._openapi_get(
            "/openapi/device/config",
            {
                "action": "set",
                "params": params_b64,
                "deviceid": dev_uuid,
                "target": "server",
            },
        )
        return resp.get("action") == "set"

    def ptz_start(self, sn_num: str, direction: str, *, use_ptz2: bool = True) -> bool:
        """Send a PTZ start command for the given direction.

        Uses IoT code 841 (ptz2) when *use_ptz2* is True, else 807 (ptz).
        The command is sent WITHOUT ``target=server`` so it reaches the
        device directly (as the official app does for codes 800-899).
        """
        ps, ts = PTZ_DIRECTIONS.get(direction, (0, 0))
        code = IOT_CODE_PTZ2_START if use_ptz2 else IOT_CODE_PTZ_START
        value_str = json.dumps({"ps": ps, "ts": ts, "zs": 0}, separators=(",", ":"))

        dev_uuid = format_sn(sn_num)
        params_payload = json.dumps(
            {"code": 100001, "action": "set", "name": "iot", "iot": {code: value_str}},
            separators=(",", ":"),
        )
        params_b64 = base64.b64encode(params_payload.encode()).decode()
        resp = self._openapi_get(
            "/openapi/device/config",
            {
                "action": "set",
                "params": params_b64,
                "deviceid": dev_uuid,
            },
        )
        return "errid" not in resp

    def ptz_stop(self, sn_num: str, *, use_ptz2: bool = True) -> bool:
        """Send a PTZ stop command."""
        code = IOT_CODE_PTZ2_STOP if use_ptz2 else IOT_CODE_PTZ_STOP

        dev_uuid = format_sn(sn_num)
        params_payload = json.dumps(
            {"code": 100001, "action": "set", "name": "iot", "iot": {code: "{}"}},
            separators=(",", ":"),
        )
        params_b64 = base64.b64encode(params_payload.encode()).decode()
        resp = self._openapi_get(
            "/openapi/device/config",
            {
                "action": "set",
                "params": params_b64,
                "deviceid": dev_uuid,
            },
        )
        return "errid" not in resp

    def wake_device(self, sn_num: str, device_id: int) -> bool:
        """Wake a dormant camera using both OpenAPI and HTTP methods."""
        success = False
        self._apply_platform_defaults()
        # Method 1: OpenAPI wake
        if self.openapi_server and self.access_id and self.access_key:
            dev_uuid = format_sn(sn_num)
            sid = (dev_uuid + str(int(time.time() * 1000)))[:30]
            try:
                sig, timeout = self._openapi_signature("/openapi/device/awaken", "set")
                params = {
                    "accessid": self.access_id,
                    "expires": timeout,
                    "signature": sig,
                    "action": "set",
                    "deviceid": dev_uuid,
                    "sid": sid,
                }
                url = self.openapi_server + "/openapi/device/awaken"
                r = self._http_get(url, params=params)
                if r.status_code == 200:
                    success = True
            except OSError as e:
                _LOGGER.debug("OpenAPI wake failed: %s", e)

        # Method 2: Bell remote wake
        try:
            self._post("/v1/app/bell/remote/wake", {"deviceID": str(device_id)})
            success = True
        except (OSError, ValueError, KeyError, RuntimeError) as e:
            _LOGGER.debug("Bell wake failed: %s", e)

        return success

    def get_snapshot_url(self, dev: dict) -> Optional[str]:
        """Get the last known snapshot URL from device info."""
        # The device info often contains an imageUrl field
        return dev.get("imageUrl") or dev.get("thumbUrl") or None

    def download_snapshot(self, url: str) -> Optional[bytes]:
        """Download a JPEG snapshot from a URL."""
        if not url:
            return None
        try:
            r = self._http_get(url, timeout=10)
            if r.status_code == 200 and len(r.content) > 100:
                return r.content
        except OSError:
            pass
        return None


def build_api_client(
    email: str,
    password: str,
    country_code: str,
    phone_code: str,
    app_profile: str,
) -> MeariApiClient:
    """Construct a MeariApiClient from resolved credential fields."""
    return MeariApiClient(
        email=email,
        password=password,
        country_code=country_code,
        phone_code=phone_code,
        app_profile=app_profile,
    )
