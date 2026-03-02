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
import time
from datetime import datetime
from typing import Any, Optional

import requests
from Crypto.Cipher import AES, DES
from Crypto.Util.Padding import pad, unpad

from .const import (
    APP_VER,
    APP_VER_CODE,
    BATTERY_CODES,
    DEFAULT_CA_KEY,
    DEFAULT_CA_SECRET,
    DES_IV,
    DES_KEY,
    PARTNER_ID,
    PHONE_TYPE,
    REDIRECT_URL,
    SOURCE_APP,
    TTID,
)

_LOGGER = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Linux; U; Android 14; en-us; Pixel Build/UP1A.231105.001) "
    "AppleWebKit/533.1 (KHTML, like Gecko) Version/5.0 Mobile Safari/533.1"
)


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


def _encode_user_account(email: str, api_path: str, timestamp_ms: int) -> str:
    raw_key = f"{api_path}{PARTNER_ID}{TTID}{timestamp_ms}"
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


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------


class MeariApiClient:
    """HTTP API client for CloudEdge / Meari / CloudPlus."""

    def __init__(
        self,
        email: str,
        password: str,
        country_code: str = "FR",
        phone_code: str = "33",
    ) -> None:
        self.email = email
        self.password = password
        self.country_code = country_code
        self.phone_code = phone_code

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

        self.api_server: str = ""
        self.user_id: Optional[int] = None
        self.user_token: Optional[str] = None

        # OpenAPI / MQTT
        self.openapi_server: str = ""
        self.access_id: str = ""
        self.access_key: str = ""
        self.mqtt_host: str = ""
        self.mqtt_port: int = 1883
        self.mqtt_signature: str = ""

        # Devices
        self.devices: dict[int, dict] = {}

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
            "sourceApp": SOURCE_APP,
            "appVer": APP_VER,
            "appVerCode": APP_VER_CODE,
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
        r = self.session.get(url, params=params, headers=headers)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, extra_params: dict | None = None) -> dict:
        params = self._base_params()
        if extra_params:
            params.update(extra_params)
        params = self._sign_params(params)
        url = self.api_server + path
        headers = self._ca_headers(path)
        r = self.session.post(url, data=params, headers=headers)
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
        action = params.get("action", "get")
        sig, timeout = self._openapi_signature(path, action)
        params["accessid"] = self.access_id
        params["expires"] = timeout
        params["signature"] = sig
        url = self.openapi_server + path
        r = self.session.get(url, params=params)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------
    # Login flow
    # ------------------------------------------------------------------

    def login(self) -> None:
        """Full login: redirect → login → IoT config → device list."""
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
            "partnerId": PARTNER_ID,
            "phoneType": PHONE_TYPE,
            "sourceApp": SOURCE_APP,
            "appVer": APP_VER,
            "appVerCode": APP_VER_CODE,
            "countryCode": self.country_code,
            "phoneCode": self.phone_code,
            "lngType": "en",
            "userAccount": _encode_user_account(
                self.email, "/ppstrongs/redirect", ts
            ),
        }
        path = "/ppstrongs/redirect"
        headers = self._ca_headers(path)
        url = REDIRECT_URL + path
        r = self.session.get(url, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()
        if data.get("resultCode") != "1001":
            raise RuntimeError(f"Redirect failed: {data}")
        result = data["result"]
        self.api_server = result["apiServer"]
        self.country_code = result.get("countryCode", self.country_code)

    def _do_login(self) -> None:
        ts = int(time.time() * 1000)
        path = "/meari/app/login"
        params = {
            "phoneType": PHONE_TYPE,
            "sourceApp": SOURCE_APP,
            "appVer": APP_VER,
            "appVerCode": APP_VER_CODE,
            "countryCode": self.country_code,
            "phoneCode": self.phone_code,
            "lngType": "en",
            "t": str(ts),
            "userAccount": _encode_user_account(self.email, path, ts),
            "localTime": str(ts),
            "password": _des_encrypt(self.password),
            "iotType": "4",
            "equipmentNo": " ",
        }
        headers = self._ca_headers(path)
        url = self.api_server + path
        r = self.session.post(url, data=params, headers=headers)
        r.raise_for_status()
        data = r.json()
        if data.get("resultCode") != "1001":
            raise PermissionError(f"Login failed: {data.get('resultCode')}")
        result = data["result"]
        self.user_id = result["userID"]
        self.user_token = result["userToken"]

    def _get_iot_config(self) -> None:
        data = self._get("/v2/app/config/pf/init", {"iotType": "4"})
        if data.get("resultCode") != "1001":
            raise RuntimeError(f"IoT config failed: {data}")
        result = data["result"]
        pf = result.get("pfApi", {})
        openapi = pf.get("openapi", {})
        if openapi.get("domain"):
            self.openapi_server = openapi["domain"]
        mqtt_cfg = pf.get("mqtt", {})
        self.mqtt_host = mqtt_cfg.get("host", "events-euce.mearicloud.com")
        self.mqtt_port = int(mqtt_cfg.get("port", 1883))
        self.mqtt_signature = pf.get("mqttSignature", "")

        # Decrypt platform signature for OpenAPI credentials
        platform = pf.get("platform", {})
        plat_signature = platform.get("signature", "")
        expire_time = str(platform.get("expireTime", ""))
        if plat_signature and expire_time:
            key_temp = f"{self.user_id}{PARTNER_ID}{TTID}{expire_time}"
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

    def _get_devices(self) -> None:
        data = self._post("/v1/app/device/info/get", {"funSwitch": "1"})
        if data.get("resultCode") != "1001":
            raise RuntimeError(f"Device list failed: {data}")
        self.devices = {}
        for category in ["ipc", "snap", "chime", "nvr"]:
            for dev in data.get("result", {}).get(category, data.get(category, [])):
                dev_id = dev.get("deviceID")
                if dev_id:
                    dev["_category"] = category
                    self.devices[dev_id] = dev

    # ------------------------------------------------------------------
    # Device queries
    # ------------------------------------------------------------------

    def get_snap_devices(self) -> list[dict]:
        """Return only battery (snap) cameras."""
        return [d for d in self.devices.values() if d.get("_category") == "snap"]

    def get_device_status(self, sn_num: str) -> str:
        """Query device status via OpenAPI. Returns online/offline/dormancy."""
        device_id = format_sn(sn_num)
        params = {"action": "query", "deviceid": device_id}
        try:
            result = self._openapi_get("/openapi/device/status", params)
            return result.get("status", "unknown")
        except Exception as e:
            _LOGGER.debug("Status query failed: %s", e)
            return "unknown"

    def get_battery_info(self, sn_num: str) -> dict[str, Any]:
        """Get battery info for a device. Returns {code: value} dict."""
        try:
            sn_map = {sn_num: BATTERY_CODES}
            data = self._get("/v2/app/iot/model/get/batch", {
                "snIdentifier": json.dumps(sn_map, separators=(",", ":")),
            })
            if data.get("resultCode") == "1001":
                return data.get("result", {}).get(sn_num, {})
        except Exception as e:
            _LOGGER.debug("Battery info failed: %s", e)
        return {}

    def wake_device(self, sn_num: str, device_id: int) -> bool:
        """Wake a dormant camera using both OpenAPI and HTTP methods."""
        success = False
        # Method 1: OpenAPI wake
        try:
            dev_uuid = format_sn(sn_num)
            sid = (dev_uuid + str(int(time.time() * 1000)))[:30]
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
            r = self.session.get(url, params=params)
            if r.status_code == 200:
                success = True
        except Exception as e:
            _LOGGER.debug("OpenAPI wake failed: %s", e)

        # Method 2: Bell remote wake
        try:
            self._post("/v1/app/bell/remote/wake", {"deviceID": str(device_id)})
            success = True
        except Exception as e:
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
            r = self.session.get(url, timeout=10)
            if r.status_code == 200 and len(r.content) > 100:
                return r.content
        except Exception:
            pass
        return None
