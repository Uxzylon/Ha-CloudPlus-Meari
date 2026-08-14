"""The CloudEdge / Meari camera integration."""

from __future__ import annotations

import logging
import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigEntryDisabler,
    SOURCE_IMPORT,
)
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er

from .const import (
    CONF_APP_PROFILE,
    CONF_COUNTRY_CODE,
    CONF_DEVICE_ID,
    CONF_DEVICE_NAME,
    CONF_EMAIL,
    CONF_PASSWORD,
    CONF_PHONE_CODE,
    CONF_SN_NUM,
    CONF_VIDEO_PASSWORD,
    DEFAULT_APP_PROFILE,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_PHONE_CODE,
    DOMAIN,
    PTZ_DIRECTIONS,
)
from .coordinator import CloudEdgeMeariCoordinator
from .api import MeariApiClient, build_api_client

_LOGGER = logging.getLogger(__name__)

ACCOUNT_ENTRY_ID = "account_entry_id"
DISABLED_BY_ACCOUNT = "disabled_by_account"
SUPPRESS_CAMERA_RECREATE = "_suppress_camera_recreate"

PLATFORMS = [
    "camera",
    "binary_sensor",
    "button",
    "sensor",
    "number",
    "select",
    "switch",
]

SERVICE_PTZ = "ptz"
SERVICE_PTZ_SCHEMA = vol.Schema(
    {
        vol.Required("action"): vol.In(["move", "stop"]),
        vol.Optional("argument"): vol.In(list(PTZ_DIRECTIONS.keys())),
    },
    extra=vol.ALLOW_EXTRA,
)


# ---------------------------------------------------------------------------
# Migration handler
# ---------------------------------------------------------------------------


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate old config entry versions.

    V1 was account-centric (no sn_num). We bump the version so setup can run
    and the account-entry path will auto-create per-camera sub-entries.
    """
    _LOGGER.debug(
        "Migrating config entry %s from version %s", entry.entry_id, entry.version
    )
    if entry.version == 1:
        # Remove entities and devices that were attached to the old account-centric
        # entry so they don't appear as duplicates alongside the new per-camera entries.
        ent_reg = er.async_get(hass)
        for ent in er.async_entries_for_config_entry(ent_reg, entry.entry_id):
            ent_reg.async_remove(ent.entity_id)
        dev_reg = dr.async_get(hass)
        for dev in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
            dev_reg.async_remove_device(dev.id)

        hass.config_entries.async_update_entry(entry, version=2)
        _LOGGER.info("Migration to version 2 successful for %s", entry.title)
        return True

    _LOGGER.warning(
        "Unknown config entry version %s for %s", entry.version, entry.title
    )
    return False


# ---------------------------------------------------------------------------
# Account entry helpers
# ---------------------------------------------------------------------------


async def _ensure_camera_entries(
    hass: HomeAssistant, account_entry: ConfigEntry, api: MeariApiClient
) -> None:
    """Create per-camera entries for any cameras not yet registered.

    Carries forward per-device video passwords from old V1 options format.
    """
    devices = await hass.async_add_executor_job(api.get_camera_devices)

    existing_sns: set[str] = {
        e.data.get(CONF_SN_NUM, "")
        for e in hass.config_entries.async_entries(DOMAIN)
        if e.data.get(CONF_SN_NUM)
    }

    account_creds = {
        CONF_EMAIL: account_entry.data[CONF_EMAIL],
        CONF_PASSWORD: account_entry.data[CONF_PASSWORD],
        CONF_COUNTRY_CODE: account_entry.data.get(
            CONF_COUNTRY_CODE, DEFAULT_COUNTRY_CODE
        ),
        CONF_PHONE_CODE: account_entry.data.get(CONF_PHONE_CODE, DEFAULT_PHONE_CODE),
        CONF_APP_PROFILE: account_entry.data.get(CONF_APP_PROFILE, DEFAULT_APP_PROFILE),
    }

    for dev in devices:
        sn = str(dev.get("snNum", "")).strip()
        if not sn or sn in existing_sns:
            continue
        name = str(dev.get("deviceName", sn)).strip() or sn

        # Carry over video password from V1 options (sn-prefixed key).
        video_password = str(
            account_entry.options.get(f"{sn}_video_password", "")
        ).strip()

        import_data = {
            **account_creds,
            "account_entry_id": account_entry.entry_id,
            CONF_SN_NUM: sn,
            CONF_DEVICE_ID: dev.get("deviceID"),
            CONF_DEVICE_NAME: name,
            "device": dev,
        }
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN,
                context={"source": SOURCE_IMPORT},
                data=import_data,
            )
        )
        if video_password:
            _LOGGER.info(
                "Camera %s had a video password in the old account entry; "
                "please re-enter it via Configure on the camera entry.",
                name,
            )


def _account_camera_entries(
    hass: HomeAssistant, account_entry_id: str
) -> list[ConfigEntry]:
    """Return all camera entries linked to an account entry."""
    return [
        config_entry
        for config_entry in hass.config_entries.async_entries(DOMAIN)
        if config_entry.data.get(ACCOUNT_ENTRY_ID) == account_entry_id
    ]


def _linked_account_entry(
    hass: HomeAssistant, camera_entry: ConfigEntry
) -> ConfigEntry | None:
    """Return the parent account entry for a camera entry."""
    account_entry_id = camera_entry.data.get(ACCOUNT_ENTRY_ID)
    if not account_entry_id:
        return None
    return hass.config_entries.async_get_entry(account_entry_id)


async def _restore_account_managed_camera_entries(
    hass: HomeAssistant, account_entry: ConfigEntry
) -> None:
    """Re-enable camera entries that were auto-disabled with the account.

    Skips restoration if this is an auto-enable triggered by a camera being
    manually enabled by the user.
    """
    # If this account is being re-enabled because a specific camera was manually
    # enabled by the user, don't restore other managed cameras
    if hass.data[DOMAIN].get("_auto_enabling_for"):
        _LOGGER.debug(
            "Skipping camera restoration: account being auto-enabled for a camera"
        )
        return

    for camera_entry in _account_camera_entries(hass, account_entry.entry_id):
        if not camera_entry.data.get(DISABLED_BY_ACCOUNT):
            continue

        # Fetch fresh entry from registry to ensure options are up-to-date
        fresh_entry = hass.config_entries.async_get_entry(camera_entry.entry_id)
        if fresh_entry is None:
            continue

        camera_data = dict(fresh_entry.data)
        camera_data.pop(DISABLED_BY_ACCOUNT, None)
        hass.config_entries.async_update_entry(
            fresh_entry, data=camera_data, options=dict(fresh_entry.options)
        )

        if fresh_entry.disabled_by is not None:
            await hass.config_entries.async_set_disabled_by(fresh_entry.entry_id, None)


async def _disable_account_camera_entries(
    hass: HomeAssistant, account_entry: ConfigEntry
) -> None:
    """Disable child camera entries when their parent account is disabled."""
    for camera_entry in _account_camera_entries(hass, account_entry.entry_id):
        if camera_entry.disabled_by is not None:
            continue

        # Fetch fresh entry from registry to ensure options are up-to-date
        fresh_entry = hass.config_entries.async_get_entry(camera_entry.entry_id)
        if fresh_entry is None:
            continue

        camera_data = dict(fresh_entry.data)
        camera_data[DISABLED_BY_ACCOUNT] = True
        hass.config_entries.async_update_entry(
            fresh_entry, data=camera_data, options=dict(fresh_entry.options)
        )
        await hass.config_entries.async_set_disabled_by(
            fresh_entry.entry_id, ConfigEntryDisabler.USER
        )


# ---------------------------------------------------------------------------
# Setup / unload
# ---------------------------------------------------------------------------


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up CloudEdge / Meari from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    # ------------------------------------------------------------------
    # Account entry (no sn_num) — login + ensure camera sub-entries exist
    # ------------------------------------------------------------------
    if CONF_SN_NUM not in entry.data:
        email = entry.data[CONF_EMAIL]
        password = entry.data[CONF_PASSWORD]
        country_code = entry.data.get(CONF_COUNTRY_CODE, DEFAULT_COUNTRY_CODE)
        phone_code = entry.data.get(CONF_PHONE_CODE, DEFAULT_PHONE_CODE)
        app_profile = entry.data.get(CONF_APP_PROFILE, DEFAULT_APP_PROFILE)

        api = build_api_client(email, password, country_code, phone_code, app_profile)
        try:
            await hass.async_add_executor_job(api.login)
        except (OSError, ValueError, KeyError, RuntimeError):
            _LOGGER.exception("Account entry %s failed to login", entry.title)
            return False

        hass.data[DOMAIN][entry.entry_id] = {"type": "account"}
        await _restore_account_managed_camera_entries(hass, entry)
        # Discover cameras and create their entries if missing.
        await _ensure_camera_entries(hass, entry, api)
        return True

    # ------------------------------------------------------------------
    # Camera entry (has sn_num) — create coordinator + set up platforms
    # ------------------------------------------------------------------
    account_entry = _linked_account_entry(hass, entry)
    if account_entry is None:
        _LOGGER.error(
            "Camera entry %s is missing its linked account entry", entry.title
        )
        return False

    # If account is disabled but camera is being enabled, auto-enable the account
    if account_entry.disabled_by is not None and entry.disabled_by is None:
        _LOGGER.info(
            "Auto-enabling account %s because camera %s is being enabled",
            account_entry.title,
            entry.title,
        )
        # Set flag to skip restoring other managed cameras during this re-enable
        hass.data[DOMAIN]["_auto_enabling_for"] = entry.entry_id
        try:
            await hass.config_entries.async_set_disabled_by(
                account_entry.entry_id, None
            )
        finally:
            hass.data[DOMAIN].pop("_auto_enabling_for", None)
        # Re-check if account is now enabled
        account_entry = _linked_account_entry(hass, entry)

    if account_entry.disabled_by is not None:
        if not entry.data.get(DISABLED_BY_ACCOUNT) and entry.disabled_by is None:
            # Fetch fresh entry from registry to ensure options are up-to-date
            fresh_entry = hass.config_entries.async_get_entry(entry.entry_id)
            if fresh_entry is not None:
                camera_data = dict(fresh_entry.data)
                camera_data[DISABLED_BY_ACCOUNT] = True
                hass.config_entries.async_update_entry(
                    fresh_entry, data=camera_data, options=dict(fresh_entry.options)
                )
                await hass.config_entries.async_set_disabled_by(
                    fresh_entry.entry_id, ConfigEntryDisabler.USER
                )

        _LOGGER.info(
            "Skipping setup for camera entry %s because account %s is disabled",
            entry.title,
            account_entry.title,
        )
        return False

    email = entry.data[CONF_EMAIL]
    password = entry.data[CONF_PASSWORD]
    country_code = entry.data.get(CONF_COUNTRY_CODE, DEFAULT_COUNTRY_CODE)
    phone_code = entry.data.get(CONF_PHONE_CODE, DEFAULT_PHONE_CODE)
    app_profile = entry.data.get(CONF_APP_PROFILE, DEFAULT_APP_PROFILE)
    device = entry.data["device"]
    video_password = str(entry.options.get(CONF_VIDEO_PASSWORD) or "").strip()

    api = build_api_client(email, password, country_code, phone_code, app_profile)
    await hass.async_add_executor_job(api.login)

    coord = CloudEdgeMeariCoordinator(
        hass,
        email,
        password,
        device,
        video_password=video_password,
        country_code=country_code,
        phone_code=phone_code,
        app_profile=app_profile,
        entry=entry,
    )

    await hass.async_add_executor_job(coord.prefetch_battery, api)
    await hass.async_add_executor_job(coord.prefetch_lamp, api)

    hass.data[DOMAIN][entry.entry_id] = coord

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    await coord.async_start()

    # Register the PTZ service once per integration.
    if not hass.services.has_service(DOMAIN, SERVICE_PTZ):

        def _find_coordinator(entity_id: str) -> CloudEdgeMeariCoordinator | None:
            ent_reg = er.async_get(hass)
            ent_entry = ent_reg.async_get(entity_id)
            if ent_entry is None:
                return None
            for value in hass.data.get(DOMAIN, {}).values():
                if (
                    isinstance(value, CloudEdgeMeariCoordinator)
                    and ent_entry.unique_id == f"{value.device_uuid}_camera"
                ):
                    return value
            return None

        async def _handle_ptz(call: ServiceCall) -> None:
            action = call.data["action"]
            argument = call.data.get("argument")
            entity_ids = call.data.get("entity_id", [])
            if isinstance(entity_ids, str):
                entity_ids = [entity_ids]
            for eid in entity_ids:
                c = _find_coordinator(eid)
                if c is None:
                    _LOGGER.warning("PTZ: no coordinator for %s", eid)
                    continue
                if not c.has_ptz:
                    _LOGGER.warning("PTZ: %s does not support PTZ", eid)
                    continue
                if action == "move":
                    if not argument:
                        _LOGGER.warning("PTZ move requires an argument")
                        continue
                    await hass.async_add_executor_job(c.ptz_move, argument)
                elif action == "stop":
                    await hass.async_add_executor_job(c.ptz_stop)

        hass.services.async_register(
            DOMAIN, SERVICE_PTZ, _handle_ptz, schema=SERVICE_PTZ_SCHEMA
        )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    stored = hass.data[DOMAIN].get(entry.entry_id)

    if isinstance(stored, CloudEdgeMeariCoordinator):
        await stored.async_stop()
        unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    else:
        # Account entry: no platforms to unload.
        unload_ok = True
        if entry.disabled_by is not None:
            await _disable_account_camera_entries(hass, entry)

    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)

    return unload_ok


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle config entry removal.

    - Removing an account entry removes all linked camera entries.
    - Removing a camera entry recreates it automatically (unless removed as part
      of account cascade removal).
    """
    hass.data.setdefault(DOMAIN, {})
    suppress_recreate: set[str] = hass.data[DOMAIN].setdefault(
        SUPPRESS_CAMERA_RECREATE, set()
    )

    # Account removal -> cascade remove all linked camera entries.
    if CONF_SN_NUM not in entry.data:
        camera_entries = _account_camera_entries(hass, entry.entry_id)
        for camera_entry in camera_entries:
            suppress_recreate.add(camera_entry.entry_id)

        for camera_entry in camera_entries:
            try:
                await hass.config_entries.async_remove(camera_entry.entry_id)
            finally:
                suppress_recreate.discard(camera_entry.entry_id)

        return

    # Camera removal during account cascade: do not recreate.
    if entry.entry_id in suppress_recreate:
        suppress_recreate.discard(entry.entry_id)
        return

    # Individual camera deletion: recreate the entry via import flow.
    account_entry = _linked_account_entry(hass, entry)
    if account_entry is None:
        return

    import_data = dict(entry.data)
    import_data.pop(DISABLED_BY_ACCOUNT, None)
    hass.async_create_task(
        hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_IMPORT},
            data=import_data,
        )
    )
    _LOGGER.info("Recreated camera entry %s after individual deletion", entry.title)
