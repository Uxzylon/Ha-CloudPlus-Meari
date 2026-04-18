"""The CloudEdge / Meari camera integration."""

from __future__ import annotations

import logging
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import entity_registry as er

from .const import (
    CONF_APP_PROFILE,
    CONF_COUNTRY_CODE,
    CONF_EMAIL,
    CONF_PASSWORD,
    CONF_PHONE_CODE,
    DEFAULT_APP_PROFILE,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_PHONE_CODE,
    DOMAIN,
    PTZ_DIRECTIONS,
)
from .coordinator import CloudEdgeMeariCoordinator
from .api import MeariApiClient

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["camera", "binary_sensor", "button", "sensor", "number", "select", "switch"]

SERVICE_PTZ = "ptz"
SERVICE_PTZ_SCHEMA = vol.Schema(
    {
        vol.Required("action"): vol.In(["move", "stop"]),
        vol.Optional("argument"): vol.In(list(PTZ_DIRECTIONS.keys())),
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up CloudEdge / Meari from a config entry (one entry = one account)."""
    hass.data.setdefault(DOMAIN, {})

    email = entry.data[CONF_EMAIL]
    password = entry.data[CONF_PASSWORD]
    country_code = entry.data.get(CONF_COUNTRY_CODE, DEFAULT_COUNTRY_CODE)
    phone_code = entry.data.get(CONF_PHONE_CODE, DEFAULT_PHONE_CODE)
    app_profile = entry.data.get(CONF_APP_PROFILE, DEFAULT_APP_PROFILE)

    # Discover supported cameras on the account
    api = MeariApiClient(
        email=email,
        password=password,
        country_code=country_code,
        phone_code=phone_code,
        app_profile=app_profile,
    )
    await hass.async_add_executor_job(api.login)
    camera_devices = api.get_camera_devices()

    coordinators: list[CloudEdgeMeariCoordinator] = []
    for dev in camera_devices:
        coord = CloudEdgeMeariCoordinator(
            hass,
            email,
            password,
            dev,
            country_code=country_code,
            phone_code=phone_code,
            app_profile=app_profile,
            entry=entry,
        )
        coordinators.append(coord)

    # Pre-fetch battery and lamp info so entities have values before platforms load.
    for coord in coordinators:
        await hass.async_add_executor_job(coord.prefetch_battery, api)
        await hass.async_add_executor_job(coord.prefetch_lamp, api)

    hass.data[DOMAIN][entry.entry_id] = coordinators

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    for coord in coordinators:
        await coord.async_start()

    # Register the PTZ service (once per integration, not per entry).
    if not hass.services.has_service(DOMAIN, SERVICE_PTZ):

        def _find_coordinator(entity_id: str) -> CloudEdgeMeariCoordinator | None:
            """Resolve a camera entity_id to its coordinator."""
            ent_reg = er.async_get(hass)
            ent_entry = ent_reg.async_get(entity_id)
            if ent_entry is None:
                return None
            # Walk every config entry's coordinators to find the matching device.
            for entry_coords in hass.data.get(DOMAIN, {}).values():
                if not isinstance(entry_coords, list):
                    continue
                for coord in entry_coords:
                    if ent_entry.unique_id == f"{coord.device_uuid}_camera":
                        return coord
            return None

        async def _handle_ptz(call: ServiceCall) -> None:
            action = call.data["action"]
            argument = call.data.get("argument")
            entity_ids = call.data.get("entity_id", [])
            if isinstance(entity_ids, str):
                entity_ids = [entity_ids]

            for eid in entity_ids:
                coord = _find_coordinator(eid)
                if coord is None:
                    _LOGGER.warning("PTZ: no coordinator for %s", eid)
                    continue
                if not coord.has_ptz:
                    _LOGGER.warning("PTZ: %s does not support PTZ", eid)
                    continue
                if action == "move":
                    if not argument:
                        _LOGGER.warning("PTZ move requires an argument (direction)")
                        continue
                    await hass.async_add_executor_job(coord.ptz_move, argument)
                elif action == "stop":
                    await hass.async_add_executor_job(coord.ptz_stop)

        hass.services.async_register(
            DOMAIN, SERVICE_PTZ, _handle_ptz, schema=SERVICE_PTZ_SCHEMA,
        )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    coordinators: list[CloudEdgeMeariCoordinator] = hass.data[DOMAIN][entry.entry_id]

    for coord in coordinators:
        await coord.async_stop()

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok
