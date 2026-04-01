"""The CloudEdge / Meari camera integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

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
)
from .coordinator import CloudEdgeMeariCoordinator
from .api import MeariApiClient

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["camera", "binary_sensor", "button", "sensor", "number", "select", "switch"]


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
        )
        coordinators.append(coord)

    hass.data[DOMAIN][entry.entry_id] = coordinators

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    for coord in coordinators:
        await coord.async_start()

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
