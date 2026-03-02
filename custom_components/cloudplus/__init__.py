"""The CloudPlus / Meari camera integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import CONF_EMAIL, CONF_PASSWORD, DOMAIN
from .coordinator import CloudPlusCoordinator
from .api import MeariApiClient

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["camera", "binary_sensor", "button", "sensor", "number", "select", "switch"]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up CloudPlus from a config entry (one entry = one account)."""
    hass.data.setdefault(DOMAIN, {})

    email = entry.data[CONF_EMAIL]
    password = entry.data[CONF_PASSWORD]

    # Discover all snap (battery) cameras on the account
    api = MeariApiClient(email=email, password=password)
    await hass.async_add_executor_job(api.login)
    snap_devices = api.get_snap_devices()

    coordinators: list[CloudPlusCoordinator] = []
    for dev in snap_devices:
        coord = CloudPlusCoordinator(hass, email, password, dev)
        coordinators.append(coord)

    hass.data[DOMAIN][entry.entry_id] = coordinators

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    for coord in coordinators:
        await coord.async_start()

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    coordinators: list[CloudPlusCoordinator] = hass.data[DOMAIN][entry.entry_id]

    for coord in coordinators:
        await coord.async_stop()

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok
