"""Select platform for CloudPlus / Meari — stream host mode."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudPlusCoordinator

_LOGGER = logging.getLogger(__name__)

STREAM_HOST_OPTIONS: dict[str, str] = {
    "ip": "IP Address",
    "docker": "Docker Hostname",
}
_OPTION_TO_KEY = {v: k for k, v in STREAM_HOST_OPTIONS.items()}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudPlus select entities from a config entry."""
    coordinators: list[CloudPlusCoordinator] = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        [CloudPlusStreamHostSelect(coord, entry) for coord in coordinators]
    )


class CloudPlusStreamHostSelect(SelectEntity):
    """Select entity to choose between IP address or Docker hostname for stream URL."""

    _attr_has_entity_name = True
    _attr_name = "Stream Host Mode"
    _attr_icon = "mdi:ip-network"
    _attr_options = list(STREAM_HOST_OPTIONS.values())

    def __init__(self, coordinator: CloudPlusCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_stream_host_mode"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, coordinator.device_uuid)},
            "name": f"CloudPlus {coordinator.device_name}",
            "manufacturer": "Meari / CloudEdge",
            "model": "Battery Camera (snap)",
        }
        self._unsub_update: Any = None

    async def async_added_to_hass(self) -> None:
        self._unsub_update = self._coordinator.register_update_callback(
            self._handle_update
        )

    async def async_will_remove_from_hass(self) -> None:
        if self._unsub_update:
            self._unsub_update()

    @callback
    def _handle_update(self) -> None:
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return self._coordinator.available

    @property
    def current_option(self) -> str:
        mode = self._coordinator.stream_host_mode
        return STREAM_HOST_OPTIONS.get(mode, STREAM_HOST_OPTIONS["ip"])

    async def async_select_option(self, option: str) -> None:
        """Change stream host mode."""
        key = _OPTION_TO_KEY.get(option)
        if key:
            self._coordinator.set_stream_host_mode(key)
        self.async_write_ha_state()
