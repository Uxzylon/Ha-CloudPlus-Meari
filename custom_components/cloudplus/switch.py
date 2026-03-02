"""Switch platform for CloudPlus / Meari — wake on motion toggle."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudPlusCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudPlus switches from a config entry."""
    coordinators: list[CloudPlusCoordinator] = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        [CloudPlusMotionWakeSwitch(coord, entry) for coord in coordinators]
    )


class CloudPlusMotionWakeSwitch(SwitchEntity):
    """Switch to enable / disable automatic wake on motion."""

    _attr_has_entity_name = True
    _attr_name = "Wake on Motion"
    _attr_icon = "mdi:motion-sensor"

    def __init__(self, coordinator: CloudPlusCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_motion_wake"
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
    def is_on(self) -> bool:
        return self._coordinator.motion_wake_enabled

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Enable wake on motion."""
        self._coordinator.set_motion_wake_enabled(True)
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Disable wake on motion."""
        self._coordinator.set_motion_wake_enabled(False)
        self.async_write_ha_state()
