"""Number platform for CloudPlus / Meari — motion timeout."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTime
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, CONF_MOTION_TIMEOUT
from .coordinator import CloudPlusCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudPlus number entities from a config entry."""
    coordinators: list[CloudPlusCoordinator] = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        [CloudPlusMotionTimeout(coord, entry) for coord in coordinators]
    )


class CloudPlusMotionTimeout(NumberEntity):
    """Number entity to control the motion-wake timeout (seconds)."""

    _attr_has_entity_name = True
    _attr_name = "Motion Timeout"
    _attr_icon = "mdi:timer-outline"
    _attr_native_min_value = 10
    _attr_native_max_value = 600
    _attr_native_step = 10
    _attr_native_unit_of_measurement = UnitOfTime.SECONDS
    _attr_mode = NumberMode.SLIDER

    def __init__(self, coordinator: CloudPlusCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_motion_timeout"
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
    def native_value(self) -> float:
        return self._coordinator.motion_timeout

    async def async_set_native_value(self, value: float) -> None:
        """Update the motion timeout."""
        self._coordinator.set_motion_timeout(int(value))
        self.async_write_ha_state()
