"""Sensor platform for CloudPlus / Meari — battery level."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import PERCENTAGE
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
    """Set up CloudPlus sensors from a config entry."""
    coordinators: list[CloudPlusCoordinator] = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        [CloudPlusBatterySensor(coord, entry) for coord in coordinators]
    )


class CloudPlusBatterySensor(SensorEntity):
    """Sensor for battery level."""

    _attr_has_entity_name = True
    _attr_name = "Battery"
    _attr_device_class = SensorDeviceClass.BATTERY
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_icon = "mdi:battery"

    def __init__(self, coordinator: CloudPlusCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_battery"
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
    def native_value(self) -> int | None:
        return self._coordinator.battery_percent

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attrs: dict[str, Any] = {}
        if self._coordinator.battery_charging:
            attrs["charging"] = True
        return attrs

    @property
    def icon(self) -> str:
        pct = self._coordinator.battery_percent
        charging = self._coordinator.battery_charging

        if pct is None:
            return "mdi:battery-unknown"

        if charging:
            if pct >= 90:
                return "mdi:battery-charging-100"
            elif pct >= 70:
                return "mdi:battery-charging-80"
            elif pct >= 50:
                return "mdi:battery-charging-60"
            elif pct >= 30:
                return "mdi:battery-charging-40"
            elif pct >= 10:
                return "mdi:battery-charging-20"
            else:
                return "mdi:battery-charging-outline"

        if pct >= 95:
            return "mdi:battery"
        elif pct >= 85:
            return "mdi:battery-90"
        elif pct >= 75:
            return "mdi:battery-80"
        elif pct >= 65:
            return "mdi:battery-70"
        elif pct >= 55:
            return "mdi:battery-60"
        elif pct >= 45:
            return "mdi:battery-50"
        elif pct >= 35:
            return "mdi:battery-40"
        elif pct >= 25:
            return "mdi:battery-30"
        elif pct >= 15:
            return "mdi:battery-20"
        elif pct >= 5:
            return "mdi:battery-10"
        else:
            return "mdi:battery-alert-variant-outline"
