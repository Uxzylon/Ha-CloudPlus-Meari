"""Binary sensor platform for CloudPlus / Meari — motion & awake state."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
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
    """Set up CloudPlus binary sensors from a config entry."""
    coordinators: list[CloudPlusCoordinator] = hass.data[DOMAIN][entry.entry_id]
    entities = []
    for coord in coordinators:
        entities.append(CloudPlusMotionSensor(coord, entry))
        entities.append(CloudPlusAwakeSensor(coord, entry))
        entities.append(CloudPlusChargingSensor(coord, entry))
    async_add_entities(entities)


class CloudPlusMotionSensor(BinarySensorEntity):
    """Binary sensor for motion detection."""

    _attr_has_entity_name = True
    _attr_name = "Motion"
    _attr_device_class = BinarySensorDeviceClass.MOTION

    def __init__(self, coordinator: CloudPlusCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_motion"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, coordinator.device_uuid)},
            "name": f"CloudPlus {coordinator.device_name}",
            "manufacturer": "Meari / CloudEdge",
            "model": "Battery Camera (snap)",
        }
        self._unsub_motion: Any = None
        self._unsub_update: Any = None

    async def async_added_to_hass(self) -> None:
        self._unsub_motion = self._coordinator.register_motion_callback(
            self._handle_motion
        )
        self._unsub_update = self._coordinator.register_update_callback(
            self._handle_update
        )

    async def async_will_remove_from_hass(self) -> None:
        if self._unsub_motion:
            self._unsub_motion()
        if self._unsub_update:
            self._unsub_update()

    @callback
    def _handle_motion(self) -> None:
        self.async_write_ha_state()

    @callback
    def _handle_update(self) -> None:
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return self._coordinator.available

    @property
    def is_on(self) -> bool:
        return self._coordinator.motion_detected

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attrs: dict[str, Any] = {}
        if self._coordinator.motion_type:
            attrs["motion_type"] = self._coordinator.motion_type
        return attrs


class CloudPlusAwakeSensor(BinarySensorEntity):
    """Binary sensor for camera awake state."""

    _attr_has_entity_name = True
    _attr_name = "Camera Awake"
    _attr_device_class = BinarySensorDeviceClass.RUNNING
    _attr_icon = "mdi:eye"

    def __init__(self, coordinator: CloudPlusCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_awake"
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
        return self._coordinator.camera_awake


class CloudPlusChargingSensor(BinarySensorEntity):
    """Binary sensor for camera charging state."""

    _attr_has_entity_name = True
    _attr_name = "Charging"
    _attr_device_class = BinarySensorDeviceClass.BATTERY_CHARGING

    def __init__(self, coordinator: CloudPlusCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_charging"
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
    def is_on(self) -> bool | None:
        if self._coordinator.battery_percent is None:
            return None
        return self._coordinator.battery_charging
