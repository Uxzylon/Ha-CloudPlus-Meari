"""Number platform for CloudEdge / Meari — motion timeout."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTime
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, CONF_MOTION_TIMEOUT
from .coordinator import CloudEdgeMeariCoordinator
from .meari_commands import (
    CHIME_PRO_MOTION_VOLUME,
    CHIME_PRO_RING_VOLUME,
    FLIGHT_BRIGHTNESS,
    FLIGHT_PIR_DURATION,
    HUMAN_SENSITIVITY_LEVEL,
    JINGLE_VOLUME,
    MOTION_DET_SENSITIVITY,
    MUSIC_VOLUME,
    PIR_DET_SENSITIVITY,
    PIR_TRIGGER_INTERVAL,
    POWER_ON_VOLUME,
    SMART_DET_SENSITIVITY,
    SOUND_DET_SENSITIVITY,
    SPEAK_VOLUME,
    WARM_LIGHT_BRI,
    WIRELESS_CHIME_VOLUME,
)

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari number entities from a config entry."""
    coordinators: list[CloudEdgeMeariCoordinator] = hass.data[DOMAIN][entry.entry_id]
    entities: list[NumberEntity] = []
    for coord in coordinators:
        if coord.is_battery_camera:
            entities.append(CloudEdgeMeariMotionTimeout(coord, entry))

        # Generic IoT Numbers
        if coord._has_motion_det:
            entities.append(CloudEdgeMeariIotNumber(
                coord, entry, MOTION_DET_SENSITIVITY, "Motion Sensitivity", 1, 5, 1, "mdi:motion-sensor"
            ))
        if coord._has_noise_det:
            entities.append(CloudEdgeMeariIotNumber(
                coord, entry, SOUND_DET_SENSITIVITY, "Sound Sensitivity", 1, 100, 1, "mdi:ear-hearing"
            ))
        if coord._has_person_det:
            entities.append(CloudEdgeMeariIotNumber(
                coord, entry, HUMAN_SENSITIVITY_LEVEL, "Human Sensitivity", 1, 3, 1, "mdi:account-search"
            ))
        if coord._has_pir:
            entities.append(CloudEdgeMeariIotNumber(
                coord, entry, PIR_DET_SENSITIVITY, "PIR Sensitivity", 1, 10, 1, "mdi:motion-sensor-pau"
            ))
            entities.append(CloudEdgeMeariIotNumber(
                coord, entry, PIR_TRIGGER_INTERVAL, "PIR Interval", 1, 60, 1, "mdi:timer-outline", "s"
            ))
        if coord._has_light_brightness:
            entities.append(CloudEdgeMeariIotNumber(
                coord, entry, FLIGHT_BRIGHTNESS, "Floodlight Brightness", 1, 100, 1, "mdi:brightness-6", "%"
            ))
            entities.append(CloudEdgeMeariIotNumber(
                coord, entry, FLIGHT_PIR_DURATION, "Floodlight Duration", 5, 300, 1, "mdi:timer-outline", "s"
            ))
        if coord._has_speaker:
            entities.append(CloudEdgeMeariIotNumber(
                coord, entry, SPEAK_VOLUME, "Speaker Volume", 0, 100, 1, "mdi:volume-high", "%"
            ))
        if coord._has_warm_light:
            entities.append(CloudEdgeMeariIotNumber(
                coord, entry, WARM_LIGHT_BRI, "Warm Light Brightness", 1, 100, 1, "mdi:brightness-6", "%"
            ))

    async_add_entities(entities)


class CloudEdgeMeariIotNumber(NumberEntity):
    """Generic number for CloudEdge / Meari IOT properties."""

    _attr_has_entity_name = True
    _attr_mode = NumberMode.SLIDER

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        code: int,
        name: str,
        min_value: float,
        max_value: float,
        step: float,
        icon: str | None = None,
        unit: str | None = None
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._code = code
        self._attr_name = name
        self._attr_icon = icon
        self._attr_native_min_value = min_value
        self._attr_native_max_value = max_value
        self._attr_native_step = step
        self._attr_native_unit_of_measurement = unit
        self._attr_unique_id = f"{coordinator.device_uuid}_iot_number_{code}"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, coordinator.device_uuid)},
            "name": f"CloudEdge / Meari {coordinator.device_name}",
            "manufacturer": "CloudEdge / Meari",
            "model": coordinator.device_model,
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
        return self._coordinator.available and self._coordinator.get_iot_value(self._code) is not None

    @property
    def native_value(self) -> float | None:
        val = self._coordinator.get_iot_value(self._code)
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    async def async_set_native_value(self, value: float) -> None:
        """Update the value."""
        await self.hass.async_add_executor_job(self._coordinator.set_iot_value, self._code, int(value))
        self.async_write_ha_state()


class CloudEdgeMeariMotionTimeout(NumberEntity):
    """Number entity to control the motion-wake timeout (seconds)."""

    _attr_has_entity_name = True
    _attr_name = "Motion Timeout"
    _attr_icon = "mdi:timer-outline"
    _attr_native_min_value = 10
    _attr_native_max_value = 600
    _attr_native_step = 10
    _attr_native_unit_of_measurement = UnitOfTime.SECONDS
    _attr_mode = NumberMode.SLIDER

    def __init__(self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_motion_timeout"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, coordinator.device_uuid)},
            "name": f"CloudEdge / Meari {coordinator.device_name}",
            "manufacturer": "CloudEdge / Meari",
            "model": coordinator.device_model,
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
