"""Number platform for CloudEdge / Meari — motion timeout."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import PERCENTAGE, UnitOfTime
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudEdgeMeariCoordinator
from .meari_commands import (
    FLIGHT_BRIGHTNESS,
    FLIGHT_PIR_DURATION,
    HUMAN_SENSITIVITY_LEVEL,
    MOTION_DET_SENSITIVITY,
    PIR_DET_SENSITIVITY,
    PIR_TRIGGER_INTERVAL,
    SOUND_DET_SENSITIVITY,
    SPEAK_VOLUME,
    WARM_LIGHT_BRI,
)


@dataclass(frozen=True)
class IotNumberSpec:
    feature: str
    code: int
    name: str
    min_value: float
    max_value: float
    step: float = 1
    icon: str | None = None
    unit: str | None = None


IOT_NUMBERS: tuple[IotNumberSpec, ...] = (
    IotNumberSpec(
        "motion_det",
        MOTION_DET_SENSITIVITY,
        "Motion Sensitivity",
        1,
        5,
        icon="mdi:motion-sensor",
    ),
    IotNumberSpec(
        "noise_det",
        SOUND_DET_SENSITIVITY,
        "Sound Sensitivity",
        1,
        100,
        icon="mdi:ear-hearing",
    ),
    IotNumberSpec(
        "person_det",
        HUMAN_SENSITIVITY_LEVEL,
        "Human Sensitivity",
        1,
        3,
        icon="mdi:account-search",
    ),
    IotNumberSpec(
        "pir",
        PIR_DET_SENSITIVITY,
        "PIR Sensitivity",
        1,
        10,
        icon="mdi:motion-sensor",
    ),
    IotNumberSpec(
        "pir",
        PIR_TRIGGER_INTERVAL,
        "PIR Interval",
        1,
        60,
        icon="mdi:timer-outline",
        unit=UnitOfTime.SECONDS,
    ),
    IotNumberSpec(
        "light_brightness",
        FLIGHT_BRIGHTNESS,
        "Floodlight Brightness",
        1,
        100,
        icon="mdi:brightness-6",
        unit=PERCENTAGE,
    ),
    IotNumberSpec(
        "light_brightness",
        FLIGHT_PIR_DURATION,
        "Floodlight Duration",
        5,
        300,
        icon="mdi:timer-outline",
        unit=UnitOfTime.SECONDS,
    ),
    IotNumberSpec(
        "speaker",
        SPEAK_VOLUME,
        "Speaker Volume",
        0,
        100,
        icon="mdi:volume-high",
        unit=PERCENTAGE,
    ),
    IotNumberSpec(
        "warm_light",
        WARM_LIGHT_BRI,
        "Warm Light Brightness",
        1,
        100,
        icon="mdi:brightness-6",
        unit=PERCENTAGE,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari number entities from a config entry."""
    coord: CloudEdgeMeariCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[NumberEntity] = []
    if coord.is_battery_camera:
        entities.append(CloudEdgeMeariMotionTimeout(coord, entry))
    entities.extend(
        CloudEdgeMeariIotNumber(coord, entry, spec)
        for spec in IOT_NUMBERS
        if coord.supports_iot(spec.feature) or coord.has_iot_code(spec.code)
    )
    async_add_entities(entities)


class CloudEdgeMeariIotNumber(NumberEntity):
    """Number entity backed by a Meari IoT value."""

    _attr_has_entity_name = True
    _attr_mode = NumberMode.SLIDER

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        spec: IotNumberSpec,
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._spec = spec
        self._attr_name = spec.name
        self._attr_icon = spec.icon
        self._attr_native_min_value = spec.min_value
        self._attr_native_max_value = spec.max_value
        self._attr_native_step = spec.step
        self._attr_native_unit_of_measurement = spec.unit
        self._attr_unique_id = f"{coordinator.device_uuid}_iot_number_{spec.code}"
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
        return self._coordinator.available and self.native_value is not None

    @property
    def native_value(self) -> float | None:
        value = self._coordinator.get_iot_value(self._spec.code)
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    async def async_set_native_value(self, value: float) -> None:
        """Update the camera IoT value."""
        await self.hass.async_add_executor_job(
            self._coordinator.set_iot_value,
            self._spec.code,
            int(value),
        )
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

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
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
