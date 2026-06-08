"""Sensor platform for CloudEdge / Meari — battery level."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import PERCENTAGE, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudEdgeMeariCoordinator
from .entity import CloudEdgeMeariEntity, CloudEdgeMeariIotNumericEntity
from .meari_commands import HUMIDITY, TEMPERATURE

# (min percent, icon) ladders, highest threshold first.
_CHARGING_BATTERY_ICONS = (
    (90, "mdi:battery-charging-100"),
    (70, "mdi:battery-charging-80"),
    (50, "mdi:battery-charging-60"),
    (30, "mdi:battery-charging-40"),
    (10, "mdi:battery-charging-20"),
)
_BATTERY_ICONS = (
    (95, "mdi:battery"),
    (85, "mdi:battery-90"),
    (75, "mdi:battery-80"),
    (65, "mdi:battery-70"),
    (55, "mdi:battery-60"),
    (45, "mdi:battery-50"),
    (35, "mdi:battery-40"),
    (25, "mdi:battery-30"),
    (15, "mdi:battery-20"),
    (5, "mdi:battery-10"),
)


@dataclass(frozen=True)
class IotSensorSpec:
    """Declarative spec for an IoT-backed sensor entity."""

    feature: str
    code: int
    name: str
    device_class: SensorDeviceClass
    unit: str
    icon: str | None = None


IOT_SENSORS: tuple[IotSensorSpec, ...] = (
    IotSensorSpec(
        "temp_sensor",
        TEMPERATURE,
        "Temperature",
        SensorDeviceClass.TEMPERATURE,
        UnitOfTemperature.CELSIUS,
    ),
    IotSensorSpec(
        "humidity_sensor",
        HUMIDITY,
        "Humidity",
        SensorDeviceClass.HUMIDITY,
        PERCENTAGE,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari sensors from a config entry."""
    coord: CloudEdgeMeariCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[SensorEntity] = []
    if coord.is_battery_camera:
        entities.append(CloudEdgeMeariBatterySensor(coord, entry))
        entities.append(CloudEdgeMeariChargeStatusSensor(coord, entry))
    entities.extend(
        CloudEdgeMeariIotSensor(coord, entry, spec)
        for spec in IOT_SENSORS
        if coord.supports_iot(spec.feature) or coord.has_iot_code(spec.code)
    )
    async_add_entities(entities)


class CloudEdgeMeariIotSensor(CloudEdgeMeariIotNumericEntity, SensorEntity):
    """Sensor entity backed by a Meari IoT value."""

    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        spec: IotSensorSpec,
    ) -> None:
        super().__init__(coordinator, entry, spec)
        self._attr_device_class = spec.device_class
        self._attr_native_unit_of_measurement = spec.unit
        self._attr_unique_id = f"{coordinator.device_uuid}_iot_sensor_{spec.code}"


class CloudEdgeMeariChargeStatusSensor(CloudEdgeMeariEntity, SensorEntity):
    """Sensor for battery charge status."""

    _attr_name = "Charge Status"
    _attr_icon = "mdi:battery-charging"

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{coordinator.device_uuid}_charge_status"

    @property
    def available(self) -> bool:
        return (
            self._coordinator.available
            and self._coordinator.battery_percent is not None
        )

    @property
    def native_value(self) -> str | None:
        if self._coordinator.battery_percent is None:
            return None
        return "Charging" if self._coordinator.battery_charging else "Not Charging"


class CloudEdgeMeariBatterySensor(CloudEdgeMeariEntity, SensorEntity):
    """Sensor for battery level."""

    _attr_name = "Battery"
    _attr_device_class = SensorDeviceClass.BATTERY
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_icon = "mdi:battery"

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{coordinator.device_uuid}_battery"

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
            for threshold, icon in _CHARGING_BATTERY_ICONS:
                if pct >= threshold:
                    return icon
            return "mdi:battery-charging-outline"

        for threshold, icon in _BATTERY_ICONS:
            if pct >= threshold:
                return icon
        return "mdi:battery-alert-variant-outline"
