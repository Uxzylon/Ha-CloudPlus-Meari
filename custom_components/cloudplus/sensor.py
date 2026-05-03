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
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudEdgeMeariCoordinator
from .meari_commands import HUMIDITY, TEMPERATURE


@dataclass(frozen=True)
class IotSensorSpec:
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


class CloudEdgeMeariIotSensor(SensorEntity):
    """Sensor entity backed by a Meari IoT value."""

    _attr_has_entity_name = True
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        spec: IotSensorSpec,
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._spec = spec
        self._attr_name = spec.name
        self._attr_device_class = spec.device_class
        self._attr_native_unit_of_measurement = spec.unit
        self._attr_icon = spec.icon
        self._attr_unique_id = f"{coordinator.device_uuid}_iot_sensor_{spec.code}"
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


class CloudEdgeMeariChargeStatusSensor(SensorEntity):
    """Sensor for battery charge status."""

    _attr_has_entity_name = True
    _attr_name = "Charge Status"
    _attr_icon = "mdi:battery-charging"

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_charge_status"
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
        return self._coordinator.available and self._coordinator.battery_percent is not None

    @property
    def native_value(self) -> str | None:
        if self._coordinator.battery_percent is None:
            return None
        return "Charging" if self._coordinator.battery_charging else "Not Charging"


class CloudEdgeMeariBatterySensor(SensorEntity):
    """Sensor for battery level."""

    _attr_has_entity_name = True
    _attr_name = "Battery"
    _attr_device_class = SensorDeviceClass.BATTERY
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_icon = "mdi:battery"

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_battery"
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
