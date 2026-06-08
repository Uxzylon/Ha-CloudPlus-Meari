"""Shared base entity for CloudEdge / Meari platforms."""

from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import callback
from homeassistant.helpers.entity import Entity

from .const import DOMAIN
from .coordinator import CloudEdgeMeariCoordinator


class CloudEdgeMeariEntity(Entity):
    """Base entity wiring device-info and coordinator update callbacks.

    Concrete platform entities mix this in alongside their HA platform base
    (``SensorEntity``, ``SwitchEntity``, …) and set ``_attr_unique_id`` plus
    any platform-specific attributes after calling ``super().__init__``.
    """

    _attr_has_entity_name = True

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        super().__init__()
        self._coordinator = coordinator
        self._entry = entry
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


class CloudEdgeMeariIotEntity(CloudEdgeMeariEntity):
    """Base for entities backed by a single Meari IoT code.

    ``spec`` is any of the per-platform ``Iot*Spec`` dataclasses; they all
    expose ``code``, ``name`` and ``icon``.
    """

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        spec: Any,
    ) -> None:
        super().__init__(coordinator, entry)
        self._spec = spec
        self._attr_name = spec.name
        self._attr_icon = spec.icon

    @property
    def _iot_value(self) -> Any:
        return self._coordinator.get_iot_value(self._spec.code)

    @property
    def available(self) -> bool:
        return self._coordinator.available and self._iot_value is not None


class CloudEdgeMeariIotNumericEntity(CloudEdgeMeariIotEntity):
    """IoT entity whose value is exposed as a float (sensor / number)."""

    @property
    def native_value(self) -> float | None:
        value = self._iot_value
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @property
    def available(self) -> bool:
        return self._coordinator.available and self.native_value is not None
