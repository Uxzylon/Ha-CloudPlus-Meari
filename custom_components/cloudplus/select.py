"""Select platform for CloudEdge / Meari — stream host mode."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudEdgeMeariCoordinator
from .meari_commands import (
    ALARM_FREQUENCY,
    DAY_NIGHT_MODE,
    FULL_COLOR_MODE,
    NO_FLK,
    SD_RECORD_TYPE,
    SOUND_LIGHT_TYPE,
)

STREAM_HOST_OPTIONS: dict[str, str] = {
    "ip": "IP Address",
    "docker": "Docker Hostname",
}
_OPTION_TO_KEY = {v: k for k, v in STREAM_HOST_OPTIONS.items()}


@dataclass(frozen=True)
class IotSelectSpec:
    feature: str | None
    code: int
    name: str
    options: dict[int, str]
    icon: str | None = None


IOT_SELECTS: tuple[IotSelectSpec, ...] = (
    IotSelectSpec(
        "sd_card",
        SD_RECORD_TYPE,
        "SD Record Type",
        {0: "Continuous", 1: "Event"},
        "mdi:sd",
    ),
    IotSelectSpec(
        None,
        DAY_NIGHT_MODE,
        "Day/Night Mode",
        {0: "Day", 1: "Night", 2: "Auto"},
        "mdi:theme-light-dark",
    ),
    IotSelectSpec(
        "alarm_frequency",
        ALARM_FREQUENCY,
        "Alarm Frequency",
        {0: "Low", 1: "Medium", 2: "High"},
        "mdi:bell-ring",
    ),
    IotSelectSpec(
        "siren_alarm",
        SOUND_LIGHT_TYPE,
        "Sound/Light Alarm Type",
        {0: "Sound", 1: "Light", 2: "Both"},
        "mdi:alarm-light",
    ),
    IotSelectSpec(
        None,
        NO_FLK,
        "Anti-Flicker",
        {0: "50Hz", 1: "60Hz"},
        "mdi:sine-wave",
    ),
    IotSelectSpec(
        "full_color",
        FULL_COLOR_MODE,
        "Full Color Mode",
        {0: "Black/White", 1: "Full Color"},
        "mdi:invert-colors",
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari select entities from a config entry."""
    coord: CloudEdgeMeariCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[SelectEntity] = [CloudEdgeMeariStreamHostSelect(coord, entry)]
    if coord.quality_profiles:
        entities.append(CloudEdgeMeariStreamQualitySelect(coord, entry))
    entities.extend(
        CloudEdgeMeariIotSelect(coord, entry, spec)
        for spec in IOT_SELECTS
        if (spec.feature and coord.supports_iot(spec.feature))
        or coord.has_iot_code(spec.code)
    )
    async_add_entities(entities)


class CloudEdgeMeariIotSelect(SelectEntity):
    """Select entity backed by a Meari IoT value."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        spec: IotSelectSpec,
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._spec = spec
        self._label_to_value = {label: value for value, label in spec.options.items()}
        self._attr_name = spec.name
        self._attr_icon = spec.icon
        self._attr_options = list(spec.options.values())
        self._attr_unique_id = f"{coordinator.device_uuid}_iot_select_{spec.code}"
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
        return (
            self._coordinator.available
            and self._coordinator.get_iot_value(self._spec.code) is not None
        )

    @property
    def current_option(self) -> str | None:
        value = self._coordinator.get_iot_value(self._spec.code)
        try:
            return self._spec.options.get(int(value))
        except (TypeError, ValueError):
            return None

    async def async_select_option(self, option: str) -> None:
        """Update the camera IoT value."""
        value = self._label_to_value.get(option)
        if value is None:
            return
        await self.hass.async_add_executor_job(
            self._coordinator.set_iot_value,
            self._spec.code,
            value,
        )
        self.async_write_ha_state()


class CloudEdgeMeariStreamHostSelect(SelectEntity):
    """Select entity to choose between IP address or Docker hostname for stream URL."""

    _attr_has_entity_name = True
    _attr_name = "Stream Host Mode"
    _attr_icon = "mdi:ip-network"
    _attr_options = list(STREAM_HOST_OPTIONS.values())

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_stream_host_mode"
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
    def current_option(self) -> str:
        mode = self._coordinator.stream_host_mode
        return STREAM_HOST_OPTIONS.get(mode, STREAM_HOST_OPTIONS["ip"])

    async def async_select_option(self, option: str) -> None:
        """Change stream host mode."""
        key = _OPTION_TO_KEY.get(option)
        if key:
            self._coordinator.set_stream_host_mode(key)
        self.async_write_ha_state()


# ---------------------------------------------------------------------------
# Stream quality profile selector
# ---------------------------------------------------------------------------

_AUTO_LABEL = "Auto (highest)"


class CloudEdgeMeariStreamQualitySelect(SelectEntity):
    """Select entity to choose the camera stream quality profile."""

    _attr_has_entity_name = True
    _attr_name = "Stream Quality"
    _attr_icon = "mdi:video-high-definition"

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_stream_quality"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, coordinator.device_uuid)},
            "name": f"CloudEdge / Meari {coordinator.device_name}",
            "manufacturer": "CloudEdge / Meari",
            "model": coordinator.device_model,
        }
        self._unsub_update: Any = None
        # Build option list from device capabilities
        self._profiles = coordinator.quality_profiles  # {int: str}
        self._label_to_id: dict[str, int | None] = {_AUTO_LABEL: None}
        for pid, label in sorted(self._profiles.items()):
            self._label_to_id[label] = pid
        self._attr_options = list(self._label_to_id.keys())

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
        quality = self._coordinator.vvp_quality
        if quality is None:
            return _AUTO_LABEL
        return self._profiles.get(quality, _AUTO_LABEL)

    async def async_select_option(self, option: str) -> None:
        """Change stream quality profile."""
        quality_id = self._label_to_id.get(option)
        self._coordinator.set_vvp_quality(quality_id)
        self.async_write_ha_state()
