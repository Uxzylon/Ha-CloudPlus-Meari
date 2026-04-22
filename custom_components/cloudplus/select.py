"""Select platform for CloudEdge / Meari — stream host mode."""

from __future__ import annotations

import logging
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

_LOGGER = logging.getLogger(__name__)

STREAM_HOST_OPTIONS: dict[str, str] = {
    "ip": "IP Address",
    "docker": "Docker Hostname",
}
_OPTION_TO_KEY = {v: k for k, v in STREAM_HOST_OPTIONS.items()}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari select entities from a config entry."""
    coordinators: list[CloudEdgeMeariCoordinator] = hass.data[DOMAIN][entry.entry_id]
    entities: list[SelectEntity] = [
        CloudEdgeMeariStreamHostSelect(coord, entry) for coord in coordinators
    ]
    for coord in coordinators:
        profiles = coord.quality_profiles
        if profiles:
            entities.append(CloudEdgeMeariStreamQualitySelect(coord, entry))

        # Generic IoT Selects
        if coord._has_sd_card:
            entities.append(CloudEdgeMeariIotSelect(
                coord, entry, SD_RECORD_TYPE, "SD Record Type",
                {0: "Continuous", 1: "Event"}, "mdi:sd"
            ))

        entities.append(CloudEdgeMeariIotSelect(
            coord, entry, DAY_NIGHT_MODE, "Day/Night Mode",
            {0: "Day", 1: "Night", 2: "Auto"}, "mdi:theme-light-dark"
        ))

        if coord._has_alarm_frequency:
            entities.append(CloudEdgeMeariIotSelect(
                coord, entry, ALARM_FREQUENCY, "Alarm Frequency",
                {0: "Low", 1: "Medium", 2: "High"}, "mdi:bell-ring"
            ))

        if coord._has_siren_alarm:
            entities.append(CloudEdgeMeariIotSelect(
                coord, entry, SOUND_LIGHT_TYPE, "Sound/Light Alarm Type",
                {0: "Sound", 1: "Light", 2: "Both"}, "mdi:alarm-light"
            ))

        entities.append(CloudEdgeMeariIotSelect(
            coord, entry, NO_FLK, "Anti-Flicker",
            {0: "50Hz", 1: "60Hz"}, "mdi:sine-wave"
        ))

        if coord._has_full_color:
            entities.append(CloudEdgeMeariIotSelect(
                coord, entry, FULL_COLOR_MODE, "Full Color Mode",
                {0: "Black/White", 1: "Full Color"}, "mdi:invert-colors"
            ))

    async_add_entities(entities)


class CloudEdgeMeariIotSelect(SelectEntity):
    """Generic select for CloudEdge / Meari IOT properties."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        code: int,
        name: str,
        options_map: dict[int, str],
        icon: str | None = None
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._code = code
        self._attr_name = name
        self._attr_icon = icon
        self._options_map = options_map
        self._reverse_map = {v: k for k, v in options_map.items()}
        self._attr_options = list(options_map.values())
        self._attr_unique_id = f"{coordinator.device_uuid}_iot_select_{code}"
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
    def current_option(self) -> str | None:
        val = self._coordinator.get_iot_value(self._code)
        if val is None:
            return None
        try:
            return self._options_map.get(int(val))
        except (ValueError, TypeError):
            return None

    async def async_select_option(self, option: str) -> None:
        """Change the selected option."""
        val = self._reverse_map.get(option)
        if val is not None:
            await self.hass.async_add_executor_job(self._coordinator.set_iot_value, self._code, val)
            self.async_write_ha_state()


class CloudEdgeMeariStreamHostSelect(SelectEntity):
    """Select entity to choose between IP address or Docker hostname for stream URL."""

    _attr_has_entity_name = True
    _attr_name = "Stream Host Mode"
    _attr_icon = "mdi:ip-network"
    _attr_options = list(STREAM_HOST_OPTIONS.values())

    def __init__(self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry) -> None:
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

    def __init__(self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry) -> None:
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
