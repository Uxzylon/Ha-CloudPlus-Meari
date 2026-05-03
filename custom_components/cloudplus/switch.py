"""Switch platform for CloudEdge / Meari — wake on motion toggle."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudEdgeMeariCoordinator
from .meari_commands import (
    ABNORMAL_NOISE_ENABLE,
    ANTI_JAMMING,
    AUTO_UPDATE,
    BLE_SWITCH,
    CRY_DET_ENABLE,
    FACE_RECOGNITION_SWITCH,
    FLIGHT_LIGHT_SWITCH,
    FLIGHT_LINK_LIGHTING_ENABLE,
    FLIGHT_LINK_SIREN_ENABLE,
    H265_ENABLE,
    HOMEKIT_ENABLE,
    HUMAN_DET_ENABLE,
    HUMAN_FRAME_ENABLE,
    HUMAN_TRACK_ENABLE,
    LASER_SWITCH,
    LED_ENABLE,
    LOGO_SWITCH,
    MECHANICAL_CHIME_ENABLE,
    MONITOR_TIME_SWITCH,
    MOTION_DET_ENABLE,
    ONVIF_ENABLE,
    OSD_ENABLE,
    PET_ALARM_ENABLE,
    PET_THROW_WARNING,
    PIR_DET_ENABLE,
    PLUG_LOW_POWER_MODE,
    RAE_SOUND,
    RECORD_SWITCH,
    RELAY_ENABLE,
    RGB_LIGHT_SWITCH,
    SLEEP_MODE,
    SMART_DET,
    SMART_DET_FRAME,
    SOUND_DET_ENABLE,
    SOUND_LIGHT_ENABLE,
    SOUND_SWITCH,
    PTZ_PATROL,
    TIMING_SHOT_SWITCH,
    UPLOAD_VIDEO,
    WIRELESS_CHIME_ENABLE,
)

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari switches from a config entry."""
    coordinators: list[CloudEdgeMeariCoordinator] = hass.data[DOMAIN][entry.entry_id]
    entities: list[SwitchEntity] = []
    for coord in coordinators:
        if coord.is_battery_camera:
            entities.append(CloudEdgeMeariMotionWakeSwitch(coord, entry))

        # Generic IoT Switches based on capabilities
        if coord._has_status_led:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, LED_ENABLE, "Status LED", "mdi:led-on"))
        if coord._has_motion_det:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, MOTION_DET_ENABLE, "Motion Detection", "mdi:motion-sensor"))
        if coord._has_person_det:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, HUMAN_DET_ENABLE, "Person Detection", "mdi:account-search"))
            # entities.append(CloudEdgeMeariIotSwitch(coord, entry, HUMAN_FRAME_ENABLE, "Human Frame", "mdi:vector-square"))
        if coord._has_human_track:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, HUMAN_TRACK_ENABLE, "Human Tracking", "mdi:target-account"))
        if coord._has_noise_det:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, SOUND_DET_ENABLE, "Sound Detection", "mdi:ear-hearing"))
        if coord._has_cry_det:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, CRY_DET_ENABLE, "Crying Detection", "mdi:baby-face-outline"))
        if coord._has_onvif:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, ONVIF_ENABLE, "ONVIF", "mdi:network-outline"))
        if coord._has_sd_card:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, RECORD_SWITCH, "SD Recording", "mdi:sd"))
        if coord._has_pir:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, PIR_DET_ENABLE, "PIR Sensor", "mdi:motion-sensor-pau"))
        if coord._has_floodlight:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, FLIGHT_LIGHT_SWITCH, "Floodlight", "mdi:lightbulb-floodlight"))
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, FLIGHT_LINK_LIGHTING_ENABLE, "Floodlight Linkage", "mdi:link-variant"))
        if coord._has_siren:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, FLIGHT_LINK_SIREN_ENABLE, "Siren Linkage", "mdi:link-variant"))
        if coord._has_face_det:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, FACE_RECOGNITION_SWITCH, "Face Recognition", "mdi:face-recognition"))
        if coord._has_sleep_mode:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, SLEEP_MODE, "Sleep Mode", "mdi:sleep"))
        if coord._has_rgb_light:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, RGB_LIGHT_SWITCH, "RGB Light", "mdi:palette"))
        if coord._has_anti_jamming:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, ANTI_JAMMING, "Anti-Jamming", "mdi:shield-check"))
        if coord._has_abnormal_noise:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, ABNORMAL_NOISE_ENABLE, "Abnormal Noise", "mdi:alert-decagram"))
        if coord._has_ptz_patrol:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, PTZ_PATROL, "PTZ Patrol", "mdi:pan-horizontal"))
        if coord._has_laser:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, LASER_SWITCH, "Laser Toy", "mdi:laser-pointer"))
        if coord._has_pet_alarm:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, PET_ALARM_ENABLE, "Pet Alarm", "mdi:dog"))
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, PET_THROW_WARNING, "Pet Throw Warning", "mdi:dog-side"))
        if coord._has_homekit:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, HOMEKIT_ENABLE, "HomeKit", "mdi:home-automation"))
        if coord._has_auto_update:
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, AUTO_UPDATE, "Auto Update", "mdi:update"))

        # Other common ones
        entities.append(CloudEdgeMeariIotSwitch(coord, entry, OSD_ENABLE, "OSD Watermark", "mdi:watermark"))
        entities.append(CloudEdgeMeariIotSwitch(coord, entry, LOGO_SWITCH, "Brand Logo", "mdi:ide-logo"))
        entities.append(CloudEdgeMeariIotSwitch(coord, entry, SOUND_SWITCH, "Sound Switch", "mdi:volume-high"))
        entities.append(CloudEdgeMeariIotSwitch(coord, entry, AUTO_UPDATE, "Auto Update", "mdi:update"))

    async_add_entities(entities)


class CloudEdgeMeariIotSwitch(SwitchEntity):
    """Generic switch for CloudEdge / Meari IOT properties."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        code: int,
        name: str,
        icon: str | None = None
    ) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._code = code
        self._attr_name = name
        self._attr_icon = icon
        self._attr_unique_id = f"{coordinator.device_uuid}_iot_{code}"
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
    def is_on(self) -> bool:
        val = self._coordinator.get_iot_value(self._code)
        return int(val) == 1 if val is not None else False

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the switch on."""
        await self.hass.async_add_executor_job(self._coordinator.set_iot_value, self._code, 1)
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the switch off."""
        await self.hass.async_add_executor_job(self._coordinator.set_iot_value, self._code, 0)
        self.async_write_ha_state()


class CloudEdgeMeariMotionWakeSwitch(SwitchEntity):
    """Switch to enable / disable automatic wake on motion."""

    _attr_has_entity_name = True
    _attr_name = "Wake on Motion"
    _attr_icon = "mdi:motion-sensor"

    def __init__(self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_uuid}_motion_wake"
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
    def is_on(self) -> bool:
        return self._coordinator.motion_wake_enabled

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Enable wake on motion."""
        self._coordinator.set_motion_wake_enabled(True)
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Disable wake on motion."""
        self._coordinator.set_motion_wake_enabled(False)
        self.async_write_ha_state()

