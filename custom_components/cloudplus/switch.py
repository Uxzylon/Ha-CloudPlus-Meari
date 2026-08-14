"""Switch platform for CloudEdge / Meari — wake on motion toggle."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudEdgeMeariCoordinator
from .entity import CloudEdgeMeariEntity, CloudEdgeMeariIotEntity
from .meari_commands import (
    ABNORMAL_NOISE_ENABLE,
    ANTI_JAMMING,
    AUTO_UPDATE,
    CRY_DET_ENABLE,
    FACE_RECOGNITION_SWITCH,
    FLIGHT_LINK_LIGHTING_ENABLE,
    FLIGHT_LINK_SIREN_ENABLE,
    HOMEKIT_ENABLE,
    HUMAN_DET_ENABLE,
    HUMAN_TRACK_ENABLE,
    LASER_SWITCH,
    LED_ENABLE,
    LOGO_SWITCH,
    MOTION_DET_ENABLE,
    ONVIF_ENABLE,
    OSD_ENABLE,
    PET_ALARM_ENABLE,
    PET_THROW_WARNING,
    PIR_DET_ENABLE,
    PTZ_PATROL,
    RECORD_SWITCH,
    RGB_LIGHT_SWITCH,
    SLEEP_MODE,
    SOUND_DET_ENABLE,
    SOUND_SWITCH,
)


@dataclass(frozen=True)
class IotSwitchSpec:
    """Declarative spec for an IoT-backed switch entity."""

    feature: str | None
    code: int
    name: str
    icon: str | None = None


IOT_SWITCHES: tuple[IotSwitchSpec, ...] = (
    IotSwitchSpec("status_led", LED_ENABLE, "Status LED", "mdi:led-on"),
    IotSwitchSpec(
        "motion_det",
        MOTION_DET_ENABLE,
        "Motion Detection",
        "mdi:motion-sensor",
    ),
    IotSwitchSpec(
        "person_det",
        HUMAN_DET_ENABLE,
        "Person Detection",
        "mdi:account-search",
    ),
    IotSwitchSpec(
        "human_track",
        HUMAN_TRACK_ENABLE,
        "Human Tracking",
        "mdi:target-account",
    ),
    IotSwitchSpec("noise_det", SOUND_DET_ENABLE, "Sound Detection", "mdi:ear-hearing"),
    IotSwitchSpec(
        "cry_det",
        CRY_DET_ENABLE,
        "Crying Detection",
        "mdi:baby-face-outline",
    ),
    IotSwitchSpec("onvif", ONVIF_ENABLE, "ONVIF", "mdi:network-outline"),
    IotSwitchSpec("sd_card", RECORD_SWITCH, "SD Recording", "mdi:sd"),
    IotSwitchSpec("pir", PIR_DET_ENABLE, "PIR Sensor", "mdi:motion-sensor"),
    IotSwitchSpec(
        "floodlight",
        FLIGHT_LINK_LIGHTING_ENABLE,
        "Floodlight Linkage",
        "mdi:link-variant",
    ),
    IotSwitchSpec(
        "siren",
        FLIGHT_LINK_SIREN_ENABLE,
        "Siren Linkage",
        "mdi:alarm-light",
    ),
    IotSwitchSpec(
        "face_det",
        FACE_RECOGNITION_SWITCH,
        "Face Recognition",
        "mdi:face-recognition",
    ),
    IotSwitchSpec("sleep_mode", SLEEP_MODE, "Sleep Mode", "mdi:sleep"),
    IotSwitchSpec("rgb_light", RGB_LIGHT_SWITCH, "RGB Light", "mdi:palette"),
    IotSwitchSpec("anti_jamming", ANTI_JAMMING, "Anti-Jamming", "mdi:shield-check"),
    IotSwitchSpec(
        "abnormal_noise",
        ABNORMAL_NOISE_ENABLE,
        "Abnormal Noise",
        "mdi:alert-decagram",
    ),
    IotSwitchSpec("ptz_patrol", PTZ_PATROL, "PTZ Patrol", "mdi:pan-horizontal"),
    IotSwitchSpec("laser", LASER_SWITCH, "Laser Toy", "mdi:laser-pointer"),
    IotSwitchSpec("pet_alarm", PET_ALARM_ENABLE, "Pet Alarm", "mdi:bell-alert"),
    IotSwitchSpec(
        "pet_alarm",
        PET_THROW_WARNING,
        "Pet Throw Warning",
        "mdi:bell-alert",
    ),
    IotSwitchSpec("homekit", HOMEKIT_ENABLE, "HomeKit", "mdi:home-automation"),
    IotSwitchSpec("auto_update", AUTO_UPDATE, "Auto Update", "mdi:update"),
    IotSwitchSpec(None, OSD_ENABLE, "OSD Watermark", "mdi:watermark"),
    IotSwitchSpec(None, LOGO_SWITCH, "Brand Logo", "mdi:label-outline"),
    IotSwitchSpec(None, SOUND_SWITCH, "Sound", "mdi:volume-high"),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari switches from a config entry."""
    coord: CloudEdgeMeariCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[SwitchEntity] = []
    if coord.is_battery_camera:
        entities.append(CloudEdgeMeariMotionWakeSwitch(coord, entry))
    if coord.has_lamp:
        entities.append(CloudEdgeMeariLampSwitch(coord, entry))
    entities.extend(
        CloudEdgeMeariIotSwitch(coord, entry, spec)
        for spec in IOT_SWITCHES
        if (spec.feature and coord.supports_iot(spec.feature))
        or coord.has_iot_code(spec.code)
    )
    async_add_entities(entities)


class CloudEdgeMeariIotSwitch(CloudEdgeMeariIotEntity, SwitchEntity):
    """Switch entity backed by a Meari IoT value."""

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        spec: IotSwitchSpec,
    ) -> None:
        super().__init__(coordinator, entry, spec)
        self._attr_unique_id = f"{coordinator.device_uuid}_iot_switch_{spec.code}"

    @property
    def is_on(self) -> bool:
        value = self._iot_value
        try:
            return int(value) == 1
        except (TypeError, ValueError):
            return False

    async def async_turn_on(self, **_kwargs: Any) -> None:
        """Turn the camera IoT value on."""
        await self.hass.async_add_executor_job(
            self._coordinator.set_iot_value,
            self._spec.code,
            1,
        )
        self.async_write_ha_state()

    async def async_turn_off(self, **_kwargs: Any) -> None:
        """Turn the camera IoT value off."""
        await self.hass.async_add_executor_job(
            self._coordinator.set_iot_value,
            self._spec.code,
            0,
        )
        self.async_write_ha_state()


class CloudEdgeMeariMotionWakeSwitch(CloudEdgeMeariEntity, SwitchEntity):
    """Switch to enable / disable automatic wake on motion."""

    _attr_name = "Wake on Motion"
    _attr_icon = "mdi:motion-sensor"

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{coordinator.device_uuid}_motion_wake"

    @property
    def is_on(self) -> bool:
        return self._coordinator.motion_wake_enabled

    async def async_turn_on(self, **_kwargs: Any) -> None:
        """Enable wake on motion."""
        self._coordinator.set_motion_wake_enabled(True)
        self.async_write_ha_state()

    async def async_turn_off(self, **_kwargs: Any) -> None:
        """Disable wake on motion."""
        self._coordinator.set_motion_wake_enabled(False)
        self.async_write_ha_state()


class CloudEdgeMeariLampSwitch(CloudEdgeMeariEntity, SwitchEntity):
    """Switch to control the camera's built-in lamp / LED light."""

    _attr_name = "Lamp"
    _attr_icon = "mdi:lightbulb"

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{coordinator.device_uuid}_lamp"

    @property
    def is_on(self) -> bool:
        return self._coordinator.lamp_on

    async def async_turn_on(self, **_kwargs: Any) -> None:
        """Turn on the lamp."""
        await self.hass.async_add_executor_job(self._coordinator.set_lamp, True)
        self.async_write_ha_state()

    async def async_turn_off(self, **_kwargs: Any) -> None:
        """Turn off the lamp."""
        await self.hass.async_add_executor_job(self._coordinator.set_lamp, False)
        self.async_write_ha_state()
