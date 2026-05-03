"""Camera platform for CloudEdge / Meari integration."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudEdgeMeariCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari camera from a config entry."""
    coord: CloudEdgeMeariCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([CloudEdgeMeariCamera(coord, entry)])


class CloudEdgeMeariCamera(Camera):
    """Representation of a CloudEdge / Meari camera."""

    _attr_has_entity_name = True
    _attr_name = "Camera"
    _attr_supported_features = CameraEntityFeature.STREAM

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        """Initialize the camera."""
        super().__init__()
        self._coordinator = coordinator
        self._entry = entry
        self._attr_entity_registry_enabled_default = coordinator.is_battery_camera
        self._attr_unique_id = f"{coordinator.device_uuid}_camera"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, coordinator.device_uuid)},
            "name": f"CloudEdge / Meari {coordinator.device_name}",
            "manufacturer": "CloudEdge / Meari",
            "model": coordinator.device_model,
        }
        self._unsub_update: Any = None

    async def async_added_to_hass(self) -> None:
        """Register update callback when entity is added."""
        self._unsub_update = self._coordinator.register_update_callback(
            self._handle_coordinator_update
        )

    async def async_will_remove_from_hass(self) -> None:
        """Unregister callback when entity is removed."""
        if self._unsub_update:
            self._unsub_update()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        """Return True if the camera is available."""
        return self._coordinator.available

    @property
    def is_streaming(self) -> bool:
        """Return True if the camera is actively streaming."""
        if not self._coordinator.is_battery_camera:
            return True
        return self._coordinator.camera_awake

    async def stream_source(self) -> str | None:
        """Return MPEG-TS stream URL for HA stream / Frigate."""
        port = self._coordinator.stream_port
        if not port:
            return None
        host = self._coordinator.stream_host
        return f"tcp://{host}:{port}"

    @property
    def motion_detection_enabled(self) -> bool:
        """Return True — motion detection is always enabled."""
        return True

    async def async_camera_image(
        self, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        """Return the latest camera image (JPEG snapshot)."""
        return self._coordinator.latest_image

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        attrs: dict[str, Any] = {}
        if self._coordinator.motion_type:
            attrs["motion_type"] = self._coordinator.motion_type
        if self._coordinator.device_id:
            attrs["device_id"] = self._coordinator.device_id
        attrs["sn_num"] = self._coordinator.device_uuid
        return attrs
