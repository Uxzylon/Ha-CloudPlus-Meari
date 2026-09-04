"""Camera platform for CloudEdge / Meari integration."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import CloudEdgeMeariCoordinator
from .entity import CloudEdgeMeariEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari camera from a config entry."""
    coord: CloudEdgeMeariCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([CloudEdgeMeariCamera(coord, entry)])


class CloudEdgeMeariCamera(CloudEdgeMeariEntity, Camera):
    """Representation of a CloudEdge / Meari camera."""

    _attr_name = "Camera"
    _attr_supported_features = CameraEntityFeature.STREAM

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        """Initialize the camera."""
        super().__init__(coordinator, entry)
        self._attr_entity_registry_enabled_default = coordinator.is_battery_camera
        self._attr_unique_id = f"{coordinator.device_uuid}_camera"

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
        del width, height  # HA passes requested dimensions; we serve the cached JPEG.
        return self._coordinator.get_latest_image()

    @property
    def use_stream_for_stills(self) -> bool:
        """Keep still images on the cached JPEG path."""
        return False

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        attrs: dict[str, Any] = {}
        if self._coordinator.motion_type:
            attrs["motion_type"] = self._coordinator.motion_type
        if self._coordinator.device_id:
            attrs["device_id"] = self._coordinator.device_id
        attrs["sn_num"] = self._coordinator.device_uuid
        attrs["image_source"] = self._coordinator.latest_image_source
        attrs["image_generation"] = self._coordinator.latest_image_generation
        attrs["image_updated_at"] = self._coordinator.latest_image_updated_at
        return attrs
