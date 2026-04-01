"""Config flow for CloudPlus / Meari integration."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.helpers import selector

from .const import (
    APP_PROFILES,
    CONF_APP_PROFILE,
    CONF_COUNTRY_CODE,
    CONF_EMAIL,
    CONF_PHONE_CODE,
    CONF_PASSWORD,
    DEFAULT_APP_PROFILE,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_PHONE_CODE,
    DOMAIN,
)
from .api import MeariApiClient

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): str,
        vol.Required(CONF_PASSWORD): str,
        vol.Optional(CONF_COUNTRY_CODE, default=DEFAULT_COUNTRY_CODE): str,
        vol.Optional(CONF_PHONE_CODE, default=DEFAULT_PHONE_CODE): str,
        vol.Optional(CONF_APP_PROFILE, default=DEFAULT_APP_PROFILE): selector.SelectSelector(
            selector.SelectSelectorConfig(
                options=[
                    selector.SelectOptionDict(value=profile, label=profile.capitalize())
                    for profile in APP_PROFILES
                ],
                mode=selector.SelectSelectorMode.DROPDOWN,
            )
        ),
    }
)


class CloudPlusConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for CloudPlus / Meari."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Single step: collect credentials, auto-add all supported cameras."""
        errors: dict[str, str] = {}

        if user_input is not None:
            email = user_input[CONF_EMAIL].strip()
            password = user_input[CONF_PASSWORD]
            country_code = user_input.get(CONF_COUNTRY_CODE, DEFAULT_COUNTRY_CODE).strip().upper()
            phone_code = str(user_input.get(CONF_PHONE_CODE, DEFAULT_PHONE_CODE)).strip().lstrip("+")
            app_profile = user_input.get(CONF_APP_PROFILE, DEFAULT_APP_PROFILE)

            client = MeariApiClient(
                email=email,
                password=password,
                country_code=country_code,
                phone_code=phone_code,
                app_profile=app_profile,
            )

            try:
                await self.hass.async_add_executor_job(client.login)
                camera_devices = client.get_camera_devices()
            except PermissionError:
                errors["base"] = "invalid_auth"
            except (ConnectionError, OSError):
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected exception during login")
                errors["base"] = "unknown"
            else:
                if not camera_devices:
                    errors["base"] = "no_devices"
                else:
                    # One config entry per account — prevent duplicates
                    await self.async_set_unique_id(email.lower())
                    self._abort_if_unique_id_configured()

                    return self.async_create_entry(
                        title=f"CloudPlus ({email})",
                        data={
                            CONF_EMAIL: email,
                            CONF_PASSWORD: password,
                            CONF_COUNTRY_CODE: country_code,
                            CONF_PHONE_CODE: phone_code,
                            CONF_APP_PROFILE: app_profile,
                        },
                    )

        return self.async_show_form(
            step_id="user", data_schema=STEP_USER_DATA_SCHEMA, errors=errors
        )
