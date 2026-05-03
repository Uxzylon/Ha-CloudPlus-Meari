"""Config flow for CloudEdge / Meari integration."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.helpers import selector

from .const import (
    APP_PROFILES,
    CONF_APP_PROFILE,
    CONF_COUNTRY_CODE,
    CONF_EMAIL,
    CONF_PHONE_CODE,
    CONF_PASSWORD,
    CONF_SN_NUM,
    CONF_VIDEO_PASSWORD,
    DEFAULT_APP_PROFILE,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_PHONE_CODE,
    DOMAIN,
)
from .api import MeariApiClient

_LOGGER = logging.getLogger(__name__)


def _normalize_phone_code(raw: Any) -> str:
    return str(raw).strip().lstrip("+")


def _clean_optional_text(raw: Any) -> str:
    return "" if raw is None else str(raw).strip()


def _app_profile_selector() -> selector.SelectSelector:
    return selector.SelectSelector(
        selector.SelectSelectorConfig(
            options=[
                selector.SelectOptionDict(value=p, label=p.capitalize())
                for p in APP_PROFILES
            ],
            mode=selector.SelectSelectorMode.DROPDOWN,
        )
    )


STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): str,
        vol.Required(CONF_PASSWORD): str,
        vol.Optional(CONF_COUNTRY_CODE, default=DEFAULT_COUNTRY_CODE): str,
        vol.Optional(CONF_PHONE_CODE, default=DEFAULT_PHONE_CODE): str,
        vol.Optional(
            CONF_APP_PROFILE, default=DEFAULT_APP_PROFILE
        ): _app_profile_selector(),
    }
)


class CloudEdgeMeariConfigFlow(ConfigFlow, domain=DOMAIN):
    """Config flow — one account entry; cameras are created automatically on setup."""

    VERSION = 2

    @staticmethod
    def async_get_options_flow(
        config_entry: ConfigEntry,
    ) -> "CloudEdgeMeariOptionsFlow":
        return CloudEdgeMeariOptionsFlow(config_entry)

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            email = user_input[CONF_EMAIL].strip()
            password = user_input[CONF_PASSWORD]
            country_code = (
                str(user_input.get(CONF_COUNTRY_CODE, DEFAULT_COUNTRY_CODE))
                .strip()
                .upper()
            )
            phone_code = _normalize_phone_code(
                user_input.get(CONF_PHONE_CODE, DEFAULT_PHONE_CODE)
            )
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
            except PermissionError:
                errors["base"] = "invalid_auth"
            except (ConnectionError, OSError):
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected exception during login")
                errors["base"] = "unknown"
            else:
                await self.async_set_unique_id(email.lower())
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"CloudEdge / Meari ({email})",
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

    async def async_step_import(self, import_data: dict[str, Any]) -> ConfigFlowResult:
        """Create a camera entry from account setup (migration + auto-discovery)."""
        sn = str(import_data.get(CONF_SN_NUM, "")).strip()
        if not sn:
            return self.async_abort(reason="unknown")

        await self.async_set_unique_id(sn)
        self._abort_if_unique_id_configured()

        name = str(import_data.get("device_name", sn)).strip() or sn
        email = str(import_data.get(CONF_EMAIL, "")).strip()
        title = f"{name} ({email})" if email else name
        return self.async_create_entry(title=title, data=import_data)


# ---------------------------------------------------------------------------
# Options flows
# ---------------------------------------------------------------------------


class CloudEdgeMeariOptionsFlow(OptionsFlow):
    """Single options flow that adapts to account vs camera entry."""

    def __init__(self, config_entry: ConfigEntry) -> None:
        self._config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if CONF_SN_NUM in self._config_entry.data:
            return await self._camera_options(user_input)
        return await self._account_options(user_input)

    # ------------------------------------------------------------------
    # Camera entry — only E2EE video password
    # ------------------------------------------------------------------

    async def _camera_options(
        self, user_input: dict[str, Any] | None
    ) -> ConfigFlowResult:
        current_options = dict(self._config_entry.options)
        current_video_password = _clean_optional_text(
            current_options.get(CONF_VIDEO_PASSWORD)
        )

        if user_input is not None:
            new_options = dict(current_options)
            video_password = _clean_optional_text(user_input.get(CONF_VIDEO_PASSWORD))
            if video_password:
                new_options[CONF_VIDEO_PASSWORD] = video_password
            else:
                new_options.pop(CONF_VIDEO_PASSWORD, None)
            self.hass.config_entries.async_update_entry(
                self._config_entry, options=new_options
            )
            await self.hass.config_entries.async_reload(self._config_entry.entry_id)
            # Return saved options to avoid the flow finalization clearing them.
            return self.async_create_entry(title="", data=new_options)

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_VIDEO_PASSWORD, default=current_video_password
                    ): str,
                }
            ),
        )

    # ------------------------------------------------------------------
    # Account entry — credentials; propagated to all child camera entries
    # ------------------------------------------------------------------

    async def _account_options(
        self, user_input: dict[str, Any] | None
    ) -> ConfigFlowResult:
        current_data = dict(self._config_entry.data)
        country_code = (
            str(current_data.get(CONF_COUNTRY_CODE, DEFAULT_COUNTRY_CODE))
            .strip()
            .upper()
        )
        phone_code = _normalize_phone_code(
            current_data.get(CONF_PHONE_CODE, DEFAULT_PHONE_CODE)
        )
        app_profile = current_data.get(CONF_APP_PROFILE, DEFAULT_APP_PROFILE)

        if user_input is not None:
            new_data = dict(current_data)

            new_password = str(user_input.get(CONF_PASSWORD, "")).strip()
            if new_password:
                new_data[CONF_PASSWORD] = new_password

            new_data[CONF_COUNTRY_CODE] = (
                str(user_input.get(CONF_COUNTRY_CODE, country_code)).strip().upper()
            )
            new_data[CONF_PHONE_CODE] = _normalize_phone_code(
                user_input.get(CONF_PHONE_CODE, phone_code)
            )
            new_data[CONF_APP_PROFILE] = user_input.get(CONF_APP_PROFILE, app_profile)

            self.hass.config_entries.async_update_entry(
                self._config_entry, data=new_data
            )

            # Propagate updated credentials to all child camera entries.
            account_entry_id = self._config_entry.entry_id
            for cam_entry in self.hass.config_entries.async_entries(DOMAIN):
                if cam_entry.data.get("account_entry_id") != account_entry_id:
                    continue
                # Fetch fresh entry from registry to ensure options are up-to-date
                fresh_cam = self.hass.config_entries.async_get_entry(cam_entry.entry_id)
                if fresh_cam is None:
                    continue
                cam_data = dict(fresh_cam.data)
                cam_data[CONF_PASSWORD] = new_data[CONF_PASSWORD]
                cam_data[CONF_COUNTRY_CODE] = new_data[CONF_COUNTRY_CODE]
                cam_data[CONF_PHONE_CODE] = new_data[CONF_PHONE_CODE]
                cam_data[CONF_APP_PROFILE] = new_data[CONF_APP_PROFILE]
                self.hass.config_entries.async_update_entry(
                    fresh_cam, data=cam_data, options=dict(fresh_cam.options)
                )
                self.hass.async_create_task(
                    self.hass.config_entries.async_reload(fresh_cam.entry_id)
                )

            await self.hass.config_entries.async_reload(self._config_entry.entry_id)
            # Preserve any existing options on the account entry.
            return self.async_create_entry(
                title="", data=dict(self._config_entry.options)
            )

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_PASSWORD): str,
                    vol.Optional(CONF_COUNTRY_CODE, default=country_code): str,
                    vol.Optional(CONF_PHONE_CODE, default=phone_code): str,
                    vol.Optional(
                        CONF_APP_PROFILE, default=app_profile
                    ): _app_profile_selector(),
                }
            ),
        )
