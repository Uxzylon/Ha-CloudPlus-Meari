"""Credential resolution (.env / CLI) and API login with fallback."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

from .common import AUTH_DEFAULTS, AUTH_ENV_KEYS, REPO_ROOT


def _parse_dotenv_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            elif " #" in value:
                value = value.split(" #", 1)[0].rstrip()
            values[key] = value
    except (OSError, ValueError, IndexError):
        logging.getLogger(__name__).debug(
            "Failed to parse .env file %s", path, exc_info=True
        )
    return values


def _load_env_auth_values() -> dict[str, str]:
    dotenv_values = _parse_dotenv_file(REPO_ROOT / ".env")
    resolved: dict[str, str] = {}
    for field, keys in AUTH_ENV_KEYS.items():
        value = None
        for key in keys:
            candidate = os.environ.get(key)
            if candidate not in (None, ""):
                value = candidate
                break
            candidate = dotenv_values.get(key)
            if candidate not in (None, ""):
                value = candidate
                break
        if value in (None, ""):
            continue
        if field == "country_code":
            value = value.upper()
        elif field == "profile":
            value = value.lower()
        resolved[field] = value
    return resolved


def _auth_fields_supplied_on_cli(args: argparse.Namespace) -> bool:
    return any(
        getattr(args, field, None) not in (None, "")
        for field in ("email", "password", "country_code", "phone_code", "profile")
    )


def _build_auth_values(
    args: argparse.Namespace,
    env_auth: dict[str, str],
    *,
    use_cli: bool,
) -> dict[str, str | None]:
    resolved: dict[str, str | None] = {}
    for field in ("email", "password", "country_code", "phone_code", "profile"):
        cli_value = getattr(args, field, None)
        if use_cli and cli_value not in (None, ""):
            resolved[field] = cli_value
            continue
        env_value = env_auth.get(field)
        if env_value not in (None, ""):
            resolved[field] = env_value
            continue
        resolved[field] = AUTH_DEFAULTS.get(field)
    return resolved


def _prepare_auth_args(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
) -> None:
    env_auth = _load_env_auth_values()
    cli_supplied = _auth_fields_supplied_on_cli(args)
    primary_auth = _build_auth_values(args, env_auth, use_cli=True)
    env_only_auth = _build_auth_values(args, env_auth, use_cli=False)

    profile = str(primary_auth.get("profile") or "").lower()
    if profile not in {"cloudedge", "cloudplus", "iegeek"}:
        parser.error(
            "Invalid profile. Use --profile cloudedge|cloudplus|iegeek or set PROFILE/CLOUDPLUS_PROFILE/CLOUDEDGE_PROFILE in .env."
        )

    if not primary_auth.get("email") or not primary_auth.get("password"):
        parser.error(
            "Missing credentials. Provide --email/--password or define EMAIL/PASSWORD (or CLOUDPLUS_EMAIL/CLOUDPLUS_PASSWORD) in .env."
        )

    args.auth_primary = primary_auth
    args.auth_env_fallback = None
    if cli_supplied and env_auth.get("email") and env_auth.get("password"):
        if any(
            primary_auth.get(field) != env_only_auth.get(field)
            for field in primary_auth
        ):
            args.auth_env_fallback = env_only_auth

    for field, value in primary_auth.items():
        setattr(args, field, value)


def _make_api_client(api_cls: Any, auth: dict[str, str | None]) -> Any:
    return api_cls(
        email=auth["email"],
        password=auth["password"],
        country_code=auth["country_code"],
        phone_code=auth["phone_code"],
        app_profile=auth["profile"],
    )


def _login_api_with_fallback(api_cls: Any, args: argparse.Namespace) -> Any:
    auth = dict(getattr(args, "auth_primary", {}))
    api = _make_api_client(api_cls, auth)
    try:
        api.login()
    except (OSError, RuntimeError, ValueError, KeyError):
        fallback_auth = getattr(args, "auth_env_fallback", None)
        if not fallback_auth:
            raise
        logging.getLogger(__name__).warning(
            "Login with CLI-priority credentials failed; retrying with .env credentials"
        )
        api = _make_api_client(api_cls, fallback_auth)
        api.login()
        for field, value in fallback_auth.items():
            setattr(args, field, value)
        args.auth_primary = dict(fallback_auth)
    return api
