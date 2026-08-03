"""Shared paths and auth defaults/env-key maps for the debug harness."""

from __future__ import annotations
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
AUTH_PROFILES = ("cloudedge", "cloudplus", "iegeek", "arenti")
AUTH_DEFAULTS = {
    "country_code": "FR",
    "phone_code": "33",
    "profile": "cloudedge",
}
AUTH_ENV_KEYS = {
    "email": ("CLOUDPLUS_EMAIL", "CLOUDEDGE_EMAIL", "EMAIL"),
    "password": ("CLOUDPLUS_PASSWORD", "CLOUDEDGE_PASSWORD", "PASSWORD"),
    "country_code": (
        "CLOUDPLUS_COUNTRY_CODE",
        "CLOUDEDGE_COUNTRY_CODE",
        "COUNTRY_CODE",
    ),
    "phone_code": ("CLOUDPLUS_PHONE_CODE", "CLOUDEDGE_PHONE_CODE", "PHONE_CODE"),
    "profile": ("CLOUDPLUS_PROFILE", "CLOUDEDGE_PROFILE", "PROFILE"),
}
