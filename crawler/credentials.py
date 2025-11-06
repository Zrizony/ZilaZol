# crawler/credentials.py
from __future__ import annotations
import json
import os
from typing import Dict

from .config import load_retailers_config


def load_publishedprices_creds() -> Dict[str, dict]:
    """Load PublishedPrices tenant credentials from data/retailers.json,
    merged with RETAILER_CREDS_JSON env (env wins).
    """
    # Start with credentials found in retailers.json (if any)
    cfg: Dict[str, any] = {}
    try:
        cfg = load_retailers_config()
    except Exception:
        cfg = {}

    tenants_from_file: Dict[str, dict] = {}
    auth_profiles = (cfg or {}).get("authProfiles", {})
    for profile in auth_profiles.values():
        # Only consider PublishedPrices-type profiles
        if isinstance(profile, dict) and profile.get("type") == "publishedprices":
            t = profile.get("tenants", {})
            if isinstance(t, dict):
                tenants_from_file.update(t)

    # Merge with env-provided credentials (highest priority)
    raw_env = os.getenv("RETAILER_CREDS_JSON", "{}")
    env_creds: Dict[str, dict] = {}
    if raw_env:
        try:
            env_creds = json.loads(raw_env) or {}
        except Exception as e:
            raise RuntimeError(f"Invalid RETAILER_CREDS_JSON: {e}")

    merged: Dict[str, dict] = {**tenants_from_file, **env_creds}
    return merged


# Global credentials map used by adapters
CREDS = load_publishedprices_creds()

