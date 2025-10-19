# crawler/config_loader.py
from __future__ import annotations
import json
import os
from typing import Dict, List, Any, Optional
from . import logger

def load_config() -> Dict[str, Any]:
    """Load the retailers configuration from data/retailers.json"""
    # Try multiple possible paths for the config file
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "..", "data", "retailers.json"),
        "/app/data/retailers.json",
        "data/retailers.json",
        "retailers.json"
    ]
    
    for path in possible_paths:
        path = os.path.abspath(path)
        logger.info("config: trying config_path=%s", path)
        if os.path.exists(path):
            logger.info("config: found config file at %s", path)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info("config: loaded config successfully")
                return data
            except Exception:
                logger.exception("config: failed reading config JSON from %s", path)
                continue
    
    logger.error("config: config file not found in any of the expected locations")
    raise FileNotFoundError("retailers.json config file not found")

def resolve_retailers(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Returns list of dicts:
    {
      "id", "name", "url", "host",
      "download_patterns": [...],
      "tags": [...],
      # present only if login:
      "login_profile": { "type", "login_url", "post_login_path" },
      "credentials": { "username", "password" }
    }
    """
    resolved = []
    auth_profiles = cfg.get("authProfiles", {})
    retailers = cfg.get("retailers", [])
    
    for retailer in retailers:
        if not retailer.get("enabled", True):
            logger.info("config: skipping disabled retailer %s", retailer.get("id", "unknown"))
            continue
            
        resolved_retailer = {
            "id": retailer.get("id"),
            "name": retailer.get("name"),
            "url": retailer.get("url"),
            "host": retailer.get("host"),
            "download_patterns": retailer.get("download_patterns", []),
            "tags": retailer.get("tags", [])
        }
        
        # Check if this retailer needs authentication
        auth_ref = retailer.get("authRef")
        tenant_key = retailer.get("tenantKey")
        
        if auth_ref and tenant_key:
            # Look up the auth profile
            if auth_ref not in auth_profiles:
                logger.error("config: auth profile '%s' not found for retailer %s", auth_ref, retailer.get("id"))
                continue
                
            auth_profile = auth_profiles[auth_ref]
            tenants = auth_profile.get("tenants", {})
            
            if tenant_key not in tenants:
                logger.error("config: tenant '%s' not found in auth profile '%s' for retailer %s", 
                           tenant_key, auth_ref, retailer.get("id"))
                continue
                
            # Add login profile and credentials
            resolved_retailer["login_profile"] = {
                "type": auth_profile.get("type"),
                "login_url": auth_profile.get("login_url"),
                "post_login_path": auth_profile.get("post_login_path")
            }
            resolved_retailer["credentials"] = tenants[tenant_key]
            
            logger.info("config: resolved retailer %s with auth (profile=%s, tenant=%s)", 
                       retailer.get("id"), auth_ref, tenant_key)
        else:
            logger.info("config: resolved retailer %s without auth", retailer.get("id"))
            
        resolved.append(resolved_retailer)
    
    logger.info("config: resolved %d retailers", len(resolved))
    return resolved
