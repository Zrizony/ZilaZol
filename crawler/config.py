# crawler/config.py
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

def load_retailers_config(path: str = "data/retailers.json") -> Dict[str, Any]:
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    # Validate minimal structure but do NOT enforce a fixed schema
    assert isinstance(data, dict), "retailers.json must be an object"
    assert "retailers" in data and isinstance(data["retailers"], list), \
        "retailers.json must contain a 'retailers' list"
    return data


def _requires_credentials(retailer: dict) -> bool:
    """
    Determine if a retailer requires credentials.
    
    A retailer needs credentials if:
    - It has a 'tenantKey' field (PublishedPrices retailers)
    - It has a 'creds_key' field
    - Any of its sources have a 'creds_key' field
    """
    # Check retailer-level keys
    if retailer.get("tenantKey") or retailer.get("creds_key"):
        return True
    
    # Check sources for creds_key
    sources = retailer.get("sources", [])
    for source in sources:
        if source.get("creds_key"):
            return True
    
    return False


def get_retailers(group: Optional[str] = None, path: str = "data/retailers.json") -> List[dict]:
    """
    Return retailers filtered by group.
    
    Args:
        group: Optional filter for retailer group
            - 'creds': only retailers that require credentials (have tenantKey or creds_key)
            - 'public': only retailers that don't require credentials
            - None: all retailers (current behavior)
        path: Path to retailers.json config file
    
    Returns:
        List of retailer dictionaries matching the group filter
    """
    cfg = load_retailers_config(path)
    retailers = cfg.get("retailers", [])
    
    if group == "creds":
        retailers = [r for r in retailers if _requires_credentials(r)]
    elif group == "public":
        retailers = [r for r in retailers if not _requires_credentials(r)]
    # If group is None or any other value, return all retailers (no filtering)
    
    return retailers