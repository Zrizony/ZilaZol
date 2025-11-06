# crawler/utils.py
from __future__ import annotations
import os
import re

from .constants import VALID_PATTERNS


def safe_name(s: str) -> str:
    """Sanitize string for use in filenames."""
    return re.sub(r"[^\w\-.]+", "_", s).strip("_")[:120]


def ensure_dirs(*paths: str):
    """Create directories if they don't exist."""
    for p in paths:
        os.makedirs(p, exist_ok=True)


def looks_like_price_file(url: str) -> bool:
    """Check if URL looks like a price file (hardened to catch mislabeled extensions)."""
    u = (url or "").lower()
    if any(p in u for p in VALID_PATTERNS):
        return True
    return ("pricefull" in u) or ("promo" in u) or ("stores" in u) or ("price" in u)

