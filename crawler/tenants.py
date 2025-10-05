# crawler/tenants.py
from typing import Dict

# Map retailer display-name substrings (as they appear on gov.il) → credential key
SHOP_TO_CREDKEY: Dict[str, str] = {
    "דור אלון": "doralon",
    "טיב טעם": "TivTaam",
    "יוחננוף": "yohananof",
    "אושר עד": "osherad",
    "סאלח דבאח": "SalachD",
    "פוליצר": "politzer",
    "פז ": "Paz_bo",           # note trailing space to avoid "פזגז" etc.
    "קשת טעמים": "Keshet",
    "רמי לוי": "RamiLevi",
    "קופיקס": "SuperCofixApp",
    "פרשמרקט": "freshmarket",
}

def cred_key_for_display_name(display_name: str) -> str | None:
    """Return a credential key (e.g. 'TivTaam') by matching a known substring in the Hebrew display name."""
    for sub, key in SHOP_TO_CREDKEY.items():
        if sub in (display_name or ""):
            return key
    return None
