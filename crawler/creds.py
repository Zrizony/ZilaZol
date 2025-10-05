# crawler/creds.py
from typing import Dict

# Public retailer creds (as published on gov.il)
CREDS: Dict[str, Dict[str, str]] = {
    "doralon":       {"username": "doralon",       "password": "doralon"},
    "TivTaam":       {"username": "TivTaam",       "password": "TivTaam"},
    "yohananof":     {"username": "yohananof",     "password": "yohananof"},
    "osherad":       {"username": "osherad",       "password": "osherad"},
    "SalachD":       {"username": "SalachD",       "password": "12345"},
    "Stop_Market":   {"username": "Stop_Market",   "password": "Stop_Market"},
    "politzer":      {"username": "politzer",      "password": "politzer"},
    "Paz_bo":        {"username": "Paz_bo",        "password": "paz468"},
    "yuda_ho":       {"username": "yuda_ho",       "password": "Yud@147"},
    "freshmarket":   {"username": "freshmarket",   "password": "freshmarket"},
    "Keshet":        {"username": "Keshet",        "password": "Keshet"},
    "RamiLevi":      {"username": "RamiLevi",      "password": "RamiLevi"},
    "SuperCofixApp": {"username": "SuperCofixApp", "password": "SuperCofixApp"},
}

# Map retailer display-name substrings → credential key (as they appear on gov.il)
SHOP_TO_CREDKEY: Dict[str, str] = {
    "דור אלון": "doralon",
    "טיב טעם": "TivTaam",
    "יוחננוף": "yohananof",
    "אושר עד": "osherad",
    "סאלח דבאח": "SalachD",
    "פוליצר": "politzer",
    "פז ": "Paz_bo",  # trailing space avoids partial collisions like "פזגז"
    "קשת טעמים": "Keshet",
    "רמי לוי": "RamiLevi",
    "קופיקס": "SuperCofixApp",
    "פרשמרקט": "freshmarket",
}

def cred_key_for_display_name(display_name: str) -> str | None:
    """Pick a credential key by matching a known Hebrew substring in gov.il display name."""
    name = display_name or ""
    for needle, key in SHOP_TO_CREDKEY.items():
        if needle in name:
            return key
    return None
