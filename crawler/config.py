# crawler/config.py
import json, os
from typing import Dict
from pydantic import BaseSettings
from .creds import CREDS as DEFAULT_CREDS

class Settings(BaseSettings):
    bucket: str = os.getenv("PRICES_BUCKET", "zila-prices")
    max_retries_login: int = int(os.getenv("MAX_RETRIES_LOGIN", "3"))
    # Optional JSON override of creds (kept for future flexibility)
    retailer_creds_json: str | None = os.getenv("RETAILER_CREDS_JSON")

    def creds(self) -> Dict[str, Dict[str, str]]:
        # start with public defaults
        merged = dict(DEFAULT_CREDS)
        # allow env override if someone decides to change in future
        if self.retailer_creds_json:
            try:
                override = json.loads(self.retailer_creds_json)
                for k, v in override.items():
                    merged[k] = v
            except Exception:
                pass
        return merged

settings = Settings()
