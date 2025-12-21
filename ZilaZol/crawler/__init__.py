# crawler/__init__.py
from __future__ import annotations
import logging
import os

# Basic logger config (Cloud Run-friendly)
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=_LOG_LEVEL,
    format="%(asctime)s %(levelname)-7s %(message)s",
)
logger = logging.getLogger("crawler")
logger.setLevel(_LOG_LEVEL)
