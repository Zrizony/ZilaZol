# crawler/constants.py
import os

# Unified bucket environment variable handling
BUCKET = (
    os.getenv("GCS_BUCKET")
    or os.getenv("PRICES_BUCKET")
    or os.getenv("BUCKET_NAME")
)
if not BUCKET:
    raise RuntimeError("No bucket configured. Set GCS_BUCKET (preferred) or PRICES_BUCKET/BUCKET_NAME.")

LOCAL_DOWNLOAD_DIR = os.getenv("LOCAL_DOWNLOAD_DIR", "downloads")
LOCAL_JSON_DIR = os.getenv("LOCAL_JSON_DIR", "json_out")
SCREENSHOTS_DIR = os.getenv("SCREENSHOTS_DIR", "screenshots")
MAX_LOGIN_RETRIES = int(os.getenv("MAX_RETRIES_LOGIN", "3"))

PUBLISHED_HOST = "url.publishedprices.co.il"
DEFAULT_DOWNLOAD_SUFFIXES = (".xml", ".gz", ".zip")
VALID_PATTERNS = (".xml", ".gz", ".zip")

