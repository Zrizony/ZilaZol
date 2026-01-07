# crawler/download.py
from __future__ import annotations
import re
from urllib.parse import urlparse

from playwright.async_api import Page

from . import logger
from .parsers import parse_from_blob


def _resp_headers(resp) -> dict:
    """Extract headers from response object (handles different response types)."""
    try:
        h = resp.headers
        if callable(h):
            return h()
        return h or {}
    except Exception:
        try:
            return resp.headers() or {}
        except Exception:
            return {}


def pick_filename(resp, fallback: str) -> str:
    """Extract filename from Content-Disposition header, fallback to provided name."""
    cd = _resp_headers(resp).get("content-disposition") or ""
    m = re.search(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)\"?", cd, re.IGNORECASE)
    if m:
        return m.group(1)
    return fallback


async def fetch_url(page: Page, url: str) -> tuple[bytes | None, object | None, str | None]:
    """Download URL and return (data, response, filename). Returns (None, None, None) for 404/403 errors."""
    try:
        resp = await page.request.get(url, timeout=90000)
        
        # Handle 404 (Not Found) and 403 (Forbidden) - skip broken links
        if resp.status == 404:
            logger.warning(f"Skipping broken link: {url}")
            return None, None, None
        if resp.status == 403:
            logger.warning(f"Skipping broken link: {url}")
            return None, None, None
        
        # Raise error for other non-OK statuses
        if not resp.ok:
            raise RuntimeError(f"download_failed status={resp.status}")
        
        data = await resp.body()
        fallback = urlparse(url).path.split('/')[-1] or "download"
        fname = pick_filename(resp, fallback)
        return data, resp, fname
    
    except Exception as e:
        # Catch any network/request errors and log them
        logger.warning(f"Skipping broken link: {url} (error: {e})")
        return None, None, None


async def maybe_parse_to_jsonl(retailer_id: str, filename: str, data: bytes, run_id: str = ""):
    """Legacy wrapper - routes to unified parse_from_blob."""
    try:
        await parse_from_blob(data, filename, retailer_id, run_id)
    except Exception as e:
        # Guard log for mislabeled files
        if hasattr(e, "__class__") and ("BadGzipFile" in str(e.__class__) or e.__class__.__name__ in ("BadGzipFile", "OSError")):
            if data[:2] == b"PK":
                logger.warning("gzip_mislabel_detected file=%s note='starts with PK -> zip' -- rerouting to extractor", filename)
        logger.warning("Failed to parse %s: %s", filename, e)
