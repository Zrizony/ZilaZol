# crawler/adapters/generic.py
from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import List, Set

from playwright.async_api import Page

from .. import logger
from ..constants import SCREENSHOTS_DIR
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..gcs import get_bucket, upload_to_gcs
from ..parsers import parse_from_blob
from ..adapters.base import collect_links_on_page
from ..utils import ensure_dirs


async def generic_adapter(page: Page, source: dict, retailer_id: str, seen_hashes: Set[str], seen_names: Set[str], run_id: str) -> RetailerResult:
    """Generic HTTP adapter (no login)"""
    result = RetailerResult(
        retailer_id=retailer_id,
        source_url=source.get("url", ""),
        errors=[],
        adapter="generic"
    )
    
    try:
        # Navigate to page with proper wait conditions
        await page.goto(source.get("url", ""), wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=15000)
        # Additional wait for dynamic content
        await page.wait_for_timeout(2000)
        
        # Collect download links with retry logic
        patterns = source.get("download_patterns") or source.get("patterns") or None
        links = await collect_links_on_page(page, patterns)
        
        # If no links found, retry with additional wait
        if not links:
            await page.wait_for_load_state("networkidle", timeout=8000)
            await page.wait_for_timeout(800)
            links = await collect_links_on_page(page, patterns)
        
        result.links_found = len(links)
        
        # If still no links, take screenshot and log
        if not links:
            result.reasons.append("no_dom_links")
            ensure_dirs(SCREENSHOTS_DIR)
            ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            fname = f"{retailer_id}_generic_no_links_{ts}.png"
            await page.screenshot(path=os.path.join(SCREENSHOTS_DIR, fname), full_page=True)
            logger.warning(f"[{retailer_id}] No links found at {page.url}. Saved screenshot: {fname}")
        
        logger.info("links.discovered slug=%s adapter=generic count=%d", retailer_id, len(links))
        
        # Process each link
        bucket = get_bucket()
        for link in links:
            filename = link.split('/')[-1] or link  # Fallback for error logging
            try:
                data, resp, filename = await fetch_url(page, link)
                kind = sniff_kind(data)
                md5_hash = md5_hex(data)
                
                # Check for duplicates
                if md5_hash in seen_hashes:
                    result.skipped_dupes += 1
                    continue
                
                # Normalize filename for name-based dedupe
                normalized_name = f"{retailer_id}/{filename.lower()}"
                if normalized_name in seen_names:
                    result.skipped_dupes += 1
                    continue
                
                # Add to seen sets
                seen_hashes.add(md5_hash)
                seen_names.add(normalized_name)
                
                # Upload to GCS
                if bucket:
                    blob_path = f"raw/{retailer_id}/{run_id}/{md5_hash}_{filename}"
                    await upload_to_gcs(bucket, blob_path, data, metadata={"md5_hex": md5_hash, "source_filename": filename})
                
                # Unified parse (logs file.downloaded, extracts, parses, logs file.processed)
                await parse_from_blob(data, filename, retailer_id, run_id)
                
                # Update counters based on sniffed kind (not filename extension)
                if kind == "zip":
                    result.zips += 1
                elif kind == "gz":
                    result.gz += 1
                result.files_downloaded += 1
                
            except Exception as e:
                result.errors.append(f"download_error:{link}:{e}")
                logger.error("download.failed retailer=%s link=%s file=%s err=%s", retailer_id, link, filename, str(e))
                continue
                
    except Exception as e:
        result.errors.append(f"fatal:{e}")
    
    return result

