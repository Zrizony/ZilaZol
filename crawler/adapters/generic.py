# crawler/adapters/generic.py
from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import List, Set, Optional

from playwright.async_api import Page

from .. import logger
from ..constants import SCREENSHOTS_DIR, DEFAULT_DOWNLOAD_SUFFIXES
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..gcs import get_bucket, upload_to_gcs
from ..parsers import parse_from_blob
from ..utils import ensure_dirs, looks_like_price_file
from ..memory_utils import log_memory


async def collect_links_on_page(page: Page, patterns: Optional[List[str]] = None) -> List[str]:
    """
    Collect download links from main frame AND all child frames.
    
    FIXED: Now scans all frames, not just main frame, to handle sites with iframes.
    """
    # Expanded selectors for better link discovery
    selectors = [
        "a[download]",
        "a[href*='download']",
        "a[href*='file']",
        "a[href$='.xml' i]",
        "a[href$='.gz' i]",
        "a[href$='.zip' i]",
        "a[href*='.xml?' i]",
        "a[href*='.gz?' i]",
        "a[href*='.zip?' i]",
    ]
    # Build suffix selectors from patterns
    pat = [p.lower() for p in (patterns or DEFAULT_DOWNLOAD_SUFFIXES)]
    for p in pat:
        selectors.append(f"a[href$='{p}' i]")
        selectors.append(f"a[href*='{p}?' i]")

    hrefs = set()
    
    # Scan ALL frames (main + child frames) - many sites use iframes
    for frame in page.frames:
        for sel in selectors:
            try:
                count = await frame.locator(sel).count()
                if count == 0:
                    continue
                
                vals = await frame.eval_on_selector_all(sel, "els => els.map(a => a.href)")
                for h in (vals or []):
                    if h and (looks_like_price_file(h) or h.lower().endswith(tuple(pat))):
                        hrefs.add(h)
            except Exception:
                # Frame scan failed for this selector, continue to next
                continue
    
    return sorted(hrefs)


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
        log_memory(logger, f"generic.before_collect_links retailer={retailer_id}")
        patterns = source.get("download_patterns") or source.get("patterns") or None
        links = await collect_links_on_page(page, patterns)
        
        # If no links found, retry with additional wait
        if not links:
            await page.wait_for_load_state("networkidle", timeout=8000)
            await page.wait_for_timeout(800)
            links = await collect_links_on_page(page, patterns)
        
        log_memory(logger, f"generic.after_collect_links retailer={retailer_id} count={len(links)}")
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
        log_memory(logger, f"generic.before_downloads retailer={retailer_id} links={len(links)}")
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
        
        log_memory(logger, f"generic.after_downloads retailer={retailer_id} downloaded={result.files_downloaded}")
                
    except Exception as e:
        result.errors.append(f"fatal:{e}")
    
    return result

