# crawler/adapters/wolt_dateindex.py
from __future__ import annotations
import re
from typing import List, Set
from urllib.parse import urljoin

from playwright.async_api import Page

from .. import logger
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..gcs import get_bucket, upload_to_gcs
from ..parsers import parse_from_blob


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$", re.ASCII)


async def discover_dates(page: Page, base_url: str) -> List[str]:
    """Discover available date links on the Wolt index page."""
    await page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_load_state("networkidle", timeout=8000)
    
    # Get all anchor inner texts
    items = await page.locator("a").all_inner_texts()
    
    # Filter visible anchors that look like dates (YYYY-MM-DD)
    dates = [t.strip() for t in items if t and DATE_RE.match(t.strip())]
    
    # Sort descending (newest first) - string sort works for YYYY-MM-DD
    dates.sort(reverse=True)
    
    return dates


async def collect_links_for_date(page: Page, base_url: str, date_str: str, max_files: int = 80) -> List[str]:
    """Collect .gz links for a specific date page."""
    url = urljoin(base_url, date_str + "/")
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_load_state("networkidle", timeout=8000)
    
    # Find all .gz links
    loc = page.locator("a[href$='.gz' i], a[href*='.gz' i]")
    count = await loc.count()
    
    links = []
    for i in range(min(count, max_files)):
        href = await loc.nth(i).get_attribute("href")
        if not href:
            continue
        abs_url = urljoin(url, href)
        links.append(abs_url)
    
    return links


async def wolt_dateindex_adapter(
    page: Page,
    source: dict,
    retailer_id: str,
    seen_hashes: Set[str],
    seen_names: Set[str],
    run_id: str
) -> RetailerResult:
    """Wolt date-index adapter - navigates to newest date and downloads files."""
    result = RetailerResult(
        retailer_id=retailer_id,
        source_url=source.get("url", ""),
        errors=[],
        adapter="wolt_dateindex"
    )
    
    base_url = source.get("url", "")
    max_files = source.get("max_files", 80)
    
    try:
        # Step 1: Discover available dates
        dates = await discover_dates(page, base_url)
        
        if not dates:
            logger.info("wolt: no_dates slug=%s url=%s", retailer_id, base_url)
            result.reasons.append("no_dates")
            return result
        
        # Step 2: Try newest date(s) until we find files
        newest = None
        links = []
        
        for date_str in dates[:3]:  # Try up to 3 newest dates
            try:
                links = await collect_links_for_date(page, base_url, date_str, max_files)
                if links:
                    newest = date_str
                    if date_str != dates[0]:
                        logger.info("wolt: date.fallback slug=%s selected=%s", retailer_id, date_str)
                    break
            except Exception as e:
                logger.warning("wolt: date.failed slug=%s date=%s err=%s", retailer_id, date_str, str(e))
                continue
        
        if not newest or not links:
            logger.info("wolt: no_files slug=%s dates_tried=%d", retailer_id, min(len(dates), 3))
            result.reasons.append("no_files")
            return result
        
        logger.info("wolt: date.selected slug=%s date=%s", retailer_id, newest)
        result.links_found = len(links)
        logger.info("links.discovered slug=%s adapter=wolt_dateindex count=%d", retailer_id, len(links))
        
        # Step 3: Download and process files
        bucket = get_bucket()
        
        for link in links:
            filename = link.split('/')[-1] or link
            try:
                # Download file
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
                
                # Update counters based on sniffed kind
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
        result.reasons.append("exception")
        logger.error("wolt_dateindex error for %s: %s", retailer_id, e)
    
    return result
