# crawler/adapters/wolt_dateindex.py
from __future__ import annotations
import re
import httpx
from typing import List, Set
from urllib.parse import urljoin

from playwright.async_api import Page

from .. import logger
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..parsers import parse_from_blob


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$", re.ASCII)


async def discover_dates_http(base_url: str) -> List[str]:
    """
    Discover available date links via HTTP (no browser needed).
    Wolt's index page is simple HTML with date links.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(base_url)
            resp.raise_for_status()
            html = resp.text
            
            # Extract dates from href or link text (YYYY-MM-DD format)
            import re as regex
            # Find all YYYY-MM-DD patterns in the HTML
            matches = regex.findall(r'(\d{4}-\d{2}-\d{2})', html)
            dates = sorted(set(matches), reverse=True)  # Newest first
            
            return dates
    except Exception as e:
        logger.error("wolt: discover_dates.http_failed url=%s error=%s", base_url, str(e))
        return []


async def collect_links_for_date_http(base_url: str, date_str: str, max_files: int = 80) -> List[str]:
    """
    Collect .gz links for a specific date via HTTP.
    Wolt pages are simple HTML listings.
    """
    try:
        url = urljoin(base_url, date_str + "/")
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            html = resp.text
            
            # Extract all .gz file links
            import re as regex
            # Match href="filename.gz" or href="/path/filename.gz"
            pattern = r'href="([^"]*\.gz)"'
            matches = regex.findall(pattern, html, regex.IGNORECASE)
            
            # Make absolute URLs
            links = []
            for match in matches[:max_files]:
                abs_url = urljoin(url, match)
                links.append(abs_url)
            
            return links
    except Exception as e:
        logger.error("wolt: collect_links.http_failed url=%s date=%s error=%s", base_url, date_str, str(e))
        return []


async def wolt_dateindex_adapter(
    page: Page,
    source: dict,
    retailer_id: str,
    seen_hashes: Set[str],
    seen_names: Set[str],
    run_id: str
) -> RetailerResult:
    """
    Wolt date-index adapter - uses HTTP to fetch JSON/HTML index and downloads files.
    No Playwright/browser needed for index discovery (only for file downloads).
    """
    result = RetailerResult(
        retailer_id=retailer_id,
        source_url=source.get("url", ""),
        errors=[],
        adapter="wolt_dateindex"
    )
    
    base_url = source.get("url", "")
    max_files = source.get("max_files", 80)
    
    try:
        # Step 1: Discover available dates via HTTP (no browser)
        dates = await discover_dates_http(base_url)
        
        if not dates:
            logger.info("wolt: no_dates slug=%s url=%s", retailer_id, base_url)
            result.reasons.append("no_dates")
            return result
        
        logger.info("wolt: dates.found slug=%s count=%d newest=%s", retailer_id, len(dates), dates[0] if dates else "none")
        
        # Step 2: Try newest date(s) until we find files
        newest = None
        links = []
        
        for date_str in dates[:3]:  # Try up to 3 newest dates
            try:
                links = await collect_links_for_date_http(base_url, date_str, max_files)
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
        
        logger.info("wolt: date.selected slug=%s date=%s files=%d", retailer_id, newest, len(links))
        result.links_found = len(links)
        logger.info("links.discovered slug=%s adapter=wolt_dateindex count=%d", retailer_id, len(links))
        
        # Step 3: Download and process files
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
