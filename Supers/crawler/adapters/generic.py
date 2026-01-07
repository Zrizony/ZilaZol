# crawler/adapters/generic.py
from __future__ import annotations
import os
import re
from datetime import datetime, timezone
from typing import List, Set, Optional

from playwright.async_api import Page

from .. import logger
from ..constants import SCREENSHOTS_DIR, DEFAULT_DOWNLOAD_SUFFIXES
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..parsers import parse_from_blob
from ..utils import ensure_dirs, looks_like_price_file
from ..memory_utils import log_memory


def extract_date_from_link(href: str, link_text: str = "") -> Optional[str]:
    """
    Extract date from URL or link text.
    Returns date string in YYYY-MM-DD format if found, None otherwise.
    """
    # Common date patterns in URLs/filenames
    date_patterns = [
        r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD (ISO format)
        r'(\d{2}-\d{2}-\d{4})',  # DD-MM-YYYY
        r'(\d{4}\d{2}\d{2})',    # YYYYMMDD
        r'(\d{2}/\d{2}/\d{4})',  # DD/MM/YYYY
        r'(\d{4}/\d{2}/\d{2})',  # YYYY/MM/DD
        r'(\d{2}\.\d{2}\.\d{4})', # DD.MM.YYYY
    ]
    
    # Search in URL first
    for pattern in date_patterns:
        match = re.search(pattern, href)
        if match:
            date_str = match.group(1)
            # Normalize to YYYY-MM-DD format
            try:
                if '-' in date_str:
                    parts = date_str.split('-')
                    if len(parts[0]) == 4:  # YYYY-MM-DD
                        return date_str
                    elif len(parts[2]) == 4:  # DD-MM-YYYY
                        return f"{parts[2]}-{parts[1]}-{parts[0]}"
                elif '/' in date_str:
                    parts = date_str.split('/')
                    if len(parts[0]) == 4:  # YYYY/MM/DD
                        return '-'.join(parts)
                    elif len(parts[2]) == 4:  # DD/MM/YYYY
                        return f"{parts[2]}-{parts[1]}-{parts[0]}"
                elif '.' in date_str:
                    parts = date_str.split('.')
                    if len(parts[2]) == 4:  # DD.MM.YYYY
                        return f"{parts[2]}-{parts[1]}-{parts[0]}"
                elif len(date_str) == 8:  # YYYYMMDD
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except Exception:
                continue
    
    # Search in link text if URL didn't have a date
    if link_text:
        for pattern in date_patterns:
            match = re.search(pattern, link_text)
            if match:
                date_str = match.group(1)
                try:
                    if '-' in date_str:
                        parts = date_str.split('-')
                        if len(parts[0]) == 4:
                            return date_str
                        elif len(parts[2]) == 4:
                            return f"{parts[2]}-{parts[1]}-{parts[0]}"
                    elif '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts[0]) == 4:
                            return '-'.join(parts)
                        elif len(parts[2]) == 4:
                            return f"{parts[2]}-{parts[1]}-{parts[0]}"
                except Exception:
                    continue
    
    return None


def is_today(date_str: Optional[str]) -> bool:
    """Check if date string matches today's date"""
    if not date_str:
        return False
    
    try:
        # Parse date string (YYYY-MM-DD format)
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        today = datetime.now().date()
        return parsed_date == today
    except Exception:
        return False


async def collect_links_on_page(page: Page, patterns: Optional[List[str]] = None, filter_today: bool = True) -> List[str]:
    """
    Collect download links from main frame AND all child frames.
    
    If filter_today=True, only returns links with today's date in URL or link text.
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
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Scan ALL frames (main + child frames) - many sites use iframes
    for frame in page.frames:
        for sel in selectors:
            try:
                count = await frame.locator(sel).count()
                if count == 0:
                    continue
                
                # Extract both href and text for date filtering
                link_data = await frame.eval_on_selector_all(sel, """
                    els => els.map(a => ({
                        href: a.href,
                        text: a.textContent || ''
                    }))
                """)
                
                for link_info in (link_data or []):
                    h = link_info.get('href')
                    link_text = link_info.get('text', '')
                    
                    if not h:
                        continue
                    
                    if not (looks_like_price_file(h) or h.lower().endswith(tuple(pat))):
                        continue
                    
                    # Date filtering
                    if filter_today:
                        date_str = extract_date_from_link(h, link_text)
                        if date_str:
                            if not is_today(date_str):
                                logger.debug(f"generic.skip_not_today url={h} date={date_str} today={today_str}")
                                continue
                        else:
                            # If no date found, skip (conservative approach)
                            logger.debug(f"generic.skip_no_date url={h}")
                            continue
                    
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
        
        # Collect download links with retry logic (filtered to today's date)
        log_memory(logger, f"generic.before_collect_links retailer={retailer_id}")
        patterns = source.get("download_patterns") or source.get("patterns") or None
        filter_today = source.get("filter_today", True)  # Default to True, can be overridden in config
        links = await collect_links_on_page(page, patterns, filter_today=filter_today)
        
        # If no links found, retry with additional wait
        if not links:
            await page.wait_for_load_state("networkidle", timeout=8000)
            await page.wait_for_timeout(800)
            links = await collect_links_on_page(page, patterns, filter_today=filter_today)
        
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
        
        filter_status = "today only" if filter_today else "all dates"
        logger.info("links.discovered slug=%s adapter=generic count=%d ({})", retailer_id, len(links), filter_status)
        
        # Process each link
        log_memory(logger, f"generic.before_downloads retailer={retailer_id} links={len(links)}")
        for link in links:
            filename = link.split('/')[-1] or link  # Fallback for error logging
            try:
                data, resp, filename = await fetch_url(page, link)
                if data is None:
                    continue
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

