# crawler/adapters/bina.py
from __future__ import annotations
import asyncio
import contextlib
import re
from typing import List, Set
from urllib.parse import urljoin

from playwright.async_api import Page, Frame

from .. import logger
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..gcs import get_bucket, upload_to_gcs
from ..parsers import parse_from_blob
from .generic import collect_links_on_page


TAB_CANDIDATES = ["מחיר מלא", "Price Full", "PriceFull", "Promo", "Promotions", "Stores", "חנויות"]


async def bina_get_content_frame(page: Page, retailer_id: str = "unknown") -> Frame:
    """
    Get the content frame (usually an iframe with Main.aspx or Default.aspx).
    Bina Projects sites typically use iframes for the main content.
    """
    # Wait for iframes to load
    await page.wait_for_timeout(500)
    
    # Try to find the main content iframe
    frames = page.frames
    
    # Priority 1: Frames with Main.aspx or Default.aspx
    for f in frames:
        with contextlib.suppress(Exception):
            url = f.url or ""
            if "Main.aspx" in url or "Default.aspx" in url:
                logger.debug("bina.frame.found retailer=%s url=%s", retailer_id, url)
                return f
    
    # Priority 2: Named iframes (often called 'main' or 'content')
    for f in frames:
        with contextlib.suppress(Exception):
            name = f.name or ""
            if name.lower() in ("main", "content", "mainframe", "contentframe"):
                logger.debug("bina.frame.found retailer=%s name=%s", retailer_id, name)
                return f
    
    # Priority 3: First non-main frame (if multiple frames exist)
    if len(frames) > 1:
        for f in frames:
            if f != page.main_frame:
                logger.debug("bina.frame.fallback retailer=%s url=%s", retailer_id, f.url)
                return f
    
    # Fallback: main frame
    logger.debug("bina.frame.using_main retailer=%s", retailer_id)
    return page.main_frame


async def bina_open_tab(frame_or_page, tab_hint: str = "PriceFull") -> bool:
    """Try to click a tab whose text matches one of the candidates or the hint."""
    candidates = [tab_hint] + TAB_CANDIDATES
    for text in candidates:
        loc = frame_or_page.locator(f"text={text}")
        if await loc.count() > 0:
            with contextlib.suppress(Exception):
                await loc.first.click()
                await asyncio.sleep(0.8)
                return True
    return False


async def bina_collect_gz_links(page: Page) -> List[str]:
    """
    Collect all .gz (and .zip) links from the main frame and all child frames
    on a Bina Projects page. Returns absolute URLs.
    
    This is the primary DOM-based link discovery strategy for Bina sites,
    especially those that render content inside iframes.
    """
    selector = "a[href$='.gz'], a[href*='.gz'], a[href$='.zip'], a[href*='.zip']"
    hrefs: Set[str] = set()
    
    # 1) Main frame - try to find .gz/.zip links
    try:
        await page.wait_for_selector(selector, timeout=20_000)
        vals = await page.eval_on_selector_all(selector, "els => els.map(a => a.href)")
        for h in vals or []:
            if h:
                hrefs.add(h)
    except Exception:
        # No links in main frame, will try child frames
        pass
    
    # 2) Child frames - Bina often renders content inside iframes
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        try:
            # Check if frame has any matching links
            if not await frame.locator(selector).count():
                continue
            
            await frame.wait_for_selector(selector, timeout=10_000)
            vals = await frame.eval_on_selector_all(selector, "els => els.map(a => a.href)")
            for h in vals or []:
                if h:
                    hrefs.add(h)
        except Exception:
            # Frame doesn't have links or timed out, move on
            continue
    
    if not hrefs:
        return []
    
    # Normalize to absolute URLs and dedupe while preserving order
    base = page.url
    seen: Set[str] = set()
    out: List[str] = []
    
    for raw in hrefs:
        try:
            href = urljoin(base, raw)
        except Exception:
            href = raw
        
        if href not in seen:
            seen.add(href)
            out.append(href)
    
    return out


async def bina_collect_links(page: Page, retailer_id: str = "unknown") -> List[str]:
    """
    Collect links from Bina Projects sites with comprehensive iframe/tab/postback handling.
    Tries multiple strategies:
    1. Click tabs (PriceFull, Promo, etc.)
    2. Click search/refresh buttons
    3. Extract links from DOM
    4. Capture from network responses as fallback
    """
    frame = await bina_get_content_frame(page, retailer_id)
    
    # Wait for frame content to load
    await page.wait_for_timeout(1000)
    
    # Strategy 1: Try to click tabs to reveal file links
    tab_clicked = False
    for candidate in ["מחיר מלא", "Price Full", "PriceFull", "מחירון", "Prices"]:
        try:
            if await frame.get_by_text(candidate, exact=False).count() > 0:
                await frame.get_by_text(candidate, exact=False).first.click(timeout=2000)
                logger.debug("bina.tab_clicked retailer=%s tab=%s", retailer_id, candidate)
                tab_clicked = True
                await page.wait_for_timeout(800)
                break
        except Exception:
            continue
    
    # Strategy 2: Try to click search/refresh/export buttons
    button_clicked = False
    for btn in ["חפש", "Search", "רענן", "Refresh", "עדכן", "Update"]:
        try:
            btn_loc = frame.get_by_role("button", name=re.compile(btn, re.I))
            if await btn_loc.count() > 0:
                await btn_loc.first.click(timeout=1500)
                logger.debug("bina.button_clicked retailer=%s button=%s", retailer_id, btn)
                button_clicked = True
                await page.wait_for_timeout(1000)
                break
        except Exception:
            continue
    
    # Strategy 3: Explicit .gz/.zip DOM scraping across main frame + all child frames
    hrefs = await bina_collect_gz_links(page)
    if hrefs:
        logger.debug("bina.dom_links retailer=%s count=%d", retailer_id, len(hrefs))
        return hrefs
    
    # Strategy 4: Fallback to network capture
    # Some Bina sites trigger downloads via postbacks that don't show as <a> links
    logger.debug("bina.network_capture retailer=%s starting", retailer_id)
    captured: Set[str] = set()
    
    def _on_response(resp):
        try:
            url = (getattr(resp, "url", "") or "").lower()
            if any(p in url for p in (".zip", ".gz", "pricefull", "promo", "stores")):
                captured.add(resp.url)
        except Exception:
            pass
    
    page.on("response", _on_response)
    
    # Try clicking refresh/update buttons multiple times to trigger downloads
    for attempt in range(3):
        try:
            # Try various refresh button selectors
            refresh_selectors = [
                "text=רענן",
                "text=Refresh",
                "input[value*='רענן']",
                "input[value*='Refresh']",
                "button:has-text('רענן')",
                "button:has-text('Refresh')"
            ]
            
            for sel in refresh_selectors:
                try:
                    if await frame.locator(sel).count() > 0:
                        await frame.locator(sel).first.click(timeout=1000)
                        break
                except Exception:
                    continue
                    
        except Exception:
            pass
        await page.wait_for_timeout(1000)
    
    page.remove_listener("response", _on_response)
    
    if captured:
        logger.debug("bina.network_captured retailer=%s count=%d", retailer_id, len(captured))
        return list(captured)
    
    # No links found via any strategy - log diagnostic info
    logger.debug(
        "bina.no_links retailer=%s url=%s frames=%d tab_clicked=%s button_clicked=%s", 
        retailer_id,
        page.url,
        len(page.frames),
        tab_clicked, 
        button_clicked
    )
    
    # Optional: take screenshot for debugging (don't crash if it fails)
    with contextlib.suppress(Exception):
        await page.screenshot(
            path=f"screenshots/{retailer_id}_bina_no_links.png",
            full_page=True,
        )
    
    return []


async def bina_fallback_click_downloads(
    page: Page, 
    frame: Frame, 
    retailer_id: str, 
    seen_hashes: Set[str], 
    seen_names: Set[str], 
    run_id: str,
    result: RetailerResult,
    max_files: int = 60,
    throttle_ms: int = 200
) -> int:
    """Fallback: click export/download buttons and capture downloads via expect_download()."""
    # Look for export/download buttons
    selectors = [
        "input[id*='btnDownload']",
        "input[type=submit][value*='הורד']",
        "input[type=submit][value*='Export']",
        "a:has-text('הורדה')",
        "img[alt*='הורד']",
        "img[alt*='Export']",
        "button:has-text('Export')",
        "input[type='image'][alt*='הורד']",
        "input[type='image'][alt*='Export']",
    ]
    
    btn = None
    for sel in selectors:
        if await frame.locator(sel).count() > 0:
            btn = frame.locator(sel)
            logger.info("discovery retailer=%s adapter=bina path=click found_controls selector=%s", retailer_id, sel)
            break
    
    if not btn:
        logger.info("discovery retailer=%s adapter=bina path=click reason=no_export_controls", retailer_id)
        return 0

    total = 0
    bucket = get_bucket()
    n = min(await btn.count(), max_files)
    
    for i in range(n):
        try:
            with page.expect_download(timeout=20000) as dl_info:
                await btn.nth(i).click()
            dl = await dl_info.value
            name = dl.suggested_filename or f"bina_{i}.bin"
            blob = await dl.content()  # bytes
            kind = sniff_kind(blob)
            md5_hash = md5_hex(blob)
            
            # Check for duplicates
            if md5_hash in seen_hashes:
                continue
            
            # Normalize filename for name-based dedupe
            normalized_name = f"{retailer_id}/{name.lower()}"
            if normalized_name in seen_names:
                continue
            
            # Add to seen sets
            seen_hashes.add(md5_hash)
            seen_names.add(normalized_name)
            
            logger.info("file.downloaded retailer=%s file=%s kind=%s bytes=%d", retailer_id, name, kind, len(blob))
            
            # Upload to GCS
            if bucket:
                blob_path = f"raw/{retailer_id}/{run_id}/{md5_hash}_{name}"
                await upload_to_gcs(bucket, blob_path, blob, metadata={"md5_hex": md5_hash, "source_filename": name})
            
            # Unified parse (logs file.downloaded, extracts, parses, logs file.processed)
            await parse_from_blob(blob, name, retailer_id, run_id)
            
            # Update counters based on sniffed kind
            if kind == "zip":
                result.zips += 1
            elif kind == "gz":
                result.gz += 1
            
            total += 1
            
            # Throttle between clicks
            if throttle_ms and i < n - 1:
                await asyncio.sleep(throttle_ms / 1000)
            
        except Exception as e:
            logger.warning("click_download.failed retailer=%s idx=%d err=%s", retailer_id, i, str(e))
    
    logger.info("discovery retailer=%s adapter=bina path=click downloads=%d", retailer_id, total)
    return total


async def bina_adapter(page: Page, source: dict, retailer_id: str, seen_hashes: Set[str], seen_names: Set[str], run_id: str) -> RetailerResult:
    """Bina projects adapter (no login)"""
    result = RetailerResult(
        retailer_id=retailer_id,
        source_url=source.get("url", ""),
        errors=[],
        adapter="bina"
    )
    
    try:
        # Navigate to page with proper wait conditions
        await page.goto(source.get("url", ""), wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=15000)
        # Additional wait for dynamic content
        await page.wait_for_timeout(2000)
        
        # Collect download links via dedicated bina strategy with fallback
        links = await collect_links_on_page(page, source.get("download_patterns") or source.get("patterns"))
        if not links:
            links = await bina_collect_links(page, retailer_id)
        result.links_found = len(links)
        logger.info("links.discovered slug=%s adapter=bina count=%d", retailer_id, len(links))
        
        # Fallback: click-to-download if no links found
        if result.links_found == 0:
            result.reasons.append("no_dom_links")
            logger.info("discovery retailer=%s adapter=bina path=click trigger", retailer_id)
            
            frame = await bina_get_content_frame(page, retailer_id)
            got = 0
            
            # Try tabs in order; stop if we get downloads
            for tab in ["PriceFull", "Promo", "Stores"]:
                await bina_open_tab(frame, tab)
                tab_downloads = await bina_fallback_click_downloads(page, frame, retailer_id, seen_hashes, seen_names, run_id, result, max_files=30, throttle_ms=200)
                got += tab_downloads
                if tab_downloads > 0:
                    break
            
            if got > 0:
                result.reasons.append("used_click_fallback")
            result.files_downloaded += got
            result.links_found = got  # Update to reflect actual downloads
        
        # Process each link (existing flow)
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

