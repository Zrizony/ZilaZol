# crawler/adapters/bina.py
from __future__ import annotations
import asyncio
import contextlib
import re
from typing import List, Set

from playwright.async_api import Page, Frame

from .. import logger
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..gcs import get_bucket, upload_to_gcs
from ..parsers import parse_from_blob
from ..adapters.base import collect_links_on_page


TAB_CANDIDATES = ["מחיר מלא", "Price Full", "PriceFull", "Promo", "Promotions", "Stores", "חנויות"]


async def bina_get_content_frame(page: Page) -> Frame:
    """Get the content frame (usually an iframe with Main.aspx)."""
    # Many Bina pages load the content in the first visible iframe
    for f in page.frames:
        with contextlib.suppress(Exception):
            url = f.url or ""
            if "Main.aspx" in url or "Default.aspx" in url:
                return f
    # fallback: main frame
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


async def bina_collect_links(page: Page) -> List[str]:
    """Collect links from Bina Projects sites with iframe/postback handling (DOM + network)."""
    frame = await bina_get_content_frame(page)
    for candidate in ["מחיר מלא", "Price Full", "PriceFull"]:
        try:
            await frame.get_by_text(candidate, exact=False).click(timeout=2000)
            break
        except Exception:
            pass
    for btn in ["חפש", "Search", "רענן"]:
        try:
            await frame.get_by_role("button", name=re.compile(btn, re.I)).click(timeout=1500)
        except Exception:
            pass
    try:
        await frame.wait_for_selector("a[href*='.zip'], a[href*='.gz'], a:has-text('Price')", timeout=6000)
        hrefs = await frame.eval_on_selector_all(
            "a",
            "els => els.map(a => a.href).filter(u => u && (u.toLowerCase().includes('.zip') || u.toLowerCase().includes('.gz') || u.toLowerCase().includes('pricefull')))"
        )
        hrefs = list(dict.fromkeys(hrefs))
        return hrefs
    except Exception:
        captured: Set[str] = set()
        def _on_response(resp):
            try:
                url = (getattr(resp, "url", "") or "").lower()
                if any(p in url for p in (".zip", ".gz", "pricefull")):
                    captured.add(resp.url)
            except Exception:
                pass
        page.on("response", _on_response)
        for _ in range(3):
            try:
                await frame.locator("text=רענן").click(timeout=1000)
            except Exception:
                pass
            await page.wait_for_timeout(1000)
        return list(captured)


async def bina_fallback_click_downloads(
    page: Page, 
    frame: Frame, 
    retailer_id: str, 
    seen_hashes: Set[str], 
    seen_names: Set[str], 
    run_id: str,
    result: RetailerResult,
    max_files: int = 60
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
            links = await bina_collect_links(page)
        result.links_found = len(links)
        logger.info("links.discovered slug=%s adapter=bina count=%d", retailer_id, len(links))
        
        # Fallback: click-to-download if no links found
        if result.links_found == 0:
            result.reasons.append("no_dom_links")
            frame = await bina_get_content_frame(page)
            await bina_open_tab(frame, tab_hint="PriceFull")
            got = await bina_fallback_click_downloads(page, frame, retailer_id, seen_hashes, seen_names, run_id, result)
            if got == 0:
                # Try other tabs
                for tab in ["Promo", "Stores"]:
                    await bina_open_tab(frame, tab)
                    got += await bina_fallback_click_downloads(page, frame, retailer_id, seen_hashes, seen_names, run_id, result)
                    if got > 0:
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

