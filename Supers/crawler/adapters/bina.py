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
from ..parsers import parse_from_blob
from ..memory_utils import log_memory
from .generic import collect_links_on_page



TAB_CANDIDATES = ["מחיר מלא", "Price Full", "PriceFull", "Promo", "Promotions", "Stores", "חנויות"]


async def bina_get_content_frame(page: Page, retailer_id: str = "unknown") -> Frame:
    """
    Get the content frame. Most Bina sites don't use iframes - they're direct pages.
    This function now just returns the main frame after ensuring page is loaded.
    """
    # Wait for page to fully load
    await page.wait_for_load_state("networkidle", timeout=10000)
    await page.wait_for_timeout(1000)  # Additional wait for dynamic content
    
    # Check if there are any iframes (some sites might still use them)
    frames = page.frames
    if len(frames) > 1:
        # If multiple frames exist, try to find content frame
        for f in frames:
            with contextlib.suppress(Exception):
                url = f.url or ""
                if "Main.aspx" in url or "Default.aspx" in url:
                    logger.debug("bina.frame.found retailer=%s url=%s", retailer_id, url)
                    return f
        # Return first non-main frame if found
        for f in frames:
            if f != page.main_frame:
                logger.debug("bina.frame.fallback retailer=%s url=%s", retailer_id, f.url)
                return f
    
    # Default: use main frame (most common case)
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


async def bina_collect_download_buttons(page: Page, frame: Frame, filter_today: bool = True) -> List[dict]:
    """
    Collect download buttons that use onclick="Download('filename.gz')" pattern.
    Returns list of dicts with 'filename', 'date', 'button_index', and other info.
    
    This is the PRIMARY strategy for Bina sites - they use JavaScript Download() buttons,
    not direct <a> links.
    
    If filter_today=True, only returns buttons from rows matching today's date.
    """
    from datetime import datetime
    
    buttons_found: List[dict] = []
    
    try:
        # Get today's date in DD/MM/YYYY format (matches the table format)
        today_str = datetime.now().strftime("%d/%m/%Y")
        
        # Strategy: Extract buttons WITH their row dates from the table
        # The table structure has rows with date cells and download buttons
        button_data = await frame.evaluate("""
            () => {
                const buttons = [];
                // Find all table rows (tr elements)
                const rows = Array.from(document.querySelectorAll('table tr, tbody tr'));
                
                rows.forEach((row, rowIndex) => {
                    // Find download button in this row
                    const btn = row.querySelector("button[onclick*='Download'], button[onclick*='download']");
                    if (!btn) return;
                    
                    // Extract filename from onclick
                    const onclick = btn.getAttribute('onclick') || '';
                    const match = onclick.match(/Download\\(['"]([^'"]+)\\)/i);
                    if (!match) return;
                    
                    const filename = match[1];
                    if (!filename.endsWith('.gz') && !filename.endsWith('.zip')) return;
                    
                    // Try to find date in this row - look for DD/MM/YYYY pattern (Bina format)
                    // Dates are in format: DD/MM/YYYY HH:MM (e.g., "25/11/2025 01:24")
                    const rowText = row.textContent || '';
                    const dateMatch = rowText.match(/(\\d{1,2}\\/\\d{1,2}\\/\\d{4})/);
                    const dateStr = dateMatch ? dateMatch[1] : null;  // Extract just DD/MM/YYYY part
                    
                    // Also try to find date in specific cells (תאריך column)
                    let cellDate = null;
                    const cells = row.querySelectorAll('td');
                    cells.forEach(cell => {
                        const cellText = cell.textContent || '';
                        // Match DD/MM/YYYY format (Bina uses this format)
                        const cellDateMatch = cellText.match(/(\\d{1,2}\\/\\d{1,2}\\/\\d{4})/);
                        if (cellDateMatch) {
                            cellDate = cellDateMatch[1];  // Extract just DD/MM/YYYY part (without time)
                        }
                    });
                    
                    buttons.push({
                        filename: filename,
                        onclick: onclick,
                        id: btn.id || null,
                        text: btn.textContent?.trim() || '',
                        date: cellDate || dateStr,
                        rowIndex: rowIndex,
                        buttonIndex: buttons.length  // Index among buttons found
                    });
                });
                
                return buttons;
            }
        """)
        
        for btn_info in button_data or []:
            filename = btn_info.get('filename')
            date_str = btn_info.get('date')
            
            # Filter by today's date if requested
            # date_str should be in DD/MM/YYYY format (extracted from table, time component removed)
            if filter_today:
                if not date_str:
                    # If no date found, skip (we want to be conservative)
                    logger.debug("bina.skip_no_date filename=%s", filename)
                    continue
                
                # Normalize date_str - remove any time component if present
                date_str_clean = date_str.split()[0] if ' ' in date_str else date_str
                
                if date_str_clean != today_str:
                    logger.debug("bina.skip_not_today filename=%s date=%s today=%s", 
                               filename, date_str_clean, today_str)
                    continue
            
            buttons_found.append({
                'filename': filename,
                'onclick': btn_info.get('onclick', ''),
                'id': btn_info.get('id'),
                'text': btn_info.get('text', ''),
                'date': date_str,
                'row_index': btn_info.get('rowIndex'),
                'button_index': btn_info.get('buttonIndex')
            })
        
        logger.info("bina.download_buttons found=%d filtered_today=%s today=%s", 
                   len(buttons_found), filter_today, today_str)
        
    except Exception as e:
        logger.debug("bina.button_extract_error error=%s", str(e))
    
    return buttons_found


async def bina_collect_gz_links(page: Page) -> List[str]:
    """
    Collect all .gz (and .zip) links from ALL frames (main + child frames).
    Returns absolute URLs.
    
    This is a fallback strategy - most Bina sites use Download() buttons, not direct links.
    """
    selector = "a[href$='.gz'], a[href*='.gz'], a[href$='.zip'], a[href*='.zip']"
    hrefs: Set[str] = set()
    
    # Scan ALL frames (main + child frames) without waiting for selector first
    for frame in page.frames:
        try:
            count = await frame.locator(selector).count()
            if count == 0:
                continue
            
            # Extract links from this frame
            vals = await frame.eval_on_selector_all(selector, "els => els.map(a => a.href)")
            for h in vals or []:
                if h:
                    hrefs.add(h)
        except Exception as e:
            logger.debug("bina.frame_scan_error frame=%s error=%s", frame.url or "unknown", str(e))
            continue
    
    if not hrefs:
        return []
    
    # Normalize to absolute URLs and dedupe
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
    Collect download links from Bina Projects sites.
    
    PRIMARY STRATEGY: Find buttons with onclick="Download('filename.gz')" and extract filenames.
    Filters to only today's files by default.
    These buttons trigger JavaScript downloads, so we return the filenames as "pseudo-links"
    that will be handled by the click-to-download fallback.
    
    FALLBACK STRATEGIES:
    1. Look for direct <a> links with .gz/.zip extensions
    2. Network capture (if buttons trigger downloads)
    """
    frame = await bina_get_content_frame(page, retailer_id)
    
    # Wait for page content to load (especially table with download buttons)
    await page.wait_for_load_state("networkidle", timeout=10000)
    await page.wait_for_timeout(2000)  # Wait for table to render
    
    # Strategy 1: Find Download() buttons and extract filenames (filtered to today)
    # This is the PRIMARY strategy based on the actual site structure
    download_buttons = await bina_collect_download_buttons(page, frame, filter_today=True)
    
    if download_buttons:
        # Return filenames as "pseudo-links" - they'll be handled by click fallback
        # Format: "download_button:filename.gz" to distinguish from real URLs
        pseudo_links = [f"download_button:{btn['filename']}" for btn in download_buttons]
        logger.info("bina.download_buttons retailer=%s count=%d (today only)", retailer_id, len(pseudo_links))
        return pseudo_links
    
    # Strategy 2: Try to click tabs/filters to reveal download buttons
    tab_clicked = False
    for candidate in ["מחיר מלא", "Price Full", "PriceFull", "מחירון", "Prices"]:
        try:
            if await frame.get_by_text(candidate, exact=False).count() > 0:
                await frame.get_by_text(candidate, exact=False).first.click(timeout=2000)
                logger.debug("bina.tab_clicked retailer=%s tab=%s", retailer_id, candidate)
                tab_clicked = True
                await page.wait_for_timeout(2000)  # Wait for table to update
                
                # Check again for download buttons after tab click
                download_buttons = await bina_collect_download_buttons(page, frame)
                if download_buttons:
                    pseudo_links = [f"download_button:{btn['filename']}" for btn in download_buttons]
                    logger.info("bina.download_buttons_after_tab retailer=%s count=%d", retailer_id, len(pseudo_links))
                    return pseudo_links
                break
        except Exception:
            continue
    
    # Strategy 3: Look for direct <a> links with .gz/.zip (fallback)
    hrefs = await bina_collect_gz_links(page)
    if hrefs:
        logger.debug("bina.dom_links retailer=%s count=%d", retailer_id, len(hrefs))
        return hrefs
    
    # Strategy 4: Network capture (if buttons trigger downloads via AJAX)
    logger.debug("bina.network_capture retailer=%s starting", retailer_id)
    captured: Set[str] = set()
    
    def _on_response(resp):
        try:
            url = (getattr(resp, "url", "") or "").lower()
            if any(p in url for p in (".zip", ".gz", "pricefull", "promo", "stores", "download")):
                captured.add(resp.url)
        except Exception:
            pass
    
    page.on("response", _on_response)
    
    # Try clicking a download button to trigger network capture
    try:
        download_btn = frame.locator("button[onclick*='Download']").first
        if await download_btn.count() > 0:
            await download_btn.click(timeout=5000)
            await page.wait_for_timeout(2000)
    except Exception:
        pass
    
    page.remove_listener("response", _on_response)
    
    if captured:
        logger.debug("bina.network_captured retailer=%s count=%d", retailer_id, len(captured))
        return list(captured)
    
    # No links found - log diagnostic info
    logger.warning(
        "bina.no_links retailer=%s url=%s frames=%d tab_clicked=%s", 
        retailer_id,
        page.url,
        len(page.frames),
        tab_clicked
    )
    
    # Take screenshot for debugging
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
    throttle_ms: int = 200,
    filter_today: bool = True
) -> int:
    """
    Click download buttons and capture downloads via expect_download().
    
    PRIMARY STRATEGY: Find buttons with onclick="Download('filename')" - these are the actual
    download buttons used by Bina sites.
    
    If filter_today=True, only clicks buttons from rows matching today's date.
    """
    from datetime import datetime
    
    total = 0
    today_str = datetime.now().strftime("%d/%m/%Y")
    
    # Strategy 1: Find Download() buttons filtered by today's date (PRIMARY)
    # First, collect button info with dates
    download_buttons_info = await bina_collect_download_buttons(page, frame, filter_today=filter_today)
    buttons_found_with_filter = len(download_buttons_info) > 0
    
    if download_buttons_info:
        logger.info("discovery retailer=%s adapter=bina path=click found_controls count=%d filter_today=%s", 
                   retailer_id, len(download_buttons_info), filter_today)
        
        # Get all download buttons locator
        download_buttons = frame.locator("button[onclick*='Download'], button[onclick*='download']")
        button_count = await download_buttons.count()
        
        # Create a mapping of button indices to download
        # We need to click buttons in order, but only those matching today
        buttons_to_click = []
        for btn_info in download_buttons_info:
            btn_idx = btn_info.get('button_index')
            if btn_idx is not None and btn_idx < button_count:
                buttons_to_click.append((btn_idx, btn_info))
        
        # Limit to max_files if specified (0 means no limit)
        if max_files > 0:
            buttons_to_click = buttons_to_click[:max_files]
        # If max_files is 0 or negative, use all buttons (no limit)
        
        logger.info("discovery retailer=%s adapter=bina buttons_to_click=%d", retailer_id, len(buttons_to_click))
        
        for btn_idx, btn_info in buttons_to_click:
            try:
                filename_expected = btn_info.get('filename', 'unknown')
                date_str = btn_info.get('date', 'unknown')
                logger.debug("bina.clicking_button retailer=%s idx=%d filename=%s date=%s", 
                           retailer_id, btn_idx, filename_expected, date_str)
                
                # Set up download expectation BEFORE clicking
                async with page.expect_download(timeout=20000) as dl_info:
                    await download_buttons.nth(btn_idx).click(timeout=5000)
                
                dl = await dl_info.value
                name = dl.suggested_filename or filename_expected or f"bina_{btn_idx}.bin"
                blob = await dl.content()  # bytes
                kind = sniff_kind(blob)
                md5_hash = md5_hex(blob)
                
                # Check for duplicates
                if md5_hash in seen_hashes:
                    logger.debug("skip_duplicate retailer=%s file=%s hash=%s", retailer_id, name, md5_hash[:8])
                    continue
                
                # Normalize filename for name-based dedupe
                normalized_name = f"{retailer_id}/{name.lower()}"
                if normalized_name in seen_names:
                    logger.debug("skip_duplicate_name retailer=%s file=%s", retailer_id, name)
                    continue
                
                # Add to seen sets
                seen_hashes.add(md5_hash)
                seen_names.add(normalized_name)
                
                logger.info("file.downloaded retailer=%s file=%s kind=%s bytes=%d", retailer_id, name, kind, len(blob))
                
                # Unified parse (logs file.downloaded, extracts, parses, logs file.processed)
                await parse_from_blob(blob, name, retailer_id, run_id)
                
                # Update counters based on sniffed kind
                if kind == "zip":
                    result.zips += 1
                elif kind == "gz":
                    result.gz += 1
                
                total += 1
                
                # Throttle between clicks
                if throttle_ms and btn_idx < len(buttons_to_click) - 1:
                    await asyncio.sleep(throttle_ms / 1000)
                
            except Exception as e:
                logger.warning("click_download.failed retailer=%s idx=%d filename=%s err=%s", 
                             retailer_id, btn_idx, filename_expected, str(e))
        
        logger.info("discovery retailer=%s adapter=bina path=click downloads=%d", retailer_id, total)
        return total
    
    # Strategy 2: Fallback - if no buttons found with today filter, try without filter
    # But only if we were filtering by today and found nothing
    if filter_today and not buttons_found_with_filter:
        logger.debug("bina.trying_without_date_filter retailer=%s", retailer_id)
        download_buttons_info_no_filter = await bina_collect_download_buttons(page, frame, filter_today=False)
        if download_buttons_info_no_filter:
            logger.info("discovery retailer=%s adapter=bina found_buttons_without_filter count=%d", 
                       retailer_id, len(download_buttons_info_no_filter))
            # Process buttons without date filter
            download_buttons = frame.locator("button[onclick*='Download'], button[onclick*='download']")
            button_count = await download_buttons.count()
            
            buttons_to_click = []
            for btn_info in download_buttons_info_no_filter:
                btn_idx = btn_info.get('button_index')
                if btn_idx is not None and btn_idx < button_count:
                    buttons_to_click.append((btn_idx, btn_info))
            
            if max_files > 0:
                buttons_to_click = buttons_to_click[:max_files]
            
            for btn_idx, btn_info in buttons_to_click:
                try:
                    filename_expected = btn_info.get('filename', 'unknown')
                    async with page.expect_download(timeout=20000) as dl_info:
                        await download_buttons.nth(btn_idx).click(timeout=5000)
                    dl = await dl_info.value
                    name = dl.suggested_filename or filename_expected or f"bina_{btn_idx}.bin"
                    blob = await dl.content()
                    kind = sniff_kind(blob)
                    md5_hash = md5_hex(blob)
                    
                    if md5_hash in seen_hashes:
                        continue
                    
                    normalized_name = f"{retailer_id}/{name.lower()}"
                    if normalized_name in seen_names:
                        continue
                    
                    seen_hashes.add(md5_hash)
                    seen_names.add(normalized_name)
                    
                    logger.info("file.downloaded retailer=%s file=%s kind=%s bytes=%d", retailer_id, name, kind, len(blob))
                    
                    await parse_from_blob(blob, name, retailer_id, run_id)
                    
                    if kind == "zip":
                        result.zips += 1
                    elif kind == "gz":
                        result.gz += 1
                    
                    total += 1
                    
                    if throttle_ms and btn_idx < len(buttons_to_click) - 1:
                        await asyncio.sleep(throttle_ms / 1000)
                    
                except Exception as e:
                    logger.warning("click_download.failed retailer=%s idx=%d filename=%s err=%s", 
                                 retailer_id, btn_idx, filename_expected, str(e))
            
            logger.info("discovery retailer=%s adapter=bina path=click downloads=%d (no_date_filter)", retailer_id, total)
            return total
    
    # Strategy 3: Fallback to other button selectors (legacy support)
    selectors = [
        "input[id*='btnDownload']",
        "input[type=submit][value*='הורד']",
        "input[type=submit][value*='Export']",
        "a:has-text('הורדה')",
        "button:has-text('להורדה')",
        "button:has-text('Export')",
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
    
    n = min(await btn.count(), max_files)
    
    for i in range(n):
        try:
            async with page.expect_download(timeout=20000) as dl_info:
                await btn.nth(i).click(timeout=5000)
            dl = await dl_info.value
            name = dl.suggested_filename or f"bina_{i}.bin"
            blob = await dl.content()
            kind = sniff_kind(blob)
            md5_hash = md5_hex(blob)
            
            if md5_hash in seen_hashes:
                continue
            
            normalized_name = f"{retailer_id}/{name.lower()}"
            if normalized_name in seen_names:
                continue
            
            seen_hashes.add(md5_hash)
            seen_names.add(normalized_name)
            
            logger.info("file.downloaded retailer=%s file=%s kind=%s bytes=%d", retailer_id, name, kind, len(blob))
            
            await parse_from_blob(blob, name, retailer_id, run_id)
            
            if kind == "zip":
                result.zips += 1
            elif kind == "gz":
                result.gz += 1
            
            total += 1
            
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
        
        # Collect download links - use Bina-specific collection FIRST (handles frames properly)
        log_memory(logger, f"bina.before_collect_links retailer={retailer_id}")
        
        # Log frame info for debugging
        logger.info("bina.page_loaded retailer=%s url=%s frames=%d", retailer_id, page.url, len(page.frames))
        for i, frame in enumerate(page.frames):
            logger.debug("bina.frame[%d] url=%s name=%s", i, frame.url or "N/A", frame.name or "N/A")
        
        # Try Bina-specific collection first (handles Download() buttons)
        links = await bina_collect_links(page, retailer_id)
        
        # Fallback to generic collection only if Bina-specific found nothing
        if not links:
            logger.debug("bina.fallback_to_generic retailer=%s", retailer_id)
            links = await collect_links_on_page(page, source.get("download_patterns") or source.get("patterns"))
        
        log_memory(logger, f"bina.after_collect_links retailer={retailer_id} count={len(links)}")
        
        # Check if we found "pseudo-links" (download_button:filename) that need clicking
        pseudo_links = [l for l in links if l.startswith("download_button:")]
        real_links = [l for l in links if not l.startswith("download_button:")]
        
        result.links_found = len(links)
        logger.info("links.discovered slug=%s adapter=bina count=%d (pseudo=%d real=%d)", 
                   retailer_id, len(links), len(pseudo_links), len(real_links))
        
        # Handle pseudo-links (Download() buttons) by clicking them
        if pseudo_links:
            result.reasons.append("found_download_buttons")
            logger.info("discovery retailer=%s adapter=bina path=click trigger count=%d", retailer_id, len(pseudo_links))
            
            frame = await bina_get_content_frame(page, retailer_id)
            got = await bina_fallback_click_downloads(
                page, frame, retailer_id, seen_hashes, seen_names, run_id, result, 
                max_files=0, throttle_ms=200, filter_today=True  # max_files=0 means no limit
            )
            
            if got > 0:
                result.reasons.append("used_click_fallback")
            result.files_downloaded += got
        
        # Fallback: click-to-download if no links found at all
        elif result.links_found == 0:
            result.reasons.append("no_dom_links")
            logger.info("discovery retailer=%s adapter=bina path=click trigger", retailer_id)
            
            frame = await bina_get_content_frame(page, retailer_id)
            got = 0
            
            # Try tabs in order; stop if we get downloads
            for tab in ["PriceFull", "Promo", "Stores"]:
                await bina_open_tab(frame, tab)
                await page.wait_for_timeout(2000)  # Wait for table to update
                tab_downloads = await bina_fallback_click_downloads(
                    page, frame, retailer_id, seen_hashes, seen_names, run_id, result, 
                    max_files=0, throttle_ms=200, filter_today=True  # max_files=0 means no limit
                )
                got += tab_downloads
                if tab_downloads > 0:
                    break
            
            if got > 0:
                result.reasons.append("used_click_fallback")
            result.files_downloaded += got
            result.links_found = got  # Update to reflect actual downloads
        
        # Process each REAL link (skip pseudo-links - they're already handled above)
        log_memory(logger, f"bina.before_downloads retailer={retailer_id} links={len(real_links)}")
        for link in real_links:
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
        
        log_memory(logger, f"bina.after_downloads retailer={retailer_id} downloaded={result.files_downloaded}")
                
    except Exception as e:
        result.errors.append(f"fatal:{e}")
    
    return result

