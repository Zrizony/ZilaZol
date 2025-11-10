# crawler/adapters/publishedprices.py
from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from playwright.async_api import Page

from .. import logger
from ..constants import DEFAULT_DOWNLOAD_SUFFIXES
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..gcs import get_bucket, upload_to_gcs
from ..parsers import parse_from_blob
from ..utils import looks_like_price_file


async def publishedprices_login(page: Page, username: str, password: str) -> bool:
    """Login to publishedprices with robust selector handling and explicit waits. Returns True if successful."""
    logger.info("login.start retailer=publishedprices username=%s", username)
    
    try:
        await page.goto("https://url.publishedprices.co.il/login", wait_until="domcontentloaded", timeout=90000)
        
        # Try username selectors
        username_selectors = ["input[name='username']", "#username", "input[name='Email']", "input[type='email']"]
        for sel in username_selectors:
            if await page.locator(sel).count():
                await page.fill(sel, username)
                break
        
        # Try password selectors
        password_selectors = ["input[name='password']", "#password", "input[type='password']"]
        for sel in password_selectors:
            if password and await page.locator(sel).count():
                await page.fill(sel, password)
                break
        
        # Try submit selectors
        submit_selectors = ["button[type='submit']", "input[type='submit']", "button:has-text('כניסה')", "button:has-text('Login')"]
        for sel in submit_selectors:
            if await page.locator(sel).count():
                await page.click(sel)
                break
        
        # Wait for successful login with explicit UI signal
        try:
            # Wait for URL change or file manager elements
            await page.wait_for_url("**/file**", timeout=25000)
            logger.info("login.success retailer=publishedprices logged_in=true")
            return True
        except:
            # Fallback: navigate to file page and wait for file manager
            await page.goto("https://url.publishedprices.co.il/file", wait_until="domcontentloaded", timeout=25000)
            # Wait for file manager to load
            await page.wait_for_selector("table, div#filemanager, div.dataTables_wrapper", timeout=15000)
            logger.info("login.success retailer=publishedprices logged_in=true method=fallback")
            return True
    except Exception as e:
        logger.error("login.failed retailer=publishedprices username=%s error=%s", username, str(e))
        return False


async def publishedprices_navigate_to_folder(page: Page, folder: str) -> bool:
    """Navigate to specific folder with robust waits and retries. Returns True if successful."""
    logger.info("folder.navigate retailer=publishedprices folder=%s", folder)
    
    try:
        # Wait for file/folder tree to render
        await page.wait_for_selector("table, div#filemanager, div.dataTables_wrapper", timeout=15000)
        await page.wait_for_load_state("networkidle", timeout=10000)
        
        # First try direct navigation
        target_url = f"https://url.publishedprices.co.il/file/cdup/{folder.strip('/')}/"
        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1000)
            
            # Check if we have files listed
            links = await publishedprices_collect_links(page)
            if links:
                logger.info("folder.navigate retailer=publishedprices folder=%s ok=true method=direct", folder)
                return True
        except Exception as e:
            logger.warning("folder.navigate.direct_failed retailer=publishedprices folder=%s error=%s", folder, str(e))
        
        # Fallback: go to /file and click the folder
        await page.goto("https://url.publishedprices.co.il/file", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector("table, div#filemanager, div.dataTables_wrapper", timeout=15000)
        await page.wait_for_load_state("networkidle", timeout=10000)
        
        # Try clicking folder by name with retries
        for attempt in range(2):
            try:
                # Try multiple selectors for folder clicking
                folder_selectors = [
                    f"a:has-text('{folder}')",
                    f"tr:has(td:has-text('{folder}')) a[href]",
                    f"td:has-text('{folder}') a",
                    f"*:has-text('{folder}'):not(script):not(style)"
                ]
                
                for sel in folder_selectors:
                    if await page.locator(sel).count():
                        await page.click(sel)
                        await page.wait_for_timeout(1500)
                        await page.wait_for_load_state("networkidle", timeout=10000)
                        
                        # Verify we're in the folder by checking for files
                        links = await publishedprices_collect_links(page)
                        if links:
                            logger.info("folder.navigate retailer=publishedprices folder=%s ok=true method=click attempt=%d", folder, attempt + 1)
                            return True
                        break
            except Exception as e:
                logger.warning("folder.navigate.click_failed retailer=publishedprices folder=%s attempt=%d error=%s", folder, attempt + 1, str(e))
                if attempt == 0:
                    await page.wait_for_timeout(2000)  # Wait before retry
        
        logger.error("folder.navigate retailer=publishedprices folder=%s ok=false", folder)
        return False
    except Exception as e:
        logger.error("folder.navigate retailer=publishedprices folder=%s ok=false error=%s", folder, str(e))
        return False


async def publishedprices_collect_links(page: Page, patterns: Optional[List[str]] = None) -> List[str]:
    """Collect download links from publishedprices file manager"""
    # Wait for page to load
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(500)
    
    # Get all hrefs
    hrefs = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => a.getAttribute('href'))"
    )
    
    # Normalize to absolute URLs and filter
    links = []
    suffixes = tuple((p.lower() for p in (patterns or DEFAULT_DOWNLOAD_SUFFIXES)))
    for h in (hrefs or []):
        if not h:
            continue
        try:
            h_abs = await page.evaluate("u => new URL(u, location.href).href", h)
            low = h_abs.lower()
            if looks_like_price_file(low) or low.endswith(suffixes) or "download" in low:
                links.append(h_abs)
        except Exception:
            pass
    
    return sorted(set(links))


async def crawl_publishedprices(page: Page, retailer: dict, creds: dict, run_id: str) -> RetailerResult:
    """
    Main entrypoint for publishedprices adapter
    retailer: dict from retailers.json (has name, url, adapter='publishedprices', cred_key, optional 'folder')
    creds: {'username': ..., 'password': ...}
    """
    retailer_id = retailer.get("id", "unknown")
    retailer_name = retailer.get("name", "Unknown")
    
    result = RetailerResult(
        retailer_id=retailer_id,
        source_url=retailer.get("url", ""),
        errors=[],
        adapter="publishedprices"
    )
    
    logger.info("publishedprices: retailer=%s", retailer_name)
    
    try:
        # Step 1: Login
        login_ok = await publishedprices_login(page, creds["username"], creds.get("password", ""))
        if not login_ok:
            result.errors.append("login_failed")
            result.reasons.append("login_failed")
            return result
        
        # Step 2: Handle folder navigation (Super Yuda special case)
        folder = retailer.get("folder")
        if folder:
            folder_ok = await publishedprices_navigate_to_folder(page, folder)
            result.subpath = folder
            if not folder_ok:
                result.reasons.append("folder_navigation_failed")
                result.errors.append(f"folder_not_found:{folder}")
        
        # Step 3: Collect files
        patterns = retailer.get("download_patterns")
        links = await publishedprices_collect_links(page, patterns)
        result.links_found = len(links)
        logger.info("links.discovered slug=%s adapter=publishedprices count=%d", retailer_id, len(links))
        
        if result.links_found == 0:
            result.reasons.append("no_dom_links")
        
        # Step 4: Download and process files
        seen_hashes: Set[str] = set()
        seen_names: Set[str] = set()
        manifest_entries: List[dict] = []
        bucket = get_bucket()
        
        for link in links:
            filename = link.split('/')[-1] or link  # Fallback for error logging
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
                
                # Upload to GCS with new path structure
                if bucket:
                    blob_path = f"raw/{retailer_id}/{run_id}/{md5_hash}_{filename}"
                    await upload_to_gcs(bucket, blob_path, data, md5_hex=md5_hash, metadata={"source_filename": filename})
                    
                    # Add to manifest
                    manifest_entries.append({
                        "filename": filename,
                        "gcs_path": blob_path,
                        "md5_hex": md5_hash,
                        "bytes": len(data),
                        "ts": datetime.now(timezone.utc).isoformat()
                    })
                    
                    logger.info("upload.ok retailer=%s file=%s gcs_path=%s", retailer_id, filename, blob_path)
                
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
                logger.error("upload.failed retailer=%s link=%s file=%s err=%s", retailer_id, link, filename, str(e))
                continue
        
        # Write manifest.json
        if manifest_entries and bucket:
            try:
                manifest_data = json.dumps({
                    "run_id": run_id,
                    "retailer_id": retailer_id,
                    "retailer_name": retailer.get("name", "Unknown"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "files": manifest_entries
                }, indent=2)
                
                manifest_path = f"raw/{retailer_id}/{run_id}/manifest.json"
                await upload_to_gcs(bucket, manifest_path, manifest_data.encode('utf-8'), "application/json")
                logger.info("manifest.written retailer=%s run_id=%s files=%d", retailer_id, run_id, len(manifest_entries))
            except Exception as e:
                logger.error("manifest.failed retailer=%s err=%s", retailer_id, str(e))
                
    except Exception as e:
        result.errors.append(f"fatal:{e}")
        logger.error(f"publishedprices error for {retailer_name}: {e}")
    
    return result

