# crawler/adapters/publishedprices.py
from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

from playwright.async_api import Page

from .. import logger
from ..constants import DEFAULT_DOWNLOAD_SUFFIXES
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..gcs import get_bucket, upload_to_gcs
from ..parsers import parse_from_blob
from ..utils import looks_like_price_file


def _normalize_dl_link(base_url: str, href: str) -> Optional[str]:
    """Normalize download link, drop anchors, make absolute, filter non-files."""
    if not href:
        return None
    
    # Drop fragment-only or anchors like "#", "file#"
    if href.startswith("#") or href.startswith("file#"):
        return None
    
    # Make absolute
    abs_url = urljoin(base_url, href)
    
    # Only keep actual file candidates
    p = urlparse(abs_url)
    if not p.scheme.startswith("http"):
        return None
    
    # Remove fragment
    abs_url = abs_url.split("#")[0]
    
    # Must contain .zip or .gz somewhere in the URL
    low = abs_url.lower()
    if not (low.endswith(".zip") or low.endswith(".gz") or ".zip" in low or ".gz" in low):
        return None
    
    return abs_url


async def publishedprices_login(page: Page, username: str, password: str, retailer_id: str = "unknown", max_retries: int = 2) -> bool:
    """
    Login to publishedprices with robust selector handling, explicit waits, and retry logic.
    Returns True if successful.
    """
    logger.info("login.start retailer=%s adapter=publishedprices username=%s", retailer_id, username)
    
    for attempt in range(max_retries):
        try:
            # Navigate to login page with longer timeout and network idle wait
            try:
                await page.goto("https://url.publishedprices.co.il/login", wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception as nav_err:
                if attempt < max_retries - 1:
                    logger.warning("login.nav_failed retailer=%s attempt=%d error=%s retrying", retailer_id, attempt + 1, str(nav_err))
                    await page.wait_for_timeout(3000)  # Wait before retry
                    continue
                else:
                    raise
            
            # Small wait for form to render
            await page.wait_for_timeout(1000)
            
            # Try username selectors
            username_filled = False
            username_selectors = ["input[name='username']", "#username", "input[name='Email']", "input[type='email']"]
            for sel in username_selectors:
                try:
                    if await page.locator(sel).count() > 0:
                        await page.fill(sel, username)
                        username_filled = True
                        logger.debug("login.username_filled retailer=%s selector=%s", retailer_id, sel)
                        break
                except Exception:
                    continue
            
            if not username_filled:
                raise Exception("username_field_not_found")
            
            # Try password selectors
            password_filled = False
            password_selectors = ["input[name='password']", "#password", "input[type='password']"]
            for sel in password_selectors:
                try:
                    if await page.locator(sel).count() > 0:
                        await page.fill(sel, password or "")
                        password_filled = True
                        logger.debug("login.password_filled retailer=%s selector=%s", retailer_id, sel)
                        break
                except Exception:
                    continue
            
            if not password_filled:
                logger.warning("login.password_field_not_found retailer=%s continuing_anyway", retailer_id)
            
            # Try submit selectors
            submit_clicked = False
            submit_selectors = [
                "button[type='submit']",
                "input[type='submit']",
                "button:has-text('כניסה')",
                "button:has-text('Login')",
                "form button",  # Fallback: any button in form
            ]
            for sel in submit_selectors:
                try:
                    if await page.locator(sel).count() > 0:
                        await page.click(sel)
                        submit_clicked = True
                        logger.debug("login.submit_clicked retailer=%s selector=%s", retailer_id, sel)
                        break
                except Exception:
                    continue
            
            if not submit_clicked:
                raise Exception("submit_button_not_found")
            
            # Wait for successful login with explicit UI signal
            try:
                # Wait for URL change or file manager elements
                await page.wait_for_url("**/file**", timeout=20000)
                logger.info("login.success retailer=%s adapter=publishedprices logged_in=true", retailer_id)
                return True
            except:
                # Fallback: navigate to file page and wait for file manager
                await page.goto("https://url.publishedprices.co.il/file", wait_until="domcontentloaded", timeout=20000)
                # Wait for file manager to load
                await page.wait_for_selector("table, div#filemanager, div.dataTables_wrapper", timeout=10000)
                logger.info("login.success retailer=%s adapter=publishedprices logged_in=true method=fallback", retailer_id)
                return True
                
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning("login.attempt_failed retailer=%s attempt=%d/%d error=%s retrying", 
                             retailer_id, attempt + 1, max_retries, str(e))
                await page.wait_for_timeout(2000)  # Wait before retry
            else:
                logger.error("login.failed retailer=%s adapter=publishedprices username=%s attempts=%d error=%s", 
                           retailer_id, username, max_retries, str(e))
                return False
    
    return False


async def publishedprices_navigate_to_folder(page: Page, folder: str, retailer_id: str = "unknown") -> bool:
    """Navigate to specific folder with robust waits and retries. Returns True if successful."""
    logger.info("folder.navigate retailer=%s adapter=publishedprices folder=%s", retailer_id, folder)
    
    try:
        # Wait for file/folder tree to render
        await page.wait_for_selector("table, div#filemanager, div.dataTables_wrapper", timeout=15000)
        await page.wait_for_load_state("networkidle", timeout=10000)
        
        # First try direct navigation
        target_url = f"https://url.publishedprices.co.il/file/cdup/{folder.strip('/')}/"
        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1000)
            
            # Check if we have files listed (don't filter by date for folder check)
            links = await publishedprices_collect_links(page, retailer_id=retailer_id, filter_today=False)
            if links:
                logger.info("folder.navigate retailer=%s adapter=publishedprices folder=%s ok=true method=direct", retailer_id, folder)
                return True
        except Exception as e:
            logger.warning("folder.navigate.direct_failed retailer=%s adapter=publishedprices folder=%s error=%s", retailer_id, folder, str(e))
        
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
                        
                        # Verify we're in the folder by checking for files (don't filter by date for folder check)
                        links = await publishedprices_collect_links(page, retailer_id=retailer_id, filter_today=False)
                        if links:
                            logger.info("folder.navigate retailer=%s adapter=publishedprices folder=%s ok=true method=click attempt=%d", retailer_id, folder, attempt + 1)
                            return True
                        break
            except Exception as e:
                logger.warning("folder.navigate.click_failed retailer=%s adapter=publishedprices folder=%s attempt=%d error=%s", retailer_id, folder, attempt + 1, str(e))
                if attempt == 0:
                    await page.wait_for_timeout(2000)  # Wait before retry
        
        logger.error("folder.navigate retailer=%s adapter=publishedprices folder=%s ok=false", retailer_id, folder)
        return False
    except Exception as e:
        logger.error("folder.navigate retailer=%s adapter=publishedprices folder=%s ok=false error=%s", retailer_id, folder, str(e))
        return False


async def publishedprices_collect_links(page: Page, patterns: Optional[List[str]] = None, retailer_id: str = "unknown", filter_today: bool = True) -> List[str]:
    """
    Collect download links from publishedprices file manager with normalization.
    
    If filter_today=True, only returns links from files matching today's date.
    Extracts dates from table rows to filter by date.
    """
    from datetime import datetime
    
    # Wait for page to load
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1000)  # Wait for table to render
    
    # PublishedPrices uses MM/DD/YYYY format (US format) like "11/25/2025 12:03 AM"
    today_str = datetime.now().strftime("%m/%d/%Y")  # MM/DD/YYYY format
    today_iso = datetime.now().strftime("%Y-%m-%d")
    
    # Extract links WITH dates from table rows
    # The file manager shows files in a table with date columns in MM/DD/YYYY HH:MM AM/PM format
    link_data = await page.evaluate("""
        () => {
            const links = [];
            // Find all table rows
            const rows = Array.from(document.querySelectorAll('table tr, tbody tr'));
            
            rows.forEach((row) => {
                // Find download link in this row
                const link = row.querySelector('a[href*=".gz"], a[href*=".zip"], a[href*=".xml"]');
                if (!link) return;
                
                const href = link.getAttribute('href');
                if (!href) return;
                
                // Try to find date in this row
                // PublishedPrices uses MM/DD/YYYY HH:MM AM/PM format (e.g., "11/25/2025 12:03 AM")
                const rowText = row.textContent || '';
                
                // Try MM/DD/YYYY format (US format used by PublishedPrices)
                const dateMatch1 = rowText.match(/(\\d{1,2}\\/\\d{1,2}\\/\\d{4})/);
                // Try YYYY-MM-DD format (ISO format, fallback)
                const dateMatch2 = rowText.match(/(\\d{4}-\\d{2}-\\d{2})/);
                // Try DD/MM/YYYY format (European format, fallback)
                const dateMatch3 = rowText.match(/(\\d{1,2}\\/\\d{1,2}\\/\\d{4})/);
                
                let dateStr = null;
                if (dateMatch1) {
                    // MM/DD/YYYY format (most likely for PublishedPrices)
                    dateStr = dateMatch1[1];
                } else if (dateMatch2) {
                    dateStr = dateMatch2[1];
                } else if (dateMatch3) {
                    dateStr = dateMatch3[1];
                } else {
                    // Try to find date in specific cells (especially Date column)
                    const cells = row.querySelectorAll('td');
                    cells.forEach(cell => {
                        const cellText = cell.textContent || '';
                        // Prioritize MM/DD/YYYY format
                        const cellDateMatch1 = cellText.match(/(\\d{1,2}\\/\\d{1,2}\\/\\d{4})/);
                        const cellDateMatch2 = cellText.match(/(\\d{4}-\\d{2}-\\d{2})/);
                        if (cellDateMatch1 && !dateStr) {
                            dateStr = cellDateMatch1[1];
                        } else if (cellDateMatch2 && !dateStr) {
                            dateStr = cellDateMatch2[1];
                        }
                    });
                }
                
                links.push({
                    href: href,
                    date: dateStr,
                    filename: href.split('/').pop() || href
                });
            });
            
            return links;
        }
    """)
    
    # Normalize to absolute URLs and filter by date
    base_url = page.url
    links = []
    skipped = 0
    skipped_not_today = 0
    
    for link_info in (link_data or []):
        h = link_info.get('href')
        if not h:
            continue
        
        # Normalize link (drops anchors, makes absolute, filters non-files)
        normalized = _normalize_dl_link(base_url, h)
        if not normalized:
            skipped += 1
            continue
        
        # Filter by today's date if requested
        if filter_today:
            date_str = link_info.get('date')
            if date_str:
                # Check if date matches today
                date_matches = False
                
                # Direct string comparison (fastest)
                if date_str == today_str:  # MM/DD/YYYY format (PublishedPrices format)
                    date_matches = True
                elif date_str == today_iso:  # YYYY-MM-DD format
                    date_matches = True
                else:
                    # Try to parse and compare dates
                    try:
                        if '/' in date_str:
                            parts = date_str.split('/')
                            if len(parts) == 3:
                                # Try MM/DD/YYYY format first (PublishedPrices format)
                                try:
                                    parsed_date = datetime(int(parts[2]), int(parts[0]), int(parts[1]))
                                    today = datetime.now()
                                    date_matches = (parsed_date.date() == today.date())
                                except ValueError:
                                    # Fallback: try DD/MM/YYYY format
                                    try:
                                        parsed_date = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                                        today = datetime.now()
                                        date_matches = (parsed_date.date() == today.date())
                                    except ValueError:
                                        pass
                        elif '-' in date_str:
                            # YYYY-MM-DD format
                            parsed_date = datetime.fromisoformat(date_str)
                            today = datetime.now()
                            date_matches = (parsed_date.date() == today.date())
                    except Exception as e:
                        logger.debug("publishedprices.date_parse_error retailer=%s date=%s error=%s", 
                                   retailer_id, date_str, str(e))
                        pass
                
                if not date_matches:
                    skipped_not_today += 1
                    logger.debug("publishedprices.skip_not_today retailer=%s filename=%s date=%s today=%s", 
                               retailer_id, link_info.get('filename', ''), date_str, today_str)
                    continue
            else:
                # If no date found, log but don't skip (might be a file without date info)
                logger.debug("publishedprices.no_date retailer=%s filename=%s", retailer_id, link_info.get('filename', ''))
        
        links.append(normalized)
    
    if skipped > 0:
        logger.debug("publishedprices.skip_href retailer=%s skipped=%d", retailer_id, skipped)
    if skipped_not_today > 0:
        logger.info("publishedprices.skip_not_today retailer=%s skipped=%d filter_today=%s", 
                   retailer_id, skipped_not_today, filter_today)
    
    logger.info("publishedprices.links_collected retailer=%s total=%d filtered_today=%s", 
               retailer_id, len(links), filter_today)
    
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
    
    logger.info("adapter=publishedprices retailer=%s name=%s", retailer_id, retailer_name)
    
    try:
        # Step 1: Login
        login_ok = await publishedprices_login(page, creds["username"], creds.get("password", ""), retailer_id)
        if not login_ok:
            result.errors.append("login_failed")
            result.reasons.append("login_failed")
            return result
        
        # Step 2: Handle folder navigation (Super Yuda special case)
        folder = retailer.get("folder")
        if folder:
            folder_ok = await publishedprices_navigate_to_folder(page, folder, retailer_id)
            result.subpath = folder
            if not folder_ok:
                result.reasons.append("folder_navigation_failed")
                result.errors.append(f"folder_not_found:{folder}")
        
        # Step 3: Collect files (filtered to today's date)
        patterns = retailer.get("download_patterns")
        links = await publishedprices_collect_links(page, patterns, retailer_id, filter_today=True)
        result.links_found = len(links)
        logger.info("links.discovered slug=%s adapter=publishedprices count=%d (today only)", retailer_id, len(links))
        
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
        logger.error("adapter=publishedprices retailer=%s error=%s", retailer_id, str(e))
    
    return result

