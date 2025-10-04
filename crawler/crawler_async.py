#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Async version of the crawler to fix Playwright async issues in Flask
"""

import re, json, zipfile, gzip, logging, io
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
import os

import requests
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

# Async Playwright
from playwright.async_api import async_playwright, TimeoutError

# Google Cloud Storage
from google.cloud import storage
from google.cloud import logging as cloud_logging

# Import the original functions we need
from crawler_cloud import (
    ROOT, UA, BUCKET_NAME, PROJECT_ID, CREDS, ZIP_RX, log,
    get_storage_client, upload_to_gcs, upload_file_to_gcs, delete_blob_from_gcs,
    slug, creds_for, _to_float, parse_xml
)

# ───────────── ASYNC RETAILER LIST ─────────────────────────────────────────
async def retailer_links_async() -> dict[str, str]:
    """Get retailer links with async Playwright"""
    log.info("🔍 Fetching retailer links from government website (async)...")
    
    try:
        # Try the fast JSON API approach first
        log.info("📡 Attempting fast JSON API method...")
        html = requests.get(ROOT, headers={"User-Agent": UA}, timeout=15).text
        m = re.search(r'data-url="([^"]+?price[^"]+?)"', html)
        if m:  # JSON endpoint (fastest)
            api = urljoin(ROOT, m.group(1))
            log.info(f"📡 Found JSON API endpoint: {api}")
            data = requests.get(api, headers={"User-Agent": UA}, timeout=10).json()
            links = {r["RetailerName"].strip(): r["PriceLink"] for r in data}
            log.info(f"✅ JSON API method successful: {len(links)} retailers found")
            return links
        else:
            log.warning("⚠️ JSON API endpoint not found, trying async Playwright fallback...")
    except Exception as e:
        log.warning(f"⚠️ JSON API method failed: {e}, trying async Playwright fallback...")

    # Fallback: render gov.il with improved async Playwright
    try:
        log.info("🎭 Starting async Playwright fallback method...")
        async with async_playwright() as pw:
            br = await pw.chromium.launch(
                args=["--ignore-certificate-errors", "--no-sandbox", "--disable-dev-shm-usage"],
                headless=True
            )
            pg = await br.new_page(user_agent=UA, locale="he-IL")
            
            # Set a reasonable timeout for page load
            await pg.goto(ROOT, timeout=45_000)
            
            # Wait for page to fully load
            try:
                await pg.wait_for_load_state("networkidle", timeout=30_000)
                log.info("✅ Page loaded and network idle")
            except TimeoutError:
                log.warning("⚠️ Network idle timeout, continuing anyway...")
            
            # Try multiple selectors for the retailer links table
            retailer_table_selectors = [
                # Look for any table with links
                "table tr:has(a[href])",
                "tbody tr:has(a)",
                "table tr",
                "table",
                # Look for divs or sections with retailer links
                "div:has(a[href])",
                "section:has(a[href])",
                # Look for any clickable links
                "a[href]",
                # Generic fallbacks
                "tr",
                "div",
                "section"
            ]
            
            table_found = False
            for selector in retailer_table_selectors:
                try:
                    await pg.wait_for_selector(selector, timeout=10_000)
                    log.info(f"✅ Found retailer content with selector: {selector}")
                    table_found = True
                    break
                except TimeoutError:
                    log.debug(f"Selector '{selector}' not found, trying next...")
                    continue
            
            if not table_found:
                log.error("❌ Could not find any table elements with any selector")
                # Take a screenshot for debugging
                try:
                    screenshot_path = f"/tmp/debug_no_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                    await pg.screenshot(path=screenshot_path, full_page=True)
                    log.info(f"📸 Debug screenshot saved: {screenshot_path}")
                except Exception as e:
                    log.warning(f"Could not save debug screenshot: {e}")
                
                await br.close()
                return {}
            
            # Extract links with multiple methods
            links = {}
            
            # Method 1: Look for rows with retailer links (any links in table rows)
            try:
                retailer_rows = await pg.query_selector_all("tr:has(a[href])")
                log.info(f"Found {len(retailer_rows)} rows with retailer links")
                
                for tr in retailer_rows:
                    # Get retailer name from first cell
                    first_cell = await tr.query_selector("td:first-child, th:first-child")
                    name = (await first_cell.inner_text()).strip() if first_cell else ""
                    
                    # Get the link
                    a_tag = await tr.query_selector("a[href]")
                    href = await a_tag.get_attribute("href") if a_tag else None
                    
                    if name and href and len(name) > 3:  # Valid retailer name
                        links[name] = urljoin(ROOT, href)
                        log.debug(f"Found retailer link: {name} -> {href}")
            except Exception as e:
                log.warning(f"Method 1 failed: {e}")
            
            # Method 2: Look for any links in table rows (broader search)
            if not links:
                try:
                    all_rows = await pg.query_selector_all("table tr")
                    log.info(f"Trying method 2 with {len(all_rows)} total rows")
                    
                    for tr in all_rows:
                        # Get the first cell (retailer name)
                        first_cell = await tr.query_selector("td:first-child, th:first-child")
                        if not first_cell:
                            continue
                            
                        name = (await first_cell.inner_text()).strip()
                        if not name or len(name) < 3:
                            continue
                        
                        # Look for any link in this row
                        link_element = await tr.query_selector("a[href]")
                        if link_element:
                            href = await link_element.get_attribute("href")
                            link_text = (await link_element.inner_text()).strip()
                            
                            # Accept any link that looks like a retailer website
                            if href and (not link_text or len(link_text) > 0):
                                links[name] = urljoin(ROOT, href)
                                log.debug(f"Found retailer link (method 2): {name} -> {href}")
                except Exception as e:
                    log.warning(f"Method 2 failed: {e}")
            
            # Method 3: Look for any links on the page (last resort)
            if not links:
                try:
                    log.info("Trying method 3: scanning all links on page")
                    all_links = await pg.query_selector_all("a[href]")
                    
                    for link in all_links:
                        href = await link.get_attribute("href")
                        link_text = (await link.inner_text()).strip()
                        
                        # Look for retailer website links (any external links)
                        if href and (href.startswith('http') or href.startswith('//')):
                            # Try to find the retailer name from nearby elements
                            parent_row = await link.query_selector("xpath=ancestor::tr")
                            if parent_row:
                                first_cell = await parent_row.query_selector("td:first-child, th:first-child")
                                if first_cell:
                                    name = (await first_cell.inner_text()).strip()
                                    if name and len(name) > 3:
                                        links[name] = href if href.startswith('http') else urljoin(ROOT, href)
                                        log.debug(f"Found retailer link (method 3): {name} -> {href}")
                except Exception as e:
                    log.warning(f"Method 3 failed: {e}")
            
            await br.close()
            
            if links:
                log.info(f"✅ Async Playwright method successful: {len(links)} retailers found")
                # Log first few for debugging
                for i, (name, url) in enumerate(list(links.items())[:3], 1):
                    log.info(f"   {i}. {name[:50]}... -> {url[:50]}...")
                return links
            else:
                log.error("❌ Async Playwright method found no retailers with any method")
                return {}
                
    except Exception as e:
        log.error(f"❌ Async Playwright fallback method failed: {e}")
        return {}

# ─────────────── ASYNC PLAYWRIGHT LOGIN + PAGE ─────────────────────────────
async def playwright_with_login_async(hub: str, creds: dict | None, shop: str = "retailer"):
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        args=["--ignore-certificate-errors", "--no-sandbox", "--disable-dev-shm-usage"],
        headless=True
    )
    ctx = await browser.new_context(
        ignore_https_errors=True, user_agent=UA, locale="he-IL", accept_downloads=True
    )
    page = await ctx.new_page()
    
    # First, go to the hub URL and wait for it to load completely
    log.info(f"Loading hub: {hub}")
    await page.goto(hub, timeout=60_000)
    
    # Wait for page to be fully loaded
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=30_000)
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except TimeoutError:
        log.warning(f"Page load timeout for {hub}, continuing anyway")
    
    # Enhanced login detection
    login_needed = False
    if creds:
        log.info(f"Checking for login requirements for {shop} with creds: {creds}")
        
        # Check multiple indicators for login requirement
        login_indicators = [
            "form input[name='username']",
            "form input[name='user']", 
            "form input[type='text']",
            "input[name='username']",
            "input[name='signin']",
            "input[name='sign in']",
            "input[name='user']",
            "input[type='text']",
            "a[href*='login']",
            "a[href*='Login']",
            "a[href*='signin']",
            "a[href*='sign in']",
            "a:has-text('התחברות')",
            "a:has-text('כניסה')",
            "a:has-text('Login')",
            "a:has-text('login')",
            "a:has-text('signin')",
            "a:has-text('sign in')",
            "button:has-text('התחברות')",
            "button:has-text('כניסה')",
            "button:has-text('Login')",
            "button:has-text('login')",
            "button:has-text('signin')",
            "button:has-text('sign in')",
            "h1:has-text('התחברות')",
            "h1:has-text('כניסה')",
            "h1:has-text('Login')",
            "h1:has-text('login')",
            "h1:has-text('signin')",
            "h1:has-text('sign in')",
        ]
        
        for indicator in login_indicators:
            element = await page.query_selector(indicator)
            if element:
                login_needed = True
                log.info(f"Login indicator found: {indicator} for {hub}")
                break
        
        # Also check if we're already on a login page by URL
        if any(x in page.url.lower() for x in ["login", "auth", "signin"]):
            login_needed = True
            log.info(f"Already on login page: {page.url}")
        
        # Log current page title and URL for debugging
        log.info(f"Current page title: {await page.title()}")
        log.info(f"Current page URL: {page.url}")
        
        if not login_needed:
            log.info(f"No login indicators found for {shop}")
        else:
            log.info(f"Login needed for {shop}")
    
    # Perform login if needed
    if login_needed and creds:
        log.info(f"Attempting login for {hub}")
        try:
            # Wait for login form to appear (more flexible selectors)
            login_form_selectors = [
                "input[name='username']",
                "input[name='user']",
                "input[type='text']",
                "input[placeholder*='user']",
                "input[placeholder*='name']",
                "input[id*='user']",
                "input[id*='name']",
                "input[name='signin']",
                "input[name='sign in']",
            ]
            
            form_found = False
            for selector in login_form_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=5_000)
                    form_found = True
                    log.info(f"Found login form with selector: {selector}")
                    break
                except TimeoutError:
                    continue
            
            if not form_found:
                log.warning(f"No login form found on {page.url}")
            else:
                # Fill username with multiple possible selectors
                username_filled = False
                username_selectors = [
                    "input[name='username']",
                    "input[name='user']",
                    "input[type='text']",
                    "input[placeholder*='user']",
                    "input[placeholder*='name']",
                    "input[id*='user']",
                    "input[id*='name']",
                    "input[name='signin']",
                    "input[name='sign in']",
                ]
                
                for selector in username_selectors:
                    username_input = await page.query_selector(selector)
                    if username_input:
                        await username_input.fill(creds["username"])
                        username_filled = True
                        log.info(f"Filled username with selector: {selector}")
                        break
                
                if not username_filled:
                    log.error("Could not find username field")
                
                # Fill password if provided
                if creds.get("password"):
                    password_filled = False
                    password_selectors = [
                        "input[name='password']",
                        "input[type='password']",
                        "input[placeholder*='pass']",
                        "input[id*='pass']",
                    ]
                    
                    for selector in password_selectors:
                        password_input = await page.query_selector(selector)
                        if password_input:
                            await password_input.fill(creds["password"])
                            password_filled = True
                            log.info(f"Filled password with selector: {selector}")
                            break
                    
                    if not password_filled:
                        log.error("Could not find password field")
                
                # Submit form with multiple methods
                submit_success = False
                submit_selectors = [
                    "input[type='submit']",
                    "button[type='submit']",
                    "button:has-text('התחבר')",
                    "button:has-text('כניסה')",
                    "button:has-text('Login')",
                    "button:has-text('login')",
                    "input[value*='התחבר']",
                    "input[value*='כניסה']",
                    "input[value*='Login']",
                    "input[value*='login']",
                    "input[value*='signin']",
                    "input[value*='sign in']",
                ]
                
                for selector in submit_selectors:
                    submit_btn = await page.query_selector(selector)
                    if submit_btn:
                        await submit_btn.click()
                        submit_success = True
                        log.info(f"Clicked submit with selector: {selector}")
                        break
                
                if not submit_success:
                    log.info("No submit button found, trying Enter key")
                    await page.keyboard.press("Enter")
                
                # Wait for login to complete
                try:
                    await page.wait_for_load_state("networkidle", timeout=20_000)
                    await page.wait_for_timeout(3_000)
                    log.info(f"Login completed, current URL: {page.url}")
                except TimeoutError:
                    log.warning(f"Login timeout for {hub}")
                
        except Exception as e:
            log.error(f"Login failed for {hub}: {e}")
    
    # Wait for page to be fully loaded after login
    try:
        await page.wait_for_load_state("networkidle", timeout=15_000)
    except TimeoutError:
        pass
    
    # Take screenshot and upload to cloud storage
    shop_str = str(shop) if shop else "retailer"
    screenshot_path = f"screenshots/login_{slug(shop_str)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    
    # Take screenshot to temporary file
    temp_screenshot = f"/tmp/{slug(shop_str)}_login.png"
    try:
        await page.screenshot(path=temp_screenshot, full_page=True)
        log.info(f"Saved screenshot to: {temp_screenshot}")
        
        # Check if file exists before uploading
        if os.path.exists(temp_screenshot):
            # Upload screenshot to cloud storage
            upload_success = upload_file_to_gcs(BUCKET_NAME, temp_screenshot, screenshot_path)
            if upload_success:
                log.info(f"Uploaded login screenshot: {screenshot_path}")
            else:
                log.error(f"Failed to upload screenshot: {screenshot_path}")
        else:
            log.error(f"Screenshot file not found: {temp_screenshot}")
    except Exception as e:
        log.error(f"Failed to take screenshot: {e}")
    
    return pw, browser, page

# ─────────────── ASYNC ZIP/GZ DISCOVERY ────────────────────────────────────
async def zip_links_async(hub: str, creds: dict | None, shop: str = "retailer") -> tuple[list[str], object]:
    """Get download URLs and return authenticated API context for downloads."""
    captured = set()
    pw, browser, page = await playwright_with_login_async(hub, creds, shop)
    
    # Set up download capture
    page.on("download", lambda d: captured.add(d.url))
    
    # Wait for page to be fully loaded before looking for downloads
    try:
        await page.wait_for_load_state("networkidle", timeout=15_000)
        await page.wait_for_timeout(3_000)
    except TimeoutError:
        log.warning(f"Page load timeout before download search for {shop}")
    
    # Enhanced download link detection with multiple selectors
    download_selectors = [
        # Direct file links
        "a[href$='.zip']",
        "a[href$='.gz']", 
        "a[href$='.xml']",
        # Download-related links
        "a[href*='download']",
        "a[href*='Download']",
        # JavaScript download triggers
        "[onclick*='download']",
        "[onclick*='Download']",
        "button[onclick*='download']",
        "button[onclick*='Download']",
        "input[onclick*='download']",
        "input[onclick*='Download']",
        # Hebrew download buttons
        "a:has-text('הורדה')",
        "button:has-text('הורדה')",
        # English download buttons
        "a:has-text('download')",
        "a:has-text('Download')",
        "button:has-text('download')",
        "button:has-text('Download')",
        # Price-related buttons
        "button:has-text('Price')",
        "button:has-text('Prices')",
        "button:has-text('Promo')",
        "button:has-text('Promotion')",
        "button:has-text('Promotions')",
        "button:has-text('Product')",
        "button:has-text('Products')",
    ]
    
    # Collect all download elements
    download_elements = []
    for selector in download_selectors:
        elements = await page.query_selector_all(selector)
        download_elements.extend(elements)
    
    log.info(f"Found {len(download_elements)} potential download elements for {shop}")
    
    # Try clicking download elements to trigger downloads
    for el in download_elements:
        try:
            await el.click(force=True, timeout=2_000)
            log.info(f"Clicked download element: {await el.tag_name()}")
        except Exception as e:
            log.debug(f"Failed to click element: {e}")
    
    # Wait for downloads to complete
    await page.wait_for_timeout(3_000)
    
    # Also collect direct href links
    for a in await page.query_selector_all("a[href]"):
        href = await a.get_attribute("href")
        if href and ZIP_RX.search(href):
            captured.add(urljoin(hub, href))
    
    # Create API request context with browser cookies for authentication
    api_context = await pw.request.new_context(
        extra_http_headers={"User-Agent": UA},
        ignore_https_errors=True
    )
    
    # Copy cookies from browser context to API context for authentication
    cookies = await page.context.cookies()
    # Convert cookies to the new format for async API
    cookie_list = []
    for cookie in cookies:
        cookie_list.append({
            "name": cookie.get("name", ""),
            "value": cookie.get("value", ""),
            "domain": cookie.get("domain", urlparse(hub).netloc),
            "path": cookie.get("path", "/")
        })
    await api_context.add_cookies(cookie_list)
    
    # Return unique URLs and the authenticated API context for downloads
    unique_urls = list(dict.fromkeys(u for u in captured if ZIP_RX.search(u)))
    log.info(f"Found {len(unique_urls)} download URLs for {shop}")
    return unique_urls, api_context

# ───────────── ASYNC MAIN CRAWLER ─────────────────────────────────────────
async def crawl_single_shop_async(shop: str, hub: str, counts: dict):
    """Crawl a single retailer hub and update counts."""
    log.info(f"[{shop}] hub scan")
    creds = creds_for(shop)
    shop_str = str(shop) if shop else "retailer"
    api_context = None
    
    try:
        # Use async Playwright-based authentication
        files, api_context = await zip_links_async(hub, creds, shop_str)
        if not files:
            log.warning("No files for %s", shop)
            return

        log.info(f"📁 Found {len(files)} files to download for {shop}")
        
        for i, url in enumerate(files, 1):
            try:
                log.info(f"📥 Downloading file {i}/{len(files)} for {shop}: {url}")
                # Use the authenticated API context to download files
                resp = await api_context.fetch(url, timeout=120_000)
                if resp.status != 200:
                    log.warning(f"Download failed for {url}: {resp.status}")
                    continue
                fname = Path(urlparse(url).path).name
                
                # Upload compressed file to cloud storage
                file_blob_name = f"downloads/{slug(shop_str)}/{fname}"
                if upload_to_gcs(BUCKET_NAME, resp.body(), file_blob_name):
                    counts["zips"] += 1
                    log.info(f"✅ Uploaded {fname} to GCS")

                # Process file content based on magic bytes (robust against wrong extensions)
                magic2 = resp.body()[:2]
                lower_name = fname.lower()

                if magic2 == b'\x1f\x8b':  # gzip magic
                    # Handle .gz files (single file compression)
                    log.info(f"Processing .gz file: {fname}")
                    try:
                        with gzip.GzipFile(fileobj=io.BytesIO(resp.body())) as gz_file:
                            xml_data = gz_file.read()
                            xml_type, rows = parse_xml(xml_data, shop_str)
                            if xml_type:
                                counts["xmls"] += 1
                                counts["rows"] += len(rows)
                                
                                # Upload JSON data to cloud storage
                                base_fname = fname[:-3] if fname.endswith('.gz') else fname
                                json_blob_name = f"json_outputs/{slug(shop_str)}/{slug(shop_str)}_{base_fname}.jsonl"
                                json_data = json.dumps(rows, ensure_ascii=False)
                                upload_to_gcs(BUCKET_NAME, json_data.encode('utf-8'), json_blob_name)
                            else:
                                log.warning("Unknown XML type for %s", fname)
                    except Exception as gz_err:
                        log.error(f"Error processing .gz file {fname}: {gz_err}")
                        
                elif magic2 == b'PK':  # zip magic
                    # Handle .zip files (archive with multiple files)
                    log.info(f"Processing .zip file: {fname}")
                    try:
                        with zipfile.ZipFile(io.BytesIO(resp.body()), "r") as zip_ref:
                            for zip_info in zip_ref.infolist():
                                if zip_info.filename.endswith((".xml", ".json")):
                                    with zip_ref.open(zip_info) as file:
                                        xml_data = file.read()
                                        xml_type, rows = parse_xml(xml_data, shop_str)
                                        if xml_type:
                                            counts["xmls"] += 1
                                            counts["rows"] += len(rows)
                                            
                                            # Upload JSON data to cloud storage
                                            json_blob_name = f"json_outputs/{slug(shop_str)}/{slug(shop_str)}_{zip_info.filename}.jsonl"
                                            json_data = json.dumps(rows, ensure_ascii=False)
                                            upload_to_gcs(BUCKET_NAME, json_data.encode('utf-8'), json_blob_name)
                                        else:
                                            log.warning("Unknown XML type for %s", zip_info.filename)
                    except Exception as zip_err:
                        log.error(f"Error processing .zip file {fname}: {zip_err}")
                        
                else:
                    # Try plain XML fallback
                    if lower_name.endswith('.xml') or resp.body().strip().startswith(b'<'):
                        log.info(f"Processing plain XML file: {fname}")
                        try:
                            xml_type, rows = parse_xml(resp.body(), shop_str)
                            if xml_type:
                                counts["xmls"] += 1
                                counts["rows"] += len(rows)
                                json_blob_name = f"json_outputs/{slug(shop_str)}/{slug(shop_str)}_{fname}.jsonl"
                                json_data = json.dumps(rows, ensure_ascii=False)
                                upload_to_gcs(BUCKET_NAME, json_data.encode('utf-8'), json_blob_name)
                            else:
                                log.warning("Unknown XML type for %s", fname)
                        except Exception as xml_err:
                            log.error(f"Error processing XML file {fname}: {xml_err}")
                    else:
                        log.warning(f"Unsupported file type or unknown magic for: {fname} (magic={magic2!r})")
                        
            except Exception as e:
                log.error("Error processing %s: %s", url, e)
                import traceback
                log.error(f"Stack trace: {traceback.format_exc()}")
    
    finally:
        # Always dispose of the API context to free memory
        if api_context:
            try:
                await api_context.dispose()
                log.debug(f"✅ Disposed API context for {shop}")
            except Exception as e:
                log.warning(f"Failed to dispose API context for {shop}: {e}")

# ───────────── ASYNC MAIN FUNCTION ─────────────────────────────────────────
async def main_async(target_shop: str | None = None):
    """Async version of the main crawler function with memory optimization"""
    import gc
    
    # Check Playwright browser installation
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            # This will fail if browsers aren't installed
            browser = await pw.chromium.launch(
                args=[
                    "--ignore-certificate-errors", 
                    "--no-sandbox", 
                    "--disable-dev-shm-usage",
                    "--memory-pressure-off",  # Disable memory pressure detection
                    "--max_old_space_size=4096",  # Limit memory usage
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding"
                ],
                headless=True
            )
            await browser.close()
        log.info("✅ Playwright browsers are properly installed")
    except Exception as e:
        log.error(f"❌ Playwright browser issue: {e}")
        log.error("Please ensure Playwright browsers are installed: playwright install chromium")
        return
    
    # Test cloud setup first
    try:
        from crawler_cloud import test_cloud_setup
        if not test_cloud_setup():
            log.error("Cloud setup failed, exiting")
            return
    except Exception as e:
        log.error(f"Cloud setup test failed: {e}")
        return
    
    counts = {"zips": 0, "xmls": 0, "rows": 0}
    visited = set()

    links = await retailer_links_async()

    # If a specific shop is requested, try to match by exact name or slug
    if target_shop:
        # Accept slug or name (case-insensitive)
        norm_target = slug(target_shop).lower()
        for shop, hub in links.items():
            if slug(shop).lower() == norm_target or shop.lower() == target_shop.lower():
                await crawl_single_shop_async(shop, hub, counts)
                break
        else:
            log.warning(f"Requested shop not found: {target_shop}")
    else:
        for shop, hub in links.items():
            if hub in visited:
                continue
            visited.add(hub)
            try:
                log.info(f"🔄 Starting crawl for shop: {shop}")
                await crawl_single_shop_async(shop, hub, counts)
                log.info(f"✅ Successfully completed crawl for shop: {shop}")
                
                # Force garbage collection after each shop to free memory
                gc.collect()
                log.debug(f"🧹 Garbage collection completed for {shop}")
                
            except Exception as e:
                log.error(f"❌ Failed to crawl shop {shop}: {e}")
                import traceback
                log.error(f"Stack trace for {shop}: {traceback.format_exc()}")
                # Continue with next shop instead of stopping
                continue

    log.info("Downloaded %d files, parsed %d XMLs, extracted %d rows",
             counts["zips"], counts["xmls"], counts["rows"])
