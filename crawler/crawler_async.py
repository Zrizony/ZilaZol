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
            
            # Target the specific Hebrew button text "לצפייה במחירים"
            links = {}
            
            # Method 1: Look for links with the specific Hebrew text
            try:
                log.info("🎯 Looking for Hebrew 'לצפייה במחירים' buttons...")
                
                # Try different selectors for the Hebrew button text
                hebrew_selectors = [
                    "a:has-text('לצפייה במחירים')",
                    "button:has-text('לצפייה במחירים')",
                    "a[href]:has-text('לצפייה במחירים')",
                    "button[onclick]:has-text('לצפייה במחירים')",
                    # Also try with partial text matching
                    "a:has-text('לצפייה')",
                    "button:has-text('לצפייה')",
                    "a:has-text('מחירים')",
                    "button:has-text('מחירים')",
                    # Look for elements containing the text (alternative syntax)
                    "a:contains('לצפייה במחירים')",
                    "button:contains('לצפייה במחירים')",
                    "a:contains('לצפייה')",
                    "button:contains('לצפייה')",
                    # Try role-based selectors
                    "a[role='button']:has-text('לצפייה')",
                    "button[type='button']:has-text('לצפייה')",
                    # Generic link/button with Hebrew text
                    "*[href]:has-text('לצפייה')",
                    "*[onclick]:has-text('לצפייה')"
                ]
                
                buttons_found = []
                for selector in hebrew_selectors:
                    try:
                        elements = await pg.query_selector_all(selector)
                        if elements:
                            buttons_found.extend(elements)
                            log.info(f"✅ Found {len(elements)} elements with selector: {selector}")
                            break  # Use the first successful selector
                    except Exception as e:
                        log.debug(f"Selector '{selector}' failed: {e}")
                        continue
                
                if not buttons_found:
                    log.warning("⚠️ No Hebrew buttons found with text selectors, trying alternative methods...")
                    
                    # Alternative: Look for buttons/links near retailer names in tables
                    try:
                        # Find all table rows that might contain retailer info
                        table_rows = await pg.query_selector_all("tr")
                        log.info(f"Found {len(table_rows)} table rows to examine")
                        
                        for i, row in enumerate(table_rows):
                            try:
                                # Get all cells in the row
                                cells = await row.query_selector_all("td, th")
                                if len(cells) < 2:
                                    continue
                                
                                # First cell usually contains retailer name
                                first_cell = cells[0]
                                retailer_name = (await first_cell.inner_text()).strip()
                                
                                if not retailer_name or len(retailer_name) < 3:
                                    continue
                                
                                # Look for buttons/links in this row
                                row_buttons = await row.query_selector_all("a, button")
                                for button in row_buttons:
                                    button_text = (await button.inner_text()).strip()
                                    button_href = await button.get_attribute("href")
                                    
                                    # Check if this button has the Hebrew text or similar
                                    if ("לצפייה" in button_text or "מחירים" in button_text or 
                                        "צפייה" in button_text or "price" in button_text.lower()):
                                        
                                        if button_href:
                                            links[retailer_name] = urljoin(ROOT, button_href)
                                            log.info(f"✅ Found retailer button: {retailer_name} -> {button_href}")
                                        elif await button.get_attribute("onclick"):
                                            # Handle onclick buttons - extract URL from onclick
                                            onclick = await button.get_attribute("onclick")
                                            if "window.open" in onclick or "location.href" in onclick:
                                                # Extract URL from onclick
                                                import re
                                                url_match = re.search(r"['\"]([^'\"]+)['\"]", onclick)
                                                if url_match:
                                                    extracted_url = url_match.group(1)
                                                    links[retailer_name] = urljoin(ROOT, extracted_url)
                                                    log.info(f"✅ Found retailer onclick: {retailer_name} -> {extracted_url}")
                                    
                            except Exception as e:
                                log.debug(f"Error processing row {i}: {e}")
                                continue
                                
                    except Exception as e:
                        log.warning(f"Alternative method failed: {e}")
                else:
                    # Process the found buttons
                    for i, button in enumerate(buttons_found):
                        try:
                            button_text = (await button.inner_text()).strip()
                            button_href = await button.get_attribute("href")
                            
                            # Try to find retailer name from parent elements
                            retailer_name = None
                            
                            # Look for retailer name in the same row or parent container
                            parent_row = await button.query_selector("xpath=ancestor::tr")
                            if parent_row:
                                first_cell = await parent_row.query_selector("td:first-child, th:first-child")
                                if first_cell:
                                    retailer_name = (await first_cell.inner_text()).strip()
                            
                            # If no name found in row, look in parent div/container
                            if not retailer_name:
                                parent_container = await button.query_selector("xpath=ancestor::div[contains(@class, 'row') or contains(@class, 'card') or contains(@class, 'item')]")
                                if parent_container:
                                    name_element = await parent_container.query_selector("h1, h2, h3, h4, h5, h6, .title, .name, .retailer")
                                    if name_element:
                                        retailer_name = (await name_element.inner_text()).strip()
                            
                            # Fallback: use button text or index
                            if not retailer_name:
                                retailer_name = button_text or f"Retailer_{i+1}"
                            
                            if button_href:
                                links[retailer_name] = urljoin(ROOT, button_href)
                                log.info(f"✅ Found retailer link: {retailer_name} -> {button_href}")
                            else:
                                # Handle onclick buttons
                                onclick = await button.get_attribute("onclick")
                                if onclick:
                                    import re
                                    url_match = re.search(r"['\"]([^'\"]+)['\"]", onclick)
                                    if url_match:
                                        extracted_url = url_match.group(1)
                                        links[retailer_name] = urljoin(ROOT, extracted_url)
                                        log.info(f"✅ Found retailer onclick: {retailer_name} -> {extracted_url}")
                                        
                        except Exception as e:
                            log.warning(f"Error processing button {i}: {e}")
                            continue
                
            except Exception as e:
                log.error(f"❌ Hebrew button detection failed: {e}")
                import traceback
                log.error(f"Stack trace: {traceback.format_exc()}")
            
            # Method 2: Fallback - scan all links on page if no Hebrew buttons found
            if not links:
                try:
                    log.info("🔄 Fallback: Scanning all links on page for retailer websites...")
                    all_links = await pg.query_selector_all("a[href]")
                    log.info(f"Found {len(all_links)} total links on page")
                    
                    for i, link in enumerate(all_links):
                        try:
                            href = await link.get_attribute("href")
                            link_text = (await link.inner_text()).strip()
                            
                            # Look for retailer website links (external URLs)
                            if href and (href.startswith('http') or href.startswith('//')):
                                # Try to find retailer name from nearby elements
                                parent_row = await link.query_selector("xpath=ancestor::tr")
                                if parent_row:
                                    first_cell = await parent_row.query_selector("td:first-child, th:first-child")
                                    if first_cell:
                                        name = (await first_cell.inner_text()).strip()
                                        if name and len(name) > 3 and not name.startswith('http'):
                                            links[name] = href if href.startswith('http') else urljoin(ROOT, href)
                                            log.info(f"✅ Found retailer link (fallback): {name} -> {href}")
                                            
                        except Exception as e:
                            log.debug(f"Error processing link {i}: {e}")
                            continue
                            
                except Exception as e:
                    log.warning(f"Fallback method failed: {e}")
            
            await br.close()
            
            if links:
                log.info(f"✅ Async Playwright method successful: {len(links)} retailers found")
                log.info(f"🎯 Found {len(links)} retailer links: {list(links.keys())}")
                
                # Log first few URLs for debugging
                for i, (name, url) in enumerate(list(links.items())[:5], 1):
                    log.info(f"   {i}. {name[:50]}... -> {url[:80]}...")
                
                return links
            else:
                log.error("❌ Async Playwright method found no retailers with any method")
                # Take a debug screenshot
                try:
                    screenshot_path = f"/tmp/debug_no_retailers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                    await pg.screenshot(path=screenshot_path, full_page=True)
                    log.info(f"📸 Debug screenshot saved: {screenshot_path}")
                except Exception as e:
                    log.warning(f"Could not save debug screenshot: {e}")
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
    
    # Always clear cookies for fresh login
    await ctx.clear_cookies()
    log.info(f"🧹 Cleared cookies for fresh login to {hub}")
    
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
                
                # Fill password if provided (optional)
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
                            log.info(f"✅ Filled password with selector: {selector}")
                            break
                    
                    if not password_filled:
                        log.warning("⚠️ Could not find password field")
                else:
                    log.info(f"ℹ️ No password provided for {shop}, username-only login")
                
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
    """Crawl a single retailer hub and update counts with proper file processing."""
    log.info(f"🔄 Starting crawl for retailer: {shop}")
    creds = creds_for(shop)
    shop_str = str(shop) if shop else "retailer"
    shop_slug = slug(shop_str)
    
    # Create local directories for this retailer
    local_download_dir = Path(f"/tmp/{shop_slug}")
    local_json_dir = Path(f"/tmp/json_out/{shop_slug}")
    local_download_dir.mkdir(parents=True, exist_ok=True)
    local_json_dir.mkdir(parents=True, exist_ok=True)
    
    api_context = None
    files_downloaded = 0
    items_parsed = 0
    
    try:
        # Use async Playwright-based authentication to get file URLs
        log.info(f"🔍 Scanning {shop} for download links...")
        files, api_context = await zip_links_async(hub, creds, shop_str)
        
        if not files:
            log.warning(f"⚠️ No files found for {shop}")
            return

        log.info(f"📁 Found {len(files)} files to download for {shop}")
        
        # Download and process each file
        for i, url in enumerate(files, 1):
            try:
                log.info(f"📥 Downloading file {i}/{len(files)} for {shop}: {url}")
                
                # Use the authenticated API context to download files
                resp = await api_context.fetch(url, timeout=120_000)
                if resp.status != 200:
                    log.warning(f"❌ Download failed for {url}: HTTP {resp.status}")
                    continue
                
                fname = Path(urlparse(url).path).name
                if not fname:
                    fname = f"file_{i}.zip"  # Fallback name
                
                # Save file locally first
                local_file_path = local_download_dir / fname
                with open(local_file_path, 'wb') as f:
                    f.write(resp.body())
                
                log.info(f"💾 Saved {fname} locally to {local_file_path}")
                
                # Upload raw file to GCS
                file_blob_name = f"downloads/{shop_slug}/{fname}"
                try:
                    if upload_to_gcs(BUCKET_NAME, resp.body(), file_blob_name):
                        files_downloaded += 1
                        log.info(f"☁️ Uploaded raw file {fname} to GCS bucket {BUCKET_NAME}")
                    else:
                        log.error(f"❌ Failed to upload raw file {fname} to GCS")
                except Exception as upload_err:
                    log.error(f"❌ Exception uploading raw file {fname}: {upload_err}")
                
                # Process file content and convert to JSON
                magic2 = resp.body()[:2]
                lower_name = fname.lower()
                
                parsed_items = 0
                
                if magic2 == b'\x1f\x8b':  # gzip magic
                    log.info(f"🗜️ Processing .gz file: {fname}")
                    try:
                        with gzip.GzipFile(fileobj=io.BytesIO(resp.body())) as gz_file:
                            xml_data = gz_file.read()
                            parsed_items = await process_xml_to_json(xml_data, shop_str, local_json_dir, fname)
                            
                    except Exception as gz_err:
                        log.error(f"❌ Error processing .gz file {fname}: {gz_err}")
                        
                elif magic2 == b'PK':  # zip magic
                    log.info(f"📦 Processing .zip file: {fname}")
                    try:
                        with zipfile.ZipFile(io.BytesIO(resp.body()), "r") as zip_ref:
                            for zip_info in zip_ref.infolist():
                                if zip_info.filename.endswith((".xml", ".json")):
                                    with zip_ref.open(zip_info) as file:
                                        xml_data = file.read()
                                        parsed_items += await process_xml_to_json(xml_data, shop_str, local_json_dir, zip_info.filename)
                                        
                    except Exception as zip_err:
                        log.error(f"❌ Error processing .zip file {fname}: {zip_err}")
                        
                else:
                    # Try plain XML fallback
                    if lower_name.endswith('.xml') or resp.body().strip().startswith(b'<'):
                        log.info(f"📄 Processing plain XML file: {fname}")
                        try:
                            parsed_items = await process_xml_to_json(resp.body(), shop_str, local_json_dir, fname)
                        except Exception as xml_err:
                            log.error(f"❌ Error processing XML file {fname}: {xml_err}")
                    else:
                        log.warning(f"⚠️ Unsupported file type for: {fname} (magic={magic2!r})")
                
                items_parsed += parsed_items
                counts["zips"] += 1
                counts["xmls"] += 1
                counts["rows"] += parsed_items
                
                log.info(f"✅ Processed {fname}: {parsed_items} items parsed")
                        
            except Exception as e:
                log.error(f"❌ Error processing {url}: {e}")
                import traceback
                log.error(f"Stack trace: {traceback.format_exc()}")
        
        # Final summary
        log.info(f"✅ Downloaded {files_downloaded} files for {shop}")
        log.info(f"🧩 Parsed {items_parsed} items into JSON")
        log.info(f"☁️ Uploaded to GCS bucket {BUCKET_NAME}")
    
    finally:
        # Always dispose of the API context to free memory
        if api_context:
            try:
                await api_context.dispose()
                log.debug(f"🧹 Disposed API context for {shop}")
            except Exception as e:
                log.warning(f"Failed to dispose API context for {shop}: {e}")

async def process_xml_to_json(xml_data: bytes, shop_str: str, local_json_dir: Path, filename: str) -> int:
    """Process XML data and convert to JSON with defined schema."""
    try:
        # Parse XML using existing parse_xml function
        log.info(f"🔍 Parsing XML data from {filename} ({len(xml_data)} bytes)")
        xml_type, rows = parse_xml(xml_data, shop_str)
        
        if not xml_type or not rows:
            log.warning(f"⚠️ No valid XML data found in {filename} (type: {xml_type}, rows: {len(rows) if rows else 0})")
            return 0
        
        log.info(f"✅ Parsed {len(rows)} rows from {filename}")
        
        # Convert to our defined schema
        json_items = []
        for row in rows:
            # Map the parsed data to our schema
            json_item = {
                "name": row.get("name", ""),
                "barcode": row.get("barcode", ""),
                "date": row.get("date", ""),
                "price": row.get("price", ""),
                "company": shop_str
            }
            json_items.append(json_item)
        
        # Save JSON locally
        json_filename = f"{slug(shop_str)}_{filename}.json"
        local_json_path = local_json_dir / json_filename
        
        with open(local_json_path, 'w', encoding='utf-8') as f:
            json.dump(json_items, f, ensure_ascii=False, indent=2)
        
        log.info(f"💾 Saved {len(json_items)} JSON items to {local_json_path}")
        
        # Upload JSON to GCS
        json_blob_name = f"json_outputs/{slug(shop_str)}/{json_filename}"
        json_data = json.dumps(json_items, ensure_ascii=False)
        
        try:
            if upload_to_gcs(BUCKET_NAME, json_data.encode('utf-8'), json_blob_name):
                log.info(f"☁️ Uploaded JSON to GCS: {json_blob_name}")
            else:
                log.error(f"❌ Failed to upload JSON to GCS: {json_blob_name}")
        except Exception as upload_err:
            log.error(f"❌ Exception uploading JSON to GCS: {upload_err}")
        
        return len(json_items)
        
    except Exception as e:
        log.error(f"❌ Error processing XML to JSON for {filename}: {e}")
        return 0

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

# ───────────── NEW FANOUT WORKFLOW ─────────────────────────────────────────
import asyncio
import contextlib
from typing import Dict, List

IGNORED_RETAILERS = {
    "וולט אופריישנס סרוויסס ישראל בע\"מ",
    "סטופ מרקט בע\"מ",
}

# Map host → credential key from CREDS
DOMAIN_TO_CREDKEY = {
    "prices.quik.co.il": "yohananof",
    "kingstore.binaprojects.com": "doralon",
    "url.publishedprices.co.il": "RamiLevi",   # same login host used by several retailers
}

# Where to store raw files locally inside container (Cloud Run ephemeral disk)
RAW_DIR = Path("/tmp/raw")
SS_DIR = Path("/tmp/screenshots")
RAW_DIR.mkdir(parents=True, exist_ok=True)
SS_DIR.mkdir(parents=True, exist_ok=True)

# ---- Helper to persist to GCS (you already use this pattern elsewhere)
def upload_to_gcs(local_path: Path, gcs_uri: str):
    from google.cloud import storage
    bkt_name, _, blob_path = gcs_uri.replace("gs://", "").partition("/")
    client = storage.Client()
    bucket = client.bucket(bkt_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))
    log.info(f"☁️ Uploaded -> {gcs_uri}")

# ---- Playwright context/bootstrap for Cloud Run
async def new_browser():
    # Cloud Run compatible flags
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
        ],
    )
    context = await browser.new_context(locale="he-IL")
    page = await context.new_page()
    return pw, browser, context, page

# ---- Core processor for each retailer
async def process_retailer(retailer_name: str, url: str) -> dict:
    # Skip ignored retailers
    for bad in IGNORED_RETAILERS:
        if bad in retailer_name:
            log.info(f"⏭️ Skipping {retailer_name}")
            return {"retailer": retailer_name, "skipped": True}

    # open a fresh context per retailer to avoid cookie reuse (as requested)
    pw, browser, ctx, page = await new_browser()
    summary = {"retailer": retailer_name, "files": 0, "screenshots": 0}
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # If this retailer uses the shared login host, perform login:
        from urllib.parse import urlparse
        host = urlparse(page.url).netloc
        credkey = DOMAIN_TO_CREDKEY.get(host)
        creds = CREDS.get(credkey or "", {})

        if "publishedprices.co.il" in host:
            # go to the login form directly if we are on /login
            if not page.url.endswith("/login"):
                await page.goto("https://url.publishedprices.co.il/login", wait_until="domcontentloaded")
            # Username only or username+password
            await page.fill('input[name="username"]', creds.get("username",""))
            with contextlib.suppress(Exception):
                if creds.get("password"):
                    await page.fill('input[name="password"]', creds["password"])
            await page.click("button[type=submit], input[type=submit]")
            await page.wait_for_load_state("networkidle", timeout=45000)
            # Guaranteed landing page for files
            await page.goto("https://url.publishedprices.co.il/file", wait_until="networkidle", timeout=60000)

        # Take screenshot after login/redirect
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        ss_path = SS_DIR / f"{retailer_name.replace(' ','_')}_{ts}.png"
        await page.screenshot(path=str(ss_path), full_page=True)
        summary["screenshots"] += 1

        # Collect download links (anchors and "Download(…)" onclick buttons)
        anchors = await page.locator("a[href$='.zip'], a[href$='.gz'], a[href$='.xml']").all()
        buttons = await page.locator("button[onclick*='Download'], a[onclick*='Download']").all()

        hrefs: List[str] = []
        for el in anchors:
            with contextlib.suppress(Exception):
                h = await el.get_attribute("href")
                if h: hrefs.append(h)
        for el in buttons:
            with contextlib.suppress(Exception):
                oc = await el.get_attribute("onclick")
                # naive extraction of the file path inside Download('...')
                m = re.search(r"Download\(['\"]([^'\"]+)['\"]\)", oc or "")
                if m:
                    hrefs.append(m.group(1))

        # Normalize to absolute URLs
        from urllib.parse import urljoin
        hrefs = [urljoin(page.url, h) for h in hrefs]
        hrefs = list(dict.fromkeys(hrefs))  # dedup, keep order

        log.info(f"📥 {retailer_name}: {len(hrefs)} file links found")

        # Reuse cookies from Playwright to download with requests
        # (safer/faster for bigger files)
        cookies = await ctx.cookies()
        jar = requests.cookies.RequestsCookieJar()
        for c in cookies:
            jar.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path","/"))

        session = requests.Session()
        session.cookies = jar
        session.headers.update({"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"})

        # Download each file and immediately push to GCS and parse to JSONL
        bucket = os.environ.get("PRICES_BUCKET", "zilazol-prices")
        today = datetime.utcnow().strftime("%Y-%m-%d")
        for h in hrefs:
            with contextlib.suppress(Exception):
                r = session.get(h, timeout=90, allow_redirects=True)
                r.raise_for_status()
                # local save
                fname = h.split("/")[-1].split("?")[0]
                local = RAW_DIR / fname
                local.write_bytes(r.content)
                summary["files"] += 1

                # upload raw
                gcs_raw = f"gs://{bucket}/raw/{retailer_name}/{today}/{fname}"
                upload_to_gcs(local, gcs_raw)

                # parse → JSONL and upload (best-effort)
                jsonl = RAW_DIR / (fname + ".jsonl")
                rows = _to_jsonl(local, jsonl)
                if rows:
                    gcs_norm = f"gs://{bucket}/json_out/{retailer_name}/{today}/{jsonl.name}"
                    upload_to_gcs(jsonl, gcs_norm)

        return summary
    finally:
        await ctx.close()
        await browser.close()
        await pw.stop()

def _to_jsonl(compressed: Path, out_path: Path) -> int:
    """
    Very tolerant parser for .gz/.zip/.xml (Price* / Promo*).
    Returns number of rows written.
    """
    def iter_xml_bytes(b: bytes):
        # .xml, .gz(xml), .zip(xml) — return raw xml bytes
        if compressed.suffix.lower() == ".gz":
            b = gzip.decompress(b)
        elif compressed.suffix.lower() == ".zip":
            with zipfile.ZipFile(io.BytesIO(b)) as zf:
                # first xml in archive
                for n in zf.namelist():
                    if n.lower().endswith(".xml"):
                        with zf.open(n) as f:
                            b = f.read()
                            break
        return b

    try:
        data = compressed.read_bytes()
        xml_bytes = iter_xml_bytes(data)
        if not xml_bytes:
            return 0

        # extremely permissive parsing
        from xml.etree import ElementTree as ET
        root = ET.fromstring(xml_bytes)

        # Simplified schema visitor
        rows = []
        for item in root.iter():
            tag = item.tag.lower()
            if tag.endswith("item") or tag.endswith("product"):
                obj = {c.tag: (c.text or "").strip() for c in item}
                if obj:
                    rows.append(obj)

        if not rows:
            return 0

        with out_path.open("w", encoding="utf-8") as w:
            for r in rows:
                w.write(json.dumps(r, ensure_ascii=False) + "\n")
        log.info(f"→ {out_path.name} ({len(rows)} rows)")
        return len(rows)
    except Exception as e:
        log.warning(f"Parse failed for {compressed.name}: {e}")
        return 0

# ---- Orchestrator with bounded concurrency
async def run_fanout_async() -> dict:
    """
    1) Discover retailers on gov page
    2) Process each retailer (login → screenshot → download → JSONL → GCS)
    """
    links = await retailer_links_async()
    if not links:
        return {"ok": False, "error": "no retailers found"}

    sem = asyncio.Semaphore(int(os.environ.get("CRAWL_CONCURRENCY", "3")))
    results = []

    async def _job(name, href):
        async with sem:
            with contextlib.suppress(Exception):
                res = await process_retailer(name, href)
                results.append(res)

    await asyncio.gather(*[_job(n, h) for n, h in links.items()])

    total_files = sum(r.get("files",0) for r in results)
    total_ss = sum(r.get("screenshots",0) for r in results)
    log.info(f"✅ DONE: {len(results)} retailers, {total_files} files, {total_ss} screenshots")
    return {"ok": True, "retailers": len(results), "files": total_files, "screenshots": total_ss}
