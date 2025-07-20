#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Israeli retailer crawler (clean, per-retailer creds, skips וולט+סטופ, screenshots after login)
"""

import re, json, zipfile, logging
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime
import requests
from playwright.sync_api import sync_playwright, TimeoutError
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

ROOT = "https://www.gov.il/he/pages/cpfta_prices_regulations"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

DL_ROOT = Path("downloads")
DL_ROOT.mkdir(exist_ok=True)
JSON_ROOT = Path("json_out")
JSON_ROOT.mkdir(exist_ok=True)
SS_ROOT = Path("screenshots")
SS_ROOT.mkdir(exist_ok=True)

CREDS = {
    'אלמשהדאוי קינג סטור בע"מ': {"username": "doralon"},
    'טיב טעם רשתות בע"מ': {"username": "TivTaam"},
    'מ. יוחננוף ובניו (1988) בע"מ': {"username": "yohananof"},
    'מרב-מזון כל בע"מ (אושר עד)': {"username": "osherad"},
    'סאלח דבאח ובניו בע"מ': {"username": "SalachD", "password": "12345"},
    'סטופ מרקט בע"מ': {"username": "Stop_Market"},
    'פוליצר חדרה (1982) בע"מ': {"username": "politzer"},
    'פז קמעונאות ואנרגיה בע"מ': {"username": "Paz_bo", "password": "paz468"},
    'נתיב החסד - סופר חסד בע"מ (כולל ברכל)': {
        "username": "yuda_ho",
        "password": "Yud@147",
    },
    'פרשמרקט בע"מ': {"username": "freshmarket"},
    'קשת טעמים בע"מ': {"username": "Keshet"},
    'רשת חנויות רמי לוי שיווק השקמה 2006 בע"מ\n\n(כולל רשת סופר קופיקס)': {
        "username": "RamiLevi"
    },
    "סופר קופיקס": {"username": "SuperCofixApp"},
}

ZIP_RX = re.compile(r"\.(zip|gz|xml)$", re.I)

SKIP_RETAILERS = {
    'סטופ מרקט בע"מ',
    'וולט אופריישנס סרוויסס ישראל בע"מ',
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

slug = lambda s: re.sub(r"[^\w]+", "_", s.strip())[:60] or "retailer"


def retailer_links():
    html = requests.get(ROOT, headers={"User-Agent": UA}, timeout=30).text
    m = re.search(r'data-url="([^"]+?price[^"]+?)"', html)
    if m:
        data = requests.get(
            urljoin(ROOT, m.group(1)), headers={"User-Agent": UA}
        ).json()
        return {row["RetailerName"].strip(): row["PriceLink"] for row in data}

    with sync_playwright() as pw:
        br = pw.chromium.launch(args=["--ignore-certificate-errors"])
        pg = br.new_page(user_agent=UA, locale="he-IL")
        pg.goto(ROOT, timeout=45_000)
        pg.wait_for_selector("tr >> text=מחיר")
        links = {}
        for tr in pg.query_selector_all("tr:has(a:has-text('מחיר'))"):
            td = tr.query_selector("td")
            name = td.inner_text().strip() if td else ""
            a_tag = tr.query_selector("a:has-text('מחיר')")
            href = a_tag.get_attribute("href") if a_tag else None
            if name and href:
                links[name] = urljoin(ROOT, href)
        br.close()
        return links


def open_hub_authenticated(hub, creds):
    pw = sync_playwright().start()
    br = pw.chromium.launch(args=["--ignore-certificate-errors"])
    ctx = br.new_context(
        ignore_https_errors=True, user_agent=UA, locale="he-IL", accept_downloads=True
    )
    page = ctx.new_page()
    
    # First, go to the hub URL and wait for it to load completely
    log.info(f"Loading hub: {hub}")
    page.goto(hub, timeout=60_000)
    
    # Wait for page to be fully loaded
    try:
        page.wait_for_load_state("domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=30_000)
    except TimeoutError:
        log.warning(f"Page load timeout for {hub}, continuing anyway")
    
    # More aggressive login detection
    login_needed = False
    if creds:
        # Check multiple indicators for login requirement
        login_indicators = [
            # Login forms
            "form input[name='username']",
            "form input[name='user']", 
            "form input[type='text']",
            "input[name='username']",
            "input[name='signin']",
            "input[name='sign in']",
            "input[name='user']",
            "input[name='signin']",
            "input[name='sign in']",
            "input[type='text']",
            # Login links
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
            # Login buttons
            "button:has-text('התחברות')",
            "button:has-text('כניסה')",
            "button:has-text('Login')",
            "button:has-text('login')",
            "button:has-text('signin')",
            "button:has-text('sign in')",
            # Common login page indicators
            "h1:has-text('התחברות')",
            "h1:has-text('כניסה')",
            "h1:has-text('Login')",
            "h1:has-text('login')",
            "h1:has-text('signin')",
            "h1:has-text('sign in')",
        ]
        
        for indicator in login_indicators:
            element = page.query_selector(indicator)
            if element:
                login_needed = True
                log.info(f"Login indicator found: {indicator} for {hub}")
                break
        
        # Also check if we're already on a login page by URL
        if any(x in page.url.lower() for x in ['login', 'auth', 'signin']):
            login_needed = True
            log.info(f"Already on login page: {page.url}")
    
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
                "input[type='text']",
                "input[placeholder*='user']",
                "input[placeholder*='name']",
                "input[id*='user']",
                "input[id*='name']"
            ]
            
            form_found = False
            for selector in login_form_selectors:
                try:
                    page.wait_for_selector(selector, timeout=5_000)
                    form_found = True
                    log.info(f"Found login form with selector: {selector}")
                    break
                except TimeoutError:
                    continue
            
            if not form_found:
                log.warning(f"No login form found on {page.url}")
                # Take a screenshot to debug
                debug_ss = SS_ROOT / f"debug_login_{slug(hub)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                page.screenshot(path=str(debug_ss), full_page=True)
                log.info(f"Debug screenshot saved: {debug_ss}")
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
                    "input[type='text']",
                    "input[placeholder*='user']",
                    "input[placeholder*='name']",
                    "input[id*='user']",
                    "input[id*='name']"
                ]
                
                for selector in username_selectors:
                    username_input = page.query_selector(selector)
                    if username_input:
                        username_input.fill(creds["username"])
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
                        "input[name='signin']",
                        "input[name='sign in']",
                        "input[type='password']",
                        "input[placeholder*='pass']",
                        "input[id*='pass']",
                    ]
                    
                    for selector in password_selectors:
                        password_input = page.query_selector(selector)
                        if password_input:
                            password_input.fill(creds["password"])
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
                    submit_btn = page.query_selector(selector)
                    if submit_btn:
                        submit_btn.click()
                        submit_success = True
                        log.info(f"Clicked submit with selector: {selector}")
                        break
                
                if not submit_success:
                    log.info("No submit button found, trying Enter key")
                    page.keyboard.press("Enter")
                
                # Wait for login to complete
                try:
                    page.wait_for_load_state("networkidle", timeout=20_000)
                    page.wait_for_timeout(3_000)
                    log.info(f"Login completed, current URL: {page.url}")
                except TimeoutError:
                    log.warning(f"Login timeout for {hub}")
                
        except Exception as e:
            log.error(f"Login failed for {hub}: {e}")
            # Take debug screenshot
            debug_ss = SS_ROOT / f"debug_login_fail_{slug(hub)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            page.screenshot(path=str(debug_ss), full_page=True)
            log.info(f"Login failure screenshot saved: {debug_ss}")
    
    # Ensure we're on the main page after login
    current_url = page.url
    if current_url != hub and not any(x in current_url for x in ['login', 'Login', 'auth', 'signin']):
        log.info(f"Redirected to {current_url}, staying here")
    elif current_url != hub:
        log.info(f"Still on login page, going back to hub: {hub}")
        page.goto(hub, timeout=45_000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15_000)
            page.wait_for_load_state("networkidle", timeout=15_000)
        except TimeoutError:
            log.warning(f"Hub page load timeout after login for {hub}")
    
    return pw, br, page


def zip_links(hub, creds, shop_slug):
    captured = set()
    pw, br, page = open_hub_authenticated(hub, creds)
    
    # Wait for page to be fully loaded before taking screenshot
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
        # Additional wait to ensure all dynamic content loads
        page.wait_for_timeout(3_000)
    except TimeoutError:
        log.warning(f"Final page load timeout for {shop_slug}")
    
    # Take screenshot after login/redirect, before any downloads
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    ss_path = SS_ROOT / f"{shop_slug}_{ts}.png"
    try:
        page.screenshot(path=str(ss_path), full_page=True)
        log.info(f"Screenshot saved: {ss_path}")
    except Exception as e:
        log.error(f"Failed to take screenshot for {shop_slug}: {e}")
    
    # Set up download capture
    page.on("download", lambda d: captured.add(d.url))
    
    # Look for download links more comprehensively
    download_selectors = [
        "a[href$='.zip']",
        "a[href$='.gz']", 
        "a[href$='.xml']",
        "a[href*='download']",
        "a[href*='Download']",
        "[onclick*='download']",
        "[onclick*='Download']",
        "button[onclick*='download']",
        "button[onclick*='Download']",
        "input[onclick*='download']",
        "input[onclick*='Download']",
        "a:has-text('הורדה')",
        "a:has-text('download')",
        "a:has-text('Download')",
        "button:has-text('הורדה')",
        "button:has-text('download')",
        "button:has-text('Download')",
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
        elements = page.query_selector_all(selector)
        download_elements.extend(elements)
    
    log.info(f"Found {len(download_elements)} potential download elements for {shop_slug}")
    
    # Click download elements
    for i, el in enumerate(download_elements):
        try:
            log.info(f"Clicking download element {i+1}/{len(download_elements)} for {shop_slug}")
            el.click(force=True, timeout=5_000)
            # Wait a bit between clicks
            page.wait_for_timeout(1_000)
        except Exception as e:
            log.warning(f"Failed to click download element {i+1} for {shop_slug}: {e}")
    
    # Wait for downloads to complete
    page.wait_for_timeout(5_000)
    
    # Also collect direct download URLs from href attributes
    all_links = page.query_selector_all("a[href]")
    for link in all_links:
        href = link.get_attribute("href")
        if href:
            full_url = urljoin(hub, href)
            if ZIP_RX.search(href) or any(x in href.lower() for x in ['download', 'xml', 'zip', 'gz']):
                captured.add(full_url)
    
    # Log what we found
    log.info(f"Captured {len(captured)} download URLs for {shop_slug}")
    for url in captured:
        log.info(f"  - {url}")
    
    br.close()
    pw.stop()
    return [u for u in dict.fromkeys(captured) if ZIP_RX.search(u)]


def parse_xml(blob, shop):
    try:
        root = ET.fromstring(blob)
    except ParseError:
        return None, []
    if root.find(".//Promotion") is not None:
        rows = [
            {
                "name": n.findtext("ItemName", "").strip(),
                "barcode": n.findtext("ItemCode", "").strip(),
                "promo_price": float(n.findtext("PromoPrice", "0").replace(",", ".")),
                "promo_description": n.findtext("PromoDesc", "").strip(),
                "promo_start": n.findtext("PromoStartDate", "").split("T")[0],
                "promo_end": n.findtext("PromoEndDate", "").split("T")[0],
                "company": shop,
            }
            for n in root.findall(".//Promotion")
        ]
        return "promo", rows
    if root.find(".//Promo") is not None:
        rows = [
            {
                "name": n.findtext("Name", "").strip(),
                "barcode": n.findtext("Code", "").strip(),
                "promo_price": float(n.findtext("PromoPrice", "0").replace(",", ".")),
                "promo_description": n.findtext("PromoType", "").strip(),
                "promo_start": n.findtext("StartDate", "").split("T")[0],
                "promo_end": n.findtext("EndDate", "").split("T")[0],
                "company": shop,
            }
            for n in root.findall(".//Promo")
        ]
        return "promo", rows
    if root.find(".//Items") is not None:
        rows = [
            {
                "name": n.findtext("ItemName", "").strip(),
                "barcode": n.findtext("ItemCode", "").strip(),
                "date": n.findtext("PriceUpdateDate", "").split("T")[0],
                "price": float(n.findtext("ItemPrice", "0").replace(",", ".")),
                "company": shop,
            }
            for n in root.findall(".//Item")
        ]
        return "price", rows
    if root.find(".//Products") is not None:
        rows = [
            {
                "name": n.findtext("Name", "").strip(),
                "barcode": n.findtext("Code", "").strip(),
                "date": n.findtext("UpdateDate", "").split("T")[0],
                "price": float(n.findtext("Price", "0").replace(",", ".")),
                "company": shop,
            }
            for n in root.findall(".//Product")
        ]
        return "price", rows
    return None, []


def process_file(file_path, shop):
    """Process a downloaded file (zip, gzip, or xml) and extract data"""
    try:
        # Try as zip file first
        with zipfile.ZipFile(file_path) as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".xml"):
                    continue
                kind, rows = parse_xml(zf.read(name), shop)
                if rows:
                    return kind, rows, name
    except zipfile.BadZipFile:
        pass
    
    try:
        # Try as gzip file
        import gzip
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            content = f.read()
            kind, rows = parse_xml(content, shop)
            if rows:
                return kind, rows, file_path.name
    except Exception:
        pass
    
    try:
        # Try as direct XML file
        content = file_path.read_text(encoding='utf-8')
        kind, rows = parse_xml(content, shop)
        if rows:
            return kind, rows, file_path.name
    except Exception:
        pass
    
    return None, [], None


def cleanup_old_json_files(js_dir, zip_path):
    """Remove old JSON files that were generated from the replaced zip file"""
    zip_stem = zip_path.stem
    for json_file in js_dir.glob(f"*{zip_stem}*.jsonl"):
        try:
            json_file.unlink()
            log.info(f"Removed old JSON file: {json_file}")
        except Exception as e:
            log.warning(f"Failed to remove old JSON file {json_file}: {e}")


def main():
    tot = {"zips": 0, "xmls": 0, "rows": 0}
    seen = set()
    links = retailer_links()
    log.info(f"Found {len(links)} retailers to process")
    
    for shop, hub in links.items():
        if shop in SKIP_RETAILERS:
            log.info(f"Skipping {shop}")
            continue
            
        shop_slug = slug(shop)
        if hub in seen:
            log.info(f"Hub {hub} already processed, skipping {shop}")
            continue
        seen.add(hub)
        
        creds = CREDS.get(shop)
        if not creds:
            log.warning(f"No credentials for {shop} – will try without login")
            creds = None  # Try without credentials
        
        log.info(f"[{shop}] Processing hub: {hub}")
        
        try:
            files = zip_links(hub, creds, shop_slug)
            if not files:
                log.warning(f"No files found for {shop}")
                continue
                
            log.info(f"Found {len(files)} files for {shop}")
            
            dl_dir = DL_ROOT / shop_slug
            dl_dir.mkdir(exist_ok=True)
            js_dir = JSON_ROOT / shop_slug
            js_dir.mkdir(exist_ok=True)
            
            for url in files:
                try:
                    log.info(f"Downloading {url}")
                    
                    # Get filename from URL
                    filename = Path(urlparse(url).path).name
                    zip_path = dl_dir / filename
                    
                    # Check if file already exists and compare
                    if zip_path.exists():
                        log.info(f"File already exists: {zip_path}")
                        # Get file size and modification time for comparison
                        existing_size = zip_path.stat().st_size
                        existing_mtime = zip_path.stat().st_mtime
                        
                        # Download new file to temporary location first
                        temp_path = dl_dir / f"temp_{filename}"
                        data = requests.get(
                            url, headers={"User-Agent": UA}, timeout=120, verify=False
                        ).content
                        temp_path.write_bytes(data)
                        new_size = temp_path.stat().st_size
                        
                        # Compare file sizes
                        if new_size == existing_size:
                            log.info(f"File sizes match, keeping existing: {zip_path}")
                            temp_path.unlink()  # Delete temp file
                            continue
                        else:
                            log.info(f"File sizes differ (old: {existing_size}, new: {new_size}), replacing")
                            # Clean up old JSON files before replacing
                            cleanup_old_json_files(js_dir, zip_path)
                            zip_path.unlink()  # Delete old file
                            temp_path.rename(zip_path)  # Rename temp to final
                    else:
                        # Download new file
                        data = requests.get(
                            url, headers={"User-Agent": UA}, timeout=120, verify=False
                        ).content
                        zip_path.write_bytes(data)
                    
                    tot["zips"] += 1
                    log.info(f"Downloaded {zip_path}")
                    
                    # Try to process as zip file
                    try:
                        kind, rows, name = process_file(zip_path, shop)
                        if rows:
                            # Clean up the filename for output
                            clean_name = re.sub(r'[^\w\-_.]', '_', name or zip_path.stem)
                            out = js_dir / f"{kind}_{clean_name}.jsonl"
                            with out.open("a", encoding="utf-8") as fp:
                                for r in rows:
                                    fp.write(json.dumps(r, ensure_ascii=False) + "\n")
                            tot["xmls"] += 1
                            tot["rows"] += len(rows)
                            log.info(f"→ {out.name} ({len(rows)} rows)")
                    except Exception as e:
                        log.error(f"Failed to process file {zip_path}: {e}")
                        
                except Exception as e:
                    log.error(f"Failed to download/process {url}: {e}")
                    
        except Exception as e:
            log.error(f"Failed to process {shop}: {e}")
            continue
            
    log.info(f"DONE: {tot['zips']} zips, {tot['xmls']} xmls, {tot['rows']} rows")


if __name__ == "__main__":
    main()
