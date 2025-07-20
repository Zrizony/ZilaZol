#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
crawler_folders.py  –  downloads & JSONL per retailer folder

Dependencies:
    pip install requests beautifulsoup4 playwright
    playwright install chromium
"""

import re, json, zipfile, logging
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime

import requests
from playwright.sync_api import sync_playwright, TimeoutError
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

# Cloud Storage - saves to Google Cloud
from google.cloud import storage
bucket_name = "your-bucket-name"

# ───────────── CONFIG ────────────────────────────────────────────────
ROOT = "https://www.gov.il/he/pages/cpfta_prices_regulations"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# In crawler.py - currently saves locally
DL_ROOT = Path("downloads")  # Local folder
JSON_ROOT = Path("json_out")  # Local folder

# Fill from gov.il table  (domain → creds)
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

ZIP_RX = re.compile(r"\.(zip|gz)$", re.I)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ───────────── HELPERS ───────────────────────────────────────────────
def slug(name: str) -> str:
    return re.sub(r"[^\w]+", "_", name.strip())[:60] or "retailer"


def domain(url: str) -> str:
    host = urlparse(url).hostname
    if not host:
        return ""
    return host.split(".")[0].lower()


def creds_for(shop_name: str):
    return CREDS.get(shop_name)


def _to_float(t: str):
    try:
        return float(t.replace(",", "."))
    except Exception:
        return 0.0


# ───────────── RETAILER LIST ─────────────────────────────────────────
def retailer_links() -> dict[str, str]:
    html = requests.get(ROOT, headers={"User-Agent": UA}, timeout=30).text
    m = re.search(r'data-url="([^"]+?price[^"]+?)"', html)
    if m:  # JSON endpoint (fastest)
        api = urljoin(ROOT, m.group(1))
        data = requests.get(api, headers={"User-Agent": UA}).json()
        return {r["RetailerName"].strip(): r["PriceLink"] for r in data}

    # fallback: render gov.il with Playwright
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


# ─────────────── PLAYWRIGHT LOGIN + PAGE ─────────────────────────────
def get_authenticated_session(hub_url: str, creds: dict | None) -> requests.Session:
    sess = requests.Session()
    if creds:
        login_url = hub_url.rstrip("/") + "/login"
        resp = sess.post(login_url, data=creds, timeout=30, verify=False)
        if resp.status_code != 200:
            log.warning("Login failed for %s (%s)", hub_url, resp.status_code)
    return sess


def playwright_with_login(hub: str, creds: dict | None, shop: str = "retailer"):
    pw = sync_playwright().start()
    browser = pw.chromium.launch(args=["--ignore-certificate-errors"])
    ctx = browser.new_context(
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
    
    # Enhanced login detection
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
        if any(x in page.url.lower() for x in ["login", "auth", "signin"]):
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
    
    # Wait for page to be fully loaded after login
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except TimeoutError:
        pass
    
    # Take screenshot of the authenticated hub page
    shop_str = str(shop) if shop else "retailer"
    shop_dir = DL_ROOT / slug(shop_str)
    shop_dir.mkdir(exist_ok=True)
    screenshot_path = shop_dir / f"login_{slug(shop_str)}.png"
    page.screenshot(path=str(screenshot_path), full_page=True)
    log.info(f"Saved login screenshot: {screenshot_path}")
    
    return pw, browser, page


# ─────────────── ZIP/GZ DISCOVERY ────────────────────────────────────
def zip_links(hub: str, creds: dict | None, shop: str = "retailer") -> list[str]:
    captured = set()
    pw, browser, page = playwright_with_login(hub, creds, shop)
    
    # Set up download capture
    page.on("download", lambda d: captured.add(d.url))
    
    # Wait for page to be fully loaded before looking for downloads
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
        page.wait_for_timeout(3_000)
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
        elements = page.query_selector_all(selector)
        download_elements.extend(elements)
    
    log.info(f"Found {len(download_elements)} potential download elements for {shop}")
    
    # Try clicking download elements to trigger downloads
    for el in download_elements:
        try:
            el.click(force=True, timeout=2_000)
            log.info(f"Clicked download element: {el.tag_name}")
        except Exception as e:
            log.debug(f"Failed to click element: {e}")
    
    # Wait for downloads to complete
    page.wait_for_timeout(3_000)
    
    # Also collect direct href links
    for a in page.query_selector_all("a[href]"):
        href = a.get_attribute("href")
        if href and ZIP_RX.search(href):
            captured.add(urljoin(hub, href))
    
    browser.close()
    pw.stop()
    
    # Return unique URLs that match our file patterns
    unique_urls = list(dict.fromkeys(u for u in captured if ZIP_RX.search(u)))
    log.info(f"Found {len(unique_urls)} download URLs for {shop}")
    return unique_urls


# ───────────── XML NORMALISERS ───────────────────────────────────────
def parse_xml(blob: bytes, shop: str):
    try:
        root = ET.fromstring(blob)
    except ParseError:
        return None, []

    if root.find(".//Promotion") is not None:  # BinaProjects promo
        rows = [
            {
                "name": n.findtext("ItemName", "").strip(),
                "barcode": n.findtext("ItemCode", "").strip(),
                "promo_price": _to_float(n.findtext("PromoPrice", "0")),
                "promo_description": n.findtext("PromoDesc", "").strip(),
                "promo_start": n.findtext("PromoStartDate", "").split("T")[0],
                "promo_end": n.findtext("PromoEndDate", "").split("T")[0],
                "company": shop,
            }
            for n in root.findall(".//Promotion")
        ]
        return "promo", rows

    if root.find(".//Promo") is not None:  # Cerberus promo
        rows = [
            {
                "name": n.findtext("Name", "").strip(),
                "barcode": n.findtext("Code", "").strip(),
                "promo_price": _to_float(n.findtext("PromoPrice", "0")),
                "promo_description": n.findtext("PromoType", "").strip(),
                "promo_start": n.findtext("StartDate", "").split("T")[0],
                "promo_end": n.findtext("EndDate", "").split("T")[0],
                "company": shop,
            }
            for n in root.findall(".//Promo")
        ]
        return "promo", rows

    if root.find(".//Items") is not None:  # BinaProjects price
        rows = [
            {
                "name": n.findtext("ItemName", "").strip(),
                "barcode": n.findtext("ItemCode", "").strip(),
                "date": n.findtext("PriceUpdateDate", "").split("T")[0],
                "price": _to_float(n.findtext("ItemPrice", "0")),
                "company": shop,
            }
            for n in root.findall(".//Item")
        ]
        return "price", rows

    if root.find(".//Products") is not None:  # Cerberus price
        rows = [
            {
                "name": n.findtext("Name", "").strip(),
                "barcode": n.findtext("Code", "").strip(),
                "date": n.findtext("UpdateDate", "").split("T")[0],
                "price": _to_float(n.findtext("Price", "0")),
                "company": shop,
            }
            for n in root.findall(".//Product")
        ]
        return "price", rows

    return None, []


# ───────────── MAIN ──────────────────────────────────────────────────
def main():
    counts = {"zips": 0, "xmls": 0, "rows": 0}
    visited = set()

    for shop, hub in retailer_links().items():
        if hub in visited:
            continue
        visited.add(hub)

        log.info(f"[{shop}] hub scan")
        creds = creds_for(shop)
        shop_str = str(shop) if shop else "retailer"
        sess = get_authenticated_session(hub, creds)
        files = zip_links(hub, creds, shop_str)
        if not files:
            log.warning("No files for %s", shop)
            continue

        shop_dir = DL_ROOT / slug(shop_str)
        shop_dir.mkdir(exist_ok=True)
        json_dir = JSON_ROOT / slug(shop_str)
        json_dir.mkdir(exist_ok=True)

        for url in files:
            try:
                resp = sess.get(url, headers={"User-Agent": UA}, timeout=120, verify=False)
                resp.raise_for_status()
                fname = Path(urlparse(url).path).name
                zip_path = shop_dir / fname
                zip_path.write_bytes(resp.content)
                counts["zips"] += 1

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    for zip_info in zip_ref.infolist():
                        if zip_info.filename.endswith((".xml", ".json")):
                            with zip_ref.open(zip_info) as file:
                                xml_data = file.read()
                                xml_type, rows = parse_xml(xml_data, shop_str)
                                if xml_type:
                                    counts["xmls"] += 1
                                    counts["rows"] += len(rows)
                                    json_path = json_dir / f"{slug(shop_str)}_{zip_info.filename}"
                                    json_path.write_text(json.dumps(rows))
                                else:
                                    log.warning("Unknown XML type for %s", zip_info.filename)
            except Exception as e:
                log.error("Error processing %s: %s", url, e)

    log.info("Downloaded %d files, parsed %d XMLs, extracted %d rows",
             counts["zips"], counts["xmls"], counts["rows"])


if __name__ == "__main__":
    main()
