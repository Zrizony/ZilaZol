#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script for login improvements
"""

import re, logging
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime
import requests
from playwright.sync_api import sync_playwright, TimeoutError

ROOT = "https://www.gov.il/he/pages/cpfta_prices_regulations"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

slug = lambda s: re.sub(r"[^\w]+", "_", s.strip())[:60] or "retailer"


def retailer_links():
    """Get retailer links from the main page"""
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


def test_login(hub, creds, shop_slug):
    """Test the improved login functionality and download first file"""
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
        if any(x in page.url.lower() for x in ["login", "auth", "signin"]):
            login_needed = True
            log.info(f"Already on login page: {page.url}")
    
    # Take screenshot before login attempt
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    ss_path = SS_ROOT / f"{shop_slug}_before_login_{ts}.png"
    page.screenshot(path=str(ss_path), full_page=True)
    log.info(f"Pre-login screenshot saved: {ss_path}")
    
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
                # Take a screenshot to debug
                debug_ss = (
                    SS_ROOT
                    / f"debug_login_{slug(hub)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                )
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
            debug_ss = (
                SS_ROOT
                / f"debug_login_fail_{slug(hub)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            )
            page.screenshot(path=str(debug_ss), full_page=True)
            log.info(f"Login failure screenshot saved: {debug_ss}")
    
    # Take screenshot after login attempt
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    ss_path = SS_ROOT / f"{shop_slug}_after_login_{ts}.png"
    page.screenshot(path=str(ss_path), full_page=True)
    log.info(f"Post-login screenshot saved: {ss_path}")
    
    # Now try to find and download the first file
    log.info("Looking for download files...")
    
    # Wait for page to be fully loaded before looking for downloads
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
        page.wait_for_timeout(3_000)
    except TimeoutError:
        log.warning(f"Page load timeout before download search for {shop_slug}")
    
    # Look for download links
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
    
    # Try to download the first file found
    downloaded_file = None
    if download_elements:
        first_element = download_elements[0]
        try:
            log.info(f"Attempting to download first file for {shop_slug}")
            
            # Get the download URL
            href = first_element.get_attribute("href")
            if href:
                download_url = urljoin(hub, href)
                log.info(f"Download URL: {download_url}")
                
                # Download the file
                response = requests.get(download_url, headers={"User-Agent": UA}, timeout=60, verify=False)
                if response.status_code == 200:
                    # Save the file
                    filename = f"{shop_slug}_test_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    if href.endswith('.zip'):
                        filename += '.zip'
                    elif href.endswith('.gz'):
                        filename += '.gz'
                    elif href.endswith('.xml'):
                        filename += '.xml'
                    else:
                        filename += '.bin'
                    
                    file_path = SS_ROOT / filename
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    
                    downloaded_file = file_path
                    log.info(f"Successfully downloaded: {file_path} ({len(response.content)} bytes)")
                else:
                    log.error(f"Failed to download file, status code: {response.status_code}")
            else:
                log.warning("No href attribute found on download element")
                
        except Exception as e:
            log.error(f"Failed to download file for {shop_slug}: {e}")
    else:
        log.warning(f"No download elements found for {shop_slug}")
    
    # Log final URL and page title
    log.info(f"Final URL: {page.url}")
    log.info(f"Page title: {page.title()}")
    
    br.close()
    pw.stop()
    return login_needed, downloaded_file


def main():
    """Test login with a few selected retailers"""
    links = retailer_links()
    log.info(f"Found {len(links)} retailers")

    # Test with retailers that have credentials
    test_retailers = ['סאלח דבאח ובניו בע"מ', 'פז קמעונאות ואנרגיה בע"מ']

    for shop in test_retailers:
        if shop in links:
            hub = links[shop]
            creds = CREDS.get(shop)
            shop_slug = slug(shop)

            log.info(f"Testing login for: {shop}")
            log.info(f"Hub URL: {hub}")
            log.info(f"Credentials: {creds}")

            try:
                login_needed, downloaded_file = test_login(hub, creds, shop_slug)
                log.info(
                    f"Login test completed for {shop}. Login needed: {login_needed}"
                )
                if downloaded_file:
                    log.info(f"Downloaded file: {downloaded_file}")
            except Exception as e:
                log.error(f"Login test failed for {shop}: {e}")

            log.info("-" * 50)


if __name__ == "__main__":
    main()
