#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
crawler_enhanced.py  –  Enhanced crawler with database integration and smart data management

Features:
- Firestore database for structured data storage
- Smart data deduplication and versioning
- Price history tracking
- Automatic cleanup of old files
- Data validation and anomaly detection
"""

import re, json, zipfile, logging, io
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Any

import requests
from playwright.sync_api import sync_playwright, TimeoutError
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

# Google Cloud Services
from google.cloud import storage
from google.cloud import logging as cloud_logging
from google.cloud import firestore
from google.cloud.firestore import Transaction

# ───────────── CONFIG ────────────────────────────────────────────────
ROOT = "https://www.gov.il/he/pages/cpfta_prices_regulations"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Cloud Configuration
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'civic-ripsaw-466109-e2-crawler-data')
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'civic-ripsaw-466109-e2')

# Data Retention Settings
RETENTION_DAYS = 30  # Keep raw files for 30 days
PRICE_HISTORY_DAYS = 365  # Keep price history for 1 year

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

# Cloud Logging Setup
try:
    client = cloud_logging.Client(project=PROJECT_ID)
    client.setup_logging()
    log = logging.getLogger(__name__)
except Exception as e:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger(__name__)
    log.warning(f"Cloud logging setup failed: {e}, using basic logging")


# ───────────── DATABASE HELPERS ─────────────────────────────────────
def get_firestore_client():
    """Get Firestore client"""
    return firestore.Client(project=PROJECT_ID)

def update_product_in_database(shop: str, product_data: dict, source_file: str):
    """Update product data in Firestore with proper versioning"""
    db = get_firestore_client()
    collection = db.collection('products')
    
    # Create document ID: shop_barcode
    doc_id = f"{shop}_{product_data['barcode']}"
    doc_ref = collection.document(doc_id)
    
    current_time = datetime.now()
    
    try:
        # Use transaction for data consistency
        @firestore.transactional
        def update_in_transaction(transaction: Transaction, doc_ref):
            doc = doc_ref.get(transaction=transaction)
            
            if doc.exists:
                # Update existing product
                current_data = doc.to_dict()
                
                # Check if this is a new price update
                current_price = current_data.get('current_price', 0)
                new_price = product_data.get('price', 0)
                
                # Add to price history if price changed or it's been more than 1 hour
                last_update = current_data.get('last_updated')
                should_add_history = (
                    abs(new_price - current_price) > 0.01 or  # Price changed significantly
                    not last_update or  # First update
                    (current_time - last_update).total_seconds() > 3600  # More than 1 hour ago
                )
                
                if should_add_history:
                    price_history = current_data.get('price_history', [])
                    price_history.append({
                        'price': new_price,
                        'date': current_time,
                        'source_file': source_file,
                        'shop': shop
                    })
                    
                    # Keep only last year of history
                    cutoff_date = current_time - timedelta(days=PRICE_HISTORY_DAYS)
                    price_history = [p for p in price_history if p['date'] > cutoff_date]
                    
                    current_data['price_history'] = price_history
                
                # Update current data
                current_data.update({
                    'current_price': new_price,
                    'last_updated': current_time,
                    'last_source_file': source_file,
                    'update_count': current_data.get('update_count', 0) + 1
                })
                
                transaction.update(doc_ref, current_data)
                log.info(f"Updated product {product_data['barcode']} in {shop}")
                
            else:
                # Create new product
                new_data = {
                    'name': product_data.get('name', ''),
                    'barcode': product_data['barcode'],
                    'shop': shop,
                    'category': product_data.get('category', ''),
                    'current_price': product_data.get('price', 0),
                    'first_seen': current_time,
                    'last_updated': current_time,
                    'last_source_file': source_file,
                    'update_count': 1,
                    'price_history': [{
                        'price': product_data.get('price', 0),
                        'date': current_time,
                        'source_file': source_file,
                        'shop': shop
                    }]
                }
                
                transaction.set(doc_ref, new_data)
                log.info(f"Created new product {product_data['barcode']} in {shop}")
        
        # Execute the transaction
        update_in_transaction(db.transaction(), doc_ref)
        return True
        
    except Exception as e:
        log.error(f"Failed to update product {product_data['barcode']} in {shop}: {e}")
        return False

def get_product_history(barcode: str, shop: str = None, days: int = 30) -> List[Dict]:
    """Get price history for a product"""
    db = get_firestore_client()
    collection = db.collection('products')
    
    try:
        if shop:
            # Get specific shop product
            doc_ref = collection.document(f"{shop}_{barcode}")
            doc = doc_ref.get()
            
            if doc.exists:
                data = doc.to_dict()
                cutoff_date = datetime.now() - timedelta(days=days)
                history = [p for p in data.get('price_history', []) if p['date'] > cutoff_date]
                return history
            else:
                return []
        else:
            # Search across all shops
            docs = collection.where('barcode', '==', barcode).stream()
            all_history = []
            
            for doc in docs:
                data = doc.to_dict()
                cutoff_date = datetime.now() - timedelta(days=days)
                history = [p for p in data.get('price_history', []) if p['date'] > cutoff_date]
                all_history.extend(history)
            
            # Sort by date
            all_history.sort(key=lambda x: x['date'])
            return all_history
            
    except Exception as e:
        log.error(f"Failed to get product history for {barcode}: {e}")
        return []


# ───────────── CLOUD STORAGE HELPERS ─────────────────────────────────
def get_storage_client():
    """Get Google Cloud Storage client"""
    return storage.Client(project=PROJECT_ID)

def upload_to_gcs(bucket_name: str, source_data: bytes, destination_blob_name: str):
    """Upload data to Google Cloud Storage"""
    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_string(source_data)
        log.info(f"Uploaded {destination_blob_name} to {bucket_name}")
        return True
    except Exception as e:
        log.error(f"Failed to upload {destination_blob_name}: {e}")
        return False

def cleanup_old_files(shop: str, days: int = RETENTION_DAYS):
    """Clean up old files from Cloud Storage"""
    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        cutoff_date = datetime.now() - timedelta(days=days)
        prefix = f"downloads/{slug(shop)}/"
        
        blobs = bucket.list_blobs(prefix=prefix)
        deleted_count = 0
        
        for blob in blobs:
            # Check if blob is older than retention period
            if blob.time_created < cutoff_date:
                blob.delete()
                deleted_count += 1
                log.info(f"Deleted old file: {blob.name}")
        
        if deleted_count > 0:
            log.info(f"Cleaned up {deleted_count} old files for {shop}")
        
    except Exception as e:
        log.error(f"Failed to cleanup old files for {shop}: {e}")


# ───────────── DATA VALIDATION ──────────────────────────────────────
def validate_price_data(product_data: dict) -> bool:
    """Validate price data for anomalies"""
    try:
        price = product_data.get('price', 0)
        name = product_data.get('name', '')
        barcode = product_data.get('barcode', '')
        
        # Basic validation
        if not barcode or len(barcode) < 5:
            log.warning(f"Invalid barcode: {barcode}")
            return False
        
        if not name or len(name.strip()) < 2:
            log.warning(f"Invalid product name: {name}")
            return False
        
        if price <= 0 or price > 10000:  # Reasonable price range
            log.warning(f"Suspicious price: {price} for {name}")
            return False
        
        return True
        
    except Exception as e:
        log.error(f"Price validation failed: {e}")
        return False

def detect_price_anomalies(product_data: dict, shop: str) -> bool:
    """Detect significant price changes"""
    try:
        barcode = product_data.get('barcode')
        new_price = product_data.get('price', 0)
        
        # Get recent price history
        history = get_product_history(barcode, shop, days=7)
        
        if len(history) < 2:
            return False  # Not enough history to detect anomalies
        
        # Calculate average price from last 7 days
        recent_prices = [p['price'] for p in history[-7:]]
        avg_price = sum(recent_prices) / len(recent_prices)
        
        # Check if new price is significantly different (>50% change)
        price_change_ratio = abs(new_price - avg_price) / avg_price
        
        if price_change_ratio > 0.5:
            log.warning(f"Price anomaly detected for {barcode}: {avg_price} -> {new_price} ({price_change_ratio:.1%} change)")
            return True
        
        return False
        
    except Exception as e:
        log.error(f"Anomaly detection failed: {e}")
        return False


# ───────────── HELPERS ───────────────────────────────────────────────
def slug(name: str) -> str:
    return re.sub(r"[^\w]+", "_", name.strip())[:60] or "retailer"

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
    
    # Take screenshot and upload to cloud storage
    shop_str = str(shop) if shop else "retailer"
    screenshot_path = f"screenshots/login_{slug(shop_str)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    
    # Take screenshot to temporary file
    temp_screenshot = f"/tmp/{slug(shop_str)}_login.png"
    page.screenshot(path=temp_screenshot, full_page=True)
    
    # Upload screenshot to cloud storage
    upload_to_gcs(BUCKET_NAME, open(temp_screenshot, 'rb').read(), screenshot_path)
    log.info(f"Uploaded login screenshot: {screenshot_path}")
    
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


# ───────────── ENHANCED DATA PROCESSING ──────────────────────────────
def process_and_store_data(shop: str, xml_data: bytes, source_file: str):
    """Process XML data and store with proper versioning and validation"""
    
    # Parse XML
    xml_type, rows = parse_xml(xml_data, shop)
    
    if not xml_type or not rows:
        log.warning(f"No valid data found in {source_file}")
        return 0, 0
    
    processed_count = 0
    error_count = 0
    
    for row in rows:
        try:
            # Validate data
            if not validate_price_data(row):
                error_count += 1
                continue
            
            # Check for price anomalies
            if detect_price_anomalies(row, shop):
                log.warning(f"Price anomaly detected for {row['barcode']} in {shop}")
            
            # Update database
            if update_product_in_database(shop, row, source_file):
                processed_count += 1
            else:
                error_count += 1
                
        except Exception as e:
            log.error(f"Failed to process row for {shop}: {e}")
            error_count += 1
    
    log.info(f"Processed {processed_count} products, {error_count} errors for {shop}")
    return processed_count, error_count


# ───────────── MAIN ──────────────────────────────────────────────────
def test_enhanced_setup():
    """Test enhanced setup with database and storage"""
    log.info("Testing enhanced setup...")
    
    # Test Firestore connection
    try:
        db = get_firestore_client()
        # Try to access a collection
        collection = db.collection('test')
        log.info("✅ Firestore connection successful")
    except Exception as e:
        log.error(f"❌ Firestore connection failed: {e}")
        return False
    
    # Test Cloud Storage
    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(BUCKET_NAME)
        if bucket.exists():
            log.info(f"✅ Bucket {BUCKET_NAME} accessible")
        else:
            log.error(f"❌ Bucket {BUCKET_NAME} not found")
            return False
    except Exception as e:
        log.error(f"❌ Cloud Storage access failed: {e}")
        return False
    
    log.info("✅ All enhanced setup tests passed")
    return True

def main():
    # Test enhanced setup first
    if not test_enhanced_setup():
        log.error("Enhanced setup failed, exiting")
        return
    
    counts = {"zips": 0, "xmls": 0, "rows": 0, "errors": 0}
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

        # Clean up old files for this shop
        cleanup_old_files(shop_str)

        for url in files:
            try:
                resp = sess.get(url, headers={"User-Agent": UA}, timeout=120, verify=False)
                resp.raise_for_status()
                fname = Path(urlparse(url).path).name
                
                # Upload zip file to cloud storage
                zip_blob_name = f"downloads/{slug(shop_str)}/{fname}"
                if upload_to_gcs(BUCKET_NAME, resp.content, zip_blob_name):
                    counts["zips"] += 1

                # Process zip file content with enhanced data management
                with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zip_ref:
                    for zip_info in zip_ref.infolist():
                        if zip_info.filename.endswith((".xml", ".json")):
                            with zip_ref.open(zip_info) as file:
                                xml_data = file.read()
                                
                                # Use enhanced processing
                                processed, errors = process_and_store_data(
                                    shop_str, xml_data, f"{fname}/{zip_info.filename}"
                                )
                                
                                counts["xmls"] += 1
                                counts["rows"] += processed
                                counts["errors"] += errors
                                
            except Exception as e:
                log.error("Error processing %s: %s", url, e)
                counts["errors"] += 1

    log.info("Enhanced crawler completed: Downloaded %d files, processed %d XMLs, extracted %d rows, %d errors",
             counts["zips"], counts["xmls"], counts["rows"], counts["errors"])


if __name__ == "__main__":
    main() 