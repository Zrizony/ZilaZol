#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
crawler_cloud.py  –  downloads & JSONL to Google Cloud Storage

Dependencies:
    pip install requests beautifulsoup4 playwright google-cloud-storage
    playwright install chromium
"""

import re, json, zipfile, logging, io
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
import os

import requests
from playwright.sync_api import sync_playwright, TimeoutError
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

# Google Cloud Storage
from google.cloud import storage
from google.cloud import logging as cloud_logging

# ───────────── CONFIG ────────────────────────────────────────────────
ROOT = "https://www.gov.il/he/pages/cpfta_prices_regulations"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Cloud Storage Configuration
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'civic-ripsaw-466109-e2-crawler-data')
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'civic-ripsaw-466109-e2')

# File retention settings
GENERAL_RETENTION_DAYS = 7  # General file retention period
SCREENSHOT_RETENTION_DAYS = 2  # Screenshot retention period (exception)

# GCS operation settings
GCS_BATCH_SIZE = 100  # Batch size for bulk operations
GCS_RETRY_ATTEMPTS = 3  # Number of retry attempts for failed operations
GCS_TIMEOUT = 30  # Timeout for GCS operations in seconds

# Fill from gov.il table  (domain → creds)
CREDS = {
    'דור אלון ניהול מתחמים קמעונאיים בע"מ': {"username": "doralon"},
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
    # Fallback to basic logging if cloud logging fails
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger(__name__)
    log.warning(f"Cloud logging setup failed: {e}, using basic logging")


# ───────────── CLOUD STORAGE HELPERS ─────────────────────────────────
def get_storage_client():
    """Get Google Cloud Storage client with optimized settings"""
    try:
        # Create client with project ID only (timeout is applied per-operation)
        client = storage.Client(project=PROJECT_ID)
        return client
    except Exception as e:
        log.error(f"Failed to create GCS client: {e}")
        raise

def upload_to_gcs(bucket_name: str, source_data: bytes, destination_blob_name: str):
    """Upload data to Google Cloud Storage with retry logic"""
    for attempt in range(GCS_RETRY_ATTEMPTS):
        try:
            storage_client = get_storage_client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            
            # Set metadata for better organization
            blob.metadata = {
                'uploaded_at': datetime.now().isoformat(),
                'content_type': 'application/octet-stream',
                'source': 'price_crawler'
            }
            
            blob.upload_from_string(source_data)
            log.info(f"✅ Uploaded {destination_blob_name} to {bucket_name} (attempt {attempt + 1})")
            return True
            
        except Exception as e:
            log.warning(f"Upload attempt {attempt + 1} failed for {destination_blob_name}: {e}")
            if attempt == GCS_RETRY_ATTEMPTS - 1:
                log.error(f"❌ All upload attempts failed for {destination_blob_name}")
                return False
            # Wait before retry (exponential backoff)
            import time
            time.sleep(2 ** attempt)
    
    return False

def upload_file_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str):
    """Upload a file to Google Cloud Storage with retry logic"""
    for attempt in range(GCS_RETRY_ATTEMPTS):
        try:
            storage_client = get_storage_client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            
            # Set metadata for better organization
            blob.metadata = {
                'uploaded_at': datetime.now().isoformat(),
                'source_file': source_file_path,
                'source': 'price_crawler'
            }
            
            blob.upload_from_filename(source_file_path)
            log.info(f"✅ Uploaded {source_file_path} to {destination_blob_name} (attempt {attempt + 1})")
            return True
            
        except Exception as e:
            log.warning(f"File upload attempt {attempt + 1} failed for {source_file_path}: {e}")
            if attempt == GCS_RETRY_ATTEMPTS - 1:
                log.error(f"❌ All file upload attempts failed for {source_file_path}")
                return False
            # Wait before retry (exponential backoff)
            import time
            time.sleep(2 ** attempt)
    
    return False

def delete_blob_from_gcs(bucket_name: str, blob_name: str):
    """Delete a blob from Google Cloud Storage with retry logic"""
    for attempt in range(GCS_RETRY_ATTEMPTS):
        try:
            storage_client = get_storage_client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            if blob.exists():
                blob.delete()
                log.info(f"✅ Deleted {blob_name} from {bucket_name} (attempt {attempt + 1})")
                return True
            else:
                log.info(f"ℹ️ Blob {blob_name} does not exist in {bucket_name}")
                return True  # Consider this a success since the goal is achieved
                
        except Exception as e:
            log.warning(f"Delete attempt {attempt + 1} failed for {blob_name}: {e}")
            if attempt == GCS_RETRY_ATTEMPTS - 1:
                log.error(f"❌ All delete attempts failed for {blob_name}")
                return False
            # Wait before retry (exponential backoff)
            import time
            time.sleep(2 ** attempt)
    
    return False

def list_blobs_with_prefix(bucket_name: str, prefix: str):
    """List all blobs with a given prefix with optimized settings"""
    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        
        # List blobs with prefix (timeout applied per operation)
        blobs = bucket.list_blobs(prefix=prefix)
        return list(blobs)
    except Exception as e:
        log.error(f"Failed to list blobs with prefix {prefix}: {e}")
        return []

def cleanup_old_screenshots():
    """Delete screenshots older than SCREENSHOT_RETENTION_DAYS days with optimized batch operations"""
    try:
        log.info(f"🔄 Starting screenshot cleanup (retention: {SCREENSHOT_RETENTION_DAYS} days)...")
        start_time = datetime.now()
        cutoff_date = start_time - timedelta(days=SCREENSHOT_RETENTION_DAYS)
        
        # List all screenshots with optimized pagination
        screenshots = list_blobs_with_prefix(BUCKET_NAME, "screenshots/")
        
        if not screenshots:
            log.info("ℹ️ No screenshots found to clean up")
            return 0
        
        log.info(f"📊 Found {len(screenshots)} total screenshots to analyze")
        
        # Separate screenshots by age for batch processing
        to_delete = []
        to_keep = []
        parse_errors = []
        
        for blob in screenshots:
            filename = blob.name
            if not filename.endswith('.png'):
                continue
            
            # Enhanced date extraction with multiple patterns
            file_date = None
            
            # Pattern 1: screenshots/login_shop_YYYYMMDD_HHMMSS.png
            date_match = re.search(r'(\d{8})_(\d{6})', filename)
            if date_match:
                try:
                    date_str = date_match.group(1)
                    time_str = date_match.group(2)
                    file_date = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
                except ValueError:
                    pass
            
            # Pattern 2: screenshots/login_shop_YYYY-MM-DD_HH-MM-SS.png
            if not file_date:
                date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})', filename)
                if date_match:
                    try:
                        file_date = datetime(
                            int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)),
                            int(date_match.group(4)), int(date_match.group(5)), int(date_match.group(6))
                        )
                    except ValueError:
                        pass
            
            # Pattern 3: Use blob creation time as fallback
            if not file_date and blob.time_created:
                try:
                    file_date = blob.time_created.replace(tzinfo=None)
                except Exception:
                    pass
            
            # Determine if file should be deleted
            if file_date:
                if file_date < cutoff_date:
                    to_delete.append((filename, file_date))
                else:
                    to_keep.append((filename, file_date))
            else:
                parse_errors.append(filename)
        
        log.info(f"📅 Screenshot analysis complete:")
        log.info(f"   • To delete: {len(to_delete)} (older than {cutoff_date.strftime('%Y-%m-%d')})")
        log.info(f"   • To keep: {len(to_keep)} (newer than {cutoff_date.strftime('%Y-%m-%d')})")
        log.info(f"   • Parse errors: {len(parse_errors)}")
        
        # Log parse errors for debugging
        if parse_errors:
            log.warning(f"⚠️ Could not parse dates for {len(parse_errors)} files:")
            for filename in parse_errors[:5]:  # Log first 5 errors
                log.warning(f"   • {filename}")
            if len(parse_errors) > 5:
                log.warning(f"   • ... and {len(parse_errors) - 5} more")
        
        # Batch delete old screenshots
        deleted_count = 0
        failed_deletions = []
        
        if to_delete:
            log.info(f"🗑️ Starting batch deletion of {len(to_delete)} old screenshots...")
            
            # Process in batches for better performance
            for i in range(0, len(to_delete), GCS_BATCH_SIZE):
                batch = to_delete[i:i + GCS_BATCH_SIZE]
                batch_start = i + 1
                batch_end = min(i + GCS_BATCH_SIZE, len(to_delete))
                
                log.info(f"   Processing batch {batch_start}-{batch_end} of {len(to_delete)}...")
                
                for filename, file_date in batch:
                    if delete_blob_from_gcs(BUCKET_NAME, filename):
                        deleted_count += 1
                        log.debug(f"   ✅ Deleted: {filename} (from {file_date.strftime('%Y-%m-%d')})")
                    else:
                        failed_deletions.append(filename)
                        log.warning(f"   ❌ Failed to delete: {filename}")
                
                # Small delay between batches to avoid overwhelming the API
                if i + GCS_BATCH_SIZE < len(to_delete):
                    import time
                    time.sleep(0.5)
        
        # Calculate and log cleanup statistics
        cleanup_duration = datetime.now() - start_time
        success_rate = (deleted_count / len(to_delete) * 100) if to_delete else 100
        
        log.info(f"🎯 Screenshot cleanup completed in {cleanup_duration.total_seconds():.2f}s:")
        log.info(f"   • Successfully deleted: {deleted_count}/{len(to_delete)} ({success_rate:.1f}%)")
        log.info(f"   • Failed deletions: {len(failed_deletions)}")
        log.info(f"   • Remaining screenshots: {len(to_keep)}")
        
        if failed_deletions:
            log.warning(f"⚠️ Failed deletions (will retry next time):")
            for filename in failed_deletions[:5]:  # Log first 5 failures
                log.warning(f"   • {filename}")
            if len(failed_deletions) > 5:
                log.warning(f"   • ... and {len(failed_deletions) - 5} more")
        
        return deleted_count
        
    except Exception as e:
        log.error(f"❌ Screenshot cleanup failed: {e}")
        import traceback
        log.error(f"Stack trace: {traceback.format_exc()}")
        return 0

def check_file_replacement_behavior():
    """Check and log file replacement behavior for different file types"""
    log.info("Checking file replacement behavior...")
    
    # Test file replacement for different file types
    test_files = {
        "test_zip": b"test zip content",
        "test_json": b'{"test": "json content"}',
        "test_screenshot": b"fake png content"
    }
    
    for file_type, content in test_files.items():
        test_blob_name = f"test_replacement/{file_type}.txt"
        
        # Upload first version
        if upload_to_gcs(BUCKET_NAME, content, test_blob_name):
            log.info(f"✅ Uploaded first version of {file_type}")
            
            # Upload second version (should replace)
            new_content = content + b" - updated"
            if upload_to_gcs(BUCKET_NAME, new_content, test_blob_name):
                log.info(f"✅ Successfully replaced {file_type} (this is correct behavior)")
                
                # Clean up test file
                delete_blob_from_gcs(BUCKET_NAME, test_blob_name)
            else:
                log.error(f"❌ Failed to replace {file_type}")
        else:
            log.error(f"❌ Failed to upload first version of {file_type}")
    
    log.info("File replacement behavior check completed")

def get_storage_analytics():
    """Get analytics about storage usage and file distribution"""
    try:
        log.info("📊 Collecting storage analytics...")
        storage_client = get_storage_client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        analytics = {
            'total_size': 0,
            'total_files': 0,
            'by_type': {},
            'by_folder': {},
            'oldest_file': None,
            'newest_file': None
        }
        
        # Collect data from all blobs
        blobs = list(bucket.list_blobs())
        
        for blob in blobs:
            # File size
            size = blob.size or 0
            analytics['total_size'] += size
            analytics['total_files'] += 1
            
            # File type
            file_ext = Path(blob.name).suffix.lower()
            if file_ext:
                analytics['by_type'][file_ext] = analytics['by_type'].get(file_ext, 0) + 1
            
            # Folder structure
            folder = str(Path(blob.name).parent)
            if folder != '.':
                analytics['by_folder'][folder] = analytics['by_folder'].get(folder, 0) + 1
            
            # Creation time
            if blob.time_created:
                created_time = blob.time_created.replace(tzinfo=None)
                if not analytics['oldest_file'] or created_time < analytics['oldest_file']:
                    analytics['oldest_file'] = created_time
                if not analytics['newest_file'] or created_time > analytics['newest_file']:
                    analytics['newest_file'] = created_time
        
        # Convert size to human readable format
        def human_readable_size(size_bytes):
            if size_bytes == 0:
                return "0 B"
            size_names = ["B", "KB", "MB", "GB", "TB"]
            i = 0
            while size_bytes >= 1024 and i < len(size_names) - 1:
                size_bytes /= 1024.0
                i += 1
            return f"{size_bytes:.2f} {size_names[i]}"
        
        # Log analytics
        log.info(f"📊 Storage Analytics for bucket: {BUCKET_NAME}")
        log.info(f"   • Total files: {analytics['total_files']:,}")
        log.info(f"   • Total size: {human_readable_size(analytics['total_size'])}")
        
        if analytics['oldest_file']:
            log.info(f"   • Oldest file: {analytics['oldest_file'].strftime('%Y-%m-%d %H:%M:%S')}")
        if analytics['newest_file']:
            log.info(f"   • Newest file: {analytics['newest_file'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Top file types
        if analytics['by_type']:
            log.info(f"   • File types:")
            sorted_types = sorted(analytics['by_type'].items(), key=lambda x: x[1], reverse=True)
            for file_type, count in sorted_types[:5]:
                log.info(f"     - {file_type}: {count:,} files")
        
        # Top folders
        if analytics['by_folder']:
            log.info(f"   • Top folders:")
            sorted_folders = sorted(analytics['by_folder'].items(), key=lambda x: x[1], reverse=True)
            for folder, count in sorted_folders[:5]:
                log.info(f"     - {folder}: {count:,} files")
        
        return analytics
        
    except Exception as e:
        log.error(f"❌ Failed to collect storage analytics: {e}")
        return None

def batch_delete_blobs(bucket_name: str, blob_names: list):
    """Delete multiple blobs in batch for better performance"""
    if not blob_names:
        return 0
    
    try:
        log.info(f"🗑️ Starting batch deletion of {len(blob_names)} blobs...")
        storage_client = get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        
        deleted_count = 0
        failed_count = 0
        
        # Process in batches
        for i in range(0, len(blob_names), GCS_BATCH_SIZE):
            batch = blob_names[i:i + GCS_BATCH_SIZE]
            batch_start = i + 1
            batch_end = min(i + GCS_BATCH_SIZE, len(blob_names))
            
            log.info(f"   Processing batch {batch_start}-{batch_end} of {len(blob_names)}...")
            
            for blob_name in batch:
                try:
                    blob = bucket.blob(blob_name)
                    if blob.exists():
                        blob.delete()
                        deleted_count += 1
                        log.debug(f"   ✅ Deleted: {blob_name}")
                    else:
                        log.debug(f"   ℹ️ Already deleted: {blob_name}")
                        deleted_count += 1  # Consider this a success
                except Exception as e:
                    failed_count += 1
                    log.warning(f"   ❌ Failed to delete {blob_name}: {e}")
            
            # Small delay between batches
            if i + GCS_BATCH_SIZE < len(blob_names):
                import time
                time.sleep(0.5)
        
        log.info(f"🎯 Batch deletion completed: {deleted_count} successful, {failed_count} failed")
        return deleted_count
        
    except Exception as e:
        log.error(f"❌ Batch deletion failed: {e}")
        return 0


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
        br = pw.chromium.launch(
            args=["--ignore-certificate-errors", "--no-sandbox", "--disable-dev-shm-usage"],
            headless=True
        )
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
    browser = pw.chromium.launch(
        args=["--ignore-certificate-errors", "--no-sandbox", "--disable-dev-shm-usage"],
        headless=True
    )
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
        log.info(f"Checking for login requirements for {shop} with creds: {creds}")
        
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
        
        # Log current page title and URL for debugging
        log.info(f"Current page title: {page.title()}")
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
    try:
        page.screenshot(path=temp_screenshot, full_page=True)
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
def test_cloud_setup():
    """Test cloud storage and logging setup"""
    log.info("Testing cloud setup...")
    
    # Test bucket access
    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(BUCKET_NAME)
        if bucket.exists():
            log.info(f"✅ Bucket {BUCKET_NAME} exists and accessible")
        else:
            log.error(f"❌ Bucket {BUCKET_NAME} does not exist")
            return False
    except Exception as e:
        log.error(f"❌ Failed to access bucket: {e}")
        return False
    
    # Test upload
    try:
        test_data = b"test data for cloud storage"
        test_blob_name = "test/test_upload.txt"
        if upload_to_gcs(BUCKET_NAME, test_data, test_blob_name):
            log.info("✅ Cloud storage upload test successful")
        else:
            log.error("❌ Cloud storage upload test failed")
            return False
    except Exception as e:
        log.error(f"❌ Upload test failed: {e}")
        return False
    
    log.info("✅ All cloud setup tests passed")
    return True

def main():
    # Check Playwright browser installation
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            # This will fail if browsers aren't installed
            browser = pw.chromium.launch(
                args=["--ignore-certificate-errors", "--no-sandbox", "--disable-dev-shm-usage"],
                headless=True
            )
            browser.close()
        log.info("✅ Playwright browsers are properly installed")
    except Exception as e:
        log.error(f"❌ Playwright browser issue: {e}")
        log.error("Please ensure Playwright browsers are installed: playwright install chromium")
        return
    
    # Test cloud setup first
    if not test_cloud_setup():
        log.error("Cloud setup failed, exiting")
        return
    
    # Get storage analytics before cleanup
    log.info("📊 Collecting initial storage analytics...")
    initial_analytics = get_storage_analytics()
    
    # Clean up old screenshots (older than 2 days)
    log.info("🧹 Starting automated cleanup process...")
    deleted_screenshots = cleanup_old_screenshots()
    
    # Get storage analytics after cleanup
    if deleted_screenshots > 0:
        log.info("📊 Collecting post-cleanup storage analytics...")
        final_analytics = get_storage_analytics()
        
        if initial_analytics and final_analytics:
            space_saved = initial_analytics['total_size'] - final_analytics['total_size']
            files_removed = initial_analytics['total_files'] - final_analytics['total_files']
            log.info(f"🎯 Cleanup Results:")
            log.info(f"   • Screenshots deleted: {deleted_screenshots}")
            log.info(f"   • Total files removed: {files_removed}")
            log.info(f"   • Space saved: {space_saved:,} bytes")
    
    # Check file replacement behavior
    check_file_replacement_behavior()
    
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

        for url in files:
            try:
                resp = sess.get(url, headers={"User-Agent": UA}, timeout=120, verify=False)
                resp.raise_for_status()
                fname = Path(urlparse(url).path).name
                
                # Upload zip file to cloud storage
                zip_blob_name = f"downloads/{slug(shop_str)}/{fname}"
                if upload_to_gcs(BUCKET_NAME, resp.content, zip_blob_name):
                    counts["zips"] += 1

                # Process zip file content
                with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zip_ref:
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
            except Exception as e:
                log.error("Error processing %s: %s", url, e)

    log.info("Downloaded %d files, parsed %d XMLs, extracted %d rows",
             counts["zips"], counts["xmls"], counts["rows"])
    
    # Final storage analytics
    log.info("📊 Final storage analytics:")
    get_storage_analytics()


if __name__ == "__main__":
    main() 