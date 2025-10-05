import logging
from typing import Dict, List
from playwright.async_api import Page
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger("crawler.adapter.publishedprices")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
async def login_and_get_file_links(page: Page, base_url: str, username: str, password: str) -> List[str]:
    """Logs in and returns list of file links from the /file page."""
    await page.goto(base_url, wait_until="domcontentloaded", timeout=60000)

    # Some portals go straight to /login; others have a button/link
    if "/login" not in page.url:
        # Try common paths
        for path in ["/login", "/Login", "/signin"]:
            try:
                await page.goto(base_url.rstrip("/") + path, wait_until="domcontentloaded", timeout=15000)
                break
            except Exception:
                pass

    # Fill username/password (common field names)
    for sel in ["input[name='username']", "input#username", "input[name='Email']", "input[type='email']"]:
        if await page.locator(sel).count():
            await page.fill(sel, username)
            break
    for sel in ["input[name='password']", "input#password", "input[type='password']"]:
        if await page.locator(sel).count():
            await page.fill(sel, password)
            break

    # Click submit
    for sel in ["button[type='submit']", "input[type='submit']", "button:has-text('כניסה')", "button:has-text('התחבר')"]:
        if await page.locator(sel).count():
            await page.click(sel)
            break

    # Wait for redirect to /file (creds tied to retailer)
    await page.wait_for_url("**/file*", wait_until="networkidle", timeout=60000)

    # Collect file links
    anchors = await page.locator("a").all()
    found = []
    for a in anchors:
        href = await a.get_attribute("href")
        if not href:
            continue
        if any(href.lower().endswith(ext) for ext in (".xml", ".gz", ".zip")) or "download" in href:
            if href.startswith("/"):
                href = page.url.split("/", 3)[:3]
            found.append(href)
    logger.info("publishedprices_links_found=%d", len(found))
    return found
