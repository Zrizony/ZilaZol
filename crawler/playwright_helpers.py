# crawler/playwright_helpers.py
from __future__ import annotations
import os
from datetime import datetime, timezone

from playwright.async_api import Page

from .constants import SCREENSHOTS_DIR
from .utils import safe_name, ensure_dirs


async def new_context(pw):
    """Create a new Playwright browser context."""
    browser = await pw.chromium.launch(
        headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
    )
    ctx = await browser.new_context(locale="he-IL")
    return browser, ctx


async def screenshot_after_login(page: Page, display_name: str):
    """Take a screenshot after login for debugging."""
    ensure_dirs(SCREENSHOTS_DIR)
    fname = f"{safe_name(display_name)}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.png"
    await page.screenshot(path=os.path.join(SCREENSHOTS_DIR, fname), full_page=True)

