# crawler/gov_il.py
from __future__ import annotations
import os
import asyncio
from typing import List, Tuple
from dataclasses import dataclass

from playwright.async_api import async_playwright, Page
from playwright_stealth import stealth_async
from . import logger

GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"

# how long we’re willing to wait (ms)
NAV_TIMEOUT = int(os.getenv("GOV_NAV_TIMEOUT_MS", "60000"))
WAIT_AFTER_LOAD = int(os.getenv("GOV_WAIT_AFTER_LOAD_MS", "3000"))
SCROLL_STEPS = int(os.getenv("GOV_SCROLL_STEPS", "8"))

# selectors we will try for cookie / consent banners
CONSENT_SELECTORS = [
    "#onetrust-accept-btn-handler",  # common OneTrust
    "button#onetrust-accept-btn-handler",
    "button:has-text('אני מסכים')",
    "button:has-text('סגור')",
    "button:has-text('קבל הכל')",
    "button:has-text('הבנתי')",
]

# anchor text selector
ANCHOR_TEXT = "לצפייה במחירים"


async def _accept_consents(page: Page) -> None:
    for sel in CONSENT_SELECTORS:
        try:
            if await page.locator(sel).first.is_visible():
                await page.locator(sel).first.click(timeout=2000)
                await page.wait_for_timeout(300)
        except Exception:
            pass


async def _progressive_scroll(
    page: Page, steps: int = SCROLL_STEPS, pause_ms: int = 400
):
    for _ in range(steps):
        await page.mouse.wheel(0, 2000)
        await page.wait_for_timeout(pause_ms)


async def _debug_dump(page: Page, tag: str):
    try:
        html = await page.content()
        logger.info("%s: html_length=%s", tag, len(html))
        # also screenshot
        os.makedirs("screenshots", exist_ok=True)
        await page.screenshot(path=f"screenshots/gov_{tag}.png", full_page=True)
    except Exception:
        pass


async def _new_context(pw):
    # Use a realistic browser “shape”
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    ctx = await browser.new_context(
        locale="he-IL",
        timezone_id="Asia/Jerusalem",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1366, "height": 900},
    )
    page = await ctx.new_page()
    await stealth_async(page)  # 🥷 reduce bot fingerprints
    return browser, ctx, page


async def discover_retailers() -> List[Tuple[str, str]]:
    """Returns [(display_name, portal_url), ...] from the gov.il table."""
    out: List[Tuple[str, str]] = []
    async with async_playwright() as pw:
        browser, ctx, page = await _new_context(pw)
        try:
            logger.info("gov: navigating…")
            await page.goto(GOV_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
            await _accept_consents(page)
            await page.wait_for_load_state("networkidle", timeout=20000)
            await page.wait_for_timeout(WAIT_AFTER_LOAD)

            # scroll to force lazy content
            await _progressive_scroll(page, steps=SCROLL_STEPS)
            await _debug_dump(page, "after_load")

            # try direct text locator first
            anchors = page.locator(f"a:has-text('{ANCHOR_TEXT}')")
            count = await anchors.count()
            logger.info("gov: anchors_by_text=%d", count)

            if count == 0:
                # sometimes the button is <span> inside <a>
                anchors = page.locator("a").filter(has_text=ANCHOR_TEXT)
                count = await anchors.count()
                logger.info("gov: anchors_fallback_count=%d", count)

            if count == 0:
                # LAST RESORT: grab all <a> and inspect innerText in JS
                links = await page.eval_on_selector_all(
                    "a",
                    """els => els.map(a => ({
                          text: (a.innerText || '').trim(),
                          href: a.getAttribute('href') || ''
                    }))""",
                )
            else:
                links = []
                for i in range(count):
                    el = anchors.nth(i)
                    href = await el.get_attribute("href")
                    txt = (await el.inner_text() or "").strip()
                    links.append({"text": txt, "href": href})

            logger.info("gov: total_links_collected=%d", len(links))

            # For each link row, climb to the table row to fetch retailer name in first cell
            rows = page.locator("table tr")
            row_count = await rows.count()
            logger.info("gov: table_row_count=%d", row_count)

            for i in range(row_count):
                tr = rows.nth(i)
                # check if this row has our anchor
                has_anchor = await tr.locator(f"a:has-text('{ANCHOR_TEXT}')").count()
                if not has_anchor:
                    continue

                # retailer name is typically in the first cell
                retailer_name = (await tr.locator("td").first.inner_text()).strip()
                # portal url from the anchor in this row
                a = tr.locator("a").filter(has_text=ANCHOR_TEXT).first
                href = await a.get_attribute("href")
                if not href:
                    # try rel link on the row
                    continue

                full_url = await page.evaluate(
                    "u => new URL(u, location.href).href", href
                )
                out.append((retailer_name, full_url))

            # final fallback: if the row-parsing missed, at least take all “לצפייה במחירים” hrefs we saw
            if not out:
                for l in links:
                    if ANCHOR_TEXT in (l.get("text") or "") and l.get("href"):
                        full_url = await page.evaluate(
                            "u => new URL(u, location.href).href", l["href"]
                        )
                        # retailer name unknown here
                        out.append(("לא זוהה (gov fallback)", full_url))

            logger.info("discovered_retailers_count=%d", len(out))
            await _debug_dump(page, "final")

        finally:
            await ctx.close()
            await browser.close()

    return out


def fetch_retailers() -> List[Tuple[str, str]]:
    """Sync facade used by Flask endpoints."""
    return asyncio.get_event_loop().run_until_complete(discover_retailers())
