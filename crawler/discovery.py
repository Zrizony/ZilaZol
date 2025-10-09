# crawler/discovery.py
from __future__ import annotations
import asyncio
from typing import List, Tuple
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from . import logger

GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"

TEXT_VIEW_PRICES = "לצפייה במחירים"  # anchor text on the button/link


async def _collect_with_playwright(debug: bool = False) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        ctx = await browser.new_context(
            locale="he-IL",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()

        try:
            await page.goto(GOV_URL, wait_until="domcontentloaded", timeout=60000)

            # If there’s a cookies/consent banner, try to accept it
            for sel in [
                "button:has-text('מאשר')",
                "button:has-text('מסכים')",
                "button:has-text('קבל הכל')",
                "[role='button']:has-text('מאשר')",
            ]:
                if await page.locator(sel).count():
                    try:
                        await page.click(sel, timeout=2000)
                        break
                    except Exception:
                        pass

            # Give the page time to render; then wait for any link with the target text
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

            # Many gov pages are built with React/Angular; the table might render late.
            # We wait for any anchor that contains the Hebrew text for "View prices".
            try:
                await page.wait_for_selector(
                    f"a:has-text('{TEXT_VIEW_PRICES}')", timeout=20000
                )
            except PWTimeout:
                if debug:
                    html = await page.content()
                    logger.error(
                        "Gov page rendered but no anchor found; HTML len=%s", len(html)
                    )
                return results  # empty

            anchors = page.locator(f"a:has-text('{TEXT_VIEW_PRICES}')")
            count = await anchors.count()
            if count == 0:
                if debug:
                    html = await page.content()
                    logger.error("No anchors matched; HTML len=%s", len(html))
                return results

            # Extract retailer name from the same row/cell; href from the anchor
            for i in range(count):
                a = anchors.nth(i)
                href = await a.get_attribute("href")
                if not href:
                    continue

                # Try to grab some text near the anchor (whole row text is often easiest)
                row = a.locator("xpath=ancestor::tr[1]")
                retail_text = (
                    (await row.inner_text()).strip()
                    if await row.count()
                    else (await a.inner_text()).strip()
                )

                # Clean retailer display name a bit (remove the button text)
                name = retail_text.replace(TEXT_VIEW_PRICES, "").strip()
                if not name:
                    name = "לא מזוהה"  # fallback Hebrew: “unidentified”

                results.append((name, href))

            return results

        finally:
            await ctx.close()
            await browser.close()


def discover_retailers(debug: bool = False) -> List[Tuple[str, str]]:
    """
    Synchronous wrapper the rest of the app can call.
    Returns: [(display_name, portal_url), ...]
    """
    return asyncio.run(_collect_with_playwright(debug=debug))
