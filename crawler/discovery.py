# crawler/discovery.py
from __future__ import annotations
import asyncio
from typing import List, Tuple
from playwright.async_api import async_playwright
from . import logger

BUTTON_TEXTS = ("לצפייה במחירים", "צפייה במחירים", "מחיר")

async def discover_retailers(gov_url: str) -> List[Tuple[str, str]]:
    """Return [(display_name, href)] for each 'לצפייה במחירים' row on gov.il page."""
    links: set[tuple[str, str]] = set()
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(locale="he-IL")
        page = await ctx.new_page()
        await page.goto(gov_url, wait_until="domcontentloaded", timeout=90000)

        # Try table-first
        rows = await page.query_selector_all("table tr")
        for tr in rows:
            a = await tr.query_selector("a[href]")
            if not a:
                continue
            text = (await a.inner_text()).strip()
            if not any(b in text for b in BUTTON_TEXTS):
                continue
            href = await a.get_attribute("href") or ""
            if href.startswith("/"):
                # Make absolute
                href = await page.evaluate("u => new URL(u, location.href).href", href)
            if not href.startswith("http"):
                continue

            # retailer name from first cell if present
            tds = await tr.query_selector_all("td")
            display = (await tds[0].inner_text()).strip() if tds else text
            links.add((display, href))

        # Fallback: scan all anchors
        if not links:
            anchors = await page.eval_on_selector_all(
                "a[href]",
                "els => els.map(a => ({text: a.textContent?.trim()||'', href: a.getAttribute('href')}))"
            )
            for r in anchors:
                t = r.get("text", "")
                href = r.get("href", "") or ""
                if any(b in t for b in BUTTON_TEXTS):
                    if href.startswith("/"):
                        href = await page.evaluate("u => new URL(u, location.href).href", href)
                    if href.startswith("http"):
                        links.add((t, href))

        await ctx.close()
        await browser.close()

    out = list(links)
    logger.info("discovered_retailers_count=%d", len(out))
    return out
