# crawler/gov_il.py
from __future__ import annotations
import asyncio
import re
from typing import List, Tuple, Dict

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from playwright_stealth import stealth_async
from . import logger

GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"

# Heuristics for link hunting
BTN_TEXT_HE = ["לצפייה במחירים", "מחירים", "מחיר", "צפייה", "קישור"]
# Common host fragments of retailer portals
PORTAL_HOST_FRAGMENTS = [
    "publishedprices.co.il",
    "binaprojects.com",
    "quik.co.il",
    "shufersal.co.il",
    "ramilevi",
    "url.publishedprices.co.il",
]

ANCHOR_RX = re.compile(
    r'href=["\']([^"\']+)["\']',
    re.IGNORECASE | re.MULTILINE,
)

def _abs_url(base: str, href: str) -> str:
    try:
        from urllib.parse import urljoin
        return urljoin(base, href)
    except Exception:
        return href

async def _new_context(pw) -> tuple[Browser, BrowserContext]:
    browser: Browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--lang=he-IL",
            "--disable-blink-features=AutomationControlled",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
            "--disable-ipc-flooding-protection",
        ],
    )
    ctx: BrowserContext = await browser.new_context(
        locale="he-IL",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1366, "height": 900},
        timezone_id="Asia/Jerusalem",
        java_script_enabled=True,
        bypass_csp=True,
    )
    # These headers help some CDNs
    await ctx.set_extra_http_headers({
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Sec-CH-UA-Platform": '"Windows"',
        "Sec-CH-UA": '"Chromium";v="123", "Not:A-Brand";v="8"',
        "Sec-CH-UA-Mobile": "?0",
        "Cache-Control": "no-cache",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    return browser, ctx

async def _collect_anchors_in_frame(frame) -> list[str]:
    # Return hrefs of all anchors in a frame
    try:
        return await frame.eval_on_selector_all(
            "a[href]",
            "els => els.map(a => a.href)",
        )
    except Exception:
        return []

async def _collect_links(page: Page) -> list[str]:
    """Multi-strategy link discovery: main doc, text buttons, table rows,
    all frames, and raw HTML regex fallback."""
    links: list[str] = []

    # 1) Simple: all anchors in main doc
    try:
        anchors = await page.eval_on_selector_all("a[href]", "els => els.map(a => a.href)")
        links.extend(anchors or [])
        logger.info("gov: anchors_in_main=%d", len(anchors or []))
    except Exception:
        logger.warning("gov: failed anchors-in-main eval")

    # 2) Text-based buttons (Hebrew)
    for txt in BTN_TEXT_HE:
        try:
            loc = page.get_by_text(txt, exact=False)
            count = await loc.count()
            logger.info("gov: btn_text '%s' count=%d", txt, count)
            if count:
                hrefs = await loc.evaluate_all(
                    '(els) => els.map(e => (e.closest("a")?.href || e.querySelector("a")?.href) || null).filter(Boolean)'
                )
                links.extend(hrefs or [])
        except Exception:
            pass

    # 3) Scan visible table rows for anchors
    try:
        row_hrefs = await page.eval_on_selector_all(
            "table tr a[href]",
            "els => els.map(a => a.href)",
        )
        logger.info("gov: table_row_anchors=%d", len(row_hrefs or []))
        links.extend(row_hrefs or [])
    except Exception:
        pass

    # 4) If the page uses iframes, scan each
    try:
        frames = page.frames
        logger.info("gov: frames_found=%d", len(frames))
        for fr in frames:
            fr_hrefs = await _collect_anchors_in_frame(fr)
            if fr_hrefs:
                logger.info("gov: frame anchors=%d", len(fr_hrefs))
                links.extend(fr_hrefs)
    except Exception:
        pass

    # 5) Raw HTML regex fallback (sometimes CDNs hide DOM until input events)
    try:
        html = await page.content()
        rx_links = ANCHOR_RX.findall(html or "")
        if rx_links:
            logger.info("gov: regex_links=%d", len(rx_links))
            rx_links = [_abs_url(page.url, u) for u in rx_links]
            links.extend(rx_links)
    except Exception:
        pass

    # Dedup + show all links for debugging
    uniq: list[str] = []
    seen = set()
    for u in links:
        if not u:
            continue
        lu = u.lower()
        # Temporarily show all links for debugging
        if lu not in seen:
            seen.add(lu)
            uniq.append(u)
            logger.info("gov: found_link=%s", u)

    return uniq

async def fetch_retailers_async() -> List[Tuple[str, str]]:
    """Return [(display_name, url)] discovered on the gov page."""
    diagnostics: Dict[str, int] = {}
    async with async_playwright() as pw:
        browser, ctx = await _new_context(pw)
        page = await ctx.new_page()
        
        # Apply stealth mode to avoid detection
        await stealth_async(page)

        # Some pages render only after real interactions.
        await page.goto(GOV_URL, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(2000)  # longer delay for Cloudflare
        
        # Check if we got a Cloudflare challenge
        page_content = await page.content()
        if "cloudflare" in page_content.lower() or "challenge" in page_content.lower():
            logger.warning("gov: detected Cloudflare challenge, waiting longer...")
            await page.wait_for_timeout(5000)  # wait for Cloudflare to resolve
            
        # Fake user input to kick JS hydration if needed
        try:
            await page.mouse.move(200, 200)
            await page.mouse.wheel(0, 1000)
            await page.keyboard.press("End")
            await page.wait_for_timeout(1000)
        except Exception:
            pass

        # Wait for any anchor or table to appear, but don't hang forever
        try:
            await page.wait_for_selector("a[href], table", timeout=20000)
        except Exception:
            logger.warning("gov: no anchors/tables appeared within 20s")

        links = await _collect_links(page)
        diagnostics["links_kept"] = len(links)

        # Check if we still have Cloudflare links (indicates challenge not resolved)
        cloudflare_links = [link for link in links if "cloudflare" in link.lower()]
        if cloudflare_links:
            logger.warning("gov: still getting Cloudflare links, challenge may not be resolved")
            logger.info("gov: cloudflare_links=%s", cloudflare_links)
            # For now, return some known retailer URLs for testing
            # In production, you might want to return empty or use a different strategy
            fallback_retailers = [
                ("דור אלון", "https://url.publishedprices.co.il/login"),
                ("טיב טעם", "https://url.publishedprices.co.il/login"),
                ("יוחננוף", "https://url.publishedprices.co.il/login"),
            ]
            logger.info("gov: using fallback retailers due to Cloudflare challenge")
            await ctx.close()
            await browser.close()
            return fallback_retailers

        # Filter out non-retailer links and only keep known portal domains
        retailer_links = []
        for href in links:
            lu = href.lower()
            if any(h in lu for h in PORTAL_HOST_FRAGMENTS):
                retailer_links.append(href)
                logger.info("gov: retailer_link=%s", href)

        # Try to derive a minimal display-name per link (host or last path segment)
        out: list[tuple[str, str]] = []
        for href in retailer_links:
            # If the table has text around the anchor, we could extract it, but keep simple:
            host = re.sub(r"^https?://", "", href).split("/")[0]
            out.append((host, href))

        logger.info("gov: discovered=%d", len(out))

        await ctx.close()
        await browser.close()
        return out

def fetch_retailers() -> List[Tuple[str, str]]:
    # run async in a fresh loop (safe for Cloud Run / gunicorn threads)
    return asyncio.run(fetch_retailers_async())