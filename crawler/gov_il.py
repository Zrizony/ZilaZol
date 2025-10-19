# crawler/gov_il.py
from __future__ import annotations
import asyncio
import os
import re
import time
from typing import List, Tuple, Dict, Optional

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from . import logger

GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"

BTN_TEXT_HE = ["לצפייה במחירים", "מחירים", "מחיר", "צפייה", "קישור"]
PORTAL_HOST_FRAGMENTS = [
    "publishedprices.co.il",
    "url.publishedprices.co.il",
    "binaprojects.com",
    "quik.co.il",
    "shufersal.co.il",
    "ramilevi",
]

ANCHOR_RX = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE | re.MULTILINE)

def _abs_url(base: str, href: str) -> str:
    from urllib.parse import urljoin
    return urljoin(base, href)

async def _new_context(pw) -> tuple[Browser, BrowserContext]:
    proxy = None
    if os.getenv("PROXY_URL"):
        proxy = {"server": os.getenv("PROXY_URL")}
        if os.getenv("PROXY_USER") and os.getenv("PROXY_PASS"):
            proxy.update({"username": os.getenv("PROXY_USER"), "password": os.getenv("PROXY_PASS")})

    browser: Browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--lang=he-IL",
            "--disable-blink-features=AutomationControlled",
            "--disable-web-security",
        ],
        proxy=proxy,
    )

    ctx: BrowserContext = await browser.new_context(
        locale="he-IL",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1366, "height": 900},
        timezone_id="Asia/Jerusalem",
    )
    await ctx.set_extra_http_headers({
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Sec-CH-UA-Platform": '"Windows"',
        "Sec-CH-UA": '"Chromium";v="124", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile": "?0",
        "Cache-Control": "no-cache",
    })
    return browser, ctx

async def _wait_cloudflare(page: Page, max_ms: int = 30000) -> bool:
    """
    Wait for Cloudflare challenge to clear. Consider cleared if:
      - URL is not a cloudflare/challenge URL, OR
      - cookie 'cf_clearance' appears (then reload once).
    """
    start = time.time()
    tries = 0
    while (time.time() - start) * 1000 < max_ms:
        url = page.url.lower()
        if "cloudflare" not in url and "challenge" not in url and "__cf_chl_rt_tk" not in url:
            logger.info("gov: cloudflare cleared -> %s", url)
            return True

        try:
            has_token = await page.evaluate('document.cookie.includes("cf_clearance")')
        except Exception:
            has_token = False

        if has_token:
            logger.info("gov: cf_clearance present, reloading to apply")
            await page.reload()
            await page.wait_for_load_state("domcontentloaded")
            return True

        tries += 1
        if tries == 1:
            logger.warning("gov: detected Cloudflare challenge, waiting longer...")
        try:
            await page.mouse.move(200, 200)
            await page.mouse.wheel(0, 1500)
            await page.keyboard.press("End")
        except Exception:
            pass
        await page.wait_for_timeout(4000)
        await page.reload()
        await page.wait_for_load_state("domcontentloaded")

    return False

async def _save_diagnostics(page: Page, tag: str = "gov") -> None:
    """Save HTML & screenshot under /tmp/diag for inspection."""
    try:
        os.makedirs("/tmp/diag", exist_ok=True)
        html_path = f"/tmp/diag/{tag}.html"
        png_path = f"/tmp/diag/{tag}.png"
        html = await page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html or "")
        await page.screenshot(path=png_path, full_page=True)
        logger.info("gov: diagnostics saved: %s , %s", html_path, png_path)
    except Exception:
        logger.exception("gov: failed saving diagnostics")

async def _collect_anchors_in_frame(frame) -> list[str]:
    try:
        return await frame.eval_on_selector_all("a[href]", "els => els.map(a => a.href)")
    except Exception:
        return []

async def _collect_links(page: Page) -> list[str]:
    links: list[str] = []

    # anchors in main document
    try:
        anchors = await page.eval_on_selector_all("a[href]", "els => els.map(a => a.href)")
        logger.info("gov: anchors_in_main=%d", len(anchors or []))
        links.extend(anchors or [])
    except Exception:
        pass

    # buttons with Hebrew text
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

    # links inside tables
    try:
        row_hrefs = await page.eval_on_selector_all("table tr a[href]", "els => els.map(a => a.href)")
        logger.info("gov: table_row_anchors=%d", len(row_hrefs or []))
        links.extend(row_hrefs or [])
    except Exception:
        pass

    # frames
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

    # regex fallback
    try:
        html = await page.content()
        rx = ANCHOR_RX.findall(html or "")
        if rx:
            logger.info("gov: regex_links=%d", len(rx))
            rx = [_abs_url(page.url, u) for u in rx]
            links.extend(rx)
    except Exception:
        pass

    # filter to plausible portals + dedup
    uniq: list[str] = []
    seen = set()
    for u in links:
        if not u:
            continue
        lu = u.lower()
        # Log all links for debugging
        logger.info("gov: found_link=%s", u)
        if not any(h in lu for h in PORTAL_HOST_FRAGMENTS):
            logger.info("gov: link_filtered_out=%s (no matching host fragments)", u)
            continue
        if lu not in seen:
            seen.add(lu)
            uniq.append(u)
            logger.info("gov: link_kept=%s", u)
    return uniq

def _read_fallback() -> List[Tuple[str, str]]:
    # Try multiple possible paths for the fallback file
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "..", "data", "retailers_fallback.json"),
        "/app/data/retailers_fallback.json",
        "data/retailers_fallback.json",
        "retailers_fallback.json"
    ]
    
    for path in possible_paths:
        path = os.path.abspath(path)
        logger.info("gov: trying fallback_path=%s", path)
        if os.path.exists(path):
            logger.info("gov: found fallback file at %s", path)
            try:
                import json
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                out = []
                for row in data:
                    name, url = row.get("name"), row.get("url")
                    if name and url:
                        out.append((name, url))
                logger.info("gov: fallback_retailers_count=%d", len(out))
                return out
            except Exception:
                logger.exception("gov: failed reading fallback retailers JSON from %s", path)
                continue
    
    logger.warning("gov: fallback file not found in any of the expected locations")
    return []

async def fetch_retailers_async() -> List[Tuple[str, str]]:
    async with async_playwright() as pw:
        browser, ctx = await _new_context(pw)
        page = await ctx.new_page()

        await page.goto(GOV_URL, wait_until="domcontentloaded", timeout=60000)
        cleared = await _wait_cloudflare(page, max_ms=30000)
        if not cleared:
            logger.warning("gov: Cloudflare challenge not cleared in time")
            await _save_diagnostics(page, tag="gov_cloudflare")
            fb = _read_fallback()
            logger.info("gov: using fallback retailers due to Cloudflare challenge")
            await ctx.close(); await browser.close()
            return fb

        try:
            await page.mouse.move(100, 200)
            await page.mouse.wheel(0, 1200)
            await page.keyboard.press("End")
        except Exception:
            pass

        try:
            await page.wait_for_selector("a[href], table", timeout=20000)
        except Exception:
            logger.warning("gov: no anchors/tables after 20s")

        links = await _collect_links(page)
        if not links:
            await _save_diagnostics(page, tag="gov_no_links")
            logger.warning("gov: no links found, using fallback retailers")
            fb = _read_fallback()
            await ctx.close()
            await browser.close()
            return fb

        out: list[Tuple[str, str]] = []
        for href in links:
            host = re.sub(r"^https?://", "", href).split("/")[0]
            out.append((host, href))

        logger.info("gov: discovered_retailers_count=%d", len(out))
        await ctx.close()
        await browser.close()
        return out

def fetch_retailers() -> List[Tuple[str, str]]:
    return asyncio.run(fetch_retailers_async())