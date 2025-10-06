# crawler/gov_il.py
from __future__ import annotations
from playwright.sync_api import sync_playwright
from urllib.parse import urlparse
import re
import time

GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"

# domains we know the gov page links to
PRICE_PORTAL_PATTERNS = [
    r"url\.publishedprices\.co\.il",
    r"binaprojects\.com",
    r"prices\.quik\.co\.il",
    r"shuk-hayir\.binaprojects\.com",
    r"shufersal\.co\.il",
    r"shefabirkat\.binaprojects\.com",
    r"goodpharm\.binaprojects\.com",
    r"maayan2000\.binaprojects\.com",
    r"kingstore\.binaprojects\.com",
    r"prices\.super-pharm\.co\.il",
    r"prices\.victory\.co\.il",
    r"(?:prices|portal)\.[\w\-]*ramilevi[\w\.\-]*",
    r"mega(?:market)?\.[\w\.\-]*",
    r"yenotbitan\.[\w\.\-]*",
]

HREF_PATTERN = re.compile(
    rf'https?://[^\s"\'<>]*?(?:{"|".join(PRICE_PORTAL_PATTERNS)})[^\s"\'<>]*',
    re.IGNORECASE,
)


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _closest_shop_name_from_row_text(txt: str, href: str) -> str:
    # Try to extract a reasonable retailer display name from the row text
    txt = _clean_text(txt)
    if not txt:
        # fallback to domain
        return urlparse(href).netloc
    # remove the call-to-action text if present
    txt = txt.replace("לצפייה במחירים", "").strip(" -•|")
    # trim long tails
    return txt[:120] if txt else urlparse(href).netloc


def _scroll_page(page, steps: int = 10, pause: float = 0.25):
    for _ in range(steps):
        page.mouse.wheel(0, 2000)
        time.sleep(pause)


def fetch_retailers() -> list[tuple[str, str]]:
    """
    Returns: list of (display_name, portal_url)
    """
    retailers: list[tuple[str, str]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = browser.new_page(locale="he-IL")
        page.goto(GOV_URL, wait_until="networkidle", timeout=120_000)

        # If the section is collapsible, open it
        try:
            section = page.locator("text=טבלת הרשתות הקמעונאיות").first
            if section and section.count():
                section.click(timeout=3_000)
                page.wait_for_timeout(500)
        except Exception:
            pass

        # Ensure lazy content renders
        _scroll_page(page, steps=12, pause=0.2)

        # Strategy A: anchors with the Hebrew CTA
        anchors = page.locator("a:has-text('לצפייה במחירים')")
        count = anchors.count()

        seen = set()
        for i in range(count):
            try:
                a = anchors.nth(i)
                href = a.get_attribute("href")
                if not href:
                    continue
                if not HREF_PATTERN.search(href):
                    # occasionally relative – resolve to absolute in the browser
                    href = page.evaluate("u => new URL(u, location.href).href", href)

                # find some row/nearby text for the name
                row_text = ""
                try:
                    row = a.locator(
                        "xpath=ancestor-or-self::*[self::tr or self::li or self::div][1]"
                    )
                    if row.count():
                        row_text = row.inner_text(timeout=1000)
                except Exception:
                    # fallback to link text
                    row_text = a.inner_text(timeout=1000)

                name = _closest_shop_name_from_row_text(row_text, href)
                key = (name, href)
                if key not in seen:
                    retailers.append(key)
                    seen.add(key)
            except Exception:
                continue

        # Strategy B: any <a href> that matches known portals (some CTAs differ)
        links = page.locator("a[href]")
        for i in range(links.count()):
            try:
                href = links.nth(i).get_attribute("href") or ""
                if not href:
                    continue
                m = HREF_PATTERN.search(href)
                if not m:
                    continue
                if not href.lower().startswith("http"):
                    href = page.evaluate("u => new URL(u, location.href).href", href)

                # use surrounding text for name
                row_text = ""
                try:
                    row = links.nth(i).locator(
                        "xpath=ancestor-or-self::*[self::tr or self::li or self::div][1]"
                    )
                    if row.count():
                        row_text = row.inner_text(timeout=1000)
                except Exception:
                    pass

                name = _closest_shop_name_from_row_text(row_text, href)
                key = (name, href)
                if key not in seen:
                    retailers.append(key)
                    seen.add(key)
            except Exception:
                continue

        # Strategy C (fallback): regex the whole HTML
        try:
            html = page.content()
            for href in set(HREF_PATTERN.findall(html)):
                name = urlparse(href).netloc
                key = (name, href)
                if key not in seen:
                    retailers.append(key)
                    seen.add(key)
        except Exception:
            pass

        browser.close()

    return retailers
