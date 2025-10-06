# crawler/gov_il.py
from __future__ import annotations
from playwright.sync_api import sync_playwright
from urllib.parse import urlparse
import re
import time

GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"

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
    r"victory[\w\.\-]*",
    r"yenotbitan[\w\.\-]*",
    r"mega[\w\.\-]*",
    r"ramilevi[\w\.\-]*",
]

HREF_RE = re.compile(
    rf'https?://[^\s"\'<>]*?(?:{"|".join(PRICE_PORTAL_PATTERNS)})[^\s"\'<>]*',
    re.IGNORECASE,
)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _name_from_context(text: str, href: str) -> str:
    text = _clean(text).replace("לצפייה במחירים", "").strip(" -•|")
    return text[:120] if text else urlparse(href).netloc


def _scroll(page, steps=12, pause=0.2):
    for _ in range(steps):
        page.mouse.wheel(0, 2000)
        time.sleep(pause)


JS_COLLECT_ANCHORS = """
() => {
  const result = [];
  const seen = new Set();
  function collect(node) {
    if (!node) return;
    // get anchors in this tree
    const as = node.querySelectorAll ? node.querySelectorAll('a[href]') : [];
    as.forEach(a => {
      try {
        const href = new URL(a.getAttribute('href'), window.location.href).href;
        if (!seen.has(href)) {
          const context = (a.innerText || '') + ' ' +
                          (a.closest('tr,li,div')?.innerText || '');
          result.push({ href, context });
          seen.add(href);
        }
      } catch (e) {}
    });
    // walk shadow roots
    if (node.shadowRoot) collect(node.shadowRoot);
    // walk children
    if (node.children) [...node.children].forEach(collect);
  }
  collect(document);
  // iframes
  const iframes = document.querySelectorAll('iframe');
  for (const f of iframes) {
    try {
      const doc = f.contentDocument;
      if (doc) {
        const as = doc.querySelectorAll('a[href]');
        as.forEach(a => {
          try {
            const href = new URL(a.getAttribute('href'), document.location.href).href;
            if (!seen.has(href)) {
              const context = (a.innerText || '') + ' ' +
                              (a.closest('tr,li,div')?.innerText || '');
              result.push({ href, context });
              seen.add(href);
            }
          } catch (e) {}
        });
      }
    } catch (e) {}
  }
  return result;
}
"""


def fetch_retailers() -> list[tuple[str, str]]:
    retailers: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = browser.new_page(locale="he-IL")

        # capture any URLs that appear in network responses too
        sniffed = set()

        def on_response(resp):
            try:
                url = resp.url
                if HREF_RE.search(url):
                    sniffed.add(url)
                # Sometimes JSON/HTML bodies include links; limit size
                if resp.request.resource_type in {
                    "xhr",
                    "fetch",
                    "document",
                } and resp.status in (200, 304):
                    if "text" in (resp.headers.get("content-type", "")):
                        body = resp.text()[:200000]
                        for m in HREF_RE.findall(body or ""):
                            sniffed.add(m)
            except Exception:
                pass

        page.on("response", on_response)

        page.goto(GOV_URL, wait_until="networkidle", timeout=120_000)

        # try expanding a collapsible section, if present
        try:
            btn = page.locator("text=טבלת הרשתות הקמעונאיות").first
            if btn and btn.count():
                btn.click(timeout=3000)
                page.wait_for_timeout(500)
        except Exception:
            pass

        _scroll(page)

        # Collect anchors across DOM + shadow roots + iframes
        anchors = page.evaluate(JS_COLLECT_ANCHORS) or []
        for a in anchors:
            href = a.get("href") or ""
            if not HREF_RE.search(href):
                continue
            name = _name_from_context(a.get("context") or "", href)
            key = (name, href)
            if key not in seen:
                retailers.append(key)
                seen.add(key)

        # Add sniffed network URLs
        for href in sniffed:
            name = urlparse(href).netloc
            key = (name, href)
            if key not in seen:
                retailers.append(key)
                seen.add(key)

        # As a last resort, regex the final HTML
        try:
            html = page.content()
            for href in set(HREF_RE.findall(html or "")):
                name = urlparse(href).netloc
                key = (name, href)
                if key not in seen:
                    retailers.append(key)
                    seen.add(key)
        except Exception:
            pass

        browser.close()

    return retailers
