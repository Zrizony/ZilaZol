# crawler/gov_il.py
from __future__ import annotations
import asyncio
import os
from typing import List, Tuple

from playwright.async_api import async_playwright

GOV_URL = os.getenv("GOV_URL", "https://www.gov.il/he/pages/cpfta_prices_regulations")

# Hebrew text on the button; we match loosely to be resilient
BUTTON_TEXT_CANDIDATES = [
    "לצפייה במחירים",
    "צפייה במחירים",
    "לצפיה במחירים",
    "לצפייה",  # very loose fallback
]


async def _discover_with_playwright() -> List[Tuple[str, str]]:
    """
    Uses Playwright to load the page, find the retailers table and extract
    (display_name, portal_url) pairs from rows that contain the 'לצפייה במחירים' link.
    """
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        ctx = await browser.new_context(locale="he-IL")
        page = await ctx.new_page()

        await page.goto(GOV_URL, wait_until="domcontentloaded", timeout=60_000)
        # Wait for any table to appear; the page sometimes loads slowly
        try:
            await page.wait_for_selector("table", timeout=20_000)
        except Exception:
            # Still try to scan the DOM even if the explicit wait failed
            pass

        # Strategy:
        # 1) Find all anchors inside any table whose text includes the button text
        # 2) For each anchor, walk up to its row <tr> and grab a sensible retailer name
        # 3) Build absolute link via new URL(href, location.href)
        script = f"""
(() => {{
  const textMatches = {BUTTON_TEXT_CANDIDATES!r};
  const anchors = Array.from(document.querySelectorAll("table a, table button, table [role='link']"));
  const items = [];
  const norm = s => (s || "").replace(/\\s+/g, " ").trim();

  for (const el of anchors) {{
    const t = norm(el.textContent);
    if (!t) continue;
    const hit = textMatches.some(x => t.includes(x));
    if (!hit) continue;

    // find the row
    const row = el.closest("tr");
    if (!row) continue;

    // choose retailer name: prefer first non-empty cell, otherwise row text
    let name = "";
    const tds = Array.from(row.querySelectorAll("td,th"));
    for (const td of tds) {{
      const cell = norm(td.innerText || td.textContent || "");
      if (cell && cell.length >= 2) {{ name = cell; break; }}
    }}
    if (!name) name = norm(row.innerText || row.textContent || "");

    // extract absolute href (buttons sometimes carry data-href)
    let href = el.getAttribute("href") || el.getAttribute("data-href") || "";
    try {{
      href = new URL(href, location.href).href;
    }} catch (e) {{
      continue;
    }}
    if (!href) continue;

    items.push({{ name, href }});
  }}

  // Deduplicate by href
  const seen = new Set();
  const out = [];
  for (const it of items) {{
    if (seen.has(it.href)) continue;
    seen.add(it.href);
    out.push(it);
  }}
  return out;
}})();
        """

        results = await page.evaluate(script)
        await ctx.close()
        await browser.close()

        # Normalize to (display_name, portal_url)
        out: List[Tuple[str, str]] = []
        for r in results or []:
            name = (r.get("name") or "").strip()
            href = (r.get("href") or "").strip()
            if name and href:
                out.append((name, href))

        return out


def fetch_retailers() -> List[Tuple[str, str]]:
    """
    Synchronous wrapper used by Flask/app. Returns [(display_name, portal_url), ...]
    If anything fails, returns an empty list (the caller will log and handle).
    """
    try:
        return asyncio.run(_discover_with_playwright())
    except RuntimeError:
        # If we're already inside an event loop (e.g., unit tests),
        # fall back to a nested run.
        return asyncio.get_event_loop().run_until_complete(_discover_with_playwright())
    except Exception:
        return []
