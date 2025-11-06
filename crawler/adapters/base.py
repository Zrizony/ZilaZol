# crawler/adapters/base.py
from __future__ import annotations
from typing import List, Optional

from playwright.async_api import Page

from ..constants import DEFAULT_DOWNLOAD_SUFFIXES
from ..utils import looks_like_price_file


async def collect_links_on_page(page: Page, patterns: Optional[List[str]] = None) -> List[str]:
    """Collect download links with broader filters for generic sites."""
    # Expanded selectors for better link discovery
    selectors = [
        "a[download]",
        "a[href*='download']",
        "a[href*='file']",
        "a[href$='.xml' i]",
        "a[href$='.gz' i]",
        "a[href$='.zip' i]",
        "a[href*='.xml?' i]",
        "a[href*='.gz?' i]",
        "a[href*='.zip?' i]",
    ]
    # Build suffix selectors from patterns
    pat = [p.lower() for p in (patterns or DEFAULT_DOWNLOAD_SUFFIXES)]
    for p in pat:
        selectors.append(f"a[href$='{p}' i]")
        selectors.append(f"a[href*='{p}?' i]")

    hrefs = set()
    for sel in selectors:
        if await page.locator(sel).count():
            vals = await page.eval_on_selector_all(sel, "els => els.map(a => a.href)")
            for h in (vals or []):
                if h and (looks_like_price_file(h) or h.lower().endswith(tuple(pat))):
                    hrefs.add(h)
    
    return sorted(hrefs)

