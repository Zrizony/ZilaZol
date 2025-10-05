# crawler/gov_il.py
from __future__ import annotations

import asyncio
import re
from typing import List, Tuple, Optional, Dict
from urllib.parse import urljoin, urlparse

from playwright.async_api import Page, TimeoutError as PwTimeoutError
from tenacity import retry, stop_after_attempt, wait_fixed

from .logger import logger

GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"
HEBREW_VIEW_TEXTS = (
    "לצפייה במחירים",
    "צפייה במחירים",
    "לצפייה",
    "לצפיה במחירים",
)
NEXT_PAGE_TEXTS = ("הבא", "לעמוד הבא", "Next")
PUBLISHED_PRICES_HOST = "url.publishedprices.co.il"


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _looks_like_view_prices(text: str) -> bool:
    t = _norm_space(text)
    return any(key in t for key in HEBREW_VIEW_TEXTS)


def _looks_like_next(text: str) -> bool:
    t = _norm_space(text)
    return any(key in t for key in NEXT_PAGE_TEXTS)


def _is_meaningful_name(s: str) -> bool:
    s = _norm_space(s)
    if not s:
        return False
    # avoid super short / boilerplate strings
    if len(s) < 2:
        return False
    if any(x in s for x in ("לצפייה", "לצפיה", "קישור", "הורדה", "מחיר")):
        return False
    return True


def _closest_text_for_anchor() -> str:
    """
    JS helper: from an <a>, walk up a little to collect a reasonable title/name.
    Prefer row/card/container headers; fallback to nearest preceding text.
    """
    return """
    (a) => {
      const isStr = v => typeof v === 'string' && v.trim().length > 0;
      const norm = s => (s || '').replace(/\\s+/g,' ').trim();

      // Try direct 'data-name' or aria labels first
      let candidate = a.getAttribute('data-name') || a.getAttribute('aria-label');
      if (isStr(candidate)) return norm(candidate);

      // Try siblings in the same container
      const maxDepth = 5;
      let node = a;
      for (let depth=0; depth<maxDepth && node; depth++) {
        // Headers are usually good names
        const header = node.querySelector('h1,h2,h3,h4,h5,h6,.card-title,.gov-title');
        if (header && isStr(header.textContent)) return norm(header.textContent);

        // table row context
        const row = node.closest('tr');
        if (row) {
          // take the first non-empty cell text (excluding the link cell)
          const tds = Array.from(row.querySelectorAll('td,th'));
          for (const td of tds) {
            if (td.contains(a)) continue;
            const txt = norm(td.textContent || '');
            if (txt && txt.length > 1) return txt;
          }
        }

        // card-like container
        const card = node.closest('.card, .gov-card, .result, .item, .media-body');
        if (card) {
          // look for a labeled title inside card
          const cand = card.querySelector('[data-name], .title, .gov-title, strong, b');
          if (cand && isStr(cand.textContent)) return norm(cand.textContent);

          // else take the most texty block
          const blocks = Array.from(card.querySelectorAll('div, p, span')).map(el => norm(el.textContent || ''));
          const best = blocks.sort((a,b) => b.length - a.length)[0];
          if (isStr(best) && best.length > 1) return best;
        }

        node = node.parentElement;
      }

      // As a last resort, try previous siblings
      let prev = a.previousElementSibling;
      while (prev) {
        const txt = norm(prev.textContent || '');
        if (txt && txt.length > 1) return txt;
        prev = prev.previousElementSibling;
      }
      return '';
    }
    """


def _normalize_portal_url(href: str, base: str) -> str:
    try:
        abs_url = urljoin(base, href)
        u = urlparse(abs_url)
        if u.netloc.lower() == PUBLISHED_PRICES_HOST and (u.path == "" or u.path == "/"):
            # Make it explicit for published-prices tenants
            return f"https://{PUBLISHED_PRICES_HOST}/login"
        return abs_url
    except Exception:
        return href


async def _extract_from_dom(page: Page) -> List[Tuple[str, str]]:
    """
    Strategy A: native DOM query for anchors that *look like* 'view prices',
    then derive a display name from the surrounding structure.
    """
    anchors: List[str] = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({href:a.getAttribute('href'), text:(a.innerText||'').trim()}))",
    )

    out: List[Tuple[str, str]] = []
    used: set[str] = set()

    for item in anchors:
        href = item.get("href") or ""
        text = item.get("text") or ""
        if not href:
            continue
        if not _looks_like_view_prices(text):
            # Some sites put the text in aria-label or nested span; peek again
            try:
                is_view = await page.evaluate(
                    """(href, keys) => {
                        const a = [...document.querySelectorAll('a[href]')].find(x => x.getAttribute('href') === href);
                        if (!a) return false;
                        const t = (a.innerText || a.textContent || '').replace(/\\s+/g,' ').trim();
                        if (keys.some(k => t.includes(k))) return true;
                        const aria = (a.getAttribute('aria-label') || '').trim();
                        if (keys.some(k => aria.includes(k))) return true;
                        const inside = (a.querySelector('*')?.innerText || '').replace(/\\s+/g,' ').trim();
                        return keys.some(k => inside.includes(k));
                    }""",
                    href,
                    list(HEBREW_VIEW_TEXTS),
                )
            except Exception:
                is_view = False
            if not is_view:
                continue

        # figure a good display name
        try:
            name = await page.evaluate(_closest_text_for_anchor(),)
            if not name:
                # feed anchor again for the helper to use
                name = await page.evaluate(
                    """(href, helper) => {
                        const a = [...document.querySelectorAll('a[href]')].find(x => x.getAttribute('href') === href);
                        if (!a) return '';
                        return (new Function('return ' + helper))()(a);
                    }""",
                    href,
                    _closest_text_for_anchor().strip(),
                )
        except Exception:
            name = ""

        name = _norm_space(name)
        if not _is_meaningful_name(name):
            # fallback: strip the link text itself of boilerplate
            name = _norm_space(re.sub("|".join(map(re.escape, HEBREW_VIEW_TEXTS)), "", text))

        abs_url = _normalize_portal_url(href, page.url)
        key = f"{name}|{abs_url}"
        if _is_meaningful_name(name) and key not in used:
            used.add(key)
            out.append((name, abs_url))

    return out


async def _extract_by_scanning_all_links(page: Page) -> List[Tuple[str, str]]:
    """
    Strategy B: scan all <a> and heuristically pick likely retailer links
    (e.g. hosts that look like known portals), then infer names from nearby text.
    """
    anchors: List[Dict[str, str]] = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({href:a.getAttribute('href'), text:(a.innerText||'').trim()}))",
    )

    candidates: List[Tuple[str, str]] = []
    used: set[str] = set()
    for item in anchors:
        href = item.get("href") or ""
        if not href:
            continue
        abs_url = _normalize_portal_url(href, page.url)
        host = urlparse(abs_url).netloc.lower()

        # Heuristic: likely retailer portal domains
        if any(k in host for k in ("binaprojects", "publishedprices", "quik", "shufersal", "ramilevi", "prices")):
            # Get name via proximity
            try:
                name = await page.evaluate(_closest_text_for_anchor(),)
            except Exception:
                name = ""
            name = _norm_space(name) or _norm_space(item.get("text") or "")
            if not _is_meaningful_name(name):
                continue
            key = f"{name}|{abs_url}"
            if key not in used:
                used.add(key)
                candidates.append((name, abs_url))

    return candidates


async def _maybe_click_next(page: Page) -> bool:
    # Some list pages paginate; try to click "next" if exists
    for text in NEXT_PAGE_TEXTS:
        try:
            btn = page.get_by_role("link", name=text, exact=False)
            if await btn.count():
                await btn.first.click()
                await page.wait_for_load_state("networkidle", timeout=8000)
                return True
        except Exception:
            pass

    # try via contains text selector
    for text in NEXT_PAGE_TEXTS:
        try:
            locator = page.locator(f"xpath=//a[contains(., '{text}')]")
            if await locator.count():
                await locator.first.click()
                await page.wait_for_load_state("networkidle", timeout=8000)
                return True
        except Exception:
            pass

    return False


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def _extract_retailers_single_page(page: Page) -> List[Tuple[str, str]]:
    # Strategy A
    items = await _extract_from_dom(page)
    if items:
        return items

    # Strategy B
    items = await _extract_by_scanning_all_links(page)
    return items


async def discover_retailers(page: Page) -> List[Tuple[str, str]]:
    """
    Public API:
      Navigates to the gov.il page and returns a list of (display_name, portal_url).
    """
    logger.info("gov_il: Navigating to %s", GOV_URL)
    try:
        await page.goto(GOV_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=10000)
    except PwTimeoutError:
        logger.warning("gov_il: initial load timeout, continuing with whatever loaded")

    all_found: List[Tuple[str, str]] = []
    seen: set[str] = set()

    page_no = 1
    while True:
        logger.info("gov_il: extracting page #%s", page_no)
        try:
            items = await _extract_retailers_single_page(page)
        except Exception as e:
            logger.exception("gov_il: extraction error on page #%s: %s", page_no, e)
            items = []

        # de-dup and collect
        added = 0
        for name, url in items:
            key = f"{name}|{url}"
            if key not in seen:
                seen.add(key)
                all_found.append((name, url))
                added += 1
        logger.info("gov_il: page #%s found %d items (total=%d)", page_no, added, len(all_found))

        # Try next page (if exists)
        try_next = await _maybe_click_next(page)
        if not try_next:
            break
        page_no += 1

        # safety stop to avoid loops
        if page_no > 10:
            logger.warning("gov_il: reached pagination safety limit")
            break

    # Final sanity: keep only distinct names/urls and good names
    final: List[Tuple[str, str]] = []
    used: set[str] = set()
    for name, url in all_found:
        name = _norm_space(name)
        url = _norm_space(url)
        if not _is_meaningful_name(name):
            continue
        key = f"{name}|{url}"
        if key not in used:
            used.add(key)
            final.append((name, url))

    logger.info("gov_il: discovered_retailers_count=%d", len(final))
    return final
