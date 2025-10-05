# crawler/gov_il.py
from playwright.sync_api import sync_playwright
import re

GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"


def fetch_retailers():
    retailers = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()
        page.goto(GOV_URL, wait_until="networkidle", timeout=120000)

        # Click the "טבלת הרשתות הקמעונאיות" section if collapsed
        try:
            if page.locator("text=טבלת הרשתות הקמעונאיות").count():
                page.locator("text=טבלת הרשתות הקמעונאיות").first.click()
        except Exception:
            pass

        # Wait for buttons containing לצפייה במחירים
        buttons = page.locator("a:has-text('לצפייה במחירים')")
        count = buttons.count()

        for i in range(count):
            try:
                text = buttons.nth(i).text_content().strip()
                href = buttons.nth(i).get_attribute("href")
                if href and re.search(r"https?://", href):
                    retailers.append((text, href))
            except Exception:
                continue

        browser.close()

    return retailers
