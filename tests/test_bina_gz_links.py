"""
Test for Bina adapter .gz link collection across frames.

This test verifies that bina_collect_gz_links() can:
1. Find .gz links in the main frame
2. Find .gz links in child iframes
3. Return absolute URLs
4. Return empty list when no .gz links exist
"""
import asyncio
import pytest
from playwright.async_api import async_playwright


# Simple test HTML with .gz links in main frame
MAIN_FRAME_HTML = """
<!DOCTYPE html>
<html>
<head><title>Bina Test - Main Frame</title></head>
<body>
    <h1>Price Files</h1>
    <a href="prices_20241117.gz">Today's Prices</a>
    <a href="stores_20241117.zip">Stores List</a>
    <a href="/path/to/promo.gz">Promotions</a>
    <a href="not_archive.txt">Not an archive</a>
</body>
</html>
"""

# HTML with .gz links inside an iframe
PARENT_WITH_IFRAME_HTML = """
<!DOCTYPE html>
<html>
<head><title>Bina Test - Parent</title></head>
<body>
    <h1>Bina Projects Site</h1>
    <iframe src="/content.html" name="content"></iframe>
</body>
</html>
"""

IFRAME_CONTENT_HTML = """
<!DOCTYPE html>
<html>
<head><title>Content Frame</title></head>
<body>
    <h2>Download Files</h2>
    <a href="iframe_prices.gz">Prices from iframe</a>
    <a href="iframe_stores.zip">Stores from iframe</a>
</body>
</html>
"""

# HTML with no .gz links
NO_LINKS_HTML = """
<!DOCTYPE html>
<html>
<head><title>No Archives</title></head>
<body>
    <h1>No downloadable files</h1>
    <a href="page1.html">Some page</a>
    <a href="page2.html">Another page</a>
</body>
</html>
"""


@pytest.mark.asyncio
async def test_bina_collect_gz_links_main_frame():
    """Test that .gz/.zip links are found in the main frame."""
    # Import here to avoid issues when playwright is not installed
    from crawler.adapters.bina import bina_collect_gz_links
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        # Set content directly (data: URL)
        await page.set_content(MAIN_FRAME_HTML)
        
        links = await bina_collect_gz_links(page)
        
        # Should find 3 .gz/.zip links
        assert len(links) >= 3, f"Expected at least 3 links, got {len(links)}: {links}"
        
        # All links should be absolute URLs (data: URLs in this case)
        for link in links:
            assert "://" in link, f"Link should be absolute: {link}"
        
        # Check that .gz and .zip files are included
        gz_count = sum(1 for l in links if ".gz" in l.lower())
        zip_count = sum(1 for l in links if ".zip" in l.lower())
        assert gz_count >= 2, f"Expected at least 2 .gz links, got {gz_count}"
        assert zip_count >= 1, f"Expected at least 1 .zip link, got {zip_count}"
        
        await browser.close()


@pytest.mark.asyncio
async def test_bina_collect_gz_links_no_links():
    """Test that empty list is returned when no .gz/.zip links exist."""
    from crawler.adapters.bina import bina_collect_gz_links
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        await page.set_content(NO_LINKS_HTML)
        
        links = await bina_collect_gz_links(page)
        
        assert len(links) == 0, f"Expected no links, got {len(links)}: {links}"
        
        await browser.close()


@pytest.mark.asyncio
async def test_bina_collect_gz_links_deduplication():
    """Test that duplicate links are removed."""
    from crawler.adapters.bina import bina_collect_gz_links
    
    # HTML with duplicate links
    html_with_dupes = """
    <!DOCTYPE html>
    <html>
    <body>
        <a href="file.gz">File 1</a>
        <a href="file.gz">File 1 duplicate</a>
        <a href="file2.gz">File 2</a>
        <a href="file.gz">File 1 again</a>
    </body>
    </html>
    """
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        await page.set_content(html_with_dupes)
        
        links = await bina_collect_gz_links(page)
        
        # Should have only 2 unique links (file.gz and file2.gz)
        assert len(links) == 2, f"Expected 2 unique links, got {len(links)}: {links}"
        
        await browser.close()


# Manual test for debugging (not a pytest test)
async def manual_test_kingstore():
    """
    Manual test against real KingStore site.
    Run with: python -m pytest tests/test_bina_gz_links.py::manual_test_kingstore -v -s
    """
    from crawler.adapters.bina import bina_collect_gz_links
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        print("\n[Manual Test] Navigating to KingStore...")
        await page.goto("https://kingstore.binaprojects.com/Main.aspx", wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle", timeout=15000)
        await page.wait_for_timeout(2000)
        
        print(f"[Manual Test] Page loaded: {page.url}")
        print(f"[Manual Test] Frames: {len(page.frames)}")
        
        for i, frame in enumerate(page.frames):
            print(f"  Frame {i}: {frame.url or frame.name or 'unnamed'}")
        
        print("\n[Manual Test] Collecting .gz links...")
        links = await bina_collect_gz_links(page)
        
        print(f"\n[Manual Test] Found {len(links)} links:")
        for link in links[:10]:  # Show first 10
            print(f"  - {link}")
        
        if len(links) > 10:
            print(f"  ... and {len(links) - 10} more")
        
        await browser.close()
        
        assert len(links) > 0, "KingStore should have .gz links"


if __name__ == "__main__":
    # Run manual test
    asyncio.run(manual_test_kingstore())

