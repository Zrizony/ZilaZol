#!/usr/bin/env python3
"""
Victory Online Image Downloader

Downloads product images from Victory Online website and saves them with product names as filenames.

Usage:
    python scripts/victory_scraper.py [--output-dir DIR] [--category-url URL]

Requirements:
    - playwright library (pip install playwright)
    - requests library (pip install requests)
"""

import asyncio
import re
import sys
from pathlib import Path
from playwright.async_api import async_playwright
import requests

# Fix Windows console encoding issues
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')


def sanitize_filename(name: str) -> str:
    """Sanitize product name for use as filename"""
    # Remove or replace invalid filename characters
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Remove leading/trailing spaces and dots
    name = name.strip(' .')
    # Limit length
    if len(name) > 200:
        name = name[:200]
    # Remove multiple consecutive underscores
    name = re.sub(r'_+', '_', name)
    return name or "product"


async def discover_categories() -> list:
    """Discover all category URLs from Victory website"""
    print("🔍 Discovering categories from Victory website...")
    category_urls = []
    
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="he-IL",
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()
        
        try:
            await page.goto("https://www.victoryonline.co.il/", wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(3000)
            
            # Extract category links
            category_links = await page.evaluate("""
                () => {
                    const categories = new Set();
                    const links = document.querySelectorAll('a[href*="/categories/"]');
                    links.forEach(link => {
                        const href = link.href || link.getAttribute('href');
                        if (href) {
                            const match = href.match(/\\/categories\\/(\\d+)(?:\\/products)?/);
                            if (match) {
                                const categoryId = match[1];
                                categories.add(`https://www.victoryonline.co.il/categories/${categoryId}/products`);
                            }
                        }
                    });
                    return Array.from(categories);
                }
            """)
            
            category_urls.extend(category_links)
            print(f"   ✅ Found {len(category_urls)} categories")
            
        except Exception as e:
            print(f"   ⚠️  Error discovering categories: {e}")
        finally:
            await browser.close()
    
    # Add known categories as fallback
    if len(category_urls) < 10:
        known_categories = [
            "79706", "79707", "79708", "79709", "79710",
            "79711", "79712", "79713", "79714", "79715",
            "79716", "79717", "79718", "79719"
        ]
        for cat_id in known_categories:
            cat_url = f"https://www.victoryonline.co.il/categories/{cat_id}/products"
            if cat_url not in category_urls:
                category_urls.append(cat_url)
        print(f"   📋 Added {len(known_categories)} known categories")
    
    return list(set(category_urls))


async def download_images_from_category(category_url: str, output_dir: Path, downloaded_urls: set) -> int:
    """Download images from a single category page"""
    count = 0
    
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="he-IL",
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()
        
        try:
            await page.goto(category_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            
            # Extract product images and names from DOM
            product_images = await page.evaluate("""
                () => {
                    const products = [];
                    const productContainers = document.querySelectorAll(
                        '[class*="product"], [data-product-id], [class*="Product"], [class*="item"]'
                    );
                    
                    productContainers.forEach(container => {
                        // Extract product name
                        const nameSelectors = [
                            '.product-name', '.name', 'h3', 'h4', '[class*="name"]',
                            '[data-product-name]', '[class*="Name"]', '[class*="title"]'
                        ];
                        
                        let productName = '';
                        for (const selector of nameSelectors) {
                            const nameEl = container.querySelector(selector);
                            if (nameEl) {
                                productName = nameEl.textContent?.trim() || 
                                             nameEl.getAttribute('data-product-name') || 
                                             nameEl.getAttribute('title') || '';
                                if (productName && productName.length > 2) break;
                            }
                        }
                        
                        // Fallback: get first meaningful text line
                        if (!productName || productName.length < 3) {
                            const containerText = container.textContent?.trim() || '';
                            const lines = containerText.split('\\n').map(l => l.trim()).filter(l => l.length > 2);
                            if (lines.length > 0 && lines[0].length < 100) {
                                productName = lines[0];
                            }
                        }
                        
                        // Find images in container
                        const images = container.querySelectorAll('img[src*="cloudfront"]');
                        images.forEach(img => {
                            const src = img.src || img.getAttribute('data-src') || 
                                       img.getAttribute('data-lazy-src') || 
                                       img.getAttribute('srcset')?.split(',')[0]?.trim().split(' ')[0];
                            
                            if (src && src.includes('cloudfront')) {
                                // Skip UI elements
                                const srcLower = src.toLowerCase();
                                if (srcLower.includes('icon') || srcLower.includes('logo') || 
                                    srcLower.includes('loading') || srcLower.includes('arrow') ||
                                    srcLower.includes('trash') || srcLower.includes('cart')) {
                                    return;
                                }
                                
                                products.push({
                                    imageUrl: src,
                                    productName: productName || ''
                                });
                            }
                        });
                    });
                    
                    return products;
                }
            """)
            
            # Scroll to load more products
            print(f"      📜 Scrolling to load all products...")
            previous_count = len(product_images)
            no_new_count = 0
            
            for _ in range(20):  # Max 20 scrolls
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)
                
                # Re-extract after scroll
                more_products = await page.evaluate("""
                    () => {
                        const products = [];
                        const containers = document.querySelectorAll(
                            '[class*="product"], [data-product-id], [class*="Product"], [class*="item"]'
                        );
                        
                        containers.forEach(container => {
                            const nameSelectors = ['.product-name', '.name', 'h3', 'h4', '[class*="name"]'];
                            let productName = '';
                            for (const selector of nameSelectors) {
                                const nameEl = container.querySelector(selector);
                                if (nameEl) {
                                    productName = nameEl.textContent?.trim() || '';
                                    if (productName && productName.length > 2) break;
                                }
                            }
                            
                            const images = container.querySelectorAll('img[src*="cloudfront"]');
                            images.forEach(img => {
                                const src = img.src || img.getAttribute('data-src');
                                if (src && src.includes('cloudfront')) {
                                    const srcLower = src.toLowerCase();
                                    if (!srcLower.includes('icon') && !srcLower.includes('logo') &&
                                        !srcLower.includes('loading') && !srcLower.includes('arrow')) {
                                        products.push({
                                            imageUrl: src,
                                            productName: productName || ''
                                        });
                                    }
                                }
                            });
                        });
                        return products;
                    }
                """)
                
                # Merge unique products
                seen_urls = {p['imageUrl'] for p in product_images}
                for p in more_products:
                    if p['imageUrl'] not in seen_urls:
                        product_images.append(p)
                        seen_urls.add(p['imageUrl'])
                
                current_count = len(product_images)
                if current_count > previous_count:
                    print(f"         📦 Found {current_count} products so far...")
                    previous_count = current_count
                    no_new_count = 0
                else:
                    no_new_count += 1
                    if no_new_count >= 3:
                        break
            
            # Download images
            print(f"      📥 Downloading {len(product_images)} images...")
            for product in product_images:
                image_url = product['imageUrl']
                product_name = product['productName']
                
                if image_url in downloaded_urls:
                    continue
                
                # Skip if no product name
                if not product_name or len(product_name) < 3:
                    # Try to extract from URL as fallback
                    url_parts = image_url.split('/')
                    for part in reversed(url_parts):
                        if part and len(part) > 8 and any(c.isdigit() for c in part):
                            product_name = part.split('?')[0].split('.')[0]
                            break
                    if not product_name or len(product_name) < 3:
                        product_name = f"product_{image_url.split('/')[-1].split('?')[0].split('.')[0]}"
                
                # Create filename
                filename = sanitize_filename(product_name) + ".jpg"
                
                # Handle duplicates
                counter = 1
                original_filename = filename
                while (output_dir / filename).exists():
                    filename = f"{sanitize_filename(product_name)}_{counter}.jpg"
                    counter += 1
                    if counter > 1000:
                        filename = original_filename.replace('.jpg', f'_{image_url.split("/")[-1][:10]}.jpg')
                        break
                
                local_path = output_dir / filename
                
                try:
                    # Download image
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Referer": "https://www.victoryonline.co.il/",
                        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                    }
                    response = requests.get(image_url, headers=headers, timeout=10)
                    response.raise_for_status()
                    
                    # Verify it's an image
                    if len(response.content) < 2000:  # Too small, probably not a product image
                        continue
                    
                    # Save file
                    with open(local_path, 'wb') as f:
                        f.write(response.content)
                    
                    downloaded_urls.add(image_url)
                    count += 1
                    print(f"         📸 {product_name[:50]} -> {filename}")
                    
                except Exception as e:
                    print(f"         ⚠️  Failed to download {product_name[:30]}: {e}")
            
        except Exception as e:
            print(f"      ⚠️  Error processing category: {e}")
        finally:
            await browser.close()
    
    return count


async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Download Victory Online product images")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for images (default: victory_images/)"
    )
    parser.add_argument(
        "--category-url",
        type=str,
        default=None,
        help="Download from specific category URL (skips discovery)"
    )
    
    args = parser.parse_args()
    
    # Set output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(__file__).parent.parent / "victory_images"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Output directory: {output_dir}")
    
    # Get category URLs
    if args.category_url:
        category_urls = [args.category_url]
        print(f"📂 Using provided category URL: {args.category_url}")
    else:
        category_urls = await discover_categories()
        print(f"📂 Found {len(category_urls)} categories to process")
    
    # Download images from each category
    downloaded_urls = set()
    total_downloaded = 0
    
    for idx, category_url in enumerate(category_urls, 1):
        print(f"\n📂 Category {idx}/{len(category_urls)}: {category_url}")
        count = await download_images_from_category(category_url, output_dir, downloaded_urls)
        total_downloaded += count
    
    print(f"\n🎉 Download complete!")
    print(f"   ✅ Downloaded: {total_downloaded} images")
    print(f"   📁 Output directory: {output_dir}")


if __name__ == "__main__":
    asyncio.run(main())
