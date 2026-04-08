"""
intel/intel_pokemon_center_uk.py
─────────────────────────────────
Scraper adapter for Pokémon Center UK.
Handles: product pages, preorder detection, sold out states.

PC UK is the highest priority retailer — exclusives and early releases.
"""

import re
import json
import sys
from bs4 import BeautifulSoup
from intel_base_scraper import BaseScraper, ScrapeResult, StockState


class PokemonCenterUKScraper(BaseScraper):
    retailer_slug = "pokemon-center-uk"
    retailer_name = "Pokémon Center UK"

    # PC UK uses Cloudflare — needs convincing browser headers
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.google.com/",
    }

    def parse(self, html: str, url: str) -> ScrapeResult:
        result = ScrapeResult(url=url)
        soup = BeautifulSoup(html, "html.parser")
        detected = []

        # ── Title ──────────────────────────────────────────────────────────────
        title_el = soup.select_one("h1.product-name, h1[data-testid='product-name'], h1")
        if title_el:
            result.raw_title = title_el.get_text(strip=True)

        # ── Price ──────────────────────────────────────────────────────────────
        price_el = soup.select_one("[data-testid='product-price'], .product-price, .price-value, [class*='price']")
        if price_el:
            price_text = price_el.get_text(strip=True)
            price_match = re.search(r"[\d]+\.[\d]{2}", price_text.replace(",", ""))
            if price_match:
                result.price = float(price_match.group())

        # ── Stock state detection ──────────────────────────────────────────────
        page_text = soup.get_text(" ", strip=True).lower()
        detected_text = []

        # Check key button states
        buttons = soup.select("button, [role='button'], input[type='submit']")
        for btn in buttons:
            btn_text = btn.get_text(strip=True).lower()
            if btn_text:
                detected_text.append(btn_text)

        # Check common stock indicators
        stock_indicators = soup.select("[data-testid*='stock'], [class*='stock'], [class*='availability'], [class*='inventory']")
        for el in stock_indicators:
            detected_text.append(el.get_text(strip=True).lower())

        result.detected_text = list(set(detected_text))
        detected_joined = " ".join(detected_text + [page_text[:2000]])

        # ── Cloudflare / bot protection detection ─────────────────────────────
        if any(x in page_text for x in ["pardon our interruption", "checking your browser", "enable javascript and cookies", "cf-browser-verification", "ray id"]):
            result.stock_state = StockState.UNKNOWN
            result.parser_confidence = 0.10
            result.error = "Cloudflare bot protection detected — try again or use Playwright"
            return result

        # ── State logic (order matters — most specific first) ──────────────────

        if any(x in detected_joined for x in ["out of stock", "sold out", "unavailable"]):
            result.stock_state = StockState.SOLD_OUT
            result.parser_confidence = 0.90

        elif any(x in detected_joined for x in ["add to bag", "add to basket", "add to cart", "buy now"]):
            result.stock_state = StockState.IN_STOCK
            result.preorder_state = False
            result.parser_confidence = 0.95

            # Check if it says "low stock" alongside add to bag
            if any(x in detected_joined for x in ["only", "left", "hurry", "limited"]):
                result.stock_state = StockState.LOW_STOCK
                result.parser_confidence = 0.85

        elif any(x in detected_joined for x in ["pre-order", "preorder", "pre order"]):
            result.stock_state = StockState.PREORDER_OPEN
            result.preorder_state = True
            result.parser_confidence = 0.92

        elif any(x in detected_joined for x in ["notify me", "email me", "get notified", "notify when"]):
            result.stock_state = StockState.NOTIFY_ME
            result.parser_confidence = 0.88

        elif any(x in detected_joined for x in ["coming soon", "releasing", "available"]):
            result.stock_state = StockState.COMING_SOON
            result.parser_confidence = 0.75

        elif "404" in page_text or "page not found" in page_text:
            result.stock_state = StockState.DISCONTINUED
            result.parser_confidence = 0.80

        else:
            result.stock_state = StockState.UNKNOWN
            result.parser_confidence = 0.30

        # ── Structured data fallback ───────────────────────────────────────────
        # PC UK often embeds availability in JSON-LD
        for script in soup.select("script[type='application/ld+json']"):
            try:
                data = json.loads(script.string or "{}")
                if isinstance(data, list):
                    data = data[0]
                avail = data.get("offers", {})
                if isinstance(avail, list):
                    avail = avail[0]
                avail_url = avail.get("availability", "")
                if "InStock" in avail_url:
                    result.stock_state = StockState.IN_STOCK
                    result.parser_confidence = 0.98
                elif "OutOfStock" in avail_url:
                    result.stock_state = StockState.SOLD_OUT
                    result.parser_confidence = 0.98
                elif "PreOrder" in avail_url:
                    result.stock_state = StockState.PREORDER_OPEN
                    result.preorder_state = True
                    result.parser_confidence = 0.98
                if not result.price and avail.get("price"):
                    result.price = float(avail["price"])
                if not result.raw_title and data.get("name"):
                    result.raw_title = data["name"]
            except Exception:
                pass

        return result


class SmythsScraper(BaseScraper):
    retailer_slug = "smyths"
    retailer_name = "Smyths Toys"

    def parse(self, html: str, url: str) -> ScrapeResult:
        result = ScrapeResult(url=url)
        soup = BeautifulSoup(html, "html.parser")

        # Title
        title_el = soup.select_one("h1.pdpProductTitle, h1.product-name, h1")
        if title_el:
            result.raw_title = title_el.get_text(strip=True)

        # Price
        price_el = soup.select_one(".pdpPrice, .product-price, [class*='price']")
        if price_el:
            price_text = price_el.get_text(strip=True)
            m = re.search(r"[\d]+\.[\d]{2}", price_text.replace(",", ""))
            if m:
                result.price = float(m.group())

        page_text = soup.get_text(" ", strip=True).lower()

        # JSON-LD first (most reliable for Smyths)
        for script in soup.select("script[type='application/ld+json']"):
            try:
                data = json.loads(script.string or "{}")
                if isinstance(data, list): data = data[0]
                offers = data.get("offers", {})
                if isinstance(offers, list): offers = offers[0]
                avail = offers.get("availability", "")
                if "InStock" in avail:
                    result.stock_state = StockState.IN_STOCK
                    result.parser_confidence = 0.95
                elif "OutOfStock" in avail:
                    result.stock_state = StockState.SOLD_OUT
                    result.parser_confidence = 0.95
                elif "PreOrder" in avail:
                    result.stock_state = StockState.PREORDER_OPEN
                    result.preorder_state = True
                    result.parser_confidence = 0.95
                if not result.price and offers.get("price"):
                    result.price = float(offers["price"])
                if result.stock_state != StockState.UNKNOWN:
                    return result
            except Exception:
                pass

        # Text fallback
        if any(x in page_text for x in ["add to basket", "add to trolley", "buy now"]):
            result.stock_state = StockState.IN_STOCK
            result.parser_confidence = 0.85
        elif any(x in page_text for x in ["out of stock", "sold out"]):
            result.stock_state = StockState.SOLD_OUT
            result.parser_confidence = 0.85
        elif "pre-order" in page_text or "preorder" in page_text:
            result.stock_state = StockState.PREORDER_OPEN
            result.preorder_state = True
            result.parser_confidence = 0.80
        elif "notify me" in page_text:
            result.stock_state = StockState.NOTIFY_ME
            result.parser_confidence = 0.75
        else:
            result.stock_state = StockState.UNKNOWN
            result.parser_confidence = 0.30

        return result


class ChaosCardsScraper(BaseScraper):
    retailer_slug = "chaos-cards"
    retailer_name = "Chaos Cards"

    def parse(self, html: str, url: str) -> ScrapeResult:
        result = ScrapeResult(url=url)
        soup = BeautifulSoup(html, "html.parser")

        title_el = soup.select_one("h1.product_title, h1.entry-title, h1")
        if title_el:
            result.raw_title = title_el.get_text(strip=True)

        price_el = soup.select_one(".price .amount, .woocommerce-Price-amount, [class*='price']")
        if price_el:
            m = re.search(r"[\d]+\.[\d]{2}", price_el.get_text(strip=True).replace(",", ""))
            if m:
                result.price = float(m.group())

        page_text = soup.get_text(" ", strip=True).lower()

        # Chaos Cards uses WooCommerce
        if soup.select_one(".single_add_to_cart_button:not(.disabled)"):
            result.stock_state = StockState.IN_STOCK
            result.parser_confidence = 0.90
        elif soup.select_one(".single_add_to_cart_button.disabled") or "out of stock" in page_text:
            result.stock_state = StockState.SOLD_OUT
            result.parser_confidence = 0.88
        elif "pre-order" in page_text or "preorder" in page_text:
            result.stock_state = StockState.PREORDER_OPEN
            result.preorder_state = True
            result.parser_confidence = 0.85
        elif "notify" in page_text:
            result.stock_state = StockState.NOTIFY_ME
            result.parser_confidence = 0.75
        else:
            result.stock_state = StockState.UNKNOWN
            result.parser_confidence = 0.30

        return result


class MagicMadhouseScraper(BaseScraper):
    retailer_slug = "magic-madhouse"
    retailer_name = "Magic Madhouse"

    def parse(self, html: str, url: str) -> ScrapeResult:
        result = ScrapeResult(url=url)
        soup = BeautifulSoup(html, "html.parser")

        title_el = soup.select_one("h1.product-name, h1.product_title, h1")
        if title_el:
            result.raw_title = title_el.get_text(strip=True)

        price_el = soup.select_one(".price, .product-price, [class*='price']")
        if price_el:
            m = re.search(r"[\d]+\.[\d]{2}", price_el.get_text(strip=True).replace(",", ""))
            if m:
                result.price = float(m.group())

        page_text = soup.get_text(" ", strip=True).lower()

        if any(x in page_text for x in ["add to basket", "add to cart", "buy now"]):
            result.stock_state = StockState.IN_STOCK
            result.parser_confidence = 0.85
        elif "out of stock" in page_text or "sold out" in page_text:
            result.stock_state = StockState.SOLD_OUT
            result.parser_confidence = 0.85
        elif "pre-order" in page_text:
            result.stock_state = StockState.PREORDER_OPEN
            result.preorder_state = True
            result.parser_confidence = 0.82
        elif "notify" in page_text:
            result.stock_state = StockState.NOTIFY_ME
            result.parser_confidence = 0.75
        else:
            result.stock_state = StockState.UNKNOWN
            result.parser_confidence = 0.30

        return result


# ── Runner ────────────────────────────────────────────────────────────────────

SCRAPERS = {
    "pokemon-center-uk": PokemonCenterUKScraper,
    "smyths": SmythsScraper,
    "chaos-cards": ChaosCardsScraper,
    "magic-madhouse": MagicMadhouseScraper,
}

if __name__ == "__main__":
    retailer = sys.argv[1] if len(sys.argv) > 1 else "pokemon-center-uk"
    dry_run = "--dry-run" in sys.argv

    if retailer == "all":
        for slug, cls in SCRAPERS.items():
            cls().run(dry_run=dry_run)
    elif retailer in SCRAPERS:
        SCRAPERS[retailer]().run(dry_run=dry_run)
    else:
        print(f"Unknown retailer: {retailer}")
        print(f"Available: {', '.join(SCRAPERS.keys())}")