"""
intel/base_scraper.py
─────────────────────
Base class for all retailer scrapers.
Each retailer adapter inherits from this and implements parse().

Usage:
  from intel_base_scraper import BaseScraper, StockState, ScrapeResult
"""

import os
import time
import hashlib
import requests
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# ── Stock state model ─────────────────────────────────────────────────────────

class StockState(str, Enum):
    UNKNOWN          = "unknown"
    UNAVAILABLE      = "unavailable"
    COMING_SOON      = "coming_soon"
    NOTIFY_ME        = "notify_me"
    PREORDER_OPEN    = "preorder_open"
    IN_STOCK         = "in_stock"
    LOW_STOCK        = "low_stock"
    SOLD_OUT         = "sold_out"
    INVITE_ONLY      = "invite_only"
    QUEUED           = "queued"
    COLLECTION_ONLY  = "collection_only"
    DISCONTINUED     = "discontinued"

# State significance — higher = more likely to trigger alert
STATE_SIGNIFICANCE = {
    StockState.IN_STOCK:       10,
    StockState.LOW_STOCK:       9,
    StockState.PREORDER_OPEN:   9,
    StockState.INVITE_ONLY:     8,
    StockState.NOTIFY_ME:       6,
    StockState.COMING_SOON:     5,
    StockState.SOLD_OUT:        4,
    StockState.UNAVAILABLE:     2,
    StockState.DISCONTINUED:    1,
    StockState.UNKNOWN:         0,
}

# Transitions worth alerting on (old -> new)
ALERT_TRANSITIONS = {
    (StockState.UNAVAILABLE,   StockState.IN_STOCK),
    (StockState.UNAVAILABLE,   StockState.PREORDER_OPEN),
    (StockState.UNAVAILABLE,   StockState.LOW_STOCK),
    (StockState.SOLD_OUT,      StockState.IN_STOCK),
    (StockState.SOLD_OUT,      StockState.PREORDER_OPEN),
    (StockState.SOLD_OUT,      StockState.LOW_STOCK),
    (StockState.COMING_SOON,   StockState.PREORDER_OPEN),
    (StockState.COMING_SOON,   StockState.IN_STOCK),
    (StockState.NOTIFY_ME,     StockState.PREORDER_OPEN),
    (StockState.NOTIFY_ME,     StockState.IN_STOCK),
    (StockState.UNKNOWN,       StockState.IN_STOCK),
    (StockState.UNKNOWN,       StockState.PREORDER_OPEN),
    (StockState.PREORDER_OPEN, StockState.IN_STOCK),
}

# ── Scrape result ─────────────────────────────────────────────────────────────

@dataclass
class ScrapeResult:
    url: str
    raw_title: str = ""
    price: Optional[float] = None
    stock_state: StockState = StockState.UNKNOWN
    preorder_state: bool = False
    invite_state: bool = False
    collection_state: bool = False
    detected_text: list = field(default_factory=list)
    parser_confidence: float = 0.5
    error: Optional[str] = None
    fetched_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return {
            "raw_title": self.raw_title,
            "price": self.price,
            "stock_state": self.stock_state.value,
            "preorder_state": self.preorder_state,
            "invite_state": self.invite_state,
            "collection_state": self.collection_state,
            "detected_text": self.detected_text,
            "parser_confidence": self.parser_confidence,
            "fetched_at": self.fetched_at,
        }

# ── Base scraper ──────────────────────────────────────────────────────────────

class BaseScraper:
    retailer_slug: str = "base"
    retailer_name: str = "Base"
    default_headers: dict = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.default_headers)

    def fetch(self, url: str, timeout: int = 15) -> Optional[str]:
        """Fetch page HTML. Returns None on error."""
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            print(f"  FETCH ERROR {url}: {e}")
            return None

    def parse(self, html: str, url: str) -> ScrapeResult:
        """Override in each adapter."""
        raise NotImplementedError

    def scrape(self, url: str) -> ScrapeResult:
        """Fetch + parse."""
        html = self.fetch(url)
        if not html:
            r = ScrapeResult(url=url)
            r.error = "Fetch failed"
            return r
        return self.parse(html, url)

    # ── Supabase helpers ──────────────────────────────────────────────────────

    def get_retailer_id(self) -> Optional[str]:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/intel_retailers?slug=eq.{self.retailer_slug}&select=id",
            headers=HEADERS, timeout=10
        )
        data = resp.json()
        return data[0]["id"] if data else None

    def get_active_pages(self, retailer_id: str) -> list:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/intel_retailer_pages?retailer_id=eq.{retailer_id}&scrape_active=eq.true&select=*",
            headers=HEADERS, timeout=10
        )
        return resp.json() if resp.ok else []

    def update_page(self, page_id: str, result: ScrapeResult):
        """Update retailer page with latest scrape result."""
        payload = {
            "raw_title": result.raw_title or None,
            "current_price": result.price,
            "current_stock_state": result.stock_state.value,
            "current_preorder_state": result.preorder_state,
            "current_invite_state": result.invite_state,
            "current_collection_state": result.collection_state,
            "raw_payload": result.to_dict(),
            "parser_confidence": result.parser_confidence,
            "last_seen_at": result.fetched_at,
            "last_error": result.error,
        }
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/intel_retailer_pages?id=eq.{page_id}",
            json=payload, headers={**HEADERS, "Prefer": "return=minimal"}, timeout=10
        )

    def detect_change(self, page: dict, result: ScrapeResult) -> Optional[dict]:
        """Compare new result to stored state. Return event dict if meaningful change."""
        old_state = StockState(page.get("current_stock_state") or "unknown")
        new_state = result.stock_state
        old_price = page.get("current_price")
        new_price = result.price

        # Stock state change
        if old_state != new_state:
            transition = (old_state, new_state)
            sig = max(STATE_SIGNIFICANCE.get(old_state, 0), STATE_SIGNIFICANCE.get(new_state, 0))
            should_alert = transition in ALERT_TRANSITIONS
            return {
                "event_type": "stock_change",
                "old_value": old_state.value,
                "new_value": new_state.value,
                "price": new_price,
                "significance_score": sig,
                "should_alert": should_alert,
            }

        # Price drop (>5% drop and now at/below assumed MSRP)
        if old_price and new_price and new_price < old_price * 0.95:
            return {
                "event_type": "price_change",
                "old_value": str(old_price),
                "new_value": str(new_price),
                "price": new_price,
                "significance_score": 7,
                "should_alert": True,
            }

        return None

    def log_event(self, page: dict, event: dict, retailer_id: str) -> Optional[str]:
        """Write stock event to database. Returns event ID."""
        event_hash = hashlib.md5(
            f"{page['id']}{event['event_type']}{event.get('new_value', '')}{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H')}".encode()
        ).hexdigest()

        payload = {
            "retailer_page_id": page["id"],
            "product_id": page.get("product_id"),
            "retailer_id": retailer_id,
            "event_type": event["event_type"],
            "old_value": event.get("old_value"),
            "new_value": event.get("new_value"),
            "price": event.get("price"),
            "significance_score": event.get("significance_score", 5),
            "event_hash": event_hash,
        }

        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/intel_stock_events",
            json=payload,
            headers={**HEADERS, "Prefer": "return=representation,resolution=ignore-duplicates"},
            timeout=10
        )
        if resp.ok and resp.json():
            return resp.json()[0]["id"]
        return None

    def log_run(self, retailer_id: str, status: str, pages: int, events: int, alerts: int, error: str = None, duration_ms: int = 0):
        requests.post(
            f"{SUPABASE_URL}/rest/v1/intel_scraper_runs",
            json={
                "retailer_id": retailer_id,
                "status": status,
                "pages_scraped": pages,
                "events_generated": events,
                "alerts_sent": alerts,
                "error_message": error,
                "duration_ms": duration_ms,
            },
            headers={**HEADERS, "Prefer": "return=minimal"}, timeout=10
        )

    # ── Telegram ──────────────────────────────────────────────────────────────

    def send_telegram(self, message: str, urgency: int = 5) -> bool:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            print(f"  [TELEGRAM DISABLED] {message}")
            return False
        try:
            prefix = "🚨" if urgency >= 9 else "🔔" if urgency >= 7 else "📦"
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": f"{prefix} {message}",
                    "parse_mode": "HTML",
                    "disable_web_page_preview": False,
                },
                timeout=10
            )
            return resp.ok
        except Exception as e:
            print(f"  TELEGRAM ERROR: {e}")
            return False

    def format_alert_message(self, page: dict, event: dict, result: ScrapeResult, product_name: str = None) -> str:
        name = product_name or result.raw_title or "Unknown product"
        old = event.get("old_value", "?").replace("_", " ").title()
        new = event.get("new_value", "?").replace("_", " ").title()
        price = f"£{result.price:.2f}" if result.price else "price unknown"

        return (
            f"<b>{name}</b>\n"
            f"{self.retailer_name} · {price}\n"
            f"{old} → <b>{new}</b>\n"
            f"<a href='{page['url']}'>View product →</a>"
        )

    # ── Main run loop ─────────────────────────────────────────────────────────

    def run(self, dry_run: bool = False):
        """Run the full scrape cycle for this retailer."""
        start = time.time()
        print(f"\n{'='*50}")
        print(f"{self.retailer_name} Scraper")
        print(f"{'='*50}")

        retailer_id = self.get_retailer_id()
        if not retailer_id:
            print(f"ERROR: Retailer '{self.retailer_slug}' not found in database")
            return

        pages = self.get_active_pages(retailer_id)
        print(f"Found {len(pages)} active pages to scrape")

        pages_done = 0
        events_gen = 0
        alerts_sent = 0

        for page in pages:
            print(f"\n  [{pages_done+1}/{len(pages)}] {page['url'][:80]}")
            result = self.scrape(page["url"])

            if result.error:
                print(f"    ERROR: {result.error}")
                if not dry_run:
                    requests.patch(
                        f"{SUPABASE_URL}/rest/v1/intel_retailer_pages?id=eq.{page['id']}",
                        json={"last_error": result.error, "last_seen_at": result.fetched_at},
                        headers={**HEADERS, "Prefer": "return=minimal"}, timeout=10
                    )
                pages_done += 1
                continue

            print(f"    State: {result.stock_state.value} | Price: £{result.price} | Confidence: {result.parser_confidence:.0%}")
            print(f"    Title: {result.raw_title[:60] if result.raw_title else 'none'}")

            event = self.detect_change(page, result)

            if not dry_run:
                self.update_page(page["id"], result)

            if event:
                print(f"    ⚡ Change detected: {event['old_value']} → {event['new_value']}")
                events_gen += 1

                if not dry_run:
                    event_id = self.log_event(page, event, retailer_id)

                    # Update last_changed_at on the page
                    requests.patch(
                        f"{SUPABASE_URL}/rest/v1/intel_retailer_pages?id=eq.{page['id']}",
                        json={"last_changed_at": result.fetched_at},
                        headers={**HEADERS, "Prefer": "return=minimal"}, timeout=10
                    )

                if event.get("should_alert"):
                    msg = self.format_alert_message(page, event, result)
                    print(f"    📣 Alert: {result.stock_state.value}")
                    if not dry_run:
                        sent = self.send_telegram(msg, urgency=event.get("significance_score", 5))
                        if sent:
                            alerts_sent += 1
                            # Log alert
                            requests.post(
                                f"{SUPABASE_URL}/rest/v1/intel_alerts",
                                json={
                                    "product_id": page.get("product_id"),
                                    "retailer_id": retailer_id,
                                    "alert_type": event["event_type"],
                                    "title": result.raw_title or "Stock alert",
                                    "body": msg,
                                    "url": page["url"],
                                    "price": result.price,
                                    "urgency_score": event.get("significance_score", 5),
                                    "sent_via": "telegram",
                                    "sent_at": result.fetched_at,
                                },
                                headers={**HEADERS, "Prefer": "return=minimal"}, timeout=10
                            )
            else:
                print(f"    ✓ No change")

            pages_done += 1
            time.sleep(1.5)  # Polite delay between requests

        duration = int((time.time() - start) * 1000)
        print(f"\n{'='*50}")
        print(f"Done: {pages_done} pages, {events_gen} events, {alerts_sent} alerts ({duration}ms)")

        if not dry_run:
            self.log_run(retailer_id, "success", pages_done, events_gen, alerts_sent, duration_ms=duration)
