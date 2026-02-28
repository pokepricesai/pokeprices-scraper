"""
PokePrices Set Price Scraper
=============================
Scrapes set-level price data from PriceCharting console pages.
Each set page has chart_data with:
  - "median": average ungraded base card value over time
  - "value": total set value over time

Pushes historical + current data to set_prices table in Supabase.

Usage:
  python scrape_set_prices.py              # All sets
  python scrape_set_prices.py --test       # First 5 sets only

Requirements:
  pip install requests
"""

import os
import sys
import re
import json
import time
import requests
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://egidpsrkqvymvioidatc.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

HEADERS_API = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

REQUEST_DELAY = 0.5
session = requests.Session()
session.headers.update(HEADERS_WEB)


# ── Helpers ─────────────────────────────────────────────────────────────────

def fetch_all(endpoint):
    """Fetch all rows from Supabase REST API with pagination."""
    rows = []
    offset = 0
    while True:
        sep = "&" if "?" in endpoint else "?"
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{endpoint}{sep}offset={offset}&limit=1000",
            headers=HEADERS_API, timeout=30,
        )
        if r.status_code != 200:
            print(f"ERROR: {r.status_code} {r.text[:200]}")
            break
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        offset += 1000
        if len(batch) < 1000:
            break
    return rows


def set_name_to_slug(set_name):
    """Convert set name to PriceCharting URL slug.
    
    'Pokemon Base Set' → 'pokemon-base-set'
    'Pokemon Ruby & Sapphire' → 'pokemon-ruby-&-sapphire'  (PC keeps the &)
    """
    slug = set_name.lower().replace(" ", "-")
    return slug


def fetch_set_page(url):
    """Fetch a set page. Returns HTML or None."""
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  HTTP {resp.status_code}")
            return None
        return resp.text
    except Exception as e:
        print(f"  Error: {e}")
        return None


def extract_set_chart_data(html):
    """Extract VGPC.chart_data from set page.
    
    Returns dict with:
      - median: list of (date_str, cents) tuples
      - value: list of (date_str, cents) tuples
    """
    match = re.search(r'VGPC\.chart_data\s*=\s*({.*?});', html)
    if not match:
        return None

    try:
        chart_data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None

    result = {"median": [], "value": []}

    for series_name in ["median", "value"]:
        if series_name not in chart_data:
            continue
        for timestamp_ms, price_cents in chart_data[series_name]:
            date_str = datetime.fromtimestamp(
                timestamp_ms / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d")
            if price_cents and price_cents > 0:
                result[series_name].append((date_str, int(price_cents)))

    return result


def push_set_prices(rows):
    """Upsert set price rows to Supabase."""
    if not rows:
        return 0

    url = f"{SUPABASE_URL}/rest/v1/set_prices?on_conflict=set_name,date,source"
    headers = {**HEADERS_API, "Prefer": "resolution=merge-duplicates"}

    pushed = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        r = requests.post(url, json=batch, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            pushed += len(batch)
        else:
            print(f"  ERROR pushing batch {i}: {r.status_code} {r.text[:200]}")
    return pushed


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    test_mode = "--test" in sys.argv

    print(f"PokePrices Set Price Scraper")
    print(f"Supabase: {SUPABASE_URL}")

    if not SUPABASE_KEY:
        print("ERROR: SUPABASE_KEY not set!")
        sys.exit(1)

    # Get unique set names from cards table
    print("Fetching set names from cards table...")
    cards = fetch_all("cards?select=set_name")
    set_names = sorted(set(c["set_name"] for c in cards if c.get("set_name")))
    print(f"  Found {len(set_names)} unique sets")

    if test_mode:
        set_names = set_names[:5]
        print(f"  TEST MODE: {len(set_names)} sets")

    total_records = 0
    sets_found = 0
    sets_failed = 0

    for i, set_name in enumerate(set_names):
        slug = set_name_to_slug(set_name)
        url = f"https://www.pricecharting.com/console/{slug}"

        print(f"[{i+1}/{len(set_names)}] {set_name}")

        html = fetch_set_page(url)
        if not html:
            sets_failed += 1
            time.sleep(REQUEST_DELAY)
            continue

        chart = extract_set_chart_data(html)
        if not chart or (not chart["median"] and not chart["value"]):
            print(f"  – No chart data found")
            sets_failed += 1
            time.sleep(REQUEST_DELAY)
            continue

        sets_found += 1

        # Combine median and value data by date
        date_data = {}
        for date_str, cents in chart["median"]:
            if date_str not in date_data:
                date_data[date_str] = {"median_usd": None, "value_usd": None}
            date_data[date_str]["median_usd"] = cents

        for date_str, cents in chart["value"]:
            if date_str not in date_data:
                date_data[date_str] = {"median_usd": None, "value_usd": None}
            date_data[date_str]["value_usd"] = cents

        # Build rows
        rows = []
        for date_str, prices in date_data.items():
            rows.append({
                "set_name": set_name,
                "date": date_str,
                "median_usd": prices["median_usd"],
                "value_usd": prices["value_usd"],
                "source": "pricecharting",
            })

        pushed = push_set_prices(rows)
        total_records += pushed
        print(f"  ✓ {pushed} records ({len(chart['median'])} median, {len(chart['value'])} value)")

        time.sleep(REQUEST_DELAY)

    print(f"\n{'='*60}")
    print(f"Set price scrape complete!")
    print(f"  Sets found:     {sets_found}")
    print(f"  Sets failed:    {sets_failed}")
    print(f"  Records pushed: {total_records}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
