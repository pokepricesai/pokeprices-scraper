"""
PokePrices Scraper v5
=====================
Reads card lists from PriceCharting CSV downloads.
Scrapes current + historical prices from PriceCharting pages.
Pushes all data to Supabase.

Card source: PriceCharting Retail CSV files (in pc_csvs/ folder)
  Format: id, console-name, product-name, loose-price
  Example: 715593, Pokemon Base Set, Charizard [1st Edition] #4, $5551.08

URL building: Product name → URL slug (100% reliable, no guessing)
  "Charizard [1st Edition] #4" + "Pokemon Base Set"
  → /game/pokemon-base-set/charizard-1st-edition-4

Card slug format: pc-{product_id}  (e.g. pc-715593)
  Unique, permanent, maps directly to PriceCharting

Price mapping (verified from page source):
  Chart Series  →  Our Field   →  Table Header
  used          →  raw_usd     →  Ungraded
  cib           →  psa7_usd    →  Grade 7
  new           →  psa8_usd    →  Grade 8
  graded        →  psa9_usd    →  Grade 9
  boxonly        →  cgc95_usd   →  Grade 9.5
  manualonly     →  psa10_usd   →  PSA 10

Usage:
  python pokeprices_scraper_v5.py --test              # 5 cards, with history
  python pokeprices_scraper_v5.py                     # All cards, daily only
  python pokeprices_scraper_v5.py --history           # All cards, full history
  python pokeprices_scraper_v5.py --set "Pokemon Base Set"  # One set only
  python pokeprices_scraper_v5.py --set "Pokemon Base Set" --history
  python pokeprices_scraper_v5.py --sets-file sets.txt      # Sets listed in a file

Requirements:
  pip install requests
"""

import requests
import json
import re
import time
import csv
import os
import sys
from datetime import datetime, timezone

# ============================================
# CONFIGURATION
# ============================================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://egidpsrkqvymvioidatc.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_secret_sVRvuUKAnzB3TnOsIUx5xg_5S6c_dR7")

# Folder containing PriceCharting CSV downloads
# CSV folder: check local path first, then repo-relative path (GitHub Actions)
LOCAL_CSV_FOLDER = r"C:\Users\lukep\OneDrive\Desktop\pokeprices\pc_csvs"
REPO_CSV_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pc_csvs")

if os.path.exists(LOCAL_CSV_FOLDER):
    PC_CSV_FOLDER = LOCAL_CSV_FOLDER
else:
    PC_CSV_FOLDER = REPO_CSV_FOLDER

# Price field mappings (verified from Alakazam Base Set page source)
CHART_SERIES_TO_FIELD = {
    "used":       "raw_usd",
    "cib":        "psa7_usd",
    "new":        "psa8_usd",
    "graded":     "psa9_usd",
    "boxonly":    "cgc95_usd",
    "manualonly": "psa10_usd",
}

TD_ID_TO_FIELD = {
    "used_price":         "raw_usd",
    "complete_price":     "psa7_usd",
    "new_price":          "psa8_usd",
    "graded_price":       "psa9_usd",
    "box_only_price":     "cgc95_usd",
    "manual_only_price":  "psa10_usd",
}

ALL_PRICE_FIELDS = [
    "raw_usd", "psa10_usd", "psa9_usd", "psa8_usd", "psa7_usd",
    "cgc10_usd", "cgc95_usd", "bgs10_usd", "bgs95_usd",
    "tcgplayer_usd", "cardmarket_eur"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

session = requests.Session()
session.headers.update(HEADERS)


# ============================================
# CSV LOADING (PriceCharting format)
# ============================================

def load_cards_from_pc_csvs(csv_folder, set_filter=None):
    """Load cards from PriceCharting CSV files.
    
    Each CSV has: id, console-name, product-name, loose-price
    Returns list of dicts with: pc_id, console_name, product_name, card_slug, url
    """
    cards = []
    
    if not os.path.exists(csv_folder):
        print(f"ERROR: Folder '{csv_folder}' not found")
        print(f"  Create it and put your PriceCharting CSV downloads there.")
        sys.exit(1)
    
    csv_files = sorted([f for f in os.listdir(csv_folder) if f.endswith(".csv")])
    
    if not csv_files:
        print(f"ERROR: No CSV files found in '{csv_folder}'")
        sys.exit(1)
    
    print(f"Found {len(csv_files)} CSV file(s) in {csv_folder}/")
    
    for csv_file in csv_files:
        filepath = os.path.join(csv_folder, csv_file)
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                console_name = row.get("console-name", "").strip()
                product_name = row.get("product-name", "").strip()
                pc_id = row.get("id", "").strip()
                
                if not pc_id or not product_name:
                    continue
                
                # Filter by set if requested
                if set_filter and console_name != set_filter:
                    continue
                
                # Build URL from console name + product name
                url = build_url(console_name, product_name)
                
                cards.append({
                    "pc_id": pc_id,
                    "console_name": console_name,
                    "product_name": product_name,
                    "card_slug": f"pc-{pc_id}",
                    "url": url,
                })
    
    print(f"Loaded {len(cards)} cards")
    return cards


# ============================================
# URL BUILDING
# ============================================

def build_url(console_name, product_name):
    """Build PriceCharting URL from console name and product name.
    
    "Pokemon Base Set" + "Charizard [1st Edition] #4"
    → https://www.pricecharting.com/game/pokemon-base-set/charizard-1st-edition-4
    """
    console_slug = console_name.lower().replace(" ", "-")
    # Handle special chars in console names like "Ruby & Sapphire"
    console_slug = console_slug.replace("&", "&")
    
    # Product name to slug
    slug = product_name.lower()
    slug = slug.replace("[", "").replace("]", "")
    slug = slug.replace("#", "")
    # Keep apostrophes - PriceCharting uses them in URLs (e.g. farfetch'd-27)
    slug = re.sub(r"[^a-z0-9\s.&'-]", '', slug)
    slug = slug.strip()
    slug = re.sub(r'\s+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    
    return f"https://www.pricecharting.com/game/{console_slug}/{slug}"


# ============================================
# PRICE EXTRACTION
# ============================================

def extract_current_prices(html):
    """Extract current prices from the price table using regex."""
    prices = {}
    
    for td_id, field in TD_ID_TO_FIELD.items():
        pattern = rf'<td\s+id="{td_id}"[^>]*>.*?<span\s+class="price\s+js-price">\s*\$([\d,]+\.?\d*)\s*</span>'
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                val = float(match.group(1).replace(",", ""))
                if val > 0:
                    prices[field] = int(val * 100)
            except ValueError:
                pass
    
    return prices


def extract_historical_prices(html):
    """Extract VGPC.chart_data from the page source.
    Returns dict of {date_str: {field: price_cents, ...}, ...}
    """
    match = re.search(r'VGPC\.chart_data\s*=\s*({.*?});', html)
    if not match:
        return {}
    
    try:
        chart_data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}
    
    date_prices = {}
    
    for series_name, data_points in chart_data.items():
        field = CHART_SERIES_TO_FIELD.get(series_name)
        if not field:
            continue
        
        for timestamp_ms, price_cents in data_points:
            date_str = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            
            if date_str not in date_prices:
                date_prices[date_str] = {}
            
            if price_cents and price_cents > 0:
                date_prices[date_str][field] = int(price_cents)
    
    return date_prices


def fetch_card_page(url):
    """Fetch a card page. Returns HTML string or None."""
    try:
        resp = session.get(url, timeout=10)
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            print(f"  HTTP {resp.status_code}")
            return None
        return resp.text
    except requests.exceptions.Timeout:
        print(f"  Timeout")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


# ============================================
# SUPABASE
# ============================================

def normalize_record(record):
    """Ensure every record has all price columns."""
    normalized = {
        "card_slug": record["card_slug"],
        "date": record["date"],
        "source": record["source"],
    }
    for field in ALL_PRICE_FIELDS:
        normalized[field] = record.get(field, None)
    return normalized


def push_batch_to_supabase(records):
    """Push records to Supabase with upsert."""
    if not records:
        return True

    url = f"{SUPABASE_URL}/rest/v1/daily_prices?on_conflict=card_slug,date,source"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    normalized = [normalize_record(r) for r in records]

    try:
        for i in range(0, len(normalized), 500):
            batch = normalized[i:i+500]
            resp = requests.post(url, json=batch, headers=headers, timeout=30)
            if resp.status_code not in [200, 201]:
                print(f"  Supabase error: {resp.status_code} - {resp.text[:200]}")
                return False
        return True
    except Exception as e:
        print(f"  Supabase error: {e}")
        return False


# ============================================
# MAIN
# ============================================

def main():
    # Parse args
    include_history = "--history" in sys.argv
    test_mode = "--test" in sys.argv
    
    set_filter = None
    if "--set" in sys.argv:
        idx = sys.argv.index("--set")
        if idx + 1 < len(sys.argv):
            set_filter = sys.argv[idx + 1]
    
    # Load cards from PriceCharting CSVs
    cards = load_cards_from_pc_csvs(PC_CSV_FOLDER, set_filter=set_filter)
    
    if not cards:
        print("No cards found. Check your CSV files and set filter.")
        sys.exit(1)
    
    if test_mode:
        cards = cards[:5]
        include_history = True
        print(f"TEST MODE: {len(cards)} cards with full history")
    
    today = datetime.now().strftime("%Y-%m-%d")
    history_label = "WITH HISTORY" if include_history else "DAILY ONLY"
    est_seconds = len(cards) * 1.2
    
    print(f"\n{'='*60}")
    print(f"PokePrices Scraper v5 — {history_label}")
    print(f"{'='*60}")
    if set_filter:
        print(f"Set:      {set_filter}")
    print(f"Cards:    {len(cards)}")
    print(f"Date:     {today}")
    print(f"Est time: ~{est_seconds/60:.0f} min ({est_seconds:.0f}s)")
    print(f"{'='*60}\n")

    found = 0
    not_found = 0
    errors = 0
    total_records = 0

    for i, card in enumerate(cards):
        product_name = card["product_name"]
        console_name = card["console_name"]
        card_slug = card["card_slug"]
        url = card["url"]

        print(f"[{i+1}/{len(cards)}] {product_name} ({console_name})")

        html = fetch_card_page(url)
        
        # Extract current prices
        current = None
        if html:
            current = extract_current_prices(html)

        if not current:
            not_found += 1
            print(f"  ✗ No price data at {url}")
            time.sleep(1)
            continue

        found += 1
        records = []

        # Today's record
        today_record = {
            "card_slug": card_slug,
            "date": today,
            "source": "pricecharting",
        }
        today_record.update(current)
        records.append(today_record)

        # Log summary
        raw = current.get("raw_usd", 0) / 100
        psa10 = current.get("psa10_usd", 0) / 100
        psa9 = current.get("psa9_usd", 0) / 100
        print(f"  Ungraded: ${raw:.2f} | PSA 9: ${psa9:.2f} | PSA 10: ${psa10:.2f}")

        # Historical data
        if include_history:
            historical = extract_historical_prices(html)
            
            for date_str, price_fields in historical.items():
                if date_str == today or not price_fields:
                    continue
                
                record = {
                    "card_slug": card_slug,
                    "date": date_str,
                    "source": "pricecharting",
                }
                record.update(price_fields)
                records.append(record)
            
            if historical:
                print(f"  Historical: {len(historical)} months")

        # Push to Supabase
        success = push_batch_to_supabase(records)
        if success:
            total_records += len(records)
            print(f"  ✓ {len(records)} records")
        else:
            errors += 1
            print(f"  ✗ Supabase push failed")

        time.sleep(1)

    # Summary
    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Cards found:      {found}")
    print(f"Cards not found:  {not_found}")
    print(f"Errors:           {errors}")
    print(f"Records pushed:   {total_records}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
