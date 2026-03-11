"""
PokePrices Scraper v9
=====================
Based on v8. Changes:
  - Added sales volume extraction from PriceCharting pages
  - Volume text (e.g. "1 sale per day", "2 sales per month") parsed to monthly figure
  - Monthly sales upserted into card_volume table (sales_30d field)
  - All other behaviour unchanged
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

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://egidpsrkqveioidatc.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))

LOCAL_CSV_FOLDER = r"C:\Users\lukep\OneDrive\Desktop\pokeprices\pc_csvs"
REPO_CSV_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pc_csvs")

if os.path.exists(LOCAL_CSV_FOLDER):
    PC_CSV_FOLDER = LOCAL_CSV_FOLDER
else:
    PC_CSV_FOLDER = REPO_CSV_FOLDER

REQUEST_DELAY = 0.4

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
# CSV LOADING
# ============================================

def load_sets_from_file(sets_file):
    if not os.path.exists(sets_file):
        print(f"ERROR: Sets file '{sets_file}' not found")
        sys.exit(1)
    with open(sets_file, "r") as f:
        sets = {line.strip() for line in f if line.strip()}
    print(f"Loaded {len(sets)} set names from {sets_file}")
    return sets


def load_cards_from_pc_csvs(csv_folder, set_filter=None, sets_filter=None):
    cards = []

    if not os.path.exists(csv_folder):
        print(f"ERROR: Folder '{csv_folder}' not found")
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
                if set_filter and console_name != set_filter:
                    continue
                if sets_filter and console_name not in sets_filter:
                    continue

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
    console_slug = console_name.lower().replace(" ", "-")
    console_slug = console_slug.replace("&", "&")

    slug = product_name.lower()
slug = slug.replace("[", "").replace("]", "")
slug = slug.replace("#", "")
slug = re.sub(r"[^a-z0-9\s&]", '', slug)
slug = slug.strip()
slug = re.sub(r'\s+', '-', slug)
slug = re.sub(r'-+', '-', slug)
    return f"https://www.pricecharting.com/game/{console_slug}/{slug}"


# ============================================
# PRICE EXTRACTION
# ============================================

def extract_current_prices(html):
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


# ============================================
# v9: VOLUME EXTRACTION
# ============================================

def extract_sales_volume(html):
    """
    Extract sales volume from PriceCharting page.

    PriceCharting shows volume as text in the page like:
      "1 sale per day", "2 sales per day"
      "1 sale per week", "3 sales per week"
      "1 sale per month", "2 sales per month"
      "1 sale per year", "1 sale per 2 years"

    These appear near the grade price cells, typically in a
    <span class="volume"> or similar element.

    We extract the "Ungraded" / raw volume since that's what
    maps to our sales_30d field.

    Returns approximate monthly sales as a float, or None.
    """

    # PriceCharting volume text patterns — appears as plain text near price cells
    # Try to find volume associated with "used" (ungraded/raw) price first
    # then fall back to any volume text on the page

    volume_patterns = [
        # Inside a volume span/div near the used price section
        r'id=["\']used_price["\'].*?volume["\']?\s*[^>]*>([^<]+sale[^<]+)',
        # Generic volume text anywhere
        r'class=["\']volume["\'][^>]*>\s*([^<]*\d+\s+sale[^<]*)',
        r'<span[^>]*>\s*(\d+\s+sales?\s+per\s+(?:day|week|month|year)[^<]*)\s*</span>',
        r'volume["\']?\s*[^>]*>\s*([^<]*\d+[^<]*sale[^<]*)',
    ]

    volume_text = None
    for pattern in volume_patterns:
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            # Make sure it actually contains sale volume info
            if re.search(r'\d+\s+sales?\s+per', candidate, re.IGNORECASE):
                volume_text = candidate
                break

    if not volume_text:
        return None

    return parse_volume_to_monthly(volume_text)


def parse_volume_to_monthly(text):
    """
    Convert PriceCharting volume text to approximate monthly sales.

    Examples:
      "1 sale per day"     → 30
      "2 sales per day"    → 60
      "1 sale per week"    → 4.3
      "3 sales per week"   → 12.9
      "1 sale per month"   → 1
      "2 sales per month"  → 2
      "1 sale per year"    → 0.08
      "1 sale per 2 years" → 0.04

    Returns rounded integer (minimum 1 if any sales found).
    """
    text = text.lower().strip()

    # Extract number of sales
    sales_match = re.search(r'(\d+(?:\.\d+)?)\s+sales?', text)
    if not sales_match:
        return None
    sales_count = float(sales_match.group(1))

    # Extract period
    if 'per day' in text:
        monthly = sales_count * 30
    elif 'per week' in text:
        monthly = sales_count * 4.33
    elif 'per month' in text:
        monthly = sales_count
    elif 'per 2 year' in text:
        monthly = sales_count / 24
    elif 'per year' in text:
        monthly = sales_count / 12
    else:
        return None

    # Round to nearest integer, minimum 1 if any sales found
    return max(1, round(monthly))


# ============================================
# IMAGE EXTRACTION
# ============================================

def extract_image_url(html):
    patterns = [
        r'<div[^>]+id=["\']product_image["\'][^>]*>.*?<img[^>]+src=["\']([^"\']+)["\']',
        r'<img[^>]+id=["\']photo["\'][^>]+src=["\']([^"\']+)["\']',
        r'<img[^>]+src=["\']([^"\']+)["\'][^>]+id=["\']photo["\']',
        r'<img[^>]+itemprop=["\']image["\'][^>]+src=["\']([^"\']+)["\']',
        r'<img[^>]+src=["\']([^"\']+)["\'][^>]+itemprop=["\']image["\']',
        r'src=["\'](https://[^"\']*pricecharting[^"\']*\.jpg[^"\']*)["\']',
        r'src=["\'](https://[^"\']*pricecharting[^"\']*\.png[^"\']*)["\']',
        r'src=["\'](https://d2n9x8p9xh9t10\.cloudfront\.net[^"\']+)["\']',
        r'src=["\'](//d2n9x8p9xh9t10\.cloudfront\.net[^"\']+)["\']',
    ]

    for pattern in patterns:
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            url = match.group(1)
            if url.startswith('//'):
                url = 'https:' + url
            if any(skip in url.lower() for skip in ['placeholder', 'blank', 'logo', 'favicon', 'icon', 'avatar']):
                continue
            return url

    return None


# ============================================
# SUPABASE
# ============================================

def normalize_record(record):
    normalized = {
        "card_slug": record["card_slug"],
        "date": record["date"],
        "source": record["source"],
    }
    for field in ALL_PRICE_FIELDS:
        normalized[field] = record.get(field, None)
    return normalized


def push_batch_to_supabase(records):
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


def update_card_image(pc_id, image_url, pc_url):
    card_slug = f"pc-{pc_id}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    update_data = {"pc_url": pc_url}
    if image_url:
        update_data["image_url"] = image_url

    try:
        resp = requests.patch(
            f"{SUPABASE_URL}/rest/v1/cards?card_slug=eq.{pc_id}&image_url=is.null",
            json=update_data,
            headers=headers,
            timeout=15,
        )
        return resp.status_code in (200, 201, 204)
    except Exception as e:
        print(f"  Image update error: {e}")
        return False


def upsert_card_volume(card_slug, sales_30d):
    """
    Upsert sales_30d into card_volume table.
    Only updates the sales_30d field — leaves all other volume fields untouched.
    """
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    today = datetime.now().strftime("%Y-%m-%d")

    payload = {
        "card_slug": card_slug,
        "sales_30d": sales_30d,
        "as_of": today,
    }

    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/card_volume?on_conflict=card_slug",
            json=payload,
            headers=headers,
            timeout=15,
        )
        return resp.status_code in (200, 201, 204)
    except Exception as e:
        print(f"  Volume upsert error: {e}")
        return False


def fetch_card_page(url):
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
# MAIN
# ============================================

def main():
    include_history = "--history" in sys.argv
    test_mode = "--test" in sys.argv

    set_filter = None
    sets_filter = None

    if "--set" in sys.argv:
        idx = sys.argv.index("--set")
        if idx + 1 < len(sys.argv):
            set_filter = sys.argv[idx + 1]

    if "--sets-file" in sys.argv:
        idx = sys.argv.index("--sets-file")
        if idx + 1 < len(sys.argv):
            sets_filter = load_sets_from_file(sys.argv[idx + 1])

    cards = load_cards_from_pc_csvs(PC_CSV_FOLDER, set_filter=set_filter, sets_filter=sets_filter)

    if not cards:
        print("No cards found. Check your CSV files and set filter.")
        sys.exit(1)

    if test_mode:
        cards = cards[:5]
        include_history = True
        print(f"TEST MODE: {len(cards)} cards with full history")

    today = datetime.now().strftime("%Y-%m-%d")
    history_label = "WITH HISTORY" if include_history else "DAILY ONLY"
    est_seconds = len(cards) * (REQUEST_DELAY + 0.8)

    print(f"\n{'='*60}")
    print(f"PokePrices Scraper v9 — {history_label}")
    print(f"{'='*60}")
    if set_filter:
        print(f"Set:      {set_filter}")
    if sets_filter:
        print(f"Batch:    {len(sets_filter)} sets from file")
    print(f"Cards:    {len(cards)}")
    print(f"Date:     {today}")
    print(f"Delay:    {REQUEST_DELAY}s per card")
    print(f"Est time: ~{est_seconds/60:.0f} min ({est_seconds/3600:.1f}h)")
    print(f"{'='*60}\n")

    found = 0
    not_found = 0
    errors = 0
    images_saved = 0
    volumes_saved = 0
    total_records = 0

    for i, card in enumerate(cards):
        product_name = card["product_name"]
        console_name = card["console_name"]
        card_slug = card["card_slug"]
        pc_id = card["pc_id"]
        url = card["url"]

        print(f"[{i+1}/{len(cards)}] {product_name} ({console_name})")

        html = fetch_card_page(url)

        current = None
        if html:
            current = extract_current_prices(html)

        if not current:
            not_found += 1
            print(f"  ✗ No price data at {url}")
            time.sleep(REQUEST_DELAY)
            continue

        found += 1
        records = []

        # v8: image extraction
        if html:
            image_url = extract_image_url(html)
            if image_url or url:
                updated = update_card_image(pc_id, image_url, url)
                if updated and image_url:
                    images_saved += 1
                    print(f"  🖼  Image saved")

        # v9: volume extraction
        if html:
            sales_monthly = extract_sales_volume(html)
            if sales_monthly is not None:
                ok = upsert_card_volume(card_slug, sales_monthly)
                if ok:
                    volumes_saved += 1
                    print(f"  📊 Volume: ~{sales_monthly}/mo")

        # Today's price record
        today_record = {
            "card_slug": card_slug,
            "date": today,
            "source": "pricecharting",
        }
        today_record.update(current)
        records.append(today_record)

        raw = current.get("raw_usd", 0) / 100
        psa10 = current.get("psa10_usd", 0) / 100
        psa9 = current.get("psa9_usd", 0) / 100
        print(f"  Ungraded: ${raw:.2f} | PSA 9: ${psa9:.2f} | PSA 10: ${psa10:.2f}")

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

        success = push_batch_to_supabase(records)
        if success:
            total_records += len(records)
            print(f"  ✓ {len(records)} records")
        else:
            errors += 1
            print(f"  ✗ Supabase push failed")

        time.sleep(REQUEST_DELAY)

    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Cards found:      {found}")
    print(f"Cards not found:  {not_found}")
    print(f"Errors:           {errors}")
    print(f"Images saved:     {images_saved}")
    print(f"Volumes saved:    {volumes_saved}")
    print(f"Records pushed:   {total_records}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
