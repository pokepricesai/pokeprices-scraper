"""
PokePrices Scraper v8
=====================
Based on v7. Changes:
  - Added sales volume extraction from PriceCharting pages
  - Volume text (e.g. "1 sale per day", "2 sales per month") parsed to monthly figure
  - Fixed card_volume upsert: correct PK (card_slug, grade), bare numeric slug, volume_label + confidence
  - All other behaviour unchanged

Block 4B-S-2A: optional, allow-list-only recent-sales ingestion behind the
RECENT_SALES_INGESTION_ENABLED feature flag. When the flag is not the exact
string "true", this scraper behaves identically to the prior version.
"""

import requests
import json
import re
import time
import csv
import os
import sys
from datetime import datetime, timezone

# Recent-sales ingestion is gated behind an env-var feature flag inside
# recent_sales_ingestion.init_for_scraper_run(). When the flag is off the
# import is the only added cost (~1 ms) and no function call is made into
# the ingestion module beyond two no-op invocations per scraper run.
try:
    import recent_sales_ingestion as _rsi
except Exception as _rsi_import_err:  # pragma: no cover — defensive import
    _rsi = None
    _rsi_import_error = _rsi_import_err
else:
    _rsi_import_error = None

# ============================================
# CONFIGURATION
# ============================================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://egidpsrkqvymvioidatc.supabase.co")
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
    "grade1_usd", "grade2_usd", "grade3_usd", "grade4_usd",
    "grade5_usd", "grade6_usd",
    "tag10_usd", "ace10_usd", "sgc10_usd",
    "bgs10black_usd", "cgc10pristine_usd",
    "tcgplayer_usd", "cardmarket_eur"
]

FULL_PRICE_LABEL_TO_FIELD = {
    "Ungraded":         "raw_usd",
    "Grade 1":          "grade1_usd",
    "Grade 2":          "grade2_usd",
    "Grade 3":          "grade3_usd",
    "Grade 4":          "grade4_usd",
    "Grade 5":          "grade5_usd",
    "Grade 6":          "grade6_usd",
    "Grade 7":          "psa7_usd",
    "Grade 8":          "psa8_usd",
    "Grade 9":          "psa9_usd",
    "Grade 9.5":        "cgc95_usd",
    "TAG 10":           "tag10_usd",
    "ACE 10":           "ace10_usd",
    "SGC 10":           "sgc10_usd",
    "CGC 10":           "cgc10_usd",
    "PSA 10":           "psa10_usd",
    "BGS 10":           "bgs10_usd",
    "BGS 10 Black":     "bgs10black_usd",
    "CGC 10 Pristine":  "cgc10pristine_usd",
}

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


def load_cards_from_pc_csvs(csv_folder, set_filter=None, sets_filter=None, pc_ids_filter=None):
    """Load rows from every CSV in `csv_folder`, filtered against either
    a single `set_filter` string, a `sets_filter` allowlist, and/or a
    `pc_ids_filter` set of PriceCharting product IDs.

    W48B additions:
      * Aggregates the distinct console-names that produce output so we
        can print a summary at the end (visibility for Luke, and a
        forensic hook if an unexpected set ever slips through the
        batch file).
      * Emits a WARN (not a hard block) any time a card is loaded from
        a console-name that contains 'japan'. The scraper itself does
        not care about language — the `cards.language` column is set
        by seed_set_cards.py — but the warning ensures Luke gets a
        heads-up if a Japanese CSV was placed in pc_csvs/ before the
        matching cards.language='jp' seed was run.
    """
    cards = []
    per_console_counts: dict[str, int] = {}

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
                # Block 5A-W-48C-FIX1 — targeted re-scrape support.
                # When --pc-ids <file> is supplied, only PC IDs listed
                # in that file are loaded. Everything else is skipped.
                if pc_ids_filter is not None and pc_id not in pc_ids_filter:
                    continue

                url = build_url(console_name, product_name)

                cards.append({
                    "pc_id": pc_id,
                    "console_name": console_name,
                    "product_name": product_name,
                    "card_slug": f"pc-{pc_id}",
                    "url": url,
                })
                per_console_counts[console_name] = per_console_counts.get(console_name, 0) + 1

    print(f"Loaded {len(cards)} cards")

    # W48B — visibility gate. Print the console-name breakdown and warn
    # on anything that looks Japanese. This block never blocks the run;
    # its purpose is to surface accidental Japanese CSV drop-ins.
    if per_console_counts:
        print("Console-name distribution in this run:")
        for console_name, n in sorted(per_console_counts.items(), key=lambda kv: -kv[1]):
            print(f"  {n:>6}  {console_name!r}")
        japanese_consoles = [c for c in per_console_counts if "japan" in c.lower()]
        if japanese_consoles:
            print("")
            print("  WARN: Japanese-labelled console-name(s) detected:")
            for c in japanese_consoles:
                print(f"    {c!r} ({per_console_counts[c]} rows)")
            print("  These will be scraped into daily_prices normally.")
            print("  Ensure seed_set_cards.py was run with --language jp for the")
            print("  matching set_name so cards.language='jp' is set — otherwise")
            print("  the site will render these prices under an English row.")
    return cards


# ============================================
# URL BUILDING
# ============================================

def build_url(console_name, product_name):
    # Block 5A-W-48D — the console slug used to just lowercase + swap
    # spaces for dashes, which failed on set names containing commas
    # (e.g. "Pokemon Japanese Gold, Silver, New World"). PC drops the
    # comma entirely and collapses the resulting double-dash to one.
    # Colons and apostrophes are kept — both verified live against PC
    # for "Pokemon Japanese Magma VS Aqua: Two Ambitions" and
    # "Pokemon Japanese 2002 McDonald's".
    console_slug = console_name.lower()
    console_slug = console_slug.replace(",", "")
    console_slug = console_slug.replace(" ", "-")
    console_slug = re.sub(r"-+", "-", console_slug)

    slug = product_name.lower()
    slug = slug.replace("[", "").replace("]", "")
    slug = slug.replace("#", "")
    # Block 5A-W-48B — preserve apostrophes; PriceCharting keeps them in
    # slugs like "hop's-bag-91". Stripping the apostrophe returns a
    # 302 redirect which the scraper counts as "not found". Verified
    # against the live JP Battle Partners set on 2026-07-29.
    # Block 5A-W-48D-FIX1 — also preserve hyphens in the card slug.
    # Names like "Jangmo-o #69", "3-Pack Blister", "Professor Sycamore
    # #XY-P" produce 302 search-page redirects when the internal hyphen
    # is stripped ("jangmoo-69" instead of "jangmo-o-69"). Verified live
    # against 4 JP cards on 2026-07-31.
    slug = re.sub(r"[^a-z0-9\s&'\-]", '', slug)
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
                    prices[field] = round(val * 100)
            except ValueError:
                pass
    return prices


def extract_full_price_guide(html):
    table_match = re.search(
        r'<div\s+id="full-prices"[^>]*>.*?<table[^>]*>(.*?)</table>',
        html, re.DOTALL | re.IGNORECASE,
    )
    if not table_match:
        return {}

    prices = {}
    row_pattern = re.compile(
        r'<tr>\s*<td>\s*([^<]+?)\s*</td>\s*<td\s+class="price\s+js-price"[^>]*>\s*([^<]+?)\s*</td>',
        re.DOTALL | re.IGNORECASE,
    )
    for m in row_pattern.finditer(table_match.group(1)):
        label = re.sub(r'\s+', ' ', m.group(1).strip())
        price_text = m.group(2).strip()
        field = FULL_PRICE_LABEL_TO_FIELD.get(label)
        if not field:
            continue
        price_match = re.match(r'\$([\d,]+\.?\d*)', price_text)
        if not price_match:
            continue
        try:
            val = float(price_match.group(1).replace(",", ""))
            if val > 0:
                prices[field] = round(val * 100)
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
# VOLUME EXTRACTION
# ============================================

def extract_sales_volume(html):
    """
    Extract sales volume for all grades from PriceCharting page.
    HTML pattern: data-show-tab="completed-auctions-used" ... volume:&nbsp;</span> <a>3 sales per week</a>

    Returns dict of {grade: (monthly_int, volume_text)} or empty dict.
    """
    # Map data-show-tab values to our grade names
    TAB_TO_GRADE = {
        'completed-auctions-used':        'Ungraded',
        'completed-auctions-graded':      'PSA 9',
        'completed-auctions-manual-only': 'PSA 10',
        'completed-auctions-cib':         'PSA 7',
        'completed-auctions-new':         'PSA 8',
        'completed-auctions-box-only':    'CGC 9.5',
    }

    # Match each td with data-show-tab and its volume text
    pattern = r'data-show-tab=["\']([^"\']+)["\'][^>]*>.*?volume:&nbsp;</span>\s*<a[^>]*>([^<]+)</a>'
    results = {}

    for m in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
        tab = m.group(1).strip()
        volume_text = m.group(2).strip()
        grade = TAB_TO_GRADE.get(tab)
        if not grade:
            continue
        if grade in results:
            continue  # take first occurrence only
        monthly = parse_volume_to_monthly(volume_text)
        if monthly is not None:
            results[grade] = (monthly, volume_text)

    return results


def parse_volume_to_monthly(text):
    """Convert PriceCharting volume text to approximate monthly sales integer."""
    text = text.lower().strip()

    sales_match = re.search(r'(\d+(?:\.\d+)?)\s+sales?', text)
    if not sales_match:
        return None
    sales_count = float(sales_match.group(1))

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

    return max(1, round(monthly))


def volume_to_label(sales_30d):
    """Convert monthly sales count to a human-readable label."""
    if sales_30d >= 60:
        return "2 sales per day"
    elif sales_30d >= 30:
        return "1 sale per day"
    elif sales_30d >= 17:
        return "4 sales per week"
    elif sales_30d >= 12:
        return "3 sales per week"
    elif sales_30d >= 8:
        return "2 sales per week"
    elif sales_30d >= 4:
        return "1 sale per week"
    elif sales_30d >= 3:
        return "3 sales per month"
    elif sales_30d >= 2:
        return "2 sales per month"
    else:
        return "1 sale per month"


def volume_to_confidence(sales_30d):
    """Convert monthly sales count to confidence level."""
    if sales_30d >= 8:
        return "high"
    elif sales_30d >= 3:
        return "medium"
    else:
        return "low"


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


def upsert_card_volume(card_slug, sales_30d, volume_text=None, grade='Ungraded'):
    """
    Upsert sales volume into card_volume table.
    PK is (card_slug, grade) — bare numeric slug, no pc- prefix.
    """
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    today = datetime.now().strftime("%Y-%m-%d")
    bare_slug = card_slug.replace("pc-", "")

    # Use raw scraped text as label if available, otherwise derive it
    label = volume_text if volume_text else volume_to_label(sales_30d)

    payload = {
        "card_slug": bare_slug,
        "grade": grade,
        "sales_30d": sales_30d,
        "volume_label": label,
        "confidence": volume_to_confidence(sales_30d),
        "as_of": today,
    }

    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/card_volume?on_conflict=card_slug,grade",
            json=payload,
            headers=headers,
            timeout=15,
        )
        return resp.status_code in (200, 201, 204)
    except Exception as e:
        print(f"  Volume upsert error: {e}")
        return False


# Block 5A-W-49-FIX2 — most-recent fetch outcome, populated by every
# call to `fetch_card_page` before it returns. Callers that need to
# categorise the attempt for `scrape_attempt_state` read this dict
# immediately after the fetch. Kept module-level (not a return-tuple)
# so the existing test suite and other callers see the unchanged
# `str | None` return shape.
LAST_FETCH_OUTCOME: dict = {"category": None, "http_status": None}


def _set_outcome(category: str, http_status=None):
    LAST_FETCH_OUTCOME["category"] = category
    LAST_FETCH_OUTCOME["http_status"] = http_status


def fetch_card_page(url, retries=2, backoff_seconds=1.5):
    """Fetch a PriceCharting product page with bounded retry.

    Block 5A-W-48C-FIX1 — the initial JP-batch run reported ~4% of
    products as "not found". Post-mortem probing showed most of those
    pages DO exist and have prices on subsequent fetches — they were
    lost to transient network hiccups or PriceCharting throttling.
    Adding a bounded retry with a short exponential-ish backoff
    recovers those without changing the URL-builder contract.

    Retries fire on:
      * requests.RequestException (connection reset, timeout)
      * HTTP 429 (too many requests)
      * HTTP 5xx (server side)
    Retries do NOT fire on HTTP 404 — that's a real "no such page"
    and the caller treats it as unresolved.

    Block 5A-W-49-FIX2 — every terminal path sets `LAST_FETCH_OUTCOME`
    so the main loop can record the correct `scrape_result_category`
    without duplicating the retry / status categorisation here.
    """
    last_err: str | None = None
    last_status: int | None = None
    for attempt in range(retries + 1):  # 1 initial + N retries
        try:
            resp = session.get(url, timeout=10)
            last_status = resp.status_code
            if resp.status_code == 404:
                _set_outcome("not_found", 404)
                return None
            if resp.status_code == 200:
                _set_outcome("ok", 200)   # main loop refines to priced / page_reached_no_price
                return resp.text
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                last_err = f"HTTP {resp.status_code} retry"
                time.sleep(backoff_seconds * (attempt + 1))
                continue
            print(f"  HTTP {resp.status_code}")
            _set_outcome("transient_http_failure", resp.status_code)
            return None
        except requests.exceptions.Timeout:
            last_err = "Timeout"
        except requests.exceptions.RequestException as e:
            last_err = f"NetErr: {str(e)[:60]}"
        except Exception as e:
            print(f"  Error: {e}")
            _set_outcome("transient_http_failure", None)
            return None
        # Retry if we didn't succeed and have attempts left
        if attempt < retries:
            time.sleep(backoff_seconds * (attempt + 1))
        else:
            print(f"  {last_err} (gave up after {retries + 1} attempts)")
            _set_outcome(
                "timeout" if last_err and last_err.startswith("Timeout") else "transient_http_failure",
                last_status,
            )
            return None
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

    # Block 5A-W-48C-FIX1 — targeted --pc-ids <file> re-scrape.
    # File has one PriceCharting product ID per line. Any row whose
    # `id` column is not in the file is skipped.
    pc_ids_filter = None
    if "--pc-ids" in sys.argv:
        idx = sys.argv.index("--pc-ids")
        if idx + 1 < len(sys.argv):
            with open(sys.argv[idx + 1], "r") as f:
                pc_ids_filter = {line.strip() for line in f if line.strip()}
            print(f"Loaded {len(pc_ids_filter)} PC IDs from {sys.argv[idx + 1]}")

    # Block 5A-W-49 — cadence filter. `--cadence {daily,weekly,all}`
    # splits the schedule into a high-value daily run and a low-value
    # weekly run. Default is `all` so that ad-hoc targeted invocations
    # (via --set / --sets-file / --pc-ids) never silently omit a
    # requested card.
    cadence = "all"
    if "--cadence" in sys.argv:
        idx = sys.argv.index("--cadence")
        if idx + 1 < len(sys.argv):
            cadence = sys.argv[idx + 1]
        if cadence not in ("daily", "weekly", "all"):
            print(f"ERROR: --cadence must be one of daily|weekly|all (got {cadence!r})")
            sys.exit(1)

    cards = load_cards_from_pc_csvs(
        PC_CSV_FOLDER,
        set_filter=set_filter, sets_filter=sets_filter,
        pc_ids_filter=pc_ids_filter,
    )

    if not cards:
        print("No cards found. Check your CSV files and set filter.")
        sys.exit(1)

    # Block 5A-W-49 — apply cadence filter after CSV load. `all` is a
    # no-op. `daily` / `weekly` bulk-load classification state once and
    # filter locally so we never issue one DB query per card.
    #
    # Block 5A-W-49-FIX2 — we ALWAYS load the state map (even for
    # cadence=all) so the scrape_attempt_state writer can seed each
    # card's prior streak. Missing scrape_attempt_state table is
    # handled gracefully inside load_state_from_supabase.
    import cadence as _cadence
    import scrape_state as _scrape_state
    cadence_state_map: dict = {}
    try:
        cadence_state_map = _cadence.load_state_from_supabase(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"WARN: cadence state load failed: {e}. Continuing without state.")
        cadence_state_map = {}
    if cadence in ("daily", "weekly"):
        try:
            before = len(cards)
            cards, hist = _cadence.filter_cards_by_cadence(cards, cadence, cadence_state_map)
            print(f"\n[cadence={cadence}] filtered {before} → {len(cards)} cards")
            for c, n in hist.items():
                print(f"  {c.value:<18} : {n}")
        except Exception as e:
            print(f"ERROR: cadence filter failed: {e}. Aborting to avoid partial coverage.")
            sys.exit(1)
        if not cards:
            print("No cards remain after cadence filter. Nothing to do.")
            sys.exit(0)

    scrape_state_buffer = _scrape_state.ScrapeStateBuffer(state_map=cadence_state_map)

    if test_mode:
        cards = cards[:5]
        include_history = True
        print(f"TEST MODE: {len(cards)} cards with full history")

    today = datetime.now().strftime("%Y-%m-%d")
    history_label = "WITH HISTORY" if include_history else "DAILY ONLY"
    est_seconds = len(cards) * (REQUEST_DELAY + 0.8)

    print(f"\n{'='*60}")
    print(f"PokePrices Scraper v8 — {history_label}")
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

    # Optional recent-sales ingestion. init_for_scraper_run returns None
    # whenever the feature flag is not exactly "true" OR when Supabase env
    # vars / the allow-list cannot be loaded. A None controller means the
    # main loop's ingestion hook is a no-op and the price-scrape proceeds
    # exactly as before.
    recent_sales_ingestion = None
    if _rsi is not None:
        try:
            recent_sales_ingestion = _rsi.init_for_scraper_run()
        except Exception as e:
            print(f"WARNING: recent-sales ingestion init failed: {e}")
            recent_sales_ingestion = None
    elif _rsi_import_error is not None:
        print(f"WARNING: recent_sales_ingestion module import failed: {_rsi_import_error}")

    for i, card in enumerate(cards):
        product_name = card["product_name"]
        console_name = card["console_name"]
        card_slug = card["card_slug"]
        pc_id = card["pc_id"]
        url = card["url"]

        print(f"[{i+1}/{len(cards)}] {product_name} ({console_name})")

        html = fetch_card_page(url)
        # Block 5A-W-49-FIX2 — capture the fetch outcome for
        # scrape_attempt_state. `LAST_FETCH_OUTCOME` is set inside
        # fetch_card_page before every return path.
        fetch_outcome = dict(LAST_FETCH_OUTCOME)

        # Recent-sales ingestion (flag-gated + allow-list-gated).
        # Runs BEFORE the current-price `not current` gate so a card whose
        # price block was missing but whose recent-sales section was present
        # still has its sales captured.
        if recent_sales_ingestion is not None and html:
            try:
                expected = _rsi.parse_expected_card_number(product_name) if _rsi else None
                recent_sales_ingestion.maybe_ingest(
                    html=html,
                    provider_card_id=pc_id,
                    page_url=url,
                    expected_card_number=expected,
                )
            except Exception as e:
                print(f"  recent-sales hook error: {e}")

        current = None
        if html:
            current = {**extract_full_price_guide(html), **extract_current_prices(html)} or None

        if not current:
            not_found += 1
            print(f"  ✗ No price data at {url}")
            # Block 5A-W-49-FIX2 — record the scrape outcome. If fetch
            # returned None (404 / transient / timeout), fetch_outcome
            # already holds the right category. If fetch returned 200
            # but no prices extracted, category is `page_reached_no_price`.
            state_category = fetch_outcome.get("category")
            if state_category == "ok":
                state_category = "page_reached_no_price"
            elif state_category is None:
                state_category = "transient_http_failure"
            scrape_state_buffer.record(
                bare_slug=str(pc_id),
                category=state_category,
                http_status=fetch_outcome.get("http_status"),
            )
            time.sleep(REQUEST_DELAY)
            continue

        found += 1
        # Block 5A-W-49-FIX2 — successful priced result resets streak.
        scrape_state_buffer.record(
            bare_slug=str(pc_id),
            category="priced",
            http_status=fetch_outcome.get("http_status") or 200,
        )
        records = []

        # Image extraction
        if html:
            image_url = extract_image_url(html)
            if image_url or url:
                updated = update_card_image(pc_id, image_url, url)
                if updated and image_url:
                    images_saved += 1
                    print(f"  🖼  Image saved")

        # Volume extraction — all grades
        if html:
            volume_by_grade = extract_sales_volume(html)
            for grade, (sales_monthly, volume_text) in volume_by_grade.items():
                ok = upsert_card_volume(card_slug, sales_monthly, volume_text, grade)
                if ok:
                    volumes_saved += 1
            if volume_by_grade:
                ungraded = volume_by_grade.get('Ungraded')
                if ungraded:
                    print(f"  📊 Volume: {ungraded[1]} ({len(volume_by_grade)} grades)")

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

        # Block 5A-W-49-FIX2 — flush the scrape_attempt_state buffer
        # every 100 cards so a mid-run interruption preserves most of
        # the outcome writes.
        if scrape_state_buffer.pending_count() >= 100:
            if not scrape_state_buffer.flush(SUPABASE_URL, SUPABASE_KEY):
                errors += 1

        time.sleep(REQUEST_DELAY)

    # Block 5A-W-49-FIX2 — final buffer flush before we tear down.
    if scrape_state_buffer.pending_count() > 0:
        if not scrape_state_buffer.flush(SUPABASE_URL, SUPABASE_KEY):
            errors += 1

    if recent_sales_ingestion is not None:
        try:
            recent_sales_ingestion.finish(status="success")
        except Exception as e:
            print(f"WARNING: recent-sales ingestion finish failed: {e}")

    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Cards found:      {found}")
    print(f"Cards not found:  {not_found}")
    print(f"Errors:           {errors}")
    print(f"Images saved:     {images_saved}")
    print(f"Volumes saved:    {volumes_saved}")
    print(f"Records pushed:   {total_records}")
    print(f"scrape_state errors: {scrape_state_buffer.error_count()}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()