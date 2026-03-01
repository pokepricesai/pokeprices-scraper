#!/usr/bin/env python3
"""
PSA Population Report Scraper for PokePrices
=============================================
Fetches PSA pop report pages for all Pokemon TCG sets,
parses the HTML tables, and upserts data into Supabase.

Runs weekly via GitHub Actions.

Usage:
    python scrape_psa_pop.py                    # Scrape all sets
    python scrape_psa_pop.py --set "Base Set"   # Scrape one set
    python scrape_psa_pop.py --dry-run          # Parse only, no DB write
"""

import json
import os
import re
import sys
import time
import argparse
import logging
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")  # Use service key for writes

REQUEST_DELAY = 3  # Seconds between requests (be polite)
MAX_PER_PAGE = 500  # PSA supports 300/400/500
REQUEST_TIMEOUT = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Grade column indices: Auth, 1, 1.5, 2, 3, 4, 5, 6, 7, 8, 9, 10, Total
GRADE_LABELS = ["auth", "psa_1", "psa_1_5", "psa_2", "psa_3", "psa_4",
                "psa_5", "psa_6", "psa_7", "psa_8", "psa_9", "psa_10", "total_graded"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("psa_scraper")

# ---------------------------------------------------------------------------
# Load set config
# ---------------------------------------------------------------------------

def load_sets_config():
    """Load set config from JSON file."""
    config_path = os.path.join(os.path.dirname(__file__), "psa_sets_config.json")
    with open(config_path, "r") as f:
        return json.load(f)

# ---------------------------------------------------------------------------
# HTML Fetching
# ---------------------------------------------------------------------------

def fetch_page(url, page=1, session=None):
    """Fetch a single PSA pop report page."""
    if session is None:
        session = requests.Session()
    
    # Add pagination params
    params = {
        "pf": MAX_PER_PAGE,  # Page size
    }
    if page > 1:
        params["page"] = page
    
    try:
        resp = session.get(url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        log.error(f"Failed to fetch {url}: {e}")
        return None

# ---------------------------------------------------------------------------
# HTML Parsing
# ---------------------------------------------------------------------------

def parse_value(text):
    """Parse a grade count value, handling dashes and commas."""
    text = text.strip()
    # Handle various dash characters (en-dash, em-dash, hyphen, minus)
    if text in ("–", "—", "-", "\u2013", "\u2014", "\u002d", ""):
        return 0
    # Remove commas from numbers like 1,234
    text = text.replace(",", "")
    try:
        return int(text)
    except ValueError:
        return 0

def parse_pop_table(html, set_name, year=""):
    """Parse a PSA pop report HTML page into card records."""
    soup = BeautifulSoup(html, "html.parser")
    
    # Find the table body
    tbody = soup.find("tbody")
    if not tbody:
        log.warning(f"No table found for {set_name}")
        return []
    
    rows = tbody.find_all("tr")
    cards = []
    today = date.today().isoformat()
    
    for row in rows:
        tds = row.find_all("td")
        if len(tds) < 17:
            continue
        
        # td[0] = control (hidden)
        # td[1] = card number
        # td[2] = card name + variant + shop link
        # td[3] = Grade/+/Q labels
        # td[4:17] = 13 grade columns (Auth, 1, 1.5, 2, 3, 4, 5, 6, 7, 8, 9, 10, Total)
        
        card_number = tds[1].get_text(strip=True)
        
        # Skip TOTAL POPULATION row (empty card number)
        if not card_number:
            continue
        
        # Extract card name from <strong> tag
        name_td = tds[2]
        strong = name_td.find("strong")
        card_name = strong.get_text(strip=True) if strong else ""
        
        if not card_name:
            continue
        
        # Extract variant - text after <br> tag
        variant = ""
        br = name_td.find("br")
        if br and br.next_sibling:
            # Get text between <br> and <a> (shop link)
            sib = br.next_sibling
            if isinstance(sib, str):
                variant = sib.strip()
            elif hasattr(sib, "get_text"):
                # Sometimes variant is wrapped in a tag
                variant = sib.get_text(strip=True)
        
        # Clean variant - remove "Shop with Affiliates" if it leaked in
        variant = variant.replace("Shop with Affiliates", "").strip()
        
        # Build full name
        full_name = card_name
        if variant:
            full_name = f"{card_name} ({variant})"
        
        # Extract PSA spec ID from shop link data-id attribute
        shop_link = name_td.find("a", class_="shop-link")
        psa_spec_id = ""
        if shop_link and shop_link.get("data-id"):
            psa_spec_id = shop_link["data-id"]
        
        # Parse grade columns
        grades = {}
        for idx, label in enumerate(GRADE_LABELS):
            td_idx = idx + 4  # Grade columns start at td[4]
            if td_idx < len(tds):
                divs = tds[td_idx].find_all("div")
                if divs:
                    # First div = Grade row (main count)
                    grades[label] = parse_value(divs[0].get_text(strip=True))
                else:
                    grades[label] = 0
            else:
                grades[label] = 0
        
        # Calculate gem rate
        total = grades.get("total_graded", 0)
        psa_10 = grades.get("psa_10", 0)
        gem_rate = round((psa_10 / total * 100), 2) if total > 0 else 0.0
        
        card = {
            "set_name": set_name,
            "release_year": year,
            "card_number": card_number,
            "card_name": card_name,
            "variant": variant,
            "full_name": full_name,
            "psa_spec_id": psa_spec_id,
            **grades,
            "gem_rate": gem_rate,
            "scraped_date": today,
        }
        cards.append(card)
    
    return cards

def get_total_pages(html):
    """Check if there are multiple pages of results."""
    soup = BeautifulSoup(html, "html.parser")
    
    # Look for pagination: "Showing 1 to 500 of 1234"
    info = soup.find("div", class_="dataTables_info")
    if info:
        text = info.get_text(strip=True)
        match = re.search(r"of\s+([\d,]+)", text)
        if match:
            total_records = int(match.group(1).replace(",", ""))
            total_pages = (total_records + MAX_PER_PAGE - 1) // MAX_PER_PAGE
            return total_pages, total_records
    
    return 1, 0

# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------

def upsert_to_supabase(cards, dry_run=False):
    """Upsert card records to Supabase psa_population table."""
    if dry_run or not SUPABASE_URL or not SUPABASE_KEY:
        log.info(f"{'DRY RUN: ' if dry_run else 'NO DB CONFIG: '}Would upsert {len(cards)} records")
        return len(cards)
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    
    # Upsert in batches of 500
    batch_size = 500
    total_upserted = 0
    
    for i in range(0, len(cards), batch_size):
        batch = cards[i:i + batch_size]
        
        try:
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/psa_population",
                headers=headers,
                json=batch,
                timeout=30,
            )
            if resp.status_code in (200, 201):
                total_upserted += len(batch)
            else:
                log.error(f"Upsert batch failed ({resp.status_code}): {resp.text[:200]}")
        except requests.RequestException as e:
            log.error(f"Upsert request failed: {e}")
    
    return total_upserted

def save_history_snapshot(cards, dry_run=False):
    """Save a snapshot to psa_pop_history for trend tracking."""
    if dry_run or not SUPABASE_URL or not SUPABASE_KEY:
        log.info(f"{'DRY RUN: ' if dry_run else 'NO DB CONFIG: '}Would save {len(cards)} history records")
        return
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    
    # Build slim history records
    history = []
    for card in cards:
        if card["total_graded"] > 0:  # Only track cards with actual grades
            history.append({
                "set_name": card["set_name"],
                "card_number": card["card_number"],
                "card_name": card["card_name"],
                "variant": card["variant"],
                "psa_spec_id": card.get("psa_spec_id", ""),
                "psa_8": card["psa_8"],
                "psa_9": card["psa_9"],
                "psa_10": card["psa_10"],
                "total_graded": card["total_graded"],
                "gem_rate": card["gem_rate"],
                "snapshot_date": card["scraped_date"],
            })
    
    # Insert in batches
    batch_size = 500
    for i in range(0, len(history), batch_size):
        batch = history[i:i + batch_size]
        try:
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/psa_pop_history",
                headers=headers,
                json=batch,
                timeout=30,
            )
            if resp.status_code not in (200, 201):
                log.error(f"History insert failed ({resp.status_code}): {resp.text[:200]}")
        except requests.RequestException as e:
            log.error(f"History request failed: {e}")

# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

def scrape_set(set_info, session, dry_run=False):
    """Scrape a single set's PSA pop report."""
    name = set_info["name"]
    url = set_info.get("url")
    year = set_info.get("year", "")
    
    if not url or url == "None":
        log.warning(f"Skipping {name} - no URL")
        return []
    
    # Skip non-pop URLs (auction prices, etc.)
    if "/auctionprices/" in url:
        log.warning(f"Skipping {name} - auction prices URL, not pop report")
        return []
    
    log.info(f"Scraping: {name} ({year})")
    
    # Fetch first page
    html = fetch_page(url, page=1, session=session)
    if not html:
        return []
    
    # Check total pages
    total_pages, total_records = get_total_pages(html)
    log.info(f"  Found {total_records} records across {total_pages} page(s)")
    
    # Parse first page
    all_cards = parse_pop_table(html, name, year)
    
    # Fetch remaining pages if needed
    if total_pages > 1:
        for page in range(2, total_pages + 1):
            log.info(f"  Fetching page {page}/{total_pages}...")
            time.sleep(REQUEST_DELAY)
            html = fetch_page(url, page=page, session=session)
            if html:
                page_cards = parse_pop_table(html, name, year)
                all_cards.extend(page_cards)
    
    log.info(f"  Parsed {len(all_cards)} card entries")
    return all_cards

def main():
    parser = argparse.ArgumentParser(description="PSA Population Report Scraper")
    parser.add_argument("--set", type=str, help="Scrape a specific set by name")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, no DB writes")
    parser.add_argument("--batch", type=int, help="Batch number (1-4) for parallel execution")
    parser.add_argument("--batches", type=int, default=4, help="Total number of batches")
    args = parser.parse_args()
    
    # Load config
    sets = load_sets_config()
    log.info(f"Loaded {len(sets)} sets from config")
    
    # Filter to specific set if requested
    if args.set:
        sets = [s for s in sets if args.set.lower() in s["name"].lower()]
        if not sets:
            log.error(f"No set found matching '{args.set}'")
            sys.exit(1)
    
    # Split into batches for parallel execution
    if args.batch:
        batch_size = len(sets) // args.batches + 1
        start = (args.batch - 1) * batch_size
        end = min(start + batch_size, len(sets))
        sets = sets[start:end]
        log.info(f"Batch {args.batch}/{args.batches}: processing sets {start+1}-{end} ({len(sets)} sets)")
    
    # Create session for connection reuse
    session = requests.Session()
    session.headers.update(HEADERS)
    
    total_cards = 0
    total_sets_scraped = 0
    failed_sets = []
    
    for i, set_info in enumerate(sets):
        try:
            cards = scrape_set(set_info, session, dry_run=args.dry_run)
            
            if cards:
                # Upsert to main table
                upserted = upsert_to_supabase(cards, dry_run=args.dry_run)
                total_cards += upserted
                total_sets_scraped += 1
                
                # Save history snapshot
                save_history_snapshot(cards, dry_run=args.dry_run)
            
            # Polite delay between sets
            if i < len(sets) - 1:
                time.sleep(REQUEST_DELAY)
                
        except Exception as e:
            log.error(f"Error scraping {set_info['name']}: {e}")
            failed_sets.append(set_info["name"])
    
    # Summary
    log.info("=" * 50)
    log.info(f"COMPLETE: {total_sets_scraped}/{len(sets)} sets scraped")
    log.info(f"Total cards upserted: {total_cards}")
    if failed_sets:
        log.warning(f"Failed sets: {', '.join(failed_sets)}")
    log.info("=" * 50)

if __name__ == "__main__":
    main()
