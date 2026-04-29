#!/usr/bin/env python3
"""
PSA Population Report - Local HTML Parser
==========================================
Parses saved PSA pop report HTML files and upserts to Supabase.

Save pages from your browser (Ctrl+S, Webpage Complete) into a folder, then run:

    python parse_psa_html.py --folder psa_html --dry-run     # Test parse only
    python parse_psa_html.py --folder psa_html                # Parse + push to Supabase
    python parse_psa_html.py --file fossil.html --dry-run     # Single file test
"""

import json
import os
import re
import sys
import glob
import argparse
import logging
from datetime import date

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

GRADE_LABELS = ["auth", "psa_1", "psa_1_5", "psa_2", "psa_3", "psa_4",
                "psa_5", "psa_6", "psa_7", "psa_8", "psa_9", "psa_10", "total_graded"]

# Foreign language variants to exclude
FOREIGN_LANGUAGES = [
    "german", "french", "italian", "spanish", "portuguese",
    "korean", "japanese", "chinese", "dutch", "russian",
    "thai", "indonesian", "polish", "czech", "turkish"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("psa_parser")

# ---------------------------------------------------------------------------
# Set config lookup
# ---------------------------------------------------------------------------

def load_sets_config():
    """Load set config and build headingID lookup."""
    config_path = os.path.join(os.path.dirname(__file__), "psa_sets_config.json")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            sets = json.load(f)
        # Build lookup by URL's numeric ID (headingID)
        id_lookup = {}
        for s in sets:
            url = s.get("url") or ""
            match = re.search(r'/(\d+)$', url.rstrip('/'))
            if match:
                id_lookup[match.group(1)] = s
        return sets, id_lookup
    return [], {}

def find_set_info(html, sets_config, id_lookup):
    """Extract set name and year from the HTML page using headingID matching."""
    soup = BeautifulSoup(html, "html.parser")
    
    # Get page title for fallback
    title = soup.find("title")
    title_text = ""
    if title:
        title_text = title.get_text(strip=True)
        title_text = re.sub(r'\s*[\|–—]\s*PSA.*$', '', title_text)
    
    # PRIMARY: Match via headingID in JavaScript
    heading_match = re.search(r'"headingID":\s*(\d+)', html)
    if heading_match:
        heading_id = heading_match.group(1)
        if heading_id in id_lookup:
            s = id_lookup[heading_id]
            year = s.get("year", "")
            # If no year in config, try to extract from title
            if not year:
                year_match = re.match(r'(\d{4})', title_text)
                year = year_match.group(1) if year_match else ""
            log.info(f"  Matched headingID {heading_id} -> {s['name']}")
            return s["name"], year, heading_id
    
    # FALLBACK: Extract from title
    set_name = ""
    year = ""
    if title_text:
        match = re.match(r'(\d{4})\s+(.+?)\s+TCG Cards', title_text)
        if match:
            year = match.group(1)
            raw_name = match.group(2)
            if raw_name.startswith("Pokemon"):
                set_name = raw_name
            else:
                set_name = f"Pokemon {raw_name}"
        else:
            match = re.match(r'(\d{4})\s+(.+?)\s+(?:Non-Sport\s+)?Cards', title_text)
            if match:
                year = match.group(1)
                raw_name = match.group(2)
                if raw_name.startswith("Pokemon"):
                    set_name = raw_name
                else:
                    set_name = f"Pokemon {raw_name}"
            else:
                set_name = title_text
    
    heading_id = heading_match.group(1) if heading_match else ""
    log.warning(f"  No config match for headingID {heading_id}, using title: {set_name}")
    return set_name, year, heading_id

# ---------------------------------------------------------------------------
# HTML Parsing
# ---------------------------------------------------------------------------

def parse_value(text):
    """Parse a grade count value, handling dashes and commas."""
    text = text.strip()
    if text in ("\u2013", "\u2014", "-", "\u002d", ""):
        return 0
    text = text.replace(",", "")
    try:
        return int(text)
    except ValueError:
        return 0

def is_foreign_language(variant):
    """Check if a variant string indicates a foreign language card."""
    variant_lower = variant.lower().strip()
    for lang in FOREIGN_LANGUAGES:
        if variant_lower == lang:
            return True
        if re.search(r'\b' + lang + r'\b', variant_lower):
            return True
    return False

def parse_pop_table(html, set_name, year=""):
    """Parse a PSA pop report HTML page into card records."""
    soup = BeautifulSoup(html, "html.parser")
    
    tbody = soup.find("tbody")
    if not tbody:
        log.warning(f"  No table found in HTML")
        return [], None
    
    rows = tbody.find_all("tr")
    cards = []
    set_total = None
    today = date.today().isoformat()
    foreign_skipped = 0
    
    for row in rows:
        tds = row.find_all("td")
        if len(tds) < 17:
            continue
        
        card_number = tds[1].get_text(strip=True)
        
        # TOTAL POPULATION row - extract set totals
        if not card_number:
            name_text = tds[2].get_text(strip=True)
            if "TOTAL POPULATION" in name_text.upper():
                grades = {}
                for idx, label in enumerate(GRADE_LABELS):
                    td_idx = idx + 4
                    if td_idx < len(tds):
                        divs = tds[td_idx].find_all("div")
                        if divs:
                            grades[label] = parse_value(divs[0].get_text(strip=True))
                        else:
                            grades[label] = 0
                total = grades.get("total_graded", 0)
                psa10 = grades.get("psa_10", 0)
                set_total = {
                    "set_name": set_name,
                    "release_year": year,
                    "total_graded": total,
                    "total_psa_10": psa10,
                    "total_psa_9": grades.get("psa_9", 0),
                    "total_psa_8": grades.get("psa_8", 0),
                    "gem_rate": round((psa10 / total * 100), 2) if total > 0 else 0,
                    "snapshot_date": today,
                }
            continue
        
        # Card name from <strong>
        name_td = tds[2]
        strong = name_td.find("strong")
        card_name = strong.get_text(strip=True) if strong else ""
        
        if not card_name:
            continue
        
        # Variant after <br>
        variant = ""
        br = name_td.find("br")
        if br and br.next_sibling:
            sib = br.next_sibling
            if isinstance(sib, str):
                variant = sib.strip()
            elif hasattr(sib, "get_text"):
                variant = sib.get_text(strip=True)
        variant = variant.replace("Shop with Affiliates", "").strip()
        
        # Skip foreign language cards
        if is_foreign_language(variant):
            foreign_skipped += 1
            continue
        
        full_name = f"{card_name} ({variant})" if variant else card_name
        
        # PSA spec ID
        shop_link = name_td.find("a", class_="shop-link")
        psa_spec_id = ""
        if shop_link and shop_link.get("data-id"):
            psa_spec_id = shop_link["data-id"]
        
        # Grade columns
        grades = {}
        for idx, label in enumerate(GRADE_LABELS):
            td_idx = idx + 4
            if td_idx < len(tds):
                divs = tds[td_idx].find_all("div")
                if divs:
                    grades[label] = parse_value(divs[0].get_text(strip=True))
                else:
                    grades[label] = 0
            else:
                grades[label] = 0
        
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
    
    if foreign_skipped > 0:
        log.info(f"  Skipped {foreign_skipped} foreign language entries")
    
    return cards, set_total

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
                log.error(f"Upsert batch failed ({resp.status_code}): {resp.text[:300]}")
        except requests.RequestException as e:
            log.error(f"Upsert request failed: {e}")
    
    return total_upserted

def save_history_snapshot(cards, dry_run=False):
    """Save a snapshot to psa_pop_history for trend tracking."""
    if dry_run or not SUPABASE_URL or not SUPABASE_KEY:
        return
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    
    history = []
    for card in cards:
        if card["total_graded"] > 0:
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

def save_set_totals(set_total, dry_run=False):
    """Save set-level totals to psa_set_totals table."""
    if not set_total:
        return
    if dry_run or not SUPABASE_URL or not SUPABASE_KEY:
        log.info(f"  {'DRY RUN: ' if dry_run else ''}Set total: {set_total['total_graded']:,} graded, {set_total['total_psa_10']:,} PSA 10s, {set_total['gem_rate']}% gem rate")
        return
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    
    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/psa_set_totals",
            headers=headers,
            json=[set_total],
            timeout=30,
        )
        if resp.status_code in (200, 201):
            log.info(f"  Saved set total: {set_total['total_graded']:,} graded, {set_total['total_psa_10']:,} PSA 10s, {set_total['gem_rate']}% gem rate")
        else:
            log.error(f"Set total upsert failed ({resp.status_code}): {resp.text[:200]}")
    except requests.RequestException as e:
        log.error(f"Set total request failed: {e}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_file(filepath, sets_config, id_lookup, dry_run=False):
    """Process a single HTML file."""
    log.info(f"Processing: {os.path.basename(filepath)}")
    
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        html = f.read()
    
    # Identify set via headingID matching
    set_name, year, heading_id = find_set_info(html, sets_config, id_lookup)
    log.info(f"  Set: {set_name} | Year: {year}")
    
    if not set_name:
        log.warning(f"  Could not identify set, skipping")
        return 0
    
    # Parse
    cards, set_total = parse_pop_table(html, set_name, year)
    log.info(f"  Parsed {len(cards)} card entries (English only)")
    
    if not cards:
        return 0
    
    # Show sample
    for c in cards[:3]:
        log.info(f"    #{c['card_number']} {c['full_name']} - Total: {c['total_graded']}, PSA10: {c['psa_10']}, Gem: {c['gem_rate']}%")
    
    # Stats
    total_graded = sum(c["total_graded"] for c in cards)
    total_10s = sum(c["psa_10"] for c in cards)
    log.info(f"  English card totals: {total_graded:,} graded, {total_10s:,} PSA 10s")
    
    if set_total:
        log.info(f"  Set total (all languages): {set_total['total_graded']:,} graded")
    
    # Upsert
    upserted = upsert_to_supabase(cards, dry_run=dry_run)
    save_history_snapshot(cards, dry_run=dry_run)
    save_set_totals(set_total, dry_run=dry_run)
    
    return upserted

def main():
    parser = argparse.ArgumentParser(description="PSA Pop Report HTML Parser")
    parser.add_argument("--folder", type=str, help="Folder containing saved HTML files")
    parser.add_argument("--file", type=str, help="Single HTML file to parse")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, no DB writes")
    args = parser.parse_args()
    
    if not args.folder and not args.file:
        print("Usage: python parse_psa_html.py --folder psa_html --dry-run")
        print("       python parse_psa_html.py --file fossil.html --dry-run")
        sys.exit(1)
    
    sets_config, id_lookup = load_sets_config()
    log.info(f"Loaded {len(sets_config)} sets from config ({len(id_lookup)} with IDs)")
    
    # Collect HTML files
    files = []
    if args.file:
        files = [args.file]
    elif args.folder:
        files = sorted(glob.glob(os.path.join(args.folder, "*.html")) +
                       glob.glob(os.path.join(args.folder, "*.htm")))
    
    if not files:
        log.error("No HTML files found")
        sys.exit(1)
    
    log.info(f"Found {len(files)} HTML file(s)")
    log.info("=" * 50)
    
    total_upserted = 0
    total_files = 0
    failed_files = []
    
    for filepath in files:
        try:
            count = process_file(filepath, sets_config, id_lookup, dry_run=args.dry_run)
            if count > 0:
                total_upserted += count
                total_files += 1
        except Exception as e:
            log.error(f"Error processing {filepath}: {e}")
            failed_files.append(os.path.basename(filepath))
        log.info("-" * 50)
    
    log.info("=" * 50)
    log.info(f"COMPLETE: {total_files}/{len(files)} files processed")
    log.info(f"Total records: {total_upserted}")
    if failed_files:
        log.warning(f"Failed: {', '.join(failed_files)}")
    log.info("=" * 50)

if __name__ == "__main__":
    main()
