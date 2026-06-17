"""
audit_recent_sales_fixtures.py — Block 4A-S1

Local-only review tool. Parses every HTML fixture in
``tests/fixtures/recent_sales/`` and prints a compact summary.

This script:
  * never makes network requests
  * never writes to Supabase
  * is not imported by the production scraper

Usage:
    python audit_recent_sales_fixtures.py
    python audit_recent_sales_fixtures.py --verbose   # per-row dump for quarantines
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter, defaultdict
from urllib.parse import urlsplit

from recent_sales_parser import (
    RECENT_SALES_PARSER_VERSION,
    parse_recent_sales,
)

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "tests", "fixtures", "recent_sales",
)

# Stable mapping from fixture filename to the page URL it was captured from.
# Keeping this here (rather than embedding it in fixture metadata) avoids
# touching the fixture files themselves and matches _refresh_fixtures.py.
FIXTURE_URLS: dict[str, tuple[str, str | None]] = {
    "base_charizard_1st.html":
        ("https://www.pricecharting.com/game/pokemon-base-set/charizard-1st-edition-4", "4"),
    "svp_pikachu_holo.html":
        ("https://www.pricecharting.com/game/pokemon-promo/pikachu-with-grey-felt-hat-085", "85"),
    "obsidian_charizard_ex.html":
        ("https://www.pricecharting.com/game/pokemon-obsidian-flames/charizard-ex-125", "125"),
    "base_booster_box.html":
        ("https://www.pricecharting.com/game/pokemon-base-set/booster-box", None),
    "pop3_celebi.html":
        ("https://www.pricecharting.com/game/pokemon-pop-series-3/celebi-3", "3"),
    "jp_promo_pikachu.html":
        ("https://www.pricecharting.com/game/jp-pokemon-promo/pikachu-001smp", None),
}


def _slug_from_url(url: str) -> str:
    try:
        last = urlsplit(url).path.rsplit("/", 1)[-1]
        return last or "pc-unknown"
    except ValueError:
        return "pc-unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit recent-sales parser against fixtures")
    parser.add_argument("--verbose", action="store_true",
                        help="dump quarantined/rejected rows in full")
    args = parser.parse_args()

    if not os.path.isdir(FIXTURE_DIR):
        print(f"ERROR: fixture dir not found: {FIXTURE_DIR}", file=sys.stderr)
        return 1

    files = sorted(f for f in os.listdir(FIXTURE_DIR) if f.endswith(".html"))
    if not files:
        print(f"No .html fixtures in {FIXTURE_DIR}")
        return 0

    print(f"recent_sales_parser version: {RECENT_SALES_PARSER_VERSION}")
    print(f"Fixture dir: {FIXTURE_DIR}")
    print(f"Fixtures: {len(files)}\n")

    grand_status = Counter()
    grand_marketplaces = Counter()
    grand_sections = Counter()
    grand_quarantine_reasons = Counter()
    grand_rejection_reasons = Counter()
    grand_row_status = Counter()

    for name in files:
        path = os.path.join(FIXTURE_DIR, name)
        try:
            with open(path, encoding="utf-8") as f:
                html = f.read()
        except OSError as e:
            print(f"[{name}] read error: {e}")
            continue

        url, expected_num = FIXTURE_URLS.get(name, (
            f"https://www.pricecharting.com/game/unknown/{name[:-5]}",
            None,
        ))
        result = parse_recent_sales(
            html,
            page_url=url,
            provider_card_id=_slug_from_url(url),
            internal_card_slug=f"pc-{_slug_from_url(url)}",
            expected_card_number=expected_num,
        )

        grand_status[result.parse_status] += 1

        marketplaces = Counter(r.marketplace_source for r in result.rows)
        sections = Counter(r.observed_section for r in result.rows)
        row_status = Counter(r.parse_status for r in result.rows)
        quarantine_reasons = Counter(
            r.rejection_reason for r in result.rows
            if r.parse_status == "quarantined" and r.rejection_reason
        )
        rejection_reasons = Counter(
            r.rejection_reason for r in result.rows
            if r.parse_status == "rejected" and r.rejection_reason
        )

        grand_marketplaces.update(marketplaces)
        grand_sections.update(sections)
        grand_quarantine_reasons.update(quarantine_reasons)
        grand_rejection_reasons.update(rejection_reasons)
        grand_row_status.update(row_status)

        print(f"--{name}")
        print(f"   url:             {url}")
        print(f"   page status:     {result.parse_status}")
        print(f"   language:        {result.language}")
        print(f"   sections:        {result.section_count}")
        print(f"   rows:            {result.row_count}")
        print(f"   ok / q / rej:    {row_status.get('ok', 0)} / "
              f"{result.quarantined_count} / {result.rejected_count}")
        if marketplaces:
            mk = ", ".join(f"{k}={v}" for k, v in marketplaces.most_common())
            print(f"   marketplaces:    {mk}")
        if sections:
            top = ", ".join(f"{k}={v}" for k, v in sections.most_common(6))
            extra = "" if len(sections) <= 6 else f" (+{len(sections)-6} more)"
            print(f"   sections (top):  {top}{extra}")
        if quarantine_reasons:
            qr = ", ".join(f"{k}={v}" for k, v in quarantine_reasons.most_common())
            print(f"   quarantine_why:  {qr}")
        if rejection_reasons:
            rr = ", ".join(f"{k}={v}" for k, v in rejection_reasons.most_common())
            print(f"   rejection_why:   {rr}")
        if result.warnings:
            for w in result.warnings[:5]:
                print(f"   warn:            {w}")
        print(f"   layout_sig:      {result.layout_signature[:16]}...")

        if args.verbose:
            for r in result.rows:
                if r.parse_status != "ok":
                    print(f"     [{r.parse_status}] {r.rejection_reason}  "
                          f"section={r.observed_section}  mkt={r.marketplace_source}  "
                          f"title={(r.listing_title or '')[:80]!r}")
        print()

    print("-" * 60)
    print("Aggregate across fixtures")
    print("-" * 60)
    print(f"page_status:        {dict(grand_status)}")
    print(f"rows_per_status:    {dict(grand_row_status)}")
    print(f"marketplaces:       {dict(grand_marketplaces.most_common())}")
    print(f"sections:           {len(grand_sections)} distinct; "
          f"top={dict(grand_sections.most_common(8))}")
    if grand_quarantine_reasons:
        print(f"quarantine_reasons: {dict(grand_quarantine_reasons)}")
    if grand_rejection_reasons:
        print(f"rejection_reasons:  {dict(grand_rejection_reasons)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
