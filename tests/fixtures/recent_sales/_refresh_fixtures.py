"""
Manual fixture-refresh script for the recent-sales parser.

DO NOT RUN AUTOMATICALLY. This script makes live HTTP GETs to PriceCharting
and is intended only for the engineer who needs to re-snap fixtures after a
suspected layout change. It uses the same headers/session as the production
scraper and adds a 1 s delay between fetches; it overwrites any existing
fixture only when the env var FORCE=1 is set.

Usage (PowerShell):
    cd <repo>
    $env:FORCE=1; python tests/fixtures/recent_sales/_refresh_fixtures.py

The set of fixtures here mirrors the Block 4A-S audit sample. If you add a
new fixture, append to SAMPLES and document the scenario in
docs/recent-sales-parser.md.
"""
import os
import sys
import time
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}
session = requests.Session()
session.headers.update(HEADERS)

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

SAMPLES = [
    ("base_charizard_1st",
     "https://www.pricecharting.com/game/pokemon-base-set/charizard-1st-edition-4"),
    ("svp_pikachu_holo",
     "https://www.pricecharting.com/game/pokemon-promo/pikachu-with-grey-felt-hat-085"),
    ("obsidian_charizard_ex",
     "https://www.pricecharting.com/game/pokemon-obsidian-flames/charizard-ex-125"),
    ("base_booster_box",
     "https://www.pricecharting.com/game/pokemon-base-set/booster-box"),
    ("pop3_celebi",
     "https://www.pricecharting.com/game/pokemon-pop-series-3/celebi-3"),
    ("jp_promo_pikachu",
     "https://www.pricecharting.com/game/jp-pokemon-promo/pikachu-001smp"),
]


def main() -> int:
    force = os.environ.get("FORCE") == "1"
    for name, url in SAMPLES:
        out = os.path.join(OUT_DIR, f"{name}.html")
        if os.path.exists(out) and not force:
            print(f"skip (exists, FORCE!=1): {name}")
            continue
        print(f"fetching: {name}  {url}")
        try:
            resp = session.get(url, timeout=15)
            print(f"  status={resp.status_code} bytes={len(resp.text)}")
            if resp.status_code != 200:
                print(f"  WARNING: non-200; fixture not written")
                continue
            with open(out, "w", encoding="utf-8") as f:
                f.write(resp.text)
        except Exception as e:
            print(f"  ERROR: {e}")
        time.sleep(1.0)
    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
