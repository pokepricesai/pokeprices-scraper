"""
refresh_card_trends.py
Replaces the inline Python in the GitHub Actions refresh-and-analytics job.
Key fix: uses per-card nearest-date window lookback instead of a single
global anchor date — prevents NULL pct for cards missing on one specific day.

Deploy: add this file to repo root, update the YAML step to run:
  python refresh_card_trends.py
"""

import os
import requests
from datetime import date, timedelta

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}
POST_HEADERS = {**HEADERS, "Prefer": "return=minimal"}

today = date.today()
today_str = today.isoformat()


def find_nearest_date(target_days_ago, window=5):
    """
    Find the most recent date in daily_prices within a window around
    target_days_ago. Looks back up to window days either side.
    Returns date string or None.
    """
    target = (today - timedelta(days=target_days_ago)).isoformat()
    low    = (today - timedelta(days=target_days_ago + window)).isoformat()

    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/daily_prices"
        f"?select=date&date=lte.{target}&date=gte.{low}&order=date.desc&limit=1",
        headers=HEADERS, timeout=15,
    )
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list) and data:
            return data[0]["date"]
    # Fall back: just find nearest before target with no lower bound
    r2 = requests.get(
        f"{SUPABASE_URL}/rest/v1/daily_prices"
        f"?select=date&date=lte.{target}&order=date.desc&limit=1",
        headers=HEADERS, timeout=15,
    )
    if r2.status_code == 200:
        data2 = r2.json()
        if isinstance(data2, list) and data2:
            return data2[0]["date"]
    return None


def fetch_all(endpoint):
    rows = []
    offset = 0
    while True:
        sep = "&" if "?" in endpoint else "?"
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{endpoint}{sep}offset={offset}&limit=1000",
            headers=HEADERS, timeout=30,
        )
        if r.status_code != 200:
            print(f"  WARN: fetch {endpoint} offset={offset}: {r.status_code}")
            break
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        offset += 1000
        if len(batch) < 1000:
            break
    return rows


def pct(current, old):
    if not old or old == 0 or not current:
        return None
    return round(((current - old) / old) * 100, 1)


# ── Find reference dates ──────────────────────────────────────────────

d_today = find_nearest_date(0, window=1)
if not d_today:
    d_today = find_nearest_date(1, window=2)
if not d_today:
    print("ERROR: No recent price data found!")
    exit(1)

# Find global anchor dates for the batch fetches
# Use a wider window (5 days) to find the best available date
d7   = find_nearest_date(7,    window=5)
d30  = find_nearest_date(30,   window=5)
d90  = find_nearest_date(90,   window=5)
d180 = find_nearest_date(180,  window=7)
d365 = find_nearest_date(365,  window=14)
d2y  = find_nearest_date(730,  window=14)
d5y  = find_nearest_date(1825, window=30)

print(f"Reference dates:")
print(f"  today={d_today} 7d={d7} 30d={d30} 90d={d90}")
print(f"  180d={d180} 365d={d365} 2y={d2y} 5y={d5y}")

# ── Fetch today's prices ──────────────────────────────────────────────

today_prices = fetch_all(
    f"daily_prices?date=eq.{d_today}"
    f"&select=card_slug,raw_usd,psa10_usd,psa9_usd"
)
print(f"  Found {len(today_prices)} cards with prices today")

if not today_prices:
    print("ERROR: No prices found for today!")
    exit(1)

# ── Fetch historical prices for each anchor date ──────────────────────
# Build per-slug lookup dicts for each period

def get_prices_for_date(ref_date):
    if not ref_date:
        return {}
    rows = fetch_all(
        f"daily_prices?date=eq.{ref_date}&select=card_slug,raw_usd,psa10_usd"
    )
    return {row["card_slug"]: row for row in rows}


print("Fetching historical price snapshots...")
hist = {}
for label, ref_date in [
    ("d7", d7), ("d30", d30), ("d90", d90),
    ("d180", d180), ("d365", d365), ("d2y", d2y), ("d5y", d5y)
]:
    print(f"  Fetching {label} ({ref_date})...")
    hist[label] = get_prices_for_date(ref_date)

# ── Build per-card fallback lookup for 30d window ────────────────────
# This is the key fix: for 30d, also fetch dates within a ±5 day window
# so cards missing on the exact anchor date still get a value

print("Building 30d fallback window...")
fallback_30d_dates = []
for offset_days in range(25, 40):  # 25 to 39 days ago
    d = (today - timedelta(days=offset_days)).isoformat()
    if d != d30:  # don't re-fetch the main anchor
        fallback_30d_dates.append(d)

# Fetch available dates in the window that actually exist in daily_prices
r = requests.get(
    f"{SUPABASE_URL}/rest/v1/daily_prices"
    f"?select=date&date=gte.{(today - timedelta(days=39)).isoformat()}"
    f"&date=lte.{(today - timedelta(days=25)).isoformat()}"
    f"&order=date.desc",
    headers=HEADERS, timeout=15,
)
available_30d_dates = list({row["date"] for row in r.json()}) if r.status_code == 200 else []
available_30d_dates = [d for d in available_30d_dates if d != d30]
print(f"  Found {len(available_30d_dates)} fallback dates in 30d window")

# Fetch prices for fallback dates (only ones not already fetched)
fallback_30d = {}  # slug -> raw_usd from nearest available date
for fb_date in sorted(available_30d_dates, reverse=True):  # most recent first
    rows = fetch_all(
        f"daily_prices?date=eq.{fb_date}&select=card_slug,raw_usd,psa10_usd"
    )
    for row in rows:
        slug = row["card_slug"]
        if slug not in fallback_30d:  # only fill if not already found
            fallback_30d[slug] = row

print(f"  Fallback 30d covers {len(fallback_30d)} additional slugs")

# ── Fetch card metadata ───────────────────────────────────────────────

print("Fetching card metadata...")
cards_meta = {}
for row in fetch_all("cards?select=card_slug,card_name,set_name"):
    cards_meta[f"pc-{row['card_slug']}"] = row

# ── Build trend rows ──────────────────────────────────────────────────

trend_rows = []
null_30d_count = 0

for row in today_prices:
    slug = row["card_slug"]
    meta = cards_meta.get(slug)
    if not meta:
        continue

    def h(period, field="raw_usd"):
        entry = hist.get(period, {}).get(slug)
        if entry is None:
            return None
        return entry.get(field)

    # 30d lookback: try main anchor first, then fallback window
    raw_30d_ago = h("d30")
    if raw_30d_ago is None:
        fb = fallback_30d.get(slug)
        if fb:
            raw_30d_ago = fb.get("raw_usd")

    psa10_30d_ago = hist.get("d30", {}).get(slug, {}).get("psa10_usd") if hist.get("d30", {}).get(slug) else None

    raw_pct_30d = pct(row.get("raw_usd"), raw_30d_ago)
    if raw_pct_30d is None:
        null_30d_count += 1

    trend_rows.append({
        "card_slug":      meta["card_slug"],
        "card_name":      meta.get("card_name"),
        "set_name":       meta.get("set_name"),
        "current_raw":    row.get("raw_usd"),
        "current_psa10":  row.get("psa10_usd"),
        "current_psa9":   row.get("psa9_usd"),
        "raw_7d_ago":     h("d7"),
        "raw_30d_ago":    raw_30d_ago,
        "raw_90d_ago":    h("d90"),
        "raw_180d_ago":   h("d180"),
        "raw_365d_ago":   h("d365"),
        "raw_2y_ago":     h("d2y"),
        "raw_5y_ago":     h("d5y"),
        "psa10_30d_ago":  psa10_30d_ago,
        "psa10_90d_ago":  hist.get("d90", {}).get(slug, {}).get("psa10_usd") if hist.get("d90", {}).get(slug) else None,
        "raw_pct_7d":     pct(row.get("raw_usd"), h("d7")),
        "raw_pct_30d":    raw_pct_30d,
        "raw_pct_90d":    pct(row.get("raw_usd"), h("d90")),
        "raw_pct_180d":   pct(row.get("raw_usd"), h("d180")),
        "raw_pct_365d":   pct(row.get("raw_usd"), h("d365")),
        "raw_pct_2y":     pct(row.get("raw_usd"), h("d2y")),
        "raw_pct_5y":     pct(row.get("raw_usd"), h("d5y")),
        "psa10_pct_30d":  pct(row.get("psa10_usd"), psa10_30d_ago),
        "psa10_pct_90d":  pct(row.get("psa10_usd"), hist.get("d90", {}).get(slug, {}).get("psa10_usd") if hist.get("d90", {}).get(slug) else None),
        "as_of":          d_today,
    })

print(f"Built {len(trend_rows)} trend rows")
print(f"  Cards with null 30d pct: {null_30d_count} ({null_30d_count/len(trend_rows)*100:.1f}%)")

# ── Push to Supabase ──────────────────────────────────────────────────

print("Truncating card_trends...")
requests.delete(
    f"{SUPABASE_URL}/rest/v1/card_trends?card_slug=neq.",
    headers=POST_HEADERS, timeout=30,
)

print(f"Pushing {len(trend_rows)} trend rows...")
push_headers = {**HEADERS, "Prefer": "resolution=merge-duplicates"}
pushed = 0
for i in range(0, len(trend_rows), 500):
    batch = trend_rows[i:i+500]
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/card_trends",
        json=batch, headers=push_headers, timeout=30,
    )
    if r.status_code in (200, 201):
        pushed += len(batch)
    else:
        print(f"  ERROR at {i}: {r.status_code} {r.text[:200]}")

print(f"  Pushed {pushed}/{len(trend_rows)} rows")
print("card_trends refresh complete!")
