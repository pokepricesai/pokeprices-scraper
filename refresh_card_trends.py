"""
refresh_card_trends.py
Fast version — no per-card fallback queries.
Uses wide global anchor windows to handle sparse historical data.
40,000 cards should complete in ~5-10 minutes.
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


def find_nearest_date(target_days_ago, window=5):
    """Find the nearest date in daily_prices within window days of target."""
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

    # Wider fallback: nearest date before target
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


# ── Find reference dates ──────────────────────────────────────────────────────
# Wide windows for long periods — monthly snapshots mean data can be 
# up to 31 days off for old cards. No per-card fallback needed if 
# windows are wide enough to catch the nearest monthly snapshot.

d_today = find_nearest_date(0, window=1)
if not d_today:
    d_today = find_nearest_date(1, window=2)
if not d_today:
    print("ERROR: No recent price data found!")
    exit(1)

d7   = find_nearest_date(7,    window=5)
d30  = find_nearest_date(30,   window=10)
d90  = find_nearest_date(90,   window=35)   # ±35 days covers monthly gaps
d180 = find_nearest_date(180,  window=35)
d365 = find_nearest_date(365,  window=45)   # ±45 days for yearly
d2y  = find_nearest_date(730,  window=60)
d5y  = find_nearest_date(1825, window=90)

print(f"Reference dates:")
print(f"  today={d_today} 7d={d7} 30d={d30} 90d={d90}")
print(f"  180d={d180} 365d={d365} 2y={d2y} 5y={d5y}")

# ── Fetch today's prices ──────────────────────────────────────────────────────
today_prices = fetch_all(
    f"daily_prices?date=eq.{d_today}"
    f"&select=card_slug,raw_usd,psa10_usd,psa9_usd"
)
print(f"Found {len(today_prices)} cards with prices today")

if not today_prices:
    print("ERROR: No prices found for today!")
    exit(1)

# ── Fetch historical snapshots — one bulk query per period ────────────────────
# This is O(periods) not O(cards×periods) — fast regardless of card count

def get_prices_for_date(ref_date):
    if not ref_date:
        return {}
    rows = fetch_all(
        f"daily_prices?date=eq.{ref_date}&select=card_slug,raw_usd,psa10_usd"
    )
    return {row["card_slug"]: row for row in rows}


print("Fetching historical snapshots...")
hist = {}
for label, ref_date in [
    ("d7",   d7),
    ("d30",  d30),
    ("d90",  d90),
    ("d180", d180),
    ("d365", d365),
    ("d2y",  d2y),
    ("d5y",  d5y),
]:
    print(f"  {label} ({ref_date})...", end=" ")
    hist[label] = get_prices_for_date(ref_date)
    print(f"{len(hist[label])} prices")

# ── Fetch 30d fallback window for cards missing on exact anchor ───────────────
# A small targeted fallback just for 30d — fetches all dates in ±10 day window
# and fills in slugs that were missing from the main anchor date.
# This is O(window_days × batch_fetches) not O(cards) — still fast.

print("Building 30d fallback window...")
fallback_30d: dict = {}
if d30:
    # Find all dates available in the 30d window
    low_30  = (today - timedelta(days=40)).isoformat()
    high_30 = (today - timedelta(days=20)).isoformat()
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/daily_prices"
        f"?select=date&date=gte.{low_30}&date=lte.{high_30}&order=date.desc",
        headers=HEADERS, timeout=15,
    )
    fb_dates = list({row["date"] for row in r.json()}) if r.status_code == 200 else []
    fb_dates = [d for d in fb_dates if d != d30]
    for fb_date in sorted(fb_dates, reverse=True):
        rows = fetch_all(
            f"daily_prices?date=eq.{fb_date}&select=card_slug,raw_usd,psa10_usd"
        )
        added = 0
        for row in rows:
            slug = row["card_slug"]
            if slug not in hist["d30"] and slug not in fallback_30d:
                fallback_30d[slug] = row
                added += 1
        if added > 0:
            print(f"  30d fallback {fb_date}: +{added} slugs")

print(f"  30d fallback total: {len(fallback_30d)} additional slugs")

# ── Fetch card metadata ───────────────────────────────────────────────────────
print("Fetching card metadata...")
cards_meta = {}
for row in fetch_all("cards?select=card_slug,card_name,set_name"):
    cards_meta[f"pc-{row['card_slug']}"] = row

# ── Build trend rows ──────────────────────────────────────────────────────────
print(f"Building trend rows for {len(today_prices)} cards...")

trend_rows = []
null_counts = {k: 0 for k in ["d7", "d30", "d90", "d180", "d365", "d2y", "d5y"]}

for row in today_prices:
    slug = row["card_slug"]
    meta = cards_meta.get(slug)
    if not meta:
        continue

    def h(period, field="raw_usd"):
        entry = hist.get(period, {}).get(slug)
        return entry.get(field) if entry else None

    def h10(period):
        entry = hist.get(period, {}).get(slug)
        return entry.get("psa10_usd") if entry else None

    # 30d: use main anchor, then fallback window
    raw_30d_ago = h("d30") or fallback_30d.get(slug, {}).get("raw_usd")
    psa10_30d_ago = h10("d30") or fallback_30d.get(slug, {}).get("psa10_usd")

    current_raw   = row.get("raw_usd")
    current_psa10 = row.get("psa10_usd")

    # Track nulls for diagnostics
    for k in ["d7", "d90", "d180", "d365", "d2y", "d5y"]:
        if h(k) is None:
            null_counts[k] += 1
    if raw_30d_ago is None:
        null_counts["d30"] += 1

    trend_rows.append({
        "card_slug":      meta["card_slug"],
        "card_name":      meta.get("card_name"),
        "set_name":       meta.get("set_name"),
        "current_raw":    current_raw,
        "current_psa10":  current_psa10,
        "current_psa9":   row.get("psa9_usd"),
        "raw_7d_ago":     h("d7"),
        "raw_30d_ago":    raw_30d_ago,
        "raw_90d_ago":    h("d90"),
        "raw_180d_ago":   h("d180"),
        "raw_365d_ago":   h("d365"),
        "raw_2y_ago":     h("d2y"),
        "raw_5y_ago":     h("d5y"),
        "psa10_30d_ago":  psa10_30d_ago,
        "psa10_90d_ago":  h10("d90"),
        "raw_pct_7d":     pct(current_raw, h("d7")),
        "raw_pct_30d":    pct(current_raw, raw_30d_ago),
        "raw_pct_90d":    pct(current_raw, h("d90")),
        "raw_pct_180d":   pct(current_raw, h("d180")),
        "raw_pct_365d":   pct(current_raw, h("d365")),
        "raw_pct_2y":     pct(current_raw, h("d2y")),
        "raw_pct_5y":     pct(current_raw, h("d5y")),
        "psa10_pct_30d":  pct(current_psa10, psa10_30d_ago),
        "psa10_pct_90d":  pct(current_psa10, h10("d90")),
        "as_of":          d_today,
    })

print(f"Built {len(trend_rows)} trend rows")
total = len(trend_rows) or 1
for k, count in null_counts.items():
    print(f"  {k}: {count} nulls ({count/total*100:.1f}%)")

# ── Push to Supabase ──────────────────────────────────────────────────────────
print("Truncating card_trends...")
requests.delete(
    f"{SUPABASE_URL}/rest/v1/card_trends?card_slug=neq.",
    headers=POST_HEADERS, timeout=30,
)

print(f"Pushing {len(trend_rows)} rows...")
push_headers = {**HEADERS, "Prefer": "resolution=merge-duplicates"}
pushed = 0
errors = 0
for i in range(0, len(trend_rows), 500):
    batch = trend_rows[i:i+500]
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/card_trends",
        json=batch, headers=push_headers, timeout=30,
    )
    if r.status_code in (200, 201):
        pushed += len(batch)
        if pushed % 5000 == 0:
            print(f"  {pushed}/{len(trend_rows)} pushed...")
    else:
        errors += 1
        print(f"  ERROR batch {i}: {r.status_code} {r.text[:150]}")

print(f"Done — pushed {pushed}/{len(trend_rows)} rows ({errors} errors)")
