"""
refresh_card_trends.py
Key fix: much wider date windows for long-period lookbacks,
since historical data is monthly snapshots not daily.
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
    target_days_ago. For long periods, uses a much wider window since
    historical data is stored as monthly snapshots.
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

    # Wider fallback: find nearest date before target with no lower bound
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
# Key: wider windows for longer periods since data is monthly for old cards
# 90d  → ±20 day window  (data could be monthly, so up to ~30 days off)
# 180d → ±30 day window
# 365d → ±45 day window  (monthly snapshots mean up to 31 days off either side)
# 2y   → ±60 day window
# 5y   → ±90 day window

d_today = find_nearest_date(0, window=1)
if not d_today:
    d_today = find_nearest_date(1, window=2)
if not d_today:
    print("ERROR: No recent price data found!")
    exit(1)

d7   = find_nearest_date(7,    window=5)
d30  = find_nearest_date(30,   window=10)
d90  = find_nearest_date(90,   window=20)   # was 5 — KEY FIX
d180 = find_nearest_date(180,  window=30)   # was 7  — KEY FIX
d365 = find_nearest_date(365,  window=45)   # was 14 — KEY FIX
d2y  = find_nearest_date(730,  window=60)   # was 14 — KEY FIX
d5y  = find_nearest_date(1825, window=90)   # was 30 — KEY FIX

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
    print(f"    → {len(hist[label])} prices")

# ── Per-card fallback for sparse cards ───────────────────────────────
# For cards with monthly-only history, the global anchor date might have
# no entry for that specific card even with a wide window.
# Solution: for each period, if slug is missing, find nearest date
# specifically for that slug using a wider per-card window.

def get_price_for_slug_near_date(slug, target_days_ago, window_days):
    """Find nearest price for a specific slug within window_days of target."""
    target = (today - timedelta(days=target_days_ago)).isoformat()
    low    = (today - timedelta(days=target_days_ago + window_days)).isoformat()
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/daily_prices"
        f"?card_slug=eq.{slug}&raw_usd=gt.0"
        f"&date=lte.{target}&date=gte.{low}"
        f"&select=raw_usd,psa10_usd&order=date.desc&limit=1",
        headers=HEADERS, timeout=10,
    )
    if r.status_code == 200:
        data = r.json()
        if data:
            return data[0]
    return None

# ── Build trend rows ──────────────────────────────────────────────────

print("Fetching card metadata...")
cards_meta = {}
for row in fetch_all("cards?select=card_slug,card_name,set_name"):
    cards_meta[f"pc-{row['card_slug']}"] = row

# Per-period config: (hist_key, days_ago, fallback_window)
# Fallback window is per-card lookup if global anchor misses the slug
PERIOD_CONFIG = [
    ("d7",   7,    10),
    ("d30",  30,   20),
    ("d90",  90,   40),   # wide fallback for sparse cards
    ("d180", 180,  60),
    ("d365", 365,  90),
    ("d2y",  730,  120),
    ("d5y",  1825, 180),
]

trend_rows = []
null_counts = {k: 0 for k, _, _ in PERIOD_CONFIG}

print(f"Building {len(today_prices)} trend rows...")

for row in today_prices:
    slug = row["card_slug"]
    meta = cards_meta.get(slug)
    if not meta:
        continue

    def h(period_key):
        return hist.get(period_key, {}).get(slug, {}).get("raw_usd")

    def h10(period_key):
        return hist.get(period_key, {}).get(slug, {}).get("psa10_usd")

    # For each period, use global anchor first, then per-card fallback
    period_raws = {}
    period_psa10s = {}
    for key, days, fallback_win in PERIOD_CONFIG:
        val = h(key)
        p10 = h10(key)
        if val is None:
            # Per-card fallback with wider window
            fb = get_price_for_slug_near_date(slug, days, fallback_win)
            if fb:
                val = fb.get("raw_usd")
                p10 = fb.get("psa10_usd")
        period_raws[key]   = val
        period_psa10s[key] = p10
        if val is None:
            null_counts[key] += 1

    current_raw   = row.get("raw_usd")
    current_psa10 = row.get("psa10_usd")

    trend_rows.append({
        "card_slug":      meta["card_slug"],
        "card_name":      meta.get("card_name"),
        "set_name":       meta.get("set_name"),
        "current_raw":    current_raw,
        "current_psa10":  current_psa10,
        "current_psa9":   row.get("psa9_usd"),
        "raw_7d_ago":     period_raws["d7"],
        "raw_30d_ago":    period_raws["d30"],
        "raw_90d_ago":    period_raws["d90"],
        "raw_180d_ago":   period_raws["d180"],
        "raw_365d_ago":   period_raws["d365"],
        "raw_2y_ago":     period_raws["d2y"],
        "raw_5y_ago":     period_raws["d5y"],
        "psa10_30d_ago":  period_psa10s["d30"],
        "psa10_90d_ago":  period_psa10s["d90"],
        "raw_pct_7d":     pct(current_raw, period_raws["d7"]),
        "raw_pct_30d":    pct(current_raw, period_raws["d30"]),
        "raw_pct_90d":    pct(current_raw, period_raws["d90"]),
        "raw_pct_180d":   pct(current_raw, period_raws["d180"]),
        "raw_pct_365d":   pct(current_raw, period_raws["d365"]),
        "raw_pct_2y":     pct(current_raw, period_raws["d2y"]),
        "raw_pct_5y":     pct(current_raw, period_raws["d5y"]),
        "psa10_pct_30d":  pct(current_psa10, period_psa10s["d30"]),
        "psa10_pct_90d":  pct(current_psa10, period_psa10s["d90"]),
        "as_of":          d_today,
    })

print(f"Built {len(trend_rows)} trend rows")
for key, _, _ in PERIOD_CONFIG:
    pct_null = null_counts[key] / len(trend_rows) * 100 if trend_rows else 0
    print(f"  {key}: {null_counts[key]} nulls ({pct_null:.1f}%)")

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
