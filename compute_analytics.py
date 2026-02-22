"""
PokePrices Nightly Analytics — Phase 1
=======================================
Computes derived tables from daily_prices:
  - metrics_daily: ATH, drawdown, recovery, slope, volatility per card+grade
  - spread_daily: grade premium ratios and trends
  - set_metrics_daily: set-level analytics

Runs after the scraper completes. Pulls data via Supabase REST API,
computes everything in Python (avoids SQL timeouts), pushes results back.

Usage:
  python compute_analytics.py                # full run
  python compute_analytics.py --table metrics  # just metrics_daily
  python compute_analytics.py --table spread   # just spread_daily
  python compute_analytics.py --table sets     # just set_metrics_daily

Requirements:
  pip install requests numpy
"""

import os
import sys
import json
import time
import math
import requests
from datetime import date, timedelta
from collections import defaultdict

# ── Config ──────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://egidpsrkqvymvioidatc.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))
TODAY = date.today().isoformat()

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

GRADES = {
    "raw":   "raw_usd",
    "psa10": "psa10_usd",
    "psa9":  "psa9_usd",
    "psa8":  "psa8_usd",
    "psa7":  "psa7_usd",
    "cgc95": "cgc95_usd",
}

# ── Helpers ─────────────────────────────────────────────────────────────────

def fetch_all(endpoint, params=""):
    """Fetch all rows from a Supabase REST endpoint, handling pagination."""
    rows = []
    offset = 0
    while True:
        sep = "&" if "?" in endpoint else "?"
        url = f"{SUPABASE_URL}/rest/v1/{endpoint}{sep}offset={offset}&limit=1000{params}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            print(f"ERROR fetching {endpoint}: {r.status_code} {r.text[:200]}")
            break
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        offset += 1000
        if len(batch) < 1000:
            break
    return rows


def push_rows(table, rows, batch_size=500):
    """Upsert rows to a Supabase table."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {**HEADERS, "Prefer": "resolution=merge-duplicates"}
    pushed = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        r = requests.post(url, json=batch, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            pushed += len(batch)
        else:
            print(f"  ERROR pushing to {table} at batch {i}: {r.status_code} {r.text[:200]}")
    return pushed


def delete_rows(table, where):
    """Delete rows from a table matching a filter."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{where}"
    headers = {**HEADERS, "Prefer": "return=minimal"}
    r = requests.delete(url, headers=headers, timeout=30)
    return r.status_code in (200, 204)


def linear_slope(points):
    """Compute linear regression slope from [(x, y), ...] pairs.
    Returns slope in y-units per x-unit (cents per day).
    """
    n = len(points)
    if n < 2:
        return None
    sum_x = sum(p[0] for p in points)
    sum_y = sum(p[1] for p in points)
    sum_xy = sum(p[0] * p[1] for p in points)
    sum_x2 = sum(p[0] ** 2 for p in points)
    denom = n * sum_x2 - sum_x ** 2
    if denom == 0:
        return None
    return round((n * sum_xy - sum_x * sum_y) / denom, 4)


def coefficient_of_variation(values):
    """Stddev / mean. Returns None if insufficient data."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    if mean == 0:
        return None
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    stddev = math.sqrt(variance)
    return round(stddev / mean, 4)


def pct_change(current, old):
    if not old or old == 0 or not current:
        return None
    return round(((current - old) / old) * 100, 1)


def safe_round(val, decimals=2):
    if val is None:
        return None
    return round(val, decimals)


# ── metrics_daily ───────────────────────────────────────────────────────────

def compute_metrics_daily():
    """Compute per-card per-grade analytics from daily_prices history."""
    print("=" * 60)
    print("Computing metrics_daily")
    print("=" * 60)

    # Fetch ALL price history (this is the big pull)
    print("Fetching price history...")
    all_prices = fetch_all("daily_prices?select=card_slug,date,raw_usd,psa10_usd,psa9_usd,psa8_usd,psa7_usd,cgc95_usd&order=card_slug,date")
    print(f"  Fetched {len(all_prices)} price records")

    if not all_prices:
        print("ERROR: No price data!")
        return

    # Fetch card metadata
    print("Fetching card metadata...")
    cards = fetch_all("cards?select=card_slug,card_name,set_name")
    card_meta = {f"pc-{c['card_slug']}": c for c in cards}

    # Group prices by card_slug
    by_card = defaultdict(list)
    for row in all_prices:
        by_card[row["card_slug"]].append(row)

    print(f"  {len(by_card)} unique cards with price data")

    # Find reference date (most recent date in data)
    today_date = max(row["date"] for row in all_prices)
    print(f"  Reference date: {today_date}")

    # Compute day offsets
    from datetime import datetime
    today_dt = datetime.strptime(today_date, "%Y-%m-%d")
    
    def days_ago(d_str):
        return (today_dt - datetime.strptime(d_str, "%Y-%m-%d")).days

    # Delete existing rows for today
    delete_rows("metrics_daily", f"as_of=eq.{today_date}")

    metrics_rows = []
    card_count = 0

    for card_slug, price_rows in by_card.items():
        # Sort by date
        price_rows.sort(key=lambda r: r["date"])
        
        for grade_name, col in GRADES.items():
            # Get all non-null prices for this grade
            grade_prices = [(r["date"], r.get(col)) for r in price_rows if r.get(col) and r[col] > 0]
            
            if not grade_prices:
                continue

            # Current price (most recent)
            current_date, current_price = grade_prices[-1]
            if current_date != today_date:
                continue  # No price today for this grade
            
            # ATH
            ath_price = max(p[1] for p in grade_prices)
            ath_date = next(p[0] for p in grade_prices if p[1] == ath_price)
            drawdown = pct_change(current_price, ath_price)  # will be negative or zero
            
            # Bottom since ATH
            since_ath = [p for p in grade_prices if p[0] >= ath_date]
            if since_ath:
                bottom_price = min(p[1] for p in since_ath)
                bottom_date = next(p[0] for p in since_ath if p[1] == bottom_price)
                # Recovery: 0% = still at bottom, 100% = back to ATH
                if ath_price > bottom_price:
                    recovery = round(((current_price - bottom_price) / (ath_price - bottom_price)) * 100, 2)
                else:
                    recovery = 100.0
            else:
                bottom_price, bottom_date, recovery = current_price, current_date, 100.0

            # 12-month high
            recent_prices = [p for p in grade_prices if days_ago(p[0]) <= 365]
            if recent_prices:
                high_12m = max(p[1] for p in recent_prices)
                high_12m_date = next(p[0] for p in recent_prices if p[1] == high_12m)
                is_new_high = (current_price >= high_12m)
            else:
                high_12m, high_12m_date, is_new_high = None, None, False

            # Slopes (cents per day)
            def calc_slope(max_days):
                pts = [(days_ago(p[0]), p[1]) for p in grade_prices if days_ago(p[0]) <= max_days]
                # Invert x so more recent = higher x (slope positive = price going up)
                pts = [(max_days - x, y) for x, y in pts]
                return linear_slope(pts)

            slope_30 = calc_slope(30)
            slope_90 = calc_slope(90)
            slope_365 = calc_slope(365)

            # Volatility
            def calc_vol(max_days):
                vals = [p[1] for p in grade_prices if days_ago(p[0]) <= max_days]
                return coefficient_of_variation(vals)

            vol_30 = calc_vol(30)
            vol_90 = calc_vol(90)

            # Percentage changes (find closest date to each lookback)
            def price_at_lookback(target_days):
                candidates = [(p[0], p[1]) for p in grade_prices if days_ago(p[0]) >= target_days]
                if not candidates:
                    return None
                # Get the one closest to target_days ago
                candidates.sort(key=lambda p: abs(days_ago(p[0]) - target_days))
                return candidates[0][1]

            # Data quality
            total_points = len(grade_prices)
            points_90d = len([p for p in grade_prices if days_ago(p[0]) <= 90])
            freshness = days_ago(current_date)

            if total_points >= 12 and points_90d >= 2:
                confidence = "high"
            elif total_points >= 6:
                confidence = "medium"
            elif total_points >= 3:
                confidence = "low"
            else:
                confidence = "very_low"

            clean_slug = card_slug.replace("pc-", "")

            row = {
                "card_slug": clean_slug,
                "grade": grade_name,
                "current_price": current_price,
                "ath_price": ath_price,
                "ath_date": ath_date,
                "drawdown_pct": safe_round(drawdown),
                "bottom_price": bottom_price,
                "bottom_date": bottom_date,
                "recovery_pct": safe_round(recovery),
                "high_12m": high_12m,
                "high_12m_date": high_12m_date,
                "is_new_12m_high": is_new_high,
                "slope_30d": slope_30,
                "slope_90d": slope_90,
                "slope_365d": slope_365,
                "volatility_30d": vol_30,
                "volatility_90d": vol_90,
                "pct_7d": pct_change(current_price, price_at_lookback(7)),
                "pct_30d": pct_change(current_price, price_at_lookback(30)),
                "pct_90d": pct_change(current_price, price_at_lookback(90)),
                "pct_180d": pct_change(current_price, price_at_lookback(180)),
                "pct_365d": pct_change(current_price, price_at_lookback(365)),
                "data_points_total": total_points,
                "data_points_90d": points_90d,
                "freshness_days": freshness,
                "confidence": confidence,
                "as_of": today_date,
            }
            metrics_rows.append(row)

        card_count += 1
        if card_count % 5000 == 0:
            print(f"  Processed {card_count}/{len(by_card)} cards...")

    print(f"\nPushing {len(metrics_rows)} metrics rows...")
    pushed = push_rows("metrics_daily", metrics_rows)
    print(f"  Pushed {pushed}/{len(metrics_rows)} rows")
    return len(metrics_rows)


# ── spread_daily ────────────────────────────────────────────────────────────

def compute_spread_daily():
    """Compute grade premium ratios and trends."""
    print("\n" + "=" * 60)
    print("Computing spread_daily")
    print("=" * 60)

    # Get today's prices for all cards (all grades in one row)
    print("Fetching today's prices...")
    today_date = None
    
    # Find latest date
    latest = fetch_all("daily_prices?select=date&order=date.desc&limit=1")
    if latest:
        today_date = latest[0]["date"]
    else:
        print("ERROR: No price data!")
        return
    
    print(f"  Reference date: {today_date}")
    
    today_prices = fetch_all(
        f"daily_prices?select=card_slug,raw_usd,psa10_usd,psa9_usd,psa8_usd,psa7_usd,cgc95_usd&date=eq.{today_date}"
    )
    print(f"  {len(today_prices)} cards with prices today")

    # Get 30d and 90d ago prices for premium trend detection
    d30_date = None
    d90_date = None
    
    d30_candidates = fetch_all(
        f"daily_prices?select=date&date=lte.{(date.today() - timedelta(days=30)).isoformat()}&order=date.desc&limit=1"
    )
    if d30_candidates:
        d30_date = d30_candidates[0]["date"]
    
    d90_candidates = fetch_all(
        f"daily_prices?select=date&date=lte.{(date.today() - timedelta(days=90)).isoformat()}&order=date.desc&limit=1"
    )
    if d90_candidates:
        d90_date = d90_candidates[0]["date"]

    print(f"  30d ref: {d30_date}, 90d ref: {d90_date}")

    # Fetch historical prices
    prices_30d = {}
    prices_90d = {}
    
    if d30_date:
        for row in fetch_all(f"daily_prices?select=card_slug,raw_usd,psa10_usd,psa9_usd&date=eq.{d30_date}"):
            prices_30d[row["card_slug"]] = row
    
    if d90_date:
        for row in fetch_all(f"daily_prices?select=card_slug,raw_usd,psa10_usd,psa9_usd&date=eq.{d90_date}"):
            prices_90d[row["card_slug"]] = row

    # Delete existing
    delete_rows("spread_daily", f"as_of=eq.{today_date}")

    def ratio(a, b):
        if not a or not b or b == 0:
            return None
        return round(a / b, 2)

    def premium_trend(current_ratio, old_ratio):
        if current_ratio is None or old_ratio is None:
            return "insufficient_data"
        diff = current_ratio - old_ratio
        if abs(diff) < 0.1:
            return "stable"
        elif diff > 0:
            return "expanding"
        else:
            return "compressing"

    spread_rows = []
    
    for row in today_prices:
        slug = row["card_slug"]
        clean_slug = slug.replace("pc-", "")
        
        raw = row.get("raw_usd")
        p7 = row.get("psa7_usd")
        p8 = row.get("psa8_usd")
        p9 = row.get("psa9_usd")
        cgc95 = row.get("cgc95_usd")
        p10 = row.get("psa10_usd")
        
        # Need at least raw + one graded price to be useful
        if not raw or raw == 0:
            continue
        if not any([p7, p8, p9, p10]):
            continue

        # Current ratios
        r_10_raw = ratio(p10, raw)
        r_10_9 = ratio(p10, p9)
        r_9_raw = ratio(p9, raw)
        r_9_8 = ratio(p9, p8)
        r_9_7 = ratio(p9, p7)

        # 30d ratios
        h30 = prices_30d.get(slug, {})
        r_10_raw_30d = ratio(h30.get("psa10_usd"), h30.get("raw_usd"))
        r_10_9_30d = ratio(h30.get("psa10_usd"), h30.get("psa9_usd"))
        r_9_raw_30d = ratio(h30.get("psa9_usd"), h30.get("raw_usd"))

        # 90d ratios
        h90 = prices_90d.get(slug, {})
        r_10_raw_90d = ratio(h90.get("psa10_usd"), h90.get("raw_usd"))
        r_10_9_90d = ratio(h90.get("psa10_usd"), h90.get("psa9_usd"))
        r_9_raw_90d = ratio(h90.get("psa9_usd"), h90.get("raw_usd"))

        # Trend detection (use 90d comparison if available, else 30d)
        prem_10 = premium_trend(r_10_raw, r_10_raw_90d if r_10_raw_90d else r_10_raw_30d)
        prem_9 = premium_trend(r_9_raw, r_9_raw_90d if r_9_raw_90d else r_9_raw_30d)

        # Best value grade: lowest price-per-"quality" ratio
        # Simple heuristic: which grade has the lowest multiplier over raw
        grade_ratios = {}
        if r_9_raw and r_9_raw > 0:
            grade_ratios["psa9"] = r_9_raw
        if r_10_raw and r_10_raw > 0:
            grade_ratios["psa10"] = r_10_raw
        if p8 and raw and raw > 0:
            r_8_raw = p8 / raw
            if r_8_raw > 0:
                grade_ratios["psa8"] = round(r_8_raw, 2)
        
        best_value = min(grade_ratios, key=grade_ratios.get) if grade_ratios else None

        spread_rows.append({
            "card_slug": clean_slug,
            "raw_price": raw,
            "psa7_price": p7,
            "psa8_price": p8,
            "psa9_price": p9,
            "cgc95_price": cgc95,
            "psa10_price": p10,
            "ratio_10_to_raw": r_10_raw,
            "ratio_10_to_9": r_10_9,
            "ratio_9_to_raw": r_9_raw,
            "ratio_9_to_8": r_9_8,
            "ratio_9_to_7": r_9_7,
            "ratio_10_to_raw_30d": r_10_raw_30d,
            "ratio_10_to_9_30d": r_10_9_30d,
            "ratio_9_to_raw_30d": r_9_raw_30d,
            "ratio_10_to_raw_90d": r_10_raw_90d,
            "ratio_10_to_9_90d": r_10_9_90d,
            "ratio_9_to_raw_90d": r_9_raw_90d,
            "premium_10_trend": prem_10,
            "premium_9_trend": prem_9,
            "best_value_grade": best_value,
            "as_of": today_date,
        })

    print(f"\nPushing {len(spread_rows)} spread rows...")
    pushed = push_rows("spread_daily", spread_rows)
    print(f"  Pushed {pushed}/{len(spread_rows)} rows")
    return len(spread_rows)


# ── set_metrics_daily ───────────────────────────────────────────────────────

def compute_set_metrics_daily():
    """Compute set-level analytics."""
    print("\n" + "=" * 60)
    print("Computing set_metrics_daily")
    print("=" * 60)

    # Get card_trends (has current prices + pct changes + set names)
    print("Fetching card_trends...")
    trends = fetch_all("card_trends?select=card_slug,card_name,set_name,current_raw,raw_pct_30d,raw_pct_90d,raw_pct_180d,raw_pct_365d")
    print(f"  {len(trends)} cards in card_trends")

    # Get total card counts per set from cards table
    print("Fetching card counts...")
    all_cards = fetch_all("cards?select=card_slug,set_name")
    
    set_total_counts = defaultdict(int)
    for c in all_cards:
        if c.get("set_name"):
            set_total_counts[c["set_name"]] += 1

    # Group trends by set
    by_set = defaultdict(list)
    for t in trends:
        if t.get("set_name") and t.get("current_raw") and t["current_raw"] > 0:
            by_set[t["set_name"]].append(t)

    # Find latest date
    latest = fetch_all("daily_prices?select=date&order=date.desc&limit=1")
    today_date = latest[0]["date"] if latest else date.today().isoformat()

    # Delete existing
    delete_rows("set_metrics_daily", f"as_of=eq.{today_date}")

    set_rows = []

    for set_name, cards in by_set.items():
        if len(cards) < 2:
            continue

        # Sort by price descending
        cards.sort(key=lambda c: c.get("current_raw", 0), reverse=True)
        
        prices = [c["current_raw"] for c in cards]
        total_value = sum(prices)
        
        if total_value == 0:
            continue

        # Concentration
        top1_value = prices[0]
        top5_value = sum(prices[:5])
        top10_value = sum(prices[:10])
        
        top1_share = round((top1_value / total_value) * 100, 1)
        top5_share = round((top5_value / total_value) * 100, 1) if len(prices) >= 5 else None
        top10_share = round((top10_value / total_value) * 100, 1) if len(prices) >= 10 else None

        # Median
        sorted_prices = sorted(prices)
        n = len(sorted_prices)
        median = sorted_prices[n // 2] if n % 2 == 1 else (sorted_prices[n // 2 - 1] + sorted_prices[n // 2]) // 2

        # Weighted average trends
        def weighted_avg_trend(field):
            pairs = [(c.get(field), c.get("current_raw", 0)) for c in cards if c.get(field) is not None]
            if not pairs:
                return None
            total_weight = sum(w for _, w in pairs)
            if total_weight == 0:
                return None
            return round(sum(v * w for v, w in pairs) / total_weight, 1)

        set_pct_30d = weighted_avg_trend("raw_pct_30d")
        set_pct_90d = weighted_avg_trend("raw_pct_90d")
        set_pct_180d = weighted_avg_trend("raw_pct_180d")
        set_pct_365d = weighted_avg_trend("raw_pct_365d")

        # Mid-tier vs chase divergence (top 5 vs rest, 90d)
        top5_cards = cards[:5]
        rest_cards = cards[5:]

        def avg_pct(card_list, field):
            vals = [c.get(field) for c in card_list if c.get(field) is not None]
            if not vals:
                return None
            return round(sum(vals) / len(vals), 1)

        top5_avg = avg_pct(top5_cards, "raw_pct_90d")
        rest_avg = avg_pct(rest_cards, "raw_pct_90d")
        
        divergence = None
        if top5_avg is not None and rest_avg is not None:
            divergence = round(rest_avg - top5_avg, 1)

        set_rows.append({
            "set_name": set_name,
            "total_cards": set_total_counts.get(set_name, len(cards)),
            "priced_cards": len(cards),
            "cards_over_10": len([p for p in prices if p >= 1000]),
            "cards_over_100": len([p for p in prices if p >= 10000]),
            "set_total_value": total_value,
            "set_median_value": median,
            "set_avg_value": total_value // len(prices),
            "top1_share_pct": top1_share,
            "top5_share_pct": top5_share,
            "top10_share_pct": top10_share,
            "top1_card_name": cards[0].get("card_name"),
            "top1_card_price": cards[0].get("current_raw"),
            "set_pct_30d": set_pct_30d,
            "set_pct_90d": set_pct_90d,
            "set_pct_180d": set_pct_180d,
            "set_pct_365d": set_pct_365d,
            "top5_avg_pct_90d": top5_avg,
            "rest_avg_pct_90d": rest_avg,
            "divergence_90d": divergence,
            "as_of": today_date,
        })

    print(f"\nPushing {len(set_rows)} set metrics rows...")
    pushed = push_rows("set_metrics_daily", set_rows)
    print(f"  Pushed {pushed}/{len(set_rows)} rows")
    return len(set_rows)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    table_filter = None
    if "--table" in sys.argv:
        idx = sys.argv.index("--table")
        if idx + 1 < len(sys.argv):
            table_filter = sys.argv[idx + 1].lower()

    print(f"PokePrices Nightly Analytics — {date.today().isoformat()}")
    print(f"Supabase: {SUPABASE_URL}")
    
    if not SUPABASE_KEY:
        print("ERROR: SUPABASE_KEY not set!")
        sys.exit(1)
    
    start = time.time()
    
    if table_filter is None or table_filter == "metrics":
        compute_metrics_daily()
    
    if table_filter is None or table_filter == "spread":
        compute_spread_daily()
    
    if table_filter is None or table_filter == "sets":
        compute_set_metrics_daily()
    
    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"Analytics complete in {elapsed/60:.1f} minutes")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
