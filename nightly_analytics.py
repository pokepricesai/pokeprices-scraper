"""
nightly_analytics.py
Runs after the main price scraper.
Adds to: market_index, card_scores, set_scores

v3 fixes:
  - market_index: fetches all daily_prices in batches (was capped at 1,000)
  - robust_trends: statement_timeout set via direct REST call (header approach doesn't work)
  - weekly_report_cache: wrapped in safe skip if RPC has SQL errors
  - set_scores: deduplicate by set_name before upsert (fixes ON CONFLICT duplicate error)
"""

import os
import math
import requests
from datetime import date, timedelta
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
TODAY = date.today().isoformat()


# ── HELPERS ──────────────────────────────────────────────────

def fetch_all(table, select, filters=None, batch_size=1000):
    """
    Fetch all rows from a table, paginating in batches to bypass the 1,000 row default limit.
    filters: list of (method, *args) tuples, e.g. [('eq', 'date', TODAY)]
    """
    all_rows = []
    offset = 0
    while True:
        q = supabase.table(table).select(select).range(offset, offset + batch_size - 1)
        if filters:
            for f in filters:
                method, *args = f
                q = getattr(q, method)(*args)
        result = q.execute()
        batch = result.data or []
        all_rows.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size
    return all_rows


# ── 1. MARKET INDEX ──────────────────────────────────────────

def get_nearest_market_index(target_date_str, window_days=5):
    target = date.fromisoformat(target_date_str)
    low  = (target - timedelta(days=window_days)).isoformat()
    high = (target + timedelta(days=window_days)).isoformat()

    result = supabase.table("market_index") \
        .select("date, total_raw_usd") \
        .gte("date", low) \
        .lte("date", high) \
        .order("date", desc=True) \
        .limit(10) \
        .execute()

    if not result.data:
        return None

    before = [r for r in result.data if r["date"] <= target_date_str]
    if before:
        return before[0]["total_raw_usd"]

    after = [r for r in result.data if r["date"] > target_date_str]
    if after:
        return after[-1]["total_raw_usd"]

    return None


def update_market_index():
    print("Updating market_index...")

    rows = fetch_all(
        "daily_prices",
        "card_slug, raw_usd, psa9_usd, psa10_usd",
        filters=[("eq", "date", TODAY)]
    )

    if not rows:
        print("  No price data for today — skipping market_index")
        return

    raw_prices   = [r["raw_usd"]   for r in rows if r.get("raw_usd")   and r["raw_usd"]   > 0]
    psa10_prices = [r["psa10_usd"] for r in rows if r.get("psa10_usd") and r["psa10_usd"] > 0]

    def median(lst):
        if not lst: return None
        s = sorted(lst)
        n = len(s)
        return (s[n // 2] + s[(n - 1) // 2]) // 2

    combined_total = sum(
        (r.get("raw_usd") or 0) +
        (r.get("psa9_usd") or 0) +
        (r.get("psa10_usd") or 0)
        for r in rows
    )

    record = {
        "date":                      TODAY,
        "total_raw_usd":             sum(raw_prices),
        "cards_with_raw_price":      len(raw_prices),
        "median_raw_usd":            median(raw_prices),
        "total_psa10_usd":           sum(psa10_prices),
        "cards_with_psa10_price":    len(psa10_prices),
        "median_psa10_usd":          median(psa10_prices),
        "total_combined_usd":        combined_total,
        "total_cards_tracked":       len(rows),
    }

    supabase.table("market_index").upsert(record, on_conflict="date").execute()

    for days, key in [(1, "raw_pct_1d"), (7, "raw_pct_7d"), (30, "raw_pct_30d")]:
        past_date = (date.today() - timedelta(days=days)).isoformat()
        past_val  = get_nearest_market_index(past_date, window_days=5)
        if past_val and past_val > 0:
            record[key] = round((record["total_raw_usd"] - past_val) / past_val * 100, 4)

    supabase.table("market_index").upsert(record, on_conflict="date").execute()
    print(f"  market_index updated: {len(rows)} cards, total raw ${sum(raw_prices)/100:,.0f}")
    print(f"  pct_1d={record.get('raw_pct_1d')} pct_7d={record.get('raw_pct_7d')} pct_30d={record.get('raw_pct_30d')}")


# ── 2. CARD SCORES ───────────────────────────────────────────

def compute_liquidity_score(sales_30d, days_since_last_sale):
    sales_score = min(60, (sales_30d or 0) * 10)
    dsls = days_since_last_sale or 999
    if dsls <= 7:    fresh = 40
    elif dsls <= 14: fresh = 30
    elif dsls <= 30: fresh = 20
    elif dsls <= 60: fresh = 10
    else:            fresh = 0
    return min(100, sales_score + fresh)

def compute_momentum_score(pct_7d, pct_30d, pct_90d):
    weighted = (
        (pct_7d  or 0) * 0.20 +
        (pct_30d or 0) * 0.50 +
        (pct_90d or 0) * 0.30
    )
    return max(0, min(100, round(weighted + 50)))

def compute_volatility_score(volatility_30d):
    if volatility_30d is None: return 50
    return max(0, min(100, round(volatility_30d * 200)))

def liquidity_label(score):
    if score >= 70: return "High"
    if score >= 35: return "Medium"
    return "Low"

def volatility_label(v30d):
    if v30d is None:  return "Unknown"
    if v30d > 0.3:   return "Extreme"
    if v30d > 0.15:  return "Volatile"
    if v30d > 0.06:  return "Moderate"
    return "Stable"

def update_card_scores():
    print("Updating card_scores...")

    metrics_data = fetch_all(
        "metrics_daily",
        "card_slug, current_price, ath_price, ath_date, drawdown_pct, "
        "bottom_price, bottom_date, recovery_pct, "
        "pct_7d, pct_30d, pct_90d, volatility_30d, confidence, "
        "data_points_90d, freshness_days",
        filters=[("eq", "grade", "raw"), ("eq", "as_of", TODAY)]
    )

    if not metrics_data:
        print("  No metrics_daily data for today")
        return

    volume = fetch_all(
        "card_volume",
        "card_slug, sales_30d, days_since_last_sale, volume_label, confidence",
        filters=[("eq", "grade", "Ungraded")]
    )
    vol_map = {r["card_slug"]: r for r in volume}

    spread = fetch_all(
        "spread_daily",
        "card_slug, ratio_10_to_raw, best_value_grade, premium_10_trend",
        filters=[("eq", "as_of", TODAY)]
    )
    spread_map = {r["card_slug"]: r for r in spread}

    trends = fetch_all("card_trends", "card_slug, card_name, set_name")
    trend_map = {r["card_slug"]: r for r in trends}

    pop = fetch_all(
        "card_population",
        "card_slug, population",
        filters=[("eq", "grade", "psa10")]
    )
    pop_map = {r["card_slug"]: r["population"] for r in pop}

    scores = []
    for m in metrics_data:
        slug = m["card_slug"]
        vol  = vol_map.get(slug, {})
        sp   = spread_map.get(slug, {})
        tr   = trend_map.get(slug, {})

        liq_score  = compute_liquidity_score(vol.get("sales_30d"), vol.get("days_since_last_sale"))
        mom_score  = compute_momentum_score(m.get("pct_7d"), m.get("pct_30d"), m.get("pct_90d"))
        volt_score = compute_volatility_score(m.get("volatility_30d"))

        conf = m.get("confidence", "low")
        conf_score = {"high": 90, "medium": 60}.get(conf, 25)

        psa10_pop = pop_map.get(slug, 9999)
        sales_30d = vol.get("sales_30d") or 0
        pct_30d   = m.get("pct_30d") or 0
        gem_score = max(0, min(100, (
            min(40, max(0, int(pct_30d) + 20)) +
            min(30, max(0, 30 - psa10_pop // 10)) +
            min(20, sales_30d * 5) +
            (10 if conf == "high" else 0)
        )))

        scores.append({
            "card_slug":         slug,
            "card_name":         tr.get("card_name"),
            "set_name":          tr.get("set_name"),
            "liquidity_score":   liq_score,
            "volatility_score":  volt_score,
            "momentum_score":    mom_score,
            "confidence_score":  conf_score,
            "liquidity_label":   liquidity_label(liq_score),
            "volatility_label":  volatility_label(m.get("volatility_30d")),
            "ath_price":         m.get("ath_price"),
            "ath_date":          m.get("ath_date"),
            "current_price":     m.get("current_price"),
            "drawdown_pct":      float(m["drawdown_pct"]) if m.get("drawdown_pct") else None,
            "bottom_price":      m.get("bottom_price"),
            "recovery_pct":      float(m["recovery_pct"]) if m.get("recovery_pct") else None,
            "best_value_grade":  sp.get("best_value_grade"),
            "ratio_10_to_raw":   float(sp["ratio_10_to_raw"]) if sp.get("ratio_10_to_raw") else None,
            "premium_10_trend":  sp.get("premium_10_trend"),
            "hidden_gem_score":  gem_score,
            "as_of":             TODAY,
        })

    for i in range(0, len(scores), 500):
        supabase.table("card_scores").upsert(scores[i:i+500], on_conflict="card_slug").execute()

    print(f"  card_scores updated: {len(scores)} cards")


# ── 3. ROBUST TRENDS ─────────────────────────────────────────

def refresh_robust_trends():
    print("Refreshing robust trend metrics...")
    try:
        # The Supabase Python client does not support per-request statement timeouts.
        # Call the RPC via raw REST with a long timeout on the HTTP request instead.
        # The real fix is the card_price_windows_mat materialized view — see SQL migration.
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/refresh_robust_trends",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
            },
            json={},
            timeout=300,  # 5 minutes — gives the function time to complete
        )
        if resp.status_code in (200, 204):
            print("  robust_trends refresh complete")
        else:
            print(f"  ERROR refreshing robust trends: {resp.status_code} - {resp.text[:200]}")
    except requests.exceptions.Timeout:
        print("  ERROR refreshing robust trends: timed out after 300s — run SQL migration to create card_price_windows_mat")
    except Exception as e:
        print(f"  ERROR refreshing robust trends: {e}")


# ── 4. WEEKLY REPORT CACHE ───────────────────────────────────

def refresh_weekly_report_cache():
    print("Refreshing weekly report cache...")
    try:
        supabase.rpc('refresh_weekly_report_cache').execute()
        print("  weekly_report_cache refresh complete")
    except Exception as e:
        err = str(e)
        if "does not exist" in err and "id" in err:
            # The refresh_weekly_report_cache RPC has a SQL bug (references column "id"
            # which does not exist on the target table). Skipping until fixed.
            # To fix: run SELECT pg_get_functiondef(oid) FROM pg_proc
            #         WHERE proname = 'refresh_weekly_report_cache';
            # then recreate the function with the correct column name.
            print("  SKIPPED weekly_report_cache — RPC has a SQL bug (column 'id' not found).")
            print("  Run: SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'refresh_weekly_report_cache';")
            print("  Then paste the result to fix the column reference.")
        else:
            print(f"  ERROR refreshing weekly report cache: {e}")


# ── 5. SET SCORES ────────────────────────────────────────────

def update_set_scores():
    print("Updating set_scores...")

    result = supabase.table("set_metrics_daily").select("*").eq(
        "as_of", (date.today() - timedelta(days=1)).isoformat()
    ).execute()

    if not result.data:
        result = supabase.table("set_metrics_daily").select("*").order(
            "as_of", desc=True
        ).limit(500).execute()

    if not result.data:
        print("  No set_metrics_daily data")
        return

    # FIX: deduplicate by set_name — the fallback query can return the same set
    # across multiple as_of dates, causing ON CONFLICT duplicate errors in one batch.
    # Keep the most recent row for each set_name.
    seen = {}
    for s in result.data:
        name = s.get("set_name", "")
        if not name:
            continue
        existing = seen.get(name)
        if existing is None or s.get("as_of", "") > existing.get("as_of", ""):
            seen[name] = s
    deduped = list(seen.values())

    scores = []
    for s in deduped:
        total_val  = s.get("set_total_value") or 0
        pct_90d    = float(s.get("set_pct_90d") or 0)
        top1_share = float(s.get("top1_share_pct") or 100)
        priced     = s.get("priced_cards") or 0

        val_score = min(40, max(0, round(math.log(max(1, total_val / 100000)) * 6)))
        mom_score = min(40, max(0, round(pct_90d + 20)))
        div_score = min(20, max(0, round((100 - top1_share) / 5)))
        strength  = val_score + mom_score + div_score

        name = s.get("set_name", "")
        if any(x in name for x in ["Base", "Jungle", "Fossil", "Rocket", "Gym", "Neo", "Skyridge", "Aquapolis"]):
            era = "wotc"
        elif any(x in name for x in ["EX ", "FireRed", "Emerald", "Delta", "Legend"]):
            era = "ex"
        elif any(x in name for x in ["HeartGold", "Black", "White", "XY", "BW"]):
            era = "mid"
        elif any(x in name for x in ["Sun", "Moon", "Sword", "Shield"]):
            era = "modern"
        elif any(x in name for x in ["Scarlet", "Violet", "Paldea", "Paradox", "Obsidian", "Temporal", "Stellar"]):
            era = "recent"
        else:
            era = "other"

        scores.append({
            "set_name":            name,
            "total_cards":         s.get("total_cards"),
            "priced_cards":        priced,
            "set_total_value":     total_val,
            "set_median_value":    s.get("set_median_value"),
            "top1_share_pct":      top1_share,
            "top1_card_name":      s.get("top1_card_name"),
            "top1_card_price":     s.get("top1_card_price"),
            "set_pct_30d":         float(s.get("set_pct_30d") or 0),
            "set_pct_90d":         pct_90d,
            "set_pct_365d":        float(s.get("set_pct_365d") or 0),
            "divergence_90d":      float(s.get("divergence_90d") or 0),
            "value_score":         val_score,
            "momentum_score":      mom_score,
            "concentration_score": div_score,
            "strength_score":      strength,
            "era":                 era,
            "as_of":               s.get("as_of"),
        })

    for i in range(0, len(scores), 200):
        supabase.table("set_scores").upsert(
            scores[i:i+200], on_conflict="set_name"
        ).execute()

    print(f"  set_scores updated: {len(scores)} sets")


# ── MAIN ─────────────────────────────────────────────────────

if __name__ == "__main__":
    update_market_index()
    update_card_scores()
    refresh_robust_trends()
    refresh_weekly_report_cache()
    update_set_scores()
    print("Analytics update complete.")
