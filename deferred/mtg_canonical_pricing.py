"""MTGPrices canonical daily-price derivation — Stage 1D.

See docs/mtg-canonical-pricing.md (in the web repo) for the full
methodology. This module implements v1.0.

Public surface:
  * :data:`METHODOLOGY_VERSION` — current version stamp.
  * :func:`derive_canonical_for_day` — read observations for one day,
    write the two canonical series (internal + public) via PostgREST.
  * :func:`refresh_current_prices` — recompute mtg_current_prices from
    the latest observation per (finish, provider, market, currency,
    price_type, condition).
  * :func:`detect_anomalies` — day-over-day ratio detector.

The module deliberately depends only on the standard library +
`mtgjson_ingestion.SupabaseClient`. It is safe to import from a GitHub
Actions runner without pulling in ijson or requests.

All writes go through the same fail-closed :class:`SupabaseClient`
pattern used by Stage 1B/1C. Read paths use direct PostgREST GETs.
"""
from __future__ import annotations

import json
import logging
import os
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

try:
    from mtgjson_ingestion import SupabaseClient, build_default_supabase_client
except ImportError:
    SupabaseClient = None                     # type: ignore
    build_default_supabase_client = None      # type: ignore


log = logging.getLogger("mtg_canonical_pricing")

METHODOLOGY_VERSION = "v1.0"

# The methodology filters observations to price_type='retail' and
# is_anomalous=false. Market + currency remain as observed.
DEFAULT_PRICE_TYPE = "retail"

# Batch size when writing canonical + current-price rows through
# PostgREST. Small enough that a single 5xx does not lose a lot of work.
DEFAULT_UPSERT_BATCH = 500


# ─── Provider policy loading ────────────────────────────────────────────────

@dataclass
class ProviderPolicy:
    provider: str
    derived_use_internal: bool
    derived_use_public:   bool
    public_display:       bool
    legal_review_status:  str


def load_provider_policies(supabase: SupabaseClient) -> dict[str, ProviderPolicy]:
    """Return {provider -> ProviderPolicy}, or empty dict if the table is
    missing (before the 2026-09-15a migration lands)."""
    code, body = supabase._req(
        "/rest/v1/mtg_provider_policies?select=provider,derived_use_internal,derived_use_public,public_display,legal_review_status"
    )
    if code == 404 or code == 406:
        log.warning("mtg_provider_policies not present; defaulting to empty policy set")
        return {}
    if code >= 400:
        raise RuntimeError(f"provider policy fetch failed: HTTP {code} {body[:400]}")
    rows = json.loads(body) if body else []
    out: dict[str, ProviderPolicy] = {}
    for r in rows:
        out[r["provider"]] = ProviderPolicy(
            provider              = r["provider"],
            derived_use_internal  = bool(r["derived_use_internal"]),
            derived_use_public    = bool(r["derived_use_public"]),
            public_display        = bool(r["public_display"]),
            legal_review_status   = r["legal_review_status"],
        )
    return out


# ─── Canonical derivation for a single day ──────────────────────────────────

@dataclass
class DerivationStats:
    day:                        str
    observations_read:          int = 0
    permitted_internal:         int = 0
    permitted_public:           int = 0
    canonical_rows_internal:    int = 0
    canonical_rows_public:      int = 0
    keys_seen:                  int = 0
    errors:                     int = 0
    contributing_providers_internal: dict[str, int] = field(default_factory=dict)
    contributing_providers_public:   dict[str, int] = field(default_factory=dict)


def _fetch_observations_for_day(
    supabase: SupabaseClient, day: str, page_size: int = 5000
) -> Iterable[dict]:
    """Yield every retail, non-anomalous observation on ``day``.

    Iterates via offset paging — good enough for ~700K rows/day. The
    partition on observed_on prunes to a single monthly partition, so
    this is not a full-table scan.
    """
    offset = 0
    while True:
        path = (
            "/rest/v1/mtg_price_observations"
            f"?observed_on=eq.{day}"
            f"&price_type=eq.{DEFAULT_PRICE_TYPE}"
            f"&is_anomalous=is.false"
            "&select=printing_finish_id,provider,market,currency,price"
            f"&limit={page_size}&offset={offset}"
            "&order=printing_finish_id,provider,market,currency"
        )
        code, body = supabase._req(path)
        if code >= 400:
            raise RuntimeError(f"observation fetch failed: HTTP {code} {body[:300]}")
        rows = json.loads(body) if body else []
        if not rows:
            return
        for r in rows:
            yield r
        if len(rows) < page_size:
            return
        offset += page_size


def _median(values: list[float]) -> float:
    return float(statistics.median(values))


def derive_canonical_for_day(
    supabase: SupabaseClient,
    day: str,
    policies: dict[str, ProviderPolicy],
    dry_run: bool = False,
) -> DerivationStats:
    """Compute + write canonical rows for one day.

    Produces both series_scope='internal' and 'public'. Idempotent via
    the UNIQUE (printing_finish, day, market, currency, series_scope,
    methodology_version, price_type) constraint.
    """
    stats = DerivationStats(day=day)

    # Group observations by (finish, market, currency).
    # Value = list of (price, provider) tuples.
    grouped: dict[tuple[str, str, str], list[tuple[float, str]]] = {}
    for row in _fetch_observations_for_day(supabase, day):
        stats.observations_read += 1
        finish   = row["printing_finish_id"]
        market   = row["market"] or ""
        currency = row["currency"]
        price    = float(row["price"])
        provider = row["provider"]
        grouped.setdefault((finish, market, currency), []).append((price, provider))

    stats.keys_seen = len(grouped)

    canonical_batch: list[dict] = []

    for (finish, market, currency), samples in grouped.items():
        for scope, gate in (
            ("internal", lambda p: policies.get(p) and policies[p].derived_use_internal),
            ("public",   lambda p: policies.get(p) and policies[p].derived_use_public),
        ):
            permitted = [(price, prov) for price, prov in samples if gate(prov)]
            if not permitted:
                continue
            if scope == "internal":
                stats.permitted_internal += len(permitted)
            else:
                stats.permitted_public += len(permitted)
            prices   = [p for p, _ in permitted]
            provs    = sorted({prov for _, prov in permitted})
            price    = _median(prices)
            row = {
                "printing_finish_id":     finish,
                "observed_on":            day,
                "market":                 market,
                "currency":               currency,
                "series_scope":           scope,
                "price":                  price,
                "sample_count":           len(permitted),
                "contributing_providers": provs,
                "methodology_version":    METHODOLOGY_VERSION,
                "price_type":             DEFAULT_PRICE_TYPE,
            }
            canonical_batch.append(row)
            for prov in provs:
                if scope == "internal":
                    stats.contributing_providers_internal[prov] = \
                        stats.contributing_providers_internal.get(prov, 0) + 1
                else:
                    stats.contributing_providers_public[prov] = \
                        stats.contributing_providers_public.get(prov, 0) + 1
            if scope == "internal":
                stats.canonical_rows_internal += 1
            else:
                stats.canonical_rows_public += 1

            if len(canonical_batch) >= DEFAULT_UPSERT_BATCH:
                _flush_canonical(supabase, canonical_batch, dry_run, stats)
                canonical_batch = []

    if canonical_batch:
        _flush_canonical(supabase, canonical_batch, dry_run, stats)

    log.info(
        "canonical %s obs=%d keys=%d rows_internal=%d rows_public=%d",
        day, stats.observations_read, stats.keys_seen,
        stats.canonical_rows_internal, stats.canonical_rows_public,
    )
    return stats


def _flush_canonical(
    supabase: SupabaseClient,
    rows: list[dict],
    dry_run: bool,
    stats: DerivationStats,
) -> None:
    if dry_run:
        return
    on_conflict = (
        "printing_finish_id,observed_on,market,currency,"
        "series_scope,methodology_version,price_type"
    )
    code, body = supabase._req(
        f"/rest/v1/mtg_price_daily_canonical?on_conflict={on_conflict}",
        method="POST", body=rows,
        prefer="resolution=ignore-duplicates,return=minimal",
    )
    if code >= 400:
        stats.errors += len(rows)
        log.error("canonical flush failed: HTTP %s %s", code, body[:400])


# ─── mtg_current_prices refresh ─────────────────────────────────────────────

def refresh_current_prices(
    supabase: SupabaseClient,
    since_day: str,
    dry_run: bool = False,
) -> dict[str, int]:
    """Upsert mtg_current_prices with the latest observation per key.

    Strategy: pull the last 3 days of observations (usually enough that
    every priced key has at least one row), reduce to the newest per key
    in memory, upsert via ON CONFLICT DO UPDATE.

    Called after the canonical derivation so anomaly-flagged rows are
    still visible in the current-price table (some clients want the
    latest even if flagged; the is_anomalous field is exposed).
    """
    since = since_day
    rows_by_key: dict[tuple, dict] = {}
    offset = 0
    fetched = 0
    while True:
        code, body = supabase._req(
            "/rest/v1/mtg_price_observations"
            f"?observed_on=gte.{since}"
            "&select=printing_finish_id,provider,market,currency,price_type,condition,price,observed_on,ingestion_source,is_anomalous"
            f"&order=observed_on.desc"
            f"&limit=5000&offset={offset}"
        )
        if code >= 400:
            raise RuntimeError(f"current-price fetch failed: HTTP {code} {body[:300]}")
        page = json.loads(body) if body else []
        if not page:
            break
        fetched += len(page)
        for r in page:
            key = (
                r["printing_finish_id"], r["provider"],
                r["market"] or "", r["currency"],
                r["price_type"] or "", r["condition"] or "",
            )
            existing = rows_by_key.get(key)
            if existing is None or r["observed_on"] > existing["observed_on"]:
                rows_by_key[key] = r
        if len(page) < 5000:
            break
        offset += 5000

    batch: list[dict] = []
    for row in rows_by_key.values():
        batch.append({
            "printing_finish_id": row["printing_finish_id"],
            "provider":           row["provider"],
            "market":             row["market"] or "",
            "currency":           row["currency"],
            "price_type":         row["price_type"] or "",
            "condition":          row["condition"] or "",
            "price":              float(row["price"]),
            "observed_on":        row["observed_on"],
            "ingestion_source":   row["ingestion_source"],
            "is_anomalous":       bool(row["is_anomalous"]),
        })
        if len(batch) >= DEFAULT_UPSERT_BATCH:
            _flush_current_prices(supabase, batch, dry_run)
            batch = []
    if batch:
        _flush_current_prices(supabase, batch, dry_run)

    return {"fetched": fetched, "written": len(rows_by_key)}


def _flush_current_prices(supabase: SupabaseClient, rows: list[dict], dry_run: bool) -> None:
    if dry_run:
        return
    on_conflict = "printing_finish_id,provider,market,currency,price_type,condition"
    code, body = supabase._req(
        f"/rest/v1/mtg_current_prices?on_conflict={on_conflict}",
        method="POST", body=rows,
        prefer="resolution=merge-duplicates,return=minimal",
    )
    if code >= 400:
        log.error("current_prices flush failed: HTTP %s %s", code, body[:400])


# ─── Anomaly detection (day-over-day ratio) ────────────────────────────────

@dataclass
class AnomalyRule:
    name: str
    version: str = "day_over_day_v1"
    upper_ratio: float = 5.0    # a >5x jump vs prior day flags
    lower_ratio: float = 0.2    # a <0.2x drop vs prior day flags
    min_price: float = 0.10     # skip micro-priced rows where ratios are noisy


DEFAULT_ANOMALY_RULE = AnomalyRule(name="day_over_day_5x")


def detect_anomalies(
    supabase: SupabaseClient,
    day: str,
    rule: AnomalyRule = DEFAULT_ANOMALY_RULE,
    dry_run: bool = False,
) -> dict[str, int]:
    """Compare today's observations to yesterday's; flag ratios beyond bounds.

    Writes to mtg_price_anomaly_flags AND flips is_anomalous=true on
    the observation row when a rule fires with severity>='warn'.
    """
    yesterday = _shift_iso_day(day, -1)

    def _pull(d: str) -> dict[tuple, tuple[int, float]]:
        # Return {(finish, provider, market, currency, price_type): (id, price)}
        out: dict[tuple, tuple[int, float]] = {}
        offset = 0
        while True:
            code, body = supabase._req(
                "/rest/v1/mtg_price_observations"
                f"?observed_on=eq.{d}"
                "&select=id,printing_finish_id,provider,market,currency,price_type,price"
                f"&limit=5000&offset={offset}"
            )
            if code >= 400:
                raise RuntimeError(f"anomaly fetch failed: HTTP {code} {body[:200]}")
            page = json.loads(body) if body else []
            if not page:
                break
            for r in page:
                key = (
                    r["printing_finish_id"], r["provider"],
                    r["market"] or "", r["currency"], r["price_type"] or "",
                )
                out[key] = (int(r["id"]), float(r["price"]))
            if len(page) < 5000:
                break
            offset += 5000
        return out

    today_map = _pull(day)
    prev_map  = _pull(yesterday)
    flagged = 0
    for key, (oid, curr) in today_map.items():
        prev = prev_map.get(key)
        if prev is None:
            continue
        _, prev_price = prev
        if prev_price < rule.min_price or curr < rule.min_price:
            continue
        ratio = curr / prev_price
        if ratio >= rule.upper_ratio or ratio <= rule.lower_ratio:
            flagged += 1
            if dry_run:
                continue
            _write_anomaly(supabase, oid, day, rule, prev_price, curr, ratio)
    return {"flagged": flagged, "today_rows": len(today_map), "prev_rows": len(prev_map)}


def _write_anomaly(
    supabase: SupabaseClient, obs_id: int, day: str, rule: AnomalyRule,
    prev: float, curr: float, ratio: float,
) -> None:
    severity = "warn" if 0.05 <= ratio <= 20 else "error"
    row = {
        "observation_id":  obs_id,
        "observed_on":     day,
        "detector_version": rule.version,
        "rule":             rule.name,
        "severity":         severity,
        "prev_price":       prev,
        "curr_price":       curr,
        "ratio":            ratio,
    }
    code, body = supabase._req(
        "/rest/v1/mtg_price_anomaly_flags?on_conflict=observation_id,rule",
        method="POST", body=[row],
        prefer="resolution=ignore-duplicates,return=minimal",
    )
    if code >= 400:
        log.error("anomaly write failed: HTTP %s %s", code, body[:200])
        return
    code, _ = supabase._req(
        f"/rest/v1/mtg_price_observations?id=eq.{obs_id}",
        method="PATCH", body={"is_anomalous": True}, prefer="return=minimal",
    )
    if code >= 400:
        log.warning("anomaly flag patch failed for obs=%s", obs_id)


def _shift_iso_day(iso: str, delta: int) -> str:
    from datetime import date, timedelta
    y, m, d = [int(x) for x in iso.split("-")]
    return (date(y, m, d) + timedelta(days=delta)).isoformat()
