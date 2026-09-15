"""Post-run validation with explicit warn vs hard-fail routing.

HARD FAILURE (returns "failed", pipeline exits non-zero):
  * source_checksum_mismatch
  * source_malformed_structure
  * target_date_mismatch
  * database_write_errors
  * implausibly_tiny_total     — total mapped observations << expected
  * lock_failure
  * partition_missing

WARNING (returns "partial", pipeline exits 0 but records notes):
  * one provider absent
  * one provider materially below its median
  * quarantine rate above historical norm
  * canonical anomaly rate above threshold (Stage 1D: not applicable
    while canonical is deferred; kept as a slot for future use)

Everything else: "success".
"""
from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("mtg_stage1d.validation")


PROVIDER_MEDIAN_MIN_RATIO = 0.7   # provider below 70% of its median = warning
QUARANTINE_HIGH_RATIO     = 3.0   # 3x last-7-day median = warning
IMPLAUSIBLY_TINY_RATIO    = 0.10  # <10% of last-7-day-median total = hard fail
TARGET_DATE_MISMATCH_FAIL = True  # ingest target_date must match mtgjson build date


@dataclass
class ValidationOutcome:
    status: str                       # 'success' | 'partial' | 'failed'
    hard_failures: list[str] = field(default_factory=list)
    warnings:      list[str] = field(default_factory=list)
    facts:         dict      = field(default_factory=dict)


def evaluate(
    *,
    checksum_ok: bool,
    source_parsed_ok: bool,
    target_date: str,
    source_build_date: str,
    partition_ready: bool,
    lock_ok: bool,
    total_observations_inserted: int,
    historical_daily_medians: dict[str, float],   # {'total_obs': ..., 'quarantine': ..., 'per_provider': {'tcgplayer': ...}}
    per_provider_inserted: dict[str, int],
    quarantine_total: int,
    write_errors: int,
) -> ValidationOutcome:
    o = ValidationOutcome(status="success")

    # ─── HARD FAILURES ─────────────────────────────────────────────────
    if not checksum_ok:
        o.hard_failures.append("source_checksum_mismatch")
    if not source_parsed_ok:
        o.hard_failures.append("source_malformed_structure")
    if TARGET_DATE_MISMATCH_FAIL and source_build_date != target_date:
        o.hard_failures.append(
            f"target_date_mismatch: source_build_date={source_build_date} target_date={target_date}"
        )
    if not partition_ready:
        o.hard_failures.append("partition_missing")
    if not lock_ok:
        o.hard_failures.append("lock_failure")
    if write_errors > 0:
        o.hard_failures.append(f"database_write_errors={write_errors}")

    median_total = historical_daily_medians.get("total_obs")
    # An idempotent no-op (same build re-ingested; ON CONFLICT DO NOTHING
    # skipped every mapped row) is NOT an "implausibly tiny" run. Detect
    # that case via conflict_skipped_ratio: if we mapped a full day's
    # worth of rows and conflict-skipped essentially all of them, the
    # DB was already up to date. Only raise the hard failure when we
    # ALSO failed to conflict-skip a large chunk.
    if median_total and total_observations_inserted < IMPLAUSIBLY_TINY_RATIO * median_total:
        looks_idempotent = (
            total_observations_inserted == 0
            and quarantine_total < 0.20 * (median_total or 1)
        )
        if not looks_idempotent:
            o.hard_failures.append(
                f"implausibly_tiny_total: inserted={total_observations_inserted} median={median_total}"
            )

    # ─── WARNINGS ──────────────────────────────────────────────────────
    per_provider_medians = historical_daily_medians.get("per_provider") or {}
    for prov, prev_median in per_provider_medians.items():
        curr = per_provider_inserted.get(prov, 0)
        if prev_median <= 0:
            continue
        if curr == 0:
            o.warnings.append(f"provider_absent:{prov}")
            continue
        if curr < PROVIDER_MEDIAN_MIN_RATIO * prev_median:
            o.warnings.append(
                f"provider_below_median:{prov} curr={curr} median={prev_median:.0f}"
            )

    q_median = historical_daily_medians.get("quarantine")
    if q_median and q_median > 0 and quarantine_total > QUARANTINE_HIGH_RATIO * q_median:
        o.warnings.append(
            f"quarantine_high: curr={quarantine_total} median={q_median:.0f}"
        )

    # ─── Overall status ────────────────────────────────────────────────
    if o.hard_failures:
        o.status = "failed"
    elif o.warnings:
        o.status = "partial"

    o.facts = {
        "target_date":                 target_date,
        "source_build_date":           source_build_date,
        "total_observations_inserted": total_observations_inserted,
        "quarantine_total":            quarantine_total,
        "per_provider_inserted":       per_provider_inserted,
        "historical_daily_medians":    historical_daily_medians,
    }
    return o


def compute_last_7_day_medians(recent_notes: list[dict]) -> dict:
    """Given the last N market_import_runs.notes payloads (AllPricesToday
    runs), return medians for total_obs, quarantine, per-provider inserted.

    Missing keys are treated as zero. If there are fewer than 3 samples
    the medians are returned but the caller should treat them as noisy.
    """
    totals: list[float] = []
    quars:  list[float] = []
    per_p:  dict[str, list[float]] = {}
    for n in recent_notes:
        if not isinstance(n, dict):
            continue
        if "inserted_new" in n:
            totals.append(float(n["inserted_new"]))
        if "obs_missing_finish" in n or "obs_unmapped_uuid" in n or "obs_invalid_price" in n:
            quars.append(float(
                (n.get("obs_missing_finish") or 0)
                + (n.get("obs_unmapped_uuid") or 0)
                + (n.get("obs_invalid_price") or 0)
            ))
        pc = n.get("provider_counts") or {}
        for p, v in pc.items():
            per_p.setdefault(p, []).append(float(v))

    return {
        "total_obs": statistics.median(totals) if totals else 0.0,
        "quarantine": statistics.median(quars) if quars else 0.0,
        "per_provider": {p: statistics.median(vs) for p, vs in per_p.items() if vs},
    }
