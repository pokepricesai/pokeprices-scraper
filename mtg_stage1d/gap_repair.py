"""Gap detection + weekly AllPrices repair for Stage 1D.

Two entry points:

* :func:`assess_recent`         — cheap daily 14-day sweep; reports.
* :func:`run_weekly_repair`     — Monday deep repair using AllPrices.

Classifications (persisted in market_import_runs.notes.gap_repair):

    still_unknown                — no AllPrices probe yet
    ingestion_side_repaired      — AllPrices had rows; we inserted them
    ingestion_side_unfilled      — AllPrices had rows but nothing inserted
    source_side_confirmed        — AllPrices had ZERO rows for that date

``source_side_confirmed`` sticks until a different AllPrices build is
consulted. The build fingerprint used is the SHA-256 of the AllPrices
file the classification was made against; a new build re-opens the
question for the affected dates.
"""
from __future__ import annotations

import hashlib
import json
import logging
import urllib.request
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

log = logging.getLogger("mtg_stage1d.gap_repair")

GAP_WINDOW_DAYS = 14
SUSPICIOUSLY_LOW_THRESHOLD = 100_000

ALLPRICES_URL     = "https://mtgjson.com/api/v5/AllPrices.json.gz"
ALLPRICES_SHA_URL = "https://mtgjson.com/api/v5/AllPrices.json.gz.sha256"

USER_AGENT = (
    "MTGPricesStage1D/1.0 (+https://www.pokeprices.io; contact@pokeprices.io)"
)


def _http_get(url: str, timeout: float):
    return urlopen(Request(url, headers={"User-Agent": USER_AGENT}), timeout=timeout)


@dataclass
class GapAssessment:
    date: str
    row_count: int
    status: str  # 'ok' | 'missing' | 'low'


# ─── Cheap daily check ─────────────────────────────────────────────────────

def assess_recent(
    supabase: Any,
    reference_day: date | None = None,
    window_days: int = GAP_WINDOW_DAYS,
) -> list[GapAssessment]:
    """Return per-day count + status for the last N days."""
    reference_day = reference_day or date.today()
    start = reference_day - timedelta(days=window_days)
    out: list[GapAssessment] = []
    for i in range(window_days):
        iso = (start + timedelta(days=i)).isoformat()
        n = _count(supabase, iso)
        status = "ok"
        if n == 0:
            status = "missing"
        elif n < SUSPICIOUSLY_LOW_THRESHOLD:
            status = "low"
        out.append(GapAssessment(date=iso, row_count=n, status=status))
    return out


def _count(supabase: Any, iso_date: str) -> int:
    headers = {
        "apikey":         supabase.key,
        "Authorization":  f"Bearer {supabase.key}",
        "Accept":         "application/json",
        "Prefer":         "count=exact",
        "Range":          "0-0",
    }
    url = (
        f"{supabase.url}/rest/v1/mtg_price_observations"
        f"?observed_on=eq.{iso_date}&select=observed_on"
    )
    req = Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        cr = resp.headers.get("content-range") or ""
    try:
        return int(cr.split("/")[-1])
    except (ValueError, IndexError):
        return 0


# ─── Classification decision ──────────────────────────────────────────────

def classify_repair_outcome(
    gap_date: str,
    source_observations_for_date: int | None,
    rows_inserted: int,
) -> str:
    """Classify a gap-repair attempt.

    :param source_observations_for_date:
        Total observations present in AllPrices for the target date,
        INCLUDING those under unmapped UUIDs. Zero here means the
        SOURCE itself has no data for the date. If we only counted
        mapped observations, an unmapped-UUID-only date could be
        mislabelled source_side_confirmed when the source actually
        has data we simply cannot resolve.
    :param rows_inserted:
        Number of rows the ingester inserted for the target date.
    """
    if source_observations_for_date is None:
        return "still_unknown"
    if source_observations_for_date == 0:
        return "source_side_confirmed"
    if rows_inserted > 0:
        return "ingestion_side_repaired"
    return "ingestion_side_unfilled"


# ─── Prior-classification lookup ──────────────────────────────────────────

def load_prior_classifications(supabase: Any) -> dict[str, dict]:
    """Return {date_iso: {"status": ..., "sha256": ...}} for dates we've
    already probed via a per-date backfill ``AllPricesIngestion`` run.

    We infer classifications from the existing
    ``market_import_runs`` rows the ingest itself writes (provider=mtgjson,
    source=backfill, notes.only_date set). No dedicated gap_repair
    notes block is required — the ingest stats already carry all the
    signals:

        obs_mapped == 0              → source_side_confirmed
        obs_mapped > 0 AND
        inserted_new + conflict_skipped == obs_mapped
                                    → ingestion_side_repaired
        obs_mapped > 0 AND
        inserted_new == 0            → ingestion_side_unfilled

    The AllPrices SHA is read from ``notes.sha256_actual``. Only the
    MOST RECENT run per date wins if multiple exist.
    """
    if supabase is None:
        return {}
    code, body = supabase._req(
        "/rest/v1/market_import_runs"
        "?provider=eq.mtgjson"
        "&source=eq.backfill"
        "&status=in.(success,partial)"
        "&order=started_at.desc&limit=200"
        "&select=notes,started_at"
    )
    if code >= 400:
        return {}
    rows = json.loads(body) if body else []
    out: dict[str, dict] = {}
    for r in rows:
        notes = r.get("notes")
        if isinstance(notes, str):
            try:
                notes = json.loads(notes)
            except json.JSONDecodeError:
                continue
        if not isinstance(notes, dict):
            continue
        only_date = notes.get("only_date")
        if not only_date:
            continue
        # Only the FIRST (most-recent) row per date wins.
        if only_date in out:
            continue
        # Prefer the date-scoped source counter when present. Fall
        # back to obs_mapped for LEGACY rows written before the
        # counter existed — those cannot distinguish "source empty"
        # from "source has only unmapped-UUID rows", so their
        # classification carries the same risk the pre-hardening
        # design had. Newer rows use the correct counter.
        sha = notes.get("sha256_actual")
        if "obs_source_for_date" in notes:
            source_for_date = notes.get("obs_source_for_date") or 0
        else:
            source_for_date = notes.get("obs_mapped") or 0
        inserted_new = notes.get("inserted_new") or 0
        if source_for_date == 0:
            status = "source_side_confirmed"
        elif inserted_new > 0:
            status = "ingestion_side_repaired"
        else:
            status = "ingestion_side_unfilled"
        out[only_date] = {"status": status, "sha256": sha,
                          "legacy": "obs_source_for_date" not in notes}
    return out


# ─── Weekly deep repair ───────────────────────────────────────────────────

@dataclass
class GapRepairRun:
    downloaded_allprices:   bool = False
    allprices_sha256:       str  = ""
    dates_probed:           list[str] = field(default_factory=list)
    dates_repaired:         list[str] = field(default_factory=list)
    dates_source_confirmed: list[str] = field(default_factory=list)
    dates_skipped_prior:    list[str] = field(default_factory=list)
    per_date_stats:         dict = field(default_factory=dict)
    errors:                 int  = 0

    def to_notes(self) -> dict:
        """Serialise for storage under market_import_runs.notes.gap_repair."""
        classifications = {}
        for iso in self.dates_repaired:
            classifications[iso] = {"status": "ingestion_side_repaired", "sha256": self.allprices_sha256}
        for iso in self.dates_source_confirmed:
            classifications[iso] = {"status": "source_side_confirmed", "sha256": self.allprices_sha256}
        for iso in self.dates_skipped_prior:
            classifications[iso] = {"status": "source_side_confirmed", "sha256": self.allprices_sha256, "skipped_prior": True}
        return {
            "downloaded_allprices":   self.downloaded_allprices,
            "allprices_sha256":       self.allprices_sha256,
            "classifications":        classifications,
            "per_date_stats":         self.per_date_stats,
            "errors":                 self.errors,
        }


def run_weekly_repair(
    supabase: Any,
    workspace: Path,
    reference_day: date | None = None,
    window_days: int = GAP_WINDOW_DAYS,
    dry_run: bool = False,
) -> GapRepairRun:
    """Perform deep repair for any candidate dates in the recent window.

    Uses the existing ``mtgjson_ingestion.AllPricesIngestion`` code path
    with ``only_date=<gap>`` — so the same immutable-identity
    ON CONFLICT DO NOTHING semantics that Stage 1C bootstrap used apply
    to repair inserts.
    """
    run = GapRepairRun()
    candidates = [
        a.date for a in assess_recent(supabase, reference_day, window_days)
        if a.status in ("missing", "low")
    ]
    if not candidates:
        log.info("gap_repair.weekly: no candidate dates in last %d days; nothing to do", window_days)
        return run

    # Load prior classifications to short-circuit
    prior = load_prior_classifications(supabase) if supabase else {}
    # We do not yet know the SHA of today's AllPrices; short-circuit only for
    # source-confirmed dates that will still be true regardless of build.
    #
    # However, "source_side_confirmed" for a specific build cannot be trusted
    # blindly against a NEWER AllPrices build (which may include the date now).
    # We conservatively re-check UNLESS the classification's SHA matches the
    # AllPrices file we're about to download.

    # Download AllPrices once (large; ~1-2 GB compressed).
    with _http_get(ALLPRICES_SHA_URL, timeout=60) as r:
        expected_sha = r.read().decode("utf-8").strip()
    run.allprices_sha256 = expected_sha

    # Fast path: if EVERY candidate is already source_side_confirmed
    # against this SHA using the MODERN date-scoped signal
    # (obs_source_for_date), we can skip the multi-GB download.
    # Legacy classifications (based on the old obs_mapped-only signal)
    # do NOT trigger the fast path — they get re-probed once with the
    # correct counter, then subsequent runs use the fast path.
    if all(
        (prior.get(d) or {}).get("status") == "source_side_confirmed"
        and (prior.get(d) or {}).get("sha256") == expected_sha
        and not (prior.get(d) or {}).get("legacy", False)
        for d in candidates
    ):
        run.dates_skipped_prior = list(candidates)
        log.info("gap_repair.weekly: all candidates already source-confirmed against SHA %s; skipping download",
                 expected_sha[:12])
        return run

    with _http_get(ALLPRICES_URL, timeout=1800) as r:
        payload = r.read()
    workspace.mkdir(parents=True, exist_ok=True)
    local_gz = workspace / "AllPrices.json.gz"
    local_gz.write_bytes(payload)
    (local_gz.parent / "AllPrices.json.gz.sha256").write_text(expected_sha, encoding="utf-8")
    actual_sha = hashlib.sha256(payload).hexdigest()
    if actual_sha != expected_sha:
        run.errors += 1
        log.error("AllPrices SHA mismatch: expected=%s actual=%s", expected_sha, actual_sha)
        return run
    run.downloaded_allprices = True

    # For each candidate date, drive one AllPricesIngestion(only_date=<gap>) run.
    import mtgjson_ingestion as mi
    for iso in candidates:
        run.dates_probed.append(iso)
        prev = prior.get(iso) or {}
        prev_status = prev.get("status")
        prev_sha    = prev.get("sha256")
        prev_legacy = prev.get("legacy", False)
        if prev_status == "source_side_confirmed" and prev_sha == expected_sha and not prev_legacy:
            run.dates_skipped_prior.append(iso)
            log.info("gap_repair: %s already source-confirmed for this build; skipping", iso)
            continue
        try:
            if supabase is not None:
                recon = mi.load_reconciliation_from_db(supabase)
            else:
                recon = mi.load_reconciliation(
                    mi.default_reconciliation_path(local_gz.parent.parent),
                    {},
                )
        except FileNotFoundError:
            log.error("gap_repair: reconciliation source unavailable; cannot map UUIDs. Aborting.")
            run.errors += 1
            break
        except Exception as e:
            log.error("gap_repair: reconciliation load failed: %s. Aborting.", e)
            run.errors += 1
            break

        ing = mi.AllPricesIngestion(
            supabase=supabase, allprices_path=local_gz, reconciliation=recon,
            dry_run=dry_run, import_type="backfill",
            upsert_batch_size=mi.DEFAULT_UPSERT_BATCH_SIZE,
            only_date=iso, sha256_expected=expected_sha, sha256_actual=actual_sha,
        )
        try:
            ing.start()
            ing.run()
            status = "success" if ing.stats.errors == 0 else "partial"
            ing.finish(status=status)
        except Exception as e:
            log.exception("gap_repair: AllPricesIngestion crashed for %s", iso)
            run.errors += 1
            continue

        source_for_date = ing.stats.obs_source_for_date
        inserted        = ing.stats.inserted_new
        classification  = classify_repair_outcome(iso, source_for_date, inserted)
        run.per_date_stats[iso] = {
            "obs_source_for_date":  ing.stats.obs_source_for_date,
            "obs_mapped":           ing.stats.obs_mapped,
            "inserted_new":         ing.stats.inserted_new,
            "conflict_skipped":     ing.stats.conflict_skipped,
            "obs_missing_finish":   ing.stats.obs_missing_finish,
            "obs_unmapped_uuid":    ing.stats.obs_unmapped_uuid,
            "errors":               ing.stats.errors,
            "classification":       classification,
        }
        if classification == "source_side_confirmed":
            run.dates_source_confirmed.append(iso)
        elif classification == "ingestion_side_repaired":
            run.dates_repaired.append(iso)

    return run
