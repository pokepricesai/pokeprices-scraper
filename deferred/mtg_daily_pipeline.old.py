"""MTGPrices Stage 1D daily pipeline orchestrator.

Runs every day (via GitHub Actions .github/workflows/mtg-daily-ingest.yml).
End-to-end steps:

  1. Fetch Scryfall bulk-data metadata; detect any new set / updated printings.
  2. Optionally re-ingest changed printings (Scryfall enrichment).
  3. Fetch Scryfall /sets; auto-enqueue new sets for admin review.
  4. Download MTGJSON AllPricesToday.json.gz; append today's observations.
  5. Derive canonical (internal + public) rows for the affected day.
  6. Refresh mtg_current_prices from the last few days.
  7. Run anomaly detection (day-over-day).
  8. Write a summary market_import_runs row.

Fail-closed. Idempotent. Each step reports success/skip/failure back to
the run summary and the workflow exits non-zero on unrecoverable
failures — but each step is written so a re-run picks up where the
previous stopped.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import mtgjson_ingestion as mi   # noqa: E402
import scryfall_ingestion as si  # noqa: E402
import mtg_canonical_pricing as canon  # noqa: E402


log = logging.getLogger("mtg_daily_pipeline")

ENV_ENABLED = "MTG_DAILY_INGEST_ENABLED"


def _iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _iso_today() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--only-day", default=None,
                   help="Explicit YYYY-MM-DD (default: yesterday UTC, so all "
                        "provider files for the day have been published)")
    p.add_argument("--skip-scryfall", action="store_true",
                   help="Skip Scryfall bulk fetch step (for pricing-only reruns)")
    p.add_argument("--skip-prices",  action="store_true",
                   help="Skip MTGJSON prices step (for catalogue-only reruns)")
    p.add_argument("--skip-canonical", action="store_true")
    p.add_argument("--skip-current",   action="store_true")
    p.add_argument("--skip-anomaly",   action="store_true")
    return p.parse_args(argv)


# ─── Scryfall delta step ────────────────────────────────────────────────────

def step_scryfall_delta(supabase, dry_run: bool) -> dict:
    """Fetch Scryfall metadata + /sets; detect new sets/cards; enqueue admin review.

    Full-catalogue re-ingest is expensive; this step only:
      1. Checks Scryfall /sets for sets we do not yet have and inserts
         them AS mtg_sets rows with public_indexable=false (in a
         downstream migration; today the flag lives on printings).
      2. Enqueues each new set as an mtg_admin_review entry.
      3. Detects mtg_printings.scryfall_id values that Scryfall's
         Default Cards feed *may* have (delta detection is deferred to
         a later step; this v1 records that Scryfall's Default Cards
         feed updated_at moved).
    """
    from scryfall_ingestion import ScryfallClient
    client = ScryfallClient()
    bulk = client.get_bulk_metadata()
    default_meta = bulk.get(si.BULK_TYPE_DEFAULT)
    oracle_meta  = bulk.get(si.BULK_TYPE_ORACLE)
    updated = {
        "default_updated_at": default_meta and default_meta.get("updated_at"),
        "oracle_updated_at":  oracle_meta  and oracle_meta.get("updated_at"),
    }

    # New-set detection: pull /sets, look for codes not in mtg_sets.
    existing_codes = _fetch_known_set_codes(supabase)
    new_sets = []
    for s in client.iter_sets():
        code = (s.get("code") or "").strip().lower()
        if not code or code in existing_codes:
            continue
        new_sets.append({
            "code":        code,
            "name":        s.get("name"),
            "released_at": s.get("released_at"),
            "set_type":    s.get("set_type"),
            "scryfall_id": s.get("id"),
        })

    log.info("scryfall delta: default_updated_at=%s new_sets=%d",
             updated["default_updated_at"], len(new_sets))

    if not dry_run and new_sets:
        # Insert new sets non-destructively — set_id remains empty on
        # existing rows; a full Scryfall ingest will enrich later.
        rows = [{
            "code":        s["code"],
            "name":        s["name"],
            "released_at": s["released_at"],
            "set_type":    s["set_type"],
            "scryfall_id": s["scryfall_id"],
        } for s in new_sets]
        code, body = supabase._req(
            "/rest/v1/mtg_sets?on_conflict=code",
            method="POST", body=rows,
            prefer="resolution=ignore-duplicates,return=minimal",
        )
        if code >= 400:
            log.error("new-set insert failed: HTTP %s %s", code, body[:300])
        # Enqueue admin review
        review_rows = [{
            "entity_type": "set",
            "entity_natural_key": s["code"],
            "reason": "new_set_detected",
            "payload": s,
        } for s in new_sets]
        supabase._req(
            "/rest/v1/mtg_admin_review",
            method="POST", body=review_rows, prefer="return=minimal",
        )

    return {"new_sets": [s["code"] for s in new_sets],
            "default_updated_at": updated["default_updated_at"]}


def _fetch_known_set_codes(supabase) -> set[str]:
    out: set[str] = set()
    offset = 0
    while True:
        code, body = supabase._req(
            f"/rest/v1/mtg_sets?select=code&limit=1000&offset={offset}"
        )
        if code >= 400:
            raise RuntimeError(f"fetch known set codes failed: HTTP {code}")
        rows = json.loads(body) if body else []
        for r in rows:
            out.add(r["code"])
        if len(rows) < 1000:
            break
        offset += 1000
    return out


# ─── MTGJSON prices step ────────────────────────────────────────────────────

def step_mtgjson_prices(supabase, only_day: str | None, dry_run: bool) -> dict:
    """Download AllPricesToday.json.gz (or AllPrices for backfill) and
    append via the Stage 1C ingestion path.

    For Stage 1D daily runs we use AllPricesToday.json.gz — smaller,
    just today's data. The ingestion module reuses the existing
    reconciliation.json for finish + UUID resolution.
    """
    workspace = _REPO_ROOT / "mtgjson_data"
    client = mi.MTGJSONClient(workspace)
    # Download today's slice.
    today_url = "https://mtgjson.com/api/v5/AllPricesToday.json.gz"
    today_sha_url = "https://mtgjson.com/api/v5/AllPricesToday.json.gz.sha256"
    from urllib.request import urlopen, Request
    with urlopen(Request(today_sha_url), timeout=60) as r:
        expected_sha = r.read().decode("utf-8").strip()
    with urlopen(Request(today_url), timeout=1800) as r:
        data = r.read()
    local = workspace / "AllPricesToday.json.gz"
    local.write_bytes(data)
    (local.parent / "AllPricesToday.json.gz.sha256").write_text(expected_sha)
    import hashlib
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha:
        raise RuntimeError(f"AllPricesToday SHA-256 mismatch: expected={expected_sha} actual={actual}")

    recon = mi.load_reconciliation(mi.default_reconciliation_path(_REPO_ROOT),
                                   mi.load_finish_lookup(supabase))
    ing = mi.AllPricesIngestion(
        supabase=supabase, allprices_path=local, reconciliation=recon,
        dry_run=dry_run, import_type="scraper_nightly",
        upsert_batch_size=mi.DEFAULT_UPSERT_BATCH_SIZE,
        only_date=only_day,
        sha256_expected=expected_sha, sha256_actual=actual,
    )
    ing.start()
    ing.run()
    status = "success" if ing.stats.errors == 0 else "partial"
    ing.finish(status=status)
    return {
        "run_id":  ing.run_id,
        "status":  status,
        "obs_total":       ing.stats.obs_total,
        "obs_mapped":      ing.stats.obs_mapped,
        "inserted_new":    ing.stats.inserted_new,
        "conflict_skipped": ing.stats.conflict_skipped,
        "date_min": ing.stats.date_min,
        "date_max": ing.stats.date_max,
    }


# ─── Canonical + current + anomaly ─────────────────────────────────────────

def step_canonical(supabase, day: str, dry_run: bool) -> dict:
    policies = canon.load_provider_policies(supabase)
    stats = canon.derive_canonical_for_day(supabase, day, policies, dry_run=dry_run)
    return {
        "canonical_rows_internal": stats.canonical_rows_internal,
        "canonical_rows_public":   stats.canonical_rows_public,
        "keys_seen":               stats.keys_seen,
        "observations_read":       stats.observations_read,
        "errors":                  stats.errors,
    }


def step_current_prices(supabase, since_day: str, dry_run: bool) -> dict:
    return canon.refresh_current_prices(supabase, since_day, dry_run=dry_run)


def step_anomaly(supabase, day: str, dry_run: bool) -> dict:
    return canon.detect_anomalies(supabase, day, dry_run=dry_run)


# ─── Orchestrator ──────────────────────────────────────────────────────────

def main(argv=None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    log.info("mtg_daily_pipeline start dry_run=%s", args.dry_run)

    if not args.dry_run and os.environ.get(ENV_ENABLED, "").strip().lower() != "true":
        log.error("Refusing to write: %s != 'true'. Set it explicitly or pass --dry-run.",
                  ENV_ENABLED)
        return 2

    supabase = mi.build_default_supabase_client()
    if not args.dry_run and supabase is None:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing")
        return 3

    day = args.only_day
    if not day:
        # Use yesterday UTC — safer than "today", every provider has published.
        from datetime import date, timedelta
        day = (date.fromisoformat(_iso_today()) - timedelta(days=1)).isoformat()

    summary: dict = {"day": day, "started_at": _iso_now(), "dry_run": args.dry_run}

    try:
        if args.skip_scryfall:
            summary["scryfall"] = {"skipped": True}
        else:
            summary["scryfall"] = step_scryfall_delta(supabase, args.dry_run) if supabase else {}
    except Exception as e:
        log.exception("scryfall step failed")
        summary["scryfall"] = {"error": str(e)}

    try:
        if args.skip_prices:
            summary["prices"] = {"skipped": True}
        else:
            summary["prices"] = step_mtgjson_prices(supabase, day, args.dry_run) if supabase else {}
    except Exception as e:
        log.exception("prices step failed")
        summary["prices"] = {"error": str(e)}

    try:
        if args.skip_canonical:
            summary["canonical"] = {"skipped": True}
        else:
            summary["canonical"] = step_canonical(supabase, day, args.dry_run) if supabase else {}
    except Exception as e:
        log.exception("canonical step failed")
        summary["canonical"] = {"error": str(e)}

    try:
        if args.skip_current:
            summary["current_prices"] = {"skipped": True}
        else:
            from datetime import date, timedelta
            since = (date.fromisoformat(day) - timedelta(days=2)).isoformat()
            summary["current_prices"] = step_current_prices(supabase, since, args.dry_run) if supabase else {}
    except Exception as e:
        log.exception("current_prices step failed")
        summary["current_prices"] = {"error": str(e)}

    try:
        if args.skip_anomaly:
            summary["anomaly"] = {"skipped": True}
        else:
            summary["anomaly"] = step_anomaly(supabase, day, args.dry_run) if supabase else {}
    except Exception as e:
        log.exception("anomaly step failed")
        summary["anomaly"] = {"error": str(e)}

    summary["completed_at"] = _iso_now()

    # Write a summary run row (idempotent — one per day). If the widen
    # migration 2026-09-15f has not landed yet, the CHECK constraint
    # will reject provider='mtgprices_daily'; we log-and-continue.
    if supabase is not None and not args.dry_run:
        run_payload = {
            "provider":         "mtgprices_daily",
            "source":           "scraper_nightly",
            "status":           "success" if not _any_step_errored(summary) else "partial",
            "started_at":       summary["started_at"],
            "completed_at":     summary["completed_at"],
            "parser_version":   "mtg_daily_pipeline@v1",
            "notes":            json.dumps(summary, sort_keys=True, default=str),
        }
        code, body = supabase._req(
            "/rest/v1/market_import_runs",
            method="POST", body=[run_payload], prefer="return=minimal",
        )
        if code >= 400:
            log.warning(
                "summary row insert failed (HTTP %s); non-fatal. "
                "Apply migrations/2026-09-15f-market-import-runs-provider-widen-daily.sql to enable.",
                code,
            )

    print(json.dumps(summary, indent=2, default=str))
    return 0 if not _any_step_errored(summary) else 1


def _any_step_errored(summary: dict) -> bool:
    for k, v in summary.items():
        if isinstance(v, dict) and "error" in v:
            return True
    return False


if __name__ == "__main__":
    sys.exit(main())
