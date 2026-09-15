"""MTGPrices Stage 1D daily ingest orchestrator.

Steps (approved 2026-09-15 rescope):
    A. acquire DB lock
    B. ensure current + next monthly partitions
    C. Scryfall bulk-metadata freshness gate (skip if unchanged)
    D. MTGJSON UUID identifier delta (small)
    E. download + SHA-256 verify AllPricesToday.json.gz
    F. determine target_date from MTGJSON meta.data.date
    G. duplicate-build guard (idempotency)
    H. append immutable observations into mtg_price_observations
    I. refresh mtg_current_prices for touched keys
    J. validate (warn vs hard-fail routing)
    K. release lock

Fail-closed. No pre-existing history is dropped or overwritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import mtgjson_ingestion as mi          # noqa: E402
from mtg_stage1d import lock            # noqa: E402
from mtg_stage1d import partitions      # noqa: E402
from mtg_stage1d import build_guard     # noqa: E402
from mtg_stage1d import current_prices  # noqa: E402
from mtg_stage1d import gap_repair      # noqa: E402
from mtg_stage1d import validation      # noqa: E402
from mtg_stage1d import scryfall_delta  # noqa: E402
from mtg_stage1d import identifier_delta  # noqa: E402
from mtg_stage1d import allprices_today   # noqa: E402


log = logging.getLogger("mtg_daily_pipeline")

ENV_ENABLED = "MTG_DAILY_INGEST_ENABLED"
LOCK_KEY = "mtg_daily_ingest"
PROCESS_NAME = "mtg_daily_pipeline"

DEFAULT_MTGJSON_TODAY_URL = "https://mtgjson.com/api/v5/AllPricesToday.json.gz"
DEFAULT_MTGJSON_TODAY_SHA_URL = "https://mtgjson.com/api/v5/AllPricesToday.json.gz.sha256"


def _iso_utc(dt: datetime | None = None) -> str:
    return (dt or datetime.now(tz=timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true",
                   help="Parse and report; do not write to DB.")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--skip-scryfall", action="store_true")
    p.add_argument("--skip-identifiers", action="store_true")
    p.add_argument("--skip-prices", action="store_true")
    p.add_argument("--skip-current-refresh", action="store_true")
    p.add_argument("--skip-gap-check", action="store_true")
    p.add_argument("--weekly-gap-repair", action="store_true",
                   help="Run the deep AllPrices gap-repair pass (Monday cron / manual test).")
    p.add_argument("--gap-window-days", type=int, default=None,
                   help="Override the 14-day gap-check window for MANUAL tests. Also widens the "
                        "candidate set passed to the weekly repair. Do not use in production.")
    p.add_argument("--initial-current-price-population", action="store_true",
                   help="First-run mode: refresh mtg_current_prices with a 30-day look-back.")
    p.add_argument("--force-current-price-refresh", action="store_true",
                   help="Refresh mtg_current_prices even when the prices step did not "
                        "actually ingest anything (default: skip in that case).")
    p.add_argument("--force-identifier-delta", action="store_true",
                   help="Run the MTGJSON identifier delta even on duplicate-build "
                        "fallback runs (default: skip in that case).")
    p.add_argument("--force-scryfall-invoke", action="store_true",
                   help="Invoke the Scryfall ingester on duplicate-build fallback "
                        "even when the price build was already ingested "
                        "(default: defer Scryfall refresh to the next primary run).")
    return p.parse_args(argv)


def _should_run_identifier_delta(
    skip_flag: bool,
    force_flag: bool,
    prices_summary: dict | None,
) -> tuple[bool, str | None]:
    """Whether the (multi-MB) MTGJSON identifier delta should run.

    Same shape as :func:`_should_skip_current_refresh` but returns
    ``(should_run, skip_reason)``. Skipping on duplicate-build is the
    core reason this reorder exists — the AllIdentifiers download is
    ~50 MB and streamed through ijson; running it on a fallback that
    won't ingest any observations is wasted network + CPU.

    Rules:
      * ``--skip-identifiers`` always skips.
      * ``--force-identifier-delta`` always runs.
      * Missing prices summary, ``skipped_duplicate_build``, prices
        error or ``--skip-prices`` → skip.
      * Otherwise: run.
    """
    if skip_flag:
        return False, "skip_flag"
    if force_flag:
        return True, None
    if not isinstance(prices_summary, dict):
        return False, "no_prices_step"
    if prices_summary.get("skipped"):
        return False, "prices_step_skipped"
    if prices_summary.get("action") == "skipped_duplicate_build":
        return False, "duplicate_build"
    if "error" in prices_summary:
        return False, "prices_step_errored"
    return True, None


def _should_skip_current_refresh(
    skip_flag: bool,
    initial_population: bool,
    force_refresh: bool,
    prices_summary: dict | None,
) -> tuple[bool, str | None]:
    """Decide whether the current-price refresh should be skipped.

    Rules (in order):
      * ``--skip-current-refresh`` always skips.
      * ``--initial-current-price-population`` or ``--force-current-price-refresh``
        never skip.
      * If the prices step reported no summary, was explicitly skipped,
        was a build-guard skip, or ingested zero rows this invocation,
        skip the refresh (current_prices was already up to date when
        the last ingested build was processed).
      * Otherwise, run the refresh.

    Returns (should_skip, reason). ``reason`` is a stable string used
    for the summary payload — safe to key logs / tests on.
    """
    if skip_flag:
        return True, "skip_flag"
    if initial_population or force_refresh:
        return False, None
    if not isinstance(prices_summary, dict):
        return True, "no_prices_step"
    if prices_summary.get("skipped"):
        return True, "prices_step_skipped"
    if prices_summary.get("action") == "skipped_duplicate_build":
        return True, "duplicate_build"
    if "error" in prices_summary:
        return True, "prices_step_errored"
    if (prices_summary.get("inserted_new") or 0) == 0:
        return True, "no_new_prices"
    return False, None


# ─── Steps ─────────────────────────────────────────────────────────────────

# NOTE: AllPricesToday download / verify / meta-read live in
# mtg_stage1d.allprices_today so they can be unit-tested against a
# small structural fixture without touching the network.


# ─── Orchestrator ──────────────────────────────────────────────────────────

def main(argv=None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    summary: dict[str, Any] = {"started_at": _iso_utc()}

    if not args.dry_run and os.environ.get(ENV_ENABLED, "").strip().lower() != "true":
        log.error("Refusing to write: %s != 'true'. Set it or pass --dry-run.", ENV_ENABLED)
        return 2

    supabase = mi.build_default_supabase_client()
    if not args.dry_run and supabase is None:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing")
        return 3

    # ─── A. Acquire lock ───────────────────────────────────────────────
    if args.dry_run:
        handle = None
        summary["lock"] = {"skipped": True, "dry_run": True}
    else:
        try:
            handle = lock.acquire(supabase, LOCK_KEY, PROCESS_NAME,
                                  metadata={"started_at": summary["started_at"]})
            summary["lock"] = {
                "acquired_by":         handle.acquired_by,
                "stale_takeover":      handle.stale_takeover,
                "expected_release_by": handle.expected_release_by,
            }
        except lock.LockAcquireFailed as e:
            log.error("lock acquire failed: %s", e)
            summary["lock"] = {"error": str(e)}
            return 4

    try:
        # ─── B. Ensure partitions ──────────────────────────────────────
        try:
            if args.dry_run:
                summary["partitions"] = {"skipped": True, "dry_run": True}
            else:
                summary["partitions"] = partitions.ensure_current_and_next(supabase)
        except Exception as e:
            log.exception("partition step failed")
            summary["partitions"] = {"error": str(e)}
            return 5

        # ─── C. Cheap Scryfall freshness probe ────────────────────────
        # ONE HTTP GET + ONE PostgREST call. Never invokes the ingester
        # here — we defer that decision until AFTER the build guard so
        # duplicate-build fallbacks stay genuinely cheap.
        scryfall_freshness = None
        if args.skip_scryfall:
            summary["scryfall"] = {"action": "skipped_by_flag"}
        else:
            try:
                scryfall_freshness = scryfall_delta.check_freshness(supabase)
                summary["scryfall"] = {
                    "action":            "unchanged" if not scryfall_freshness.changed else "changed_pending",
                    "remote_updated_at": scryfall_freshness.remote_updated_at,
                    "local_updated_at":  scryfall_freshness.local_updated_at,
                }
            except Exception as e:
                log.exception("scryfall freshness probe failed (non-fatal)")
                summary["scryfall"] = {"error": str(e), "non_fatal": True}

        # ─── D. Download AllPricesToday + SHA-256 verify (small file) ─
        target_date: str | None = None
        local_gz = None
        allprices_expected = None
        allprices_actual = None
        if args.skip_prices:
            summary["prices"] = {"skipped": True}
            outcome = None
        else:
            workspace = _REPO_ROOT / "mtgjson_data"
            try:
                local_gz, allprices_expected, allprices_actual = allprices_today.download_and_verify(workspace)
            except allprices_today.ChecksumMismatch as e:
                summary["prices"] = {"error": f"checksum_mismatch: {e}"}
                return 5
            except Exception as e:
                log.exception("download failed")
                summary["prices"] = {"error": f"download: {e}"}
                return 5

            # ─── E. target_date = MTGJSON meta.date ────────────────────
            try:
                target_date = allprices_today.read_meta_date(local_gz)
            except allprices_today.MalformedSource as e:
                log.exception("meta.date read failed (malformed)")
                summary["prices"] = {"error": f"malformed: {e}"}
                return 5
            except Exception as e:
                log.exception("meta.date read failed")
                summary["prices"] = {"error": f"meta_read: {e}"}
                return 5

            # ─── F. Duplicate-build guard ──────────────────────────────
            most_recent = build_guard.most_recent_ingested_build_date(supabase) if supabase else None
            if build_guard.should_skip(target_date, most_recent):
                log.info("build_guard: target_date %s <= most_recent %s; skipping ingest",
                         target_date, most_recent)
                summary["prices"] = {
                    "target_date":            target_date,
                    "most_recent_ingested":   most_recent,
                    "action":                 "skipped_duplicate_build",
                }
                outcome = None
            else:
                # Not-a-duplicate path: continue to the expensive steps below.
                summary["prices"] = {"target_date": target_date, "action": "pending_ingest"}
                outcome = None   # will be set to `ing` below

        # ─── G. Scryfall invocation (only if changed AND not duplicate) ─
        # This is the reorder from the 2026-09-15 hardening: the
        # ScryfallCatalogueIngestion downloads ~185 MB when it runs, so
        # we do NOT trigger it on a duplicate-build fallback unless
        # explicitly forced.
        if scryfall_freshness and scryfall_freshness.changed and not args.skip_scryfall:
            is_duplicate_build = (
                isinstance(summary.get("prices"), dict)
                and summary["prices"].get("action") == "skipped_duplicate_build"
            )
            if is_duplicate_build and not args.force_scryfall_invoke:
                log.info("scryfall bulk moved but today's price build already ingested; "
                         "deferring catalogue refresh to the next primary run")
                summary["scryfall"]["action"] = "changed_deferred"
                summary["scryfall"]["reason"] = (
                    "scryfall bulk moved after the last primary run; catalogue "
                    "refresh deferred to the next primary run to keep the "
                    "18:00 UTC fallback cheap"
                )
            else:
                try:
                    outcome_scry = scryfall_delta.invoke_ingester(
                        supabase, dry_run=args.dry_run, freshness=scryfall_freshness,
                    )
                    summary["scryfall"] = {
                        "action":            outcome_scry.action,
                        "remote_updated_at": outcome_scry.remote_updated_at,
                        "local_updated_at":  outcome_scry.local_updated_at,
                        "scryfall_run_id":   outcome_scry.scryfall_run_id,
                        "status":            outcome_scry.status,
                        "error":             outcome_scry.error,
                    }
                    if outcome_scry.action == "failed":
                        log.warning("scryfall delta failed: %s", outcome_scry.error)
                except Exception as e:
                    log.exception("scryfall invocation failed (non-fatal)")
                    summary["scryfall"] = {"error": str(e), "non_fatal": True}

        # ─── H. MTGJSON UUID identifier delta ─────────────────────────
        # Gated by the build guard: on duplicate-build fallback we skip
        # the ~50 MB AllIdentifiers download entirely.
        should_run_id, id_skip_reason = _should_run_identifier_delta(
            args.skip_identifiers,
            args.force_identifier_delta,
            summary.get("prices"),
        )
        if not should_run_id:
            summary["identifiers"] = {"skipped": True, "reason": id_skip_reason}
            log.info("identifier delta skipped: reason=%s", id_skip_reason)
        else:
            try:
                workspace = _REPO_ROOT / "mtgjson_data"
                stats = identifier_delta.run(
                    supabase, workspace, dry_run=args.dry_run,
                )
                summary["identifiers"] = {
                    "scanned":                    stats.identifiers_scanned,
                    "already_known":              stats.already_known,
                    "newly_inserted":             stats.newly_inserted,
                    "unknown_scryfall_printing":  stats.unknown_scryfall_printing,
                    "malformed_rows":             stats.malformed_rows,
                    "errors":                     stats.errors,
                    "mtgjson_meta":               stats.mtgjson_meta,
                }
            except Exception as e:
                log.exception("identifier delta failed (non-fatal)")
                summary["identifiers"] = {"error": str(e), "non_fatal": True}

        # ─── I. AllPricesToday ingest (only if new build) ──────────────
        if (isinstance(summary.get("prices"), dict)
                and summary["prices"].get("action") == "pending_ingest"
                and local_gz is not None and target_date is not None):
            try:
                recon = mi.load_reconciliation(
                    mi.default_reconciliation_path(_REPO_ROOT),
                    mi.load_finish_lookup(supabase),
                )
            except Exception as e:
                log.exception("reconciliation load failed")
                summary["prices"] = {"error": f"reconciliation: {e}"}
                return 5

            # Override the reconciliation's stale bootstrap meta with the
            # FRESH AllPricesToday meta so the build-guard on the next
            # run sees the actual ingested build_date, not the
            # bootstrap's Sep-13 marker.
            try:
                recon.mtgjson_meta = allprices_today.read_meta_full(local_gz) or recon.mtgjson_meta
            except Exception:
                log.warning("could not refresh recon.mtgjson_meta; build-guard may see stale date")

            ing = mi.AllPricesIngestion(
                supabase=supabase, allprices_path=local_gz,
                reconciliation=recon,
                dry_run=args.dry_run,
                import_type="scraper_nightly",
                upsert_batch_size=mi.DEFAULT_UPSERT_BATCH_SIZE,
                only_date=target_date,
                sha256_expected=allprices_expected, sha256_actual=allprices_actual,
            )
            ing.start()
            ing.run()
            status = "success" if ing.stats.errors == 0 else "partial"
            ing.finish(status=status)

            outcome = ing
            summary["prices"] = {
                "run_id":            ing.run_id,
                "status":            status,
                "target_date":       target_date,
                "obs_total":         ing.stats.obs_total,
                "obs_mapped":        ing.stats.obs_mapped,
                "inserted_new":      ing.stats.inserted_new,
                "conflict_skipped":  ing.stats.conflict_skipped,
                "obs_missing_finish": ing.stats.obs_missing_finish,
                "obs_unmapped_uuid": ing.stats.obs_unmapped_uuid,
                "obs_invalid_price": ing.stats.obs_invalid_price,
                "errors":            ing.stats.errors,
                "provider_counts":   dict(ing.stats.provider_counts),
            }

        # ─── I. Refresh current prices ─────────────────────────────────
        should_skip_cp, skip_reason = _should_skip_current_refresh(
            args.skip_current_refresh,
            args.initial_current_price_population,
            args.force_current_price_refresh,
            summary.get("prices"),
        )
        if should_skip_cp:
            summary["current_prices"] = {"skipped": True, "reason": skip_reason}
            log.info("current-price refresh skipped: reason=%s", skip_reason)
        else:
            try:
                if args.initial_current_price_population:
                    summary["current_prices"] = current_prices.initial_populate(
                        supabase, dry_run=args.dry_run,
                    )
                else:
                    summary["current_prices"] = current_prices.refresh(
                        supabase, dry_run=args.dry_run,
                    )
            except Exception as e:
                log.exception("current_prices refresh failed (non-fatal)")
                summary["current_prices"] = {"error": str(e), "non_fatal": True}

        # ─── J. Gap detection (cheap 14-day sweep) + optional weekly repair
        gap_window = args.gap_window_days if args.gap_window_days is not None else gap_repair.GAP_WINDOW_DAYS
        if args.skip_gap_check:
            summary["gap_check"] = {"skipped": True}
        else:
            try:
                assessments = gap_repair.assess_recent(supabase, window_days=gap_window)
                summary["gap_check"] = [
                    {"date": a.date, "row_count": a.row_count, "status": a.status}
                    for a in assessments
                ]
            except Exception as e:
                log.exception("gap check failed (non-fatal)")
                summary["gap_check"] = {"error": str(e), "non_fatal": True}

        if args.weekly_gap_repair:
            try:
                workspace = _REPO_ROOT / "mtgjson_data"
                repair = gap_repair.run_weekly_repair(
                    supabase, workspace,
                    window_days=gap_window,
                    dry_run=args.dry_run,
                )
                summary["gap_repair"] = repair.to_notes()
            except Exception as e:
                log.exception("weekly gap repair failed (non-fatal)")
                summary["gap_repair"] = {"error": str(e), "non_fatal": True}

        # ─── J. Validate ───────────────────────────────────────────────
        if outcome is not None and supabase is not None:
            try:
                recent_notes = _fetch_recent_mtgjson_notes(supabase, limit=10)
                medians = validation.compute_last_7_day_medians(recent_notes)
                v = validation.evaluate(
                    checksum_ok=True,
                    source_parsed_ok=True,
                    target_date=summary["prices"]["target_date"],
                    source_build_date=summary["prices"]["target_date"],
                    partition_ready=True,
                    lock_ok=True,
                    total_observations_inserted=outcome.stats.inserted_new,
                    historical_daily_medians=medians,
                    per_provider_inserted=dict(outcome.stats.provider_counts),
                    quarantine_total=(
                        outcome.stats.obs_missing_finish +
                        outcome.stats.obs_unmapped_uuid +
                        outcome.stats.obs_invalid_price
                    ),
                    write_errors=outcome.stats.errors,
                )
                summary["validation"] = {
                    "status":         v.status,
                    "hard_failures":  v.hard_failures,
                    "warnings":       v.warnings,
                }
                if v.hard_failures:
                    summary["completed_at"] = _iso_utc()
                    return 6
            except Exception as e:
                log.exception("validation step failed (non-fatal)")
                summary["validation"] = {"error": str(e), "non_fatal": True}

    finally:
        # ─── K. Release lock ──────────────────────────────────────────
        if handle is not None:
            try:
                lock.release(supabase, handle)
            except Exception as e:
                log.warning("lock release failed: %s", e)
        summary["completed_at"] = _iso_utc()
        print(json.dumps(summary, indent=2, default=str))

    return 0


def _fetch_recent_mtgjson_notes(supabase: Any, limit: int = 10) -> list[dict]:
    """Return notes from the last N SCRAPER_NIGHTLY mtgjson runs.

    Filters to ``source='scraper_nightly'`` so the daily validation
    medians are computed only from prior daily-shaped runs. Stage 1C
    bootstrap runs (source='backfill' / 'admin_manual') each represent
    89 days of data and would otherwise inflate the medians by ~89x.
    """
    code, body = supabase._req(
        "/rest/v1/market_import_runs"
        "?provider=eq.mtgjson"
        "&source=eq.scraper_nightly"
        "&status=in.(success,partial)"
        f"&order=started_at.desc&limit={limit}"
        "&select=notes"
    )
    if code >= 400:
        return []
    rows = json.loads(body) if body else []
    out: list[dict] = []
    for r in rows:
        n = r.get("notes")
        if isinstance(n, str):
            try:
                out.append(json.loads(n))
            except json.JSONDecodeError:
                continue
        elif isinstance(n, dict):
            out.append(n)
    return out


if __name__ == "__main__":
    sys.exit(main())
