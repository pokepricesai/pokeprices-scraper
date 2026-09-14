"""
scripts/run_scryfall_catalogue_import.py — Block MTG-1B.

Manual runner for the fresh Scryfall catalogue + game-data ingestion.
Populates the eight MTGPrices production tables from Scryfall's
canonical bulk feeds; does NOT touch pricing (Stage 1C owns that).

Safety belt
-----------
Writes only happen when BOTH:

  * ``--dry-run`` is NOT set
  * ``MTG_CATALOGUE_INGESTION_ENABLED=true`` in env

is satisfied.

Usage (PowerShell)
------------------
    # Dry run — parses + validates but writes nothing:
    $env:SUPABASE_URL="..."; $env:SUPABASE_SERVICE_KEY="..."
    python scripts/run_scryfall_catalogue_import.py --dry-run

    # Real ingestion — writes to production Supabase:
    $env:SUPABASE_URL="..."; $env:SUPABASE_SERVICE_KEY="..."
    $env:MTG_CATALOGUE_INGESTION_ENABLED="true"
    python scripts/run_scryfall_catalogue_import.py

    # Only ingest sets (smoke test):
    python scripts/run_scryfall_catalogue_import.py --sets-only

Return codes
------------
    0   success
    1   ingestion crashed mid-run (market_import_runs row marked failed)
    2   env-flag safety belt refused the run
    3   Supabase client could not be built (missing env)
    4   market_import_runs row could not be opened; zero catalogue writes
    130 KeyboardInterrupt
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import scryfall_ingestion as si  # noqa: E402


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true",
                   help="parse only; do not write to Supabase or open a market_import_runs row")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="DEBUG-level logging")
    p.add_argument("--import-type", default="admin_manual",
                   help="market_import_runs.source value ('admin_manual' or 'scraper_nightly'; default admin_manual)")
    p.add_argument("--batch-size", type=int, default=si.DEFAULT_UPSERT_BATCH_SIZE,
                   help=f"upsert batch size (default {si.DEFAULT_UPSERT_BATCH_SIZE})")
    # Phase toggles. All default True. Useful for smoke testing.
    p.add_argument("--sets-only", action="store_true",
                   help="only run the sets phase; skip oracle/printings/rulings")
    p.add_argument("--no-sets", action="store_true",
                   help="skip the sets phase (assumes it has already run)")
    p.add_argument("--no-oracle", action="store_true",
                   help="skip oracle-cards phase")
    p.add_argument("--no-legalities", action="store_true",
                   help="skip legalities phase")
    p.add_argument("--no-printings", action="store_true",
                   help="skip printings phase")
    p.add_argument("--no-finishes", action="store_true",
                   help="skip finishes phase (still requires printings to have run)")
    p.add_argument("--no-rulings", action="store_true",
                   help="skip rulings phase")
    return p.parse_args(argv)


def _phase_toggles(args: argparse.Namespace) -> dict[str, bool]:
    if args.sets_only:
        return dict(run_sets=True, run_oracle=False, run_legalities=False,
                    run_printings=False, run_finishes=False, run_rulings=False)
    return dict(
        run_sets       = not args.no_sets,
        run_oracle     = not args.no_oracle,
        run_legalities = not args.no_legalities,
        run_printings  = not args.no_printings,
        run_finishes   = not args.no_finishes,
        run_rulings    = not args.no_rulings,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    log = logging.getLogger("run_scryfall_catalogue_import")

    dry_run = bool(args.dry_run) or si.is_dry_run()
    will_write = (not dry_run) and si.is_ingestion_enabled()
    if not dry_run and not will_write:
        log.error(
            "Refusing to run a writing catalogue-import: "
            "MTG_CATALOGUE_INGESTION_ENABLED is not 'true'. "
            "Set it explicitly, or pass --dry-run."
        )
        return 2

    scryfall = si.ScryfallClient()

    supabase: si.SupabaseClient | None = None
    if not dry_run:
        supabase = si.build_default_supabase_client()
        if supabase is None:
            log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing")
            return 3
    else:
        # Dry-run: still build a client if creds are available so lookups
        # against the real DB (oracle map / set map / printing map) work.
        # If creds are missing we degrade to a no-op.
        supabase = si.build_default_supabase_client()

    phases = _phase_toggles(args)
    ingestion = si.ScryfallCatalogueIngestion(
        supabase, scryfall,
        dry_run=dry_run,
        import_type=args.import_type,
        upsert_batch_size=args.batch_size,
        **phases,
    )
    # Fail-closed: if market_import_runs cannot be opened in a real
    # (non-dry-run) import, we ABORT before any catalogue writes.
    try:
        ingestion.start()
    except si.MarketImportRunAbort as e:
        log.error(
            "aborting import before any catalogue write: %s", e,
        )
        return 4
    try:
        ingestion.run()
    except KeyboardInterrupt:
        log.warning("interrupted; marking run as failed")
        ingestion.finish(status="failed")
        return 130
    except Exception as e:
        log.exception("catalogue import crashed: %s", e)
        ingestion.finish(status="failed")
        return 1

    ingestion.finish(status="success" if ingestion.stats.errors == 0 else "partial")
    return 0


if __name__ == "__main__":
    sys.exit(main())
