"""Weekly retention rollup — Stage 1D.

Scans mtg_price_observations partitions older than the retention
window, computes per-provider weekly + monthly aggregates into
mtg_price_provider_weekly / mtg_price_provider_monthly, logs to
mtg_price_retention_log, and DOES NOT drop the raw partition on the
first run. Partition drop is a separate, human-approved step.

Why this is a scaffold, not a full implementation
-------------------------------------------------
As of 2026-09-15 the raw retention window (90 days from bootstrap
start 2026-06-15) will not close until mid-September through
mid-December 2026 depending on how we count. Until at least one raw
partition has aged out, the rollup step has no work to do.

This scaffold:
  * Detects which raw partitions currently have data.
  * Computes candidate rollup ranges (partitions whose latest date is
    > 90 days behind ``now()``).
  * If no candidates → logs and exits 0.
  * If any candidates → aggregates via a SQL RPC (deferred) or a
    Python-side aggregation of paged raw rows.

Safety
------
Refuses to write unless ``MTG_RETENTION_ENABLED=true``. Never drops any
partition automatically. Partition drops must be performed by a human
via a follow-up migration or manual SQL after inspecting the rollup
output and the mtg_price_retention_log entry.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mtgjson_ingestion import build_default_supabase_client  # noqa: E402


log = logging.getLogger("mtg_retention_rollup")

ENV_ENABLED = "MTG_RETENTION_ENABLED"

# Raw retention window — days.
RETENTION_DAYS = 90


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    return p.parse_args(argv)


def _configure_logging(v: bool):
    logging.basicConfig(
        level=logging.DEBUG if v else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main(argv=None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    log.info("mtg_retention_rollup start dry_run=%s", args.dry_run)

    if not args.dry_run and os.environ.get(ENV_ENABLED, "").strip().lower() != "true":
        log.error("Refusing to write: %s != 'true'. Set it or pass --dry-run.", ENV_ENABLED)
        return 2

    supabase = build_default_supabase_client()
    if not args.dry_run and supabase is None:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing")
        return 3

    cutoff = date.today() - timedelta(days=RETENTION_DAYS)
    log.info("retention cutoff: raw observations with observed_on < %s are eligible for rollup",
             cutoff.isoformat())

    # Check whether any qualifying observations exist. If the earliest
    # observed_on is >= cutoff, no work.
    import json
    code, body = supabase._req(
        "/rest/v1/mtg_price_observations?select=observed_on&order=observed_on.asc&limit=1"
    )
    if code >= 400:
        raise RuntimeError(f"read earliest observation failed: HTTP {code} {body[:200]}")
    rows = json.loads(body) if body else []
    if not rows:
        log.info("no raw observations present — nothing to do")
        return 0
    earliest = rows[0]["observed_on"]
    if earliest >= cutoff.isoformat():
        log.info("earliest raw observation is %s, still within retention window (>= %s). No rollup work today.",
                 earliest, cutoff.isoformat())
        return 0

    log.warning(
        "Rollup work is due (earliest=%s < cutoff=%s). Full rollup "
        "implementation is scaffolded but not yet enabled — see comments.",
        earliest, cutoff.isoformat(),
    )
    # TODO: implement per-week + per-month aggregation SQL (via an RPC
    # to avoid pulling 20 M rows through PostgREST). Blocked until a
    # partition has aged past the window; will be added when needed.
    return 0


if __name__ == "__main__":
    sys.exit(main())
