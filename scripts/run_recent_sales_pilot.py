"""
scripts/run_recent_sales_pilot.py — Block 4B-S-2A

Manual runner for the recent-sales pilot. Fetches **only allow-listed**
PriceCharting card pages, parses them with the standalone parser, and
optionally writes the OK rows to ``recent_sales`` via the Supabase REST
client used by the nightly scraper.

This script is deliberately NOT scheduled. It exists so an engineer can
run a small, observable pilot end-to-end without invoking the nightly
40k-card scrape.

Safety guarantees
-----------------
* Default mode is dry-run + ingestion-flag-off → script refuses to write.
* The ingestion flag (``RECENT_SALES_INGESTION_ENABLED=true``) is checked
  in addition to ``--dry-run``. Both must be aligned to write.
* ``--limit`` bounds the number of HTTP fetches; the allow-list has 58
  entries today.
* No row is written when ``--dry-run`` is set. A market_import_runs row is
  NOT created in dry-run.

Rate-limit policy (HTTP 429)
----------------------------
PriceCharting throttles bursty clients. The runner uses linear-multiplier
backoff (``retry_backoff_seconds * attempt``) and gives up after
``--max-retries`` attempts, counting the card as ``skipped_429``. Non-429
non-200 statuses (e.g. 404, 5xx) are NOT retried — they count as
``skipped_http_error`` and the run continues.

Usage (PowerShell)
------------------
    # Dry run, allow-list only, summary only, no writes:
    $env:SUPABASE_URL="..."; $env:SUPABASE_SERVICE_KEY="..."
    python scripts/run_recent_sales_pilot.py --dry-run

    # Limit to first 5 cards in the allow-list, write to Supabase:
    $env:SUPABASE_URL="..."; $env:SUPABASE_SERVICE_KEY="..."
    $env:RECENT_SALES_INGESTION_ENABLED="true"
    python scripts/run_recent_sales_pilot.py --limit 5

    # Full 58-card pilot with gentler pacing:
    python scripts/run_recent_sales_pilot.py --delay-seconds 2.0 --max-retries 4
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Allow ``python scripts/run_recent_sales_pilot.py`` to find sibling modules
# without a package install.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pokeprices_scraper_v8 as v8  # noqa: E402
import recent_sales_ingestion as rsi  # noqa: E402

DEFAULT_DELAY_SECONDS = 1.5
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 10.0

# Fetch outcome tokens — kept module-level so tests can import them.
FETCH_OK = "ok"
FETCH_SKIPPED_NO_HTML = "skipped_no_html"
FETCH_SKIPPED_429 = "skipped_429"
FETCH_SKIPPED_HTTP_ERROR = "skipped_http_error"


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--limit", type=int, default=None,
                   help="cap on number of allow-listed cards to fetch")
    p.add_argument("--dry-run", action="store_true",
                   help="parse only; do not write recent_sales or market_import_runs")
    p.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS,
                   dest="delay_seconds",
                   help=f"seconds between successful HTTP fetches "
                        f"(default {DEFAULT_DELAY_SECONDS})")
    p.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                   dest="max_retries",
                   help=f"retries on HTTP 429 before skipping a card "
                        f"(default {DEFAULT_MAX_RETRIES})")
    p.add_argument("--retry-backoff-seconds", type=float,
                   default=DEFAULT_RETRY_BACKOFF_SECONDS,
                   dest="retry_backoff_seconds",
                   help=f"base backoff seconds for HTTP 429; effective wait "
                        f"is backoff * attempt (default {DEFAULT_RETRY_BACKOFF_SECONDS})")
    p.add_argument("--import-type", default="recent_sales_pilot",
                   dest="import_type",
                   help="logical label preserved in market_import_runs.notes JSON")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def _fetch_with_retry(
    url: str,
    *,
    max_retries: int,
    retry_backoff_seconds: float,
    session=None,
    log: logging.Logger | None = None,
    sleep=time.sleep,
) -> tuple[str | None, str]:
    """
    Fetch a single PriceCharting card page with bounded 429-retry policy.

    Returns ``(html, outcome)``:

      ``("...", "ok")``                    — 200, content returned
      ``(None, "skipped_429")``            — 429 after all retries exhausted
      ``(None, "skipped_http_error")``     — any other non-200 (no retry)
      ``(None, "skipped_no_html")``        — network/exception before HTTP

    The function never raises. Backoff between 429 retries is
    ``retry_backoff_seconds * attempt`` (linear in attempt number), and a
    single fetch is bounded to ``max_retries + 1`` total HTTP attempts.

    ``session`` defaults to ``pokeprices_scraper_v8.session`` (same
    User-Agent / Accept-* headers as the nightly scraper). ``sleep`` is
    injectable so tests can monkeypatch without delaying the suite.
    """
    log = log or logging.getLogger("recent_sales_pilot")
    sess = session if session is not None else v8.session
    attempts = max(1, max_retries + 1)
    last_status: int | None = None

    for attempt in range(1, attempts + 1):
        try:
            resp = sess.get(url, timeout=15)
        except Exception as e:
            log.warning("  fetch error attempt=%d: %s", attempt, e)
            return (None, FETCH_SKIPPED_NO_HTML)
        last_status = resp.status_code
        if resp.status_code == 200:
            return (resp.text, FETCH_OK)
        if resp.status_code == 429:
            if attempt > max_retries:
                log.warning("  HTTP 429 after %d retries — skipping card", max_retries)
                return (None, FETCH_SKIPPED_429)
            wait = retry_backoff_seconds * attempt
            log.warning(
                "  HTTP 429 (attempt %d/%d) — backing off %.1fs before retry",
                attempt, max_retries, wait,
            )
            sleep(wait)
            continue
        # Any other non-200 — log status and skip (no retry)
        log.warning("  HTTP %d — skipping card (no retry)", resp.status_code)
        return (None, FETCH_SKIPPED_HTTP_ERROR)

    # Defensive — only reached if attempts is 0 (max_retries < 0)
    log.warning("  fetch attempts exhausted; last status=%s", last_status)
    return (None, FETCH_SKIPPED_429 if last_status == 429 else FETCH_SKIPPED_HTTP_ERROR)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    log = logging.getLogger("recent_sales_pilot")

    dry_run_arg = bool(args.dry_run)
    dry_run = dry_run_arg or rsi.is_dry_run()

    # Refuse to write unless BOTH the env flag is "true" AND we are not in
    # dry-run. This is intentional belt-and-braces in the manual runner.
    will_write = (not dry_run) and rsi.is_ingestion_enabled()
    if not dry_run and not will_write:
        log.error(
            "Refusing to run a writing pilot: RECENT_SALES_INGESTION_ENABLED is "
            "not 'true'. Set it explicitly, or pass --dry-run."
        )
        return 2

    supabase = rsi.build_default_supabase_client()
    if supabase is None:
        log.error("SUPABASE_URL / SUPABASE_KEY (or SUPABASE_SERVICE_KEY) is missing")
        return 2

    try:
        allow_list = supabase.get_allow_list()
    except Exception as e:
        log.exception("failed to load allow-list: %s", e)
        return 3
    log.info("allow-list loaded: %d card_ids (provider=pricecharting, enabled=true)",
             len(allow_list))
    if not allow_list:
        log.warning("allow-list is empty — nothing to do")
        return 0

    # Reuse the scraper's existing CSV-driven catalogue so we map
    # provider_card_id -> URL via the same logic the nightly run uses.
    log.info("loading PriceCharting card catalogue from %s", v8.PC_CSV_FOLDER)
    all_cards = v8.load_cards_from_pc_csvs(v8.PC_CSV_FOLDER)
    allowed_cards = [c for c in all_cards if c["pc_id"] in allow_list]
    log.info("allow-list matches in CSVs: %d/%d", len(allowed_cards), len(allow_list))
    unmatched = sorted(allow_list - {c["pc_id"] for c in allowed_cards})
    if unmatched:
        log.warning("%d allow-list ids not found in pc_csvs (sample=%s)",
                    len(unmatched), unmatched[:5])

    if args.limit is not None:
        allowed_cards = allowed_cards[: args.limit]
        log.info("--limit %d applied; processing %d cards", args.limit, len(allowed_cards))

    if not allowed_cards:
        log.warning("no allow-listed cards to process; exiting")
        return 0

    ingestion = rsi.RecentSalesIngestion(
        supabase if not dry_run else None,
        allow_list,
        dry_run=dry_run,
        import_type=args.import_type,
    )
    ingestion.start()

    # Fetch-outcome counters live in the runner; the ingestion module owns
    # parse/upsert counters. Both flow into the final summary log AND into
    # market_import_runs.notes via ingestion.add_run_notes().
    fetched = 0
    skipped_no_html = 0
    skipped_429 = 0
    skipped_http_error = 0

    try:
        for i, card in enumerate(allowed_cards, start=1):
            pcid = card["pc_id"]
            url = card["url"]
            log.info("[%d/%d] fetching pcid=%s %s", i, len(allowed_cards), pcid, url)
            html, outcome = _fetch_with_retry(
                url,
                max_retries=args.max_retries,
                retry_backoff_seconds=args.retry_backoff_seconds,
                log=log,
            )
            if outcome == FETCH_OK:
                fetched += 1
            elif outcome == FETCH_SKIPPED_429:
                skipped_429 += 1
                continue
            elif outcome == FETCH_SKIPPED_HTTP_ERROR:
                skipped_http_error += 1
                continue
            else:  # FETCH_SKIPPED_NO_HTML
                skipped_no_html += 1
                continue

            expected = rsi.parse_expected_card_number(card.get("product_name"))
            ingestion.maybe_ingest(
                html=html,
                provider_card_id=pcid,
                page_url=url,
                expected_card_number=expected,
            )
            time.sleep(max(0.0, args.delay_seconds))
    except KeyboardInterrupt:
        log.warning("interrupted; finishing run with status=failed")
        ingestion.add_run_notes(
            fetched=fetched,
            skipped_no_html=skipped_no_html,
            skipped_429=skipped_429,
            skipped_http_error=skipped_http_error,
        )
        ingestion.finish(status="failed")
        return 130
    except Exception as e:
        log.exception("pilot crashed: %s", e)
        ingestion.add_run_notes(
            fetched=fetched,
            skipped_no_html=skipped_no_html,
            skipped_429=skipped_429,
            skipped_http_error=skipped_http_error,
        )
        ingestion.finish(status="failed")
        return 1

    ingestion.add_run_notes(
        fetched=fetched,
        skipped_no_html=skipped_no_html,
        skipped_429=skipped_429,
        skipped_http_error=skipped_http_error,
    )
    ingestion.finish(status="success")
    log.info(
        "pilot done. mode=%s allow_listed=%d fetched=%d parsed=%d ok=%d "
        "quarantined=%d rejected=%d upserted=%d errors=%d "
        "skipped_no_html=%d skipped_429=%d skipped_http_error=%d",
        "DRY-RUN" if dry_run else "WRITE",
        ingestion.cards_allowlisted, fetched, ingestion.cards_parsed,
        ingestion.rows_ok, ingestion.rows_quarantined, ingestion.rows_rejected,
        ingestion.rows_upserted, ingestion.errors_count,
        skipped_no_html, skipped_429, skipped_http_error,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
