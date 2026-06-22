"""
recent_sales_ingestion.py — Block 4B-S-2A

Flag-gated wrapper that connects the standalone ``recent_sales_parser`` to
Supabase. This module is the only place in the scraper that reads the
``recent_sales_card_allow_list`` table or writes ``recent_sales`` /
``market_import_runs`` rows.

Two safety gates:

  RECENT_SALES_INGESTION_ENABLED  — must equal the exact string "true".
                                    Any other value (typo, empty, missing,
                                    "1", "True") disables ingestion.
  RECENT_SALES_DRY_RUN            — when "true", the allow-list is read and
                                    pages are parsed, but no row is written
                                    to recent_sales and no market_import_runs
                                    row is created.

The production scraper imports this module and invokes it from a flag-gated
hook in its main loop. When the flag is off, the only added overhead is the
import statement (~1 ms) and a single ``os.environ.get`` per scraper start.

Block 4A-S1 (parser) and 4B-W-2A (web migration, allow-list) are
prerequisites. The 100-card pilot has not started yet; this module makes
the pilot possible but does not initiate it.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

from recent_sales_parser import (
    RECENT_SALES_PARSER_VERSION,
    RecentSalesParseResult,
    SaleRow,
    parse_recent_sales,
)

log = logging.getLogger("pokeprices.recent_sales_ingestion")

# ────────────────────────────────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────────────────────────────────

INGESTION_ENABLED_ENV = "RECENT_SALES_INGESTION_ENABLED"
DRY_RUN_ENV = "RECENT_SALES_DRY_RUN"

ALLOW_LIST_TABLE = "recent_sales_card_allow_list"
RECENT_SALES_TABLE = "recent_sales"
MARKET_IMPORT_RUNS_TABLE = "market_import_runs"

DEFAULT_IMPORT_TYPE = "recent_sales_pilot"
DEFAULT_UPSERT_BATCH_SIZE = 200

# Fields produced by the parser that we never POST to recent_sales.
# Block 4B-W-1 schema confirmed: the recent_sales table requires
# ``anomaly_flags`` (NOT NULL jsonb) and accepts ``raw_metadata`` (nullable
# jsonb), so both stay in the payload. ``schema_version`` is a parser-side
# constant that has no column on the remote schema and would trigger
# PGRST204 — drop it.
_PARSER_INTERNAL_FIELDS = {"schema_version"}

# ── Block 4B-W-1 CHECK-constraint vocabulary alignment ──────────────────────
# The parser uses a richer vocabulary than the DB CHECK lists allow. We map
# at write time so the parser stays the single source of truth.

# Parser values → recent_sales.best_offer_status CHECK
#   CHECK (best_offer_status IS NULL OR best_offer_status IN ('none','accepted','unknown'))
_DB_BEST_OFFER_STATUS_MAP = {
    "not_best_offer": "none",
    "accepted":       "accepted",
    "unknown":        "unknown",
}

# Parser values → recent_sales.condition_bucket CHECK
#   CHECK (condition_bucket IS NULL OR condition_bucket IN
#          ('mint','near_mint','lightly_played','played','poor','unknown'))
# Information loss is acceptable: condition_text preserves the matched
# phrase verbatim, and the grade column carries the graded-vs-raw split.
_DB_CONDITION_BUCKET_MAP = {
    "near_mint":         "near_mint",
    "lightly_played":    "lightly_played",
    "moderately_played": "played",
    "heavily_played":    "played",
    "damaged":           "poor",
    "graded":            "unknown",
    "raw_unknown":       "unknown",
    "unknown":           "unknown",
}

# Schema-allowed final statuses for market_import_runs.status. Block 4B-W-1
# disallows 'completed'; success rows must use 'success'.
_DB_STATUSES = frozenset({"running", "success", "partial", "failed"})

# Internal logical labels → schema-allowed market_import_runs.source values.
# Pass-through entries permit a caller that already supplied a DB-valid
# value to use it unchanged. Unknown values cause _resolve_db_source() to
# raise so a future typo fails loud rather than at PostgREST time.
_IMPORT_TYPE_TO_DB_SOURCE = {
    "recent_sales_pilot":            "pilot",
    "recent_sales_scraper_nightly":  "scraper_nightly",
    "recent_sales_admin_manual":     "admin_manual",
    "recent_sales_backfill":         "backfill",
    "pilot":                         "pilot",
    "scraper_nightly":               "scraper_nightly",
    "admin_manual":                  "admin_manual",
    "backfill":                      "backfill",
}


def _resolve_db_source(import_type: str) -> str:
    """Map an internal import_type label to a Block 4B-W-1 source value.

    Raises ValueError for unknown labels so the mismatch surfaces at run
    start (where it can be fixed) rather than at PostgREST insert time.
    """
    if import_type not in _IMPORT_TYPE_TO_DB_SOURCE:
        raise ValueError(
            f"unknown import_type={import_type!r}; expected one of "
            f"{sorted(_IMPORT_TYPE_TO_DB_SOURCE)}"
        )
    return _IMPORT_TYPE_TO_DB_SOURCE[import_type]

# Strong-key path the spec mandates for the recent_sales upsert.
UPSERT_CONFLICT_COLUMN = "provider_sale_key"


# ────────────────────────────────────────────────────────────────────────────
# Flag helpers
# ────────────────────────────────────────────────────────────────────────────

def is_ingestion_enabled(environ: Any = None) -> bool:
    """
    Strict, fail-closed check on RECENT_SALES_INGESTION_ENABLED.

    Returns True only when the variable is the exact string ``"true"``.
    Empty / missing / "True" / "1" / "yes" all return False.
    """
    env = environ if environ is not None else os.environ
    return env.get(INGESTION_ENABLED_ENV, "") == "true"


def is_dry_run(environ: Any = None) -> bool:
    """Strict check on RECENT_SALES_DRY_RUN (same semantics)."""
    env = environ if environ is not None else os.environ
    return env.get(DRY_RUN_ENV, "") == "true"


# ────────────────────────────────────────────────────────────────────────────
# Supabase REST client (thin, injectable for tests)
# ────────────────────────────────────────────────────────────────────────────

class SupabaseClient:
    """
    Tiny PostgREST wrapper for the three tables this block touches.

    Designed to be replaced by ``FakeSupabaseClient`` in unit tests — every
    network call goes through one of these methods, so tests never need to
    monkey-patch ``requests``.
    """

    def __init__(self, url: str, key: str, *, session: requests.Session | None = None,
                 timeout: float = 30.0):
        if not url or not key:
            raise ValueError("SupabaseClient requires non-empty url and key")
        self.url = url.rstrip("/")
        self.key = key
        self.session = session or requests.Session()
        self.timeout = timeout
        self._headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }

    # — allow-list ————————————————————————————————————————————————————————
    def get_allow_list(self, *, provider: str = "pricecharting") -> set[str]:
        """Return the set of provider_card_id strings enabled for ingestion."""
        url = (
            f"{self.url}/rest/v1/{ALLOW_LIST_TABLE}"
            f"?select=provider_card_id"
            f"&provider=eq.{provider}"
            f"&enabled=eq.true"
        )
        resp = self.session.get(url, headers=self._headers, timeout=self.timeout)
        if resp.status_code != 200:
            raise RuntimeError(
                f"allow-list fetch failed: HTTP {resp.status_code} {resp.text[:200]}"
            )
        rows = resp.json() if resp.text else []
        out: set[str] = set()
        for row in rows:
            v = row.get("provider_card_id")
            if v is None:
                continue
            out.add(str(v))
        return out

    # — recent_sales ——————————————————————————————————————————————————————
    def upsert_recent_sales(self, rows: list[dict]) -> int:
        """Upsert on provider_sale_key. Returns count of rows submitted."""
        if not rows:
            return 0
        url = (
            f"{self.url}/rest/v1/{RECENT_SALES_TABLE}"
            f"?on_conflict={UPSERT_CONFLICT_COLUMN}"
        )
        headers = {
            **self._headers,
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
        resp = self.session.post(url, json=rows, headers=headers, timeout=self.timeout)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(
                f"recent_sales upsert failed: HTTP {resp.status_code} {resp.text[:300]}"
            )
        return len(rows)

    # — market_import_runs ————————————————————————————————————————————————
    def insert_market_run(self, payload: dict) -> str | None:
        url = f"{self.url}/rest/v1/{MARKET_IMPORT_RUNS_TABLE}"
        headers = {**self._headers, "Prefer": "return=representation"}
        resp = self.session.post(url, json=[payload], headers=headers, timeout=self.timeout)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"market_import_runs insert failed: HTTP {resp.status_code} {resp.text[:300]}"
            )
        body = resp.json() if resp.text else []
        if isinstance(body, list) and body:
            row = body[0]
            for key in ("id", "run_id", "import_run_id", "uuid"):
                if key in row and row[key] is not None:
                    return str(row[key])
        return None

    def update_market_run(self, run_id: str, payload: dict) -> bool:
        if not run_id:
            return False
        url = f"{self.url}/rest/v1/{MARKET_IMPORT_RUNS_TABLE}?id=eq.{run_id}"
        headers = {**self._headers, "Prefer": "return=minimal"}
        resp = self.session.patch(url, json=payload, headers=headers, timeout=self.timeout)
        if resp.status_code not in (200, 204):
            raise RuntimeError(
                f"market_import_runs update failed: HTTP {resp.status_code} {resp.text[:300]}"
            )
        return True


def build_default_supabase_client() -> SupabaseClient | None:
    """
    Build a SupabaseClient from env. Returns None when required env vars are
    missing, so callers can degrade gracefully rather than crash.
    """
    url = os.environ.get("SUPABASE_URL")
    key = (
        os.environ.get("SUPABASE_SERVICE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
    )
    if not url or not key:
        log.warning(
            "recent-sales ingestion: SUPABASE_URL or SUPABASE_KEY/SUPABASE_SERVICE_KEY "
            "missing; ingestion cannot run"
        )
        return None
    return SupabaseClient(url, key)


# ────────────────────────────────────────────────────────────────────────────
# Ingestion controller
# ────────────────────────────────────────────────────────────────────────────

class RecentSalesIngestion:
    """
    Per-run, stateful controller. Owns one ``market_import_runs`` row and a
    write buffer for ``recent_sales``.

    Lifecycle:
        ig = RecentSalesIngestion(client, allow_list, dry_run=False)
        ig.start()                       # creates market_import_runs row
        for each card the scraper visits:
            ig.maybe_ingest(html=..., provider_card_id=..., page_url=...)
        ig.finish(status="completed")    # flushes + updates market_import_runs

    All Supabase writes are skipped in dry-run mode.
    """

    def __init__(
        self,
        supabase: SupabaseClient | None,
        allow_list: Iterable[str],
        *,
        dry_run: bool = False,
        import_type: str = DEFAULT_IMPORT_TYPE,
        upsert_batch_size: int = DEFAULT_UPSERT_BATCH_SIZE,
    ):
        self.supabase = supabase
        self.allow_list: set[str] = {str(x) for x in allow_list}
        self.dry_run = bool(dry_run)
        self.import_type = import_type
        self.upsert_batch_size = max(1, upsert_batch_size)
        self.run_id: str | None = None

        # Stats — surfaced to logs + market_import_runs on finish().
        self.cards_seen = 0
        self.cards_allowlisted = 0
        self.cards_parsed = 0
        self.rows_ok = 0
        self.rows_quarantined = 0
        self.rows_rejected = 0
        self.rows_upserted = 0
        self.errors_count = 0

        self._buffer: list[dict] = []
        # Monotonic timestamp set in start(); used to populate
        # market_import_runs.duration_ms on finish().
        self._started_monotonic: float | None = None
        # Runner-supplied stats that should be preserved alongside the
        # built-in ingestion counters inside the schema's ``notes`` JSON
        # column. Use add_run_notes() to add keys; built-in keys win on
        # collision so external callers cannot silently overwrite them.
        self._extra_notes: dict[str, Any] = {}

    # — Lifecycle ————————————————————————————————————————————————————————

    def add_run_notes(self, **kwargs: Any) -> None:
        """Stash extra key/values that should appear inside the run's notes
        JSON on finish. Intended for runner-level counters such as fetch
        outcomes (fetched/skipped_429/etc.) that do not belong on built-in
        ``RecentSalesIngestion`` state. Built-in keys take precedence on
        collision in _finalize so external callers cannot overwrite the
        audit-critical fields (``import_type``, ``cards_*``, ``rows_*``,
        ``errors_count``).
        """
        self._extra_notes.update(kwargs)

    def start(self) -> str | None:
        """Open the import run. Returns the new run_id or None in dry-run."""
        self._started_monotonic = time.monotonic()
        if self.dry_run:
            log.info("recent-sales: DRY-RUN mode, skipping market_import_runs insert")
            return None
        if self.supabase is None:
            log.warning("recent-sales: no Supabase client available; ingestion disabled")
            return None
        # Schema-aligned per Block 4B-W-1: market_import_runs columns are
        # (provider, source, status, started_at, parser_version, …). The
        # spec-listed "import_type" column does NOT exist, and the source
        # CHECK accepts only ('scraper_nightly','admin_manual','backfill',
        # 'pilot'). We translate our richer internal import_type label via
        # _resolve_db_source() and preserve the original label inside the
        # notes JSON written on finish().
        try:
            db_source = _resolve_db_source(self.import_type)
        except ValueError as e:
            log.exception("recent-sales: cannot resolve DB source: %s", e)
            self.errors_count += 1
            self.run_id = None
            return None
        payload = {
            "provider": "pricecharting",
            "source": db_source,
            "status": "running",
            "started_at": _iso_now(),
            "parser_version": RECENT_SALES_PARSER_VERSION,
        }
        try:
            self.run_id = self.supabase.insert_market_run(payload)
        except Exception as e:
            log.exception("recent-sales: failed to create market_import_runs row: %s", e)
            self.errors_count += 1
            self.run_id = None
        if self.run_id:
            log.info(
                "recent-sales: import run started run_id=%s allow_list_size=%d import_type=%s",
                self.run_id, len(self.allow_list), self.import_type,
            )
        else:
            log.warning("recent-sales: market_import_runs row not assigned; will run without an FK")
        return self.run_id

    def maybe_ingest(
        self,
        *,
        html: str | None,
        provider_card_id: str,
        page_url: str,
        expected_card_number: str | None = None,
    ) -> RecentSalesParseResult | None:
        """
        Allow-list-gated parser invocation. Safe to call on every card —
        returns None and does nothing when the card is not allow-listed.
        """
        self.cards_seen += 1
        pcid = str(provider_card_id)
        if pcid not in self.allow_list:
            log.debug("recent-sales: skip non-allowlisted card_id=%s", pcid)
            return None
        if not html:
            log.info("recent-sales: card_id=%s has no html; nothing to parse", pcid)
            return None

        self.cards_allowlisted += 1
        try:
            result = parse_recent_sales(
                html,
                page_url=page_url,
                provider_card_id=pcid,
                internal_card_slug=pcid,  # per spec: bare numeric card slug
                import_run_id=self.run_id,
                expected_card_number=expected_card_number,
            )
        except Exception as e:
            self.errors_count += 1
            log.exception(
                "recent-sales: parser raised for card_id=%s url=%s: %s",
                pcid, page_url, e,
            )
            return None

        self.cards_parsed += 1

        ok_rows: list[SaleRow] = []
        q_count = 0
        r_count = 0
        for row in result.rows:
            if row.parse_status == "ok":
                ok_rows.append(row)
            elif row.parse_status == "quarantined":
                q_count += 1
            elif row.parse_status == "rejected":
                r_count += 1

        self.rows_ok += len(ok_rows)
        self.rows_quarantined += q_count
        self.rows_rejected += r_count

        log.info(
            "recent-sales: card_id=%s status=%s sections=%d total_rows=%d ok=%d q=%d rej=%d",
            pcid, result.parse_status, result.section_count, result.row_count,
            len(ok_rows), q_count, r_count,
        )

        if ok_rows and not self.dry_run and self.supabase is not None:
            self._buffer.extend(_sale_row_to_db_row(r) for r in ok_rows)
            if len(self._buffer) >= self.upsert_batch_size:
                self.flush()

        return result

    def flush(self) -> int:
        """Push the buffer to recent_sales. Returns the number of rows submitted."""
        if not self._buffer:
            return 0
        if self.dry_run or self.supabase is None:
            n = len(self._buffer)
            self._buffer.clear()
            log.info("recent-sales: dry-run flush would have written %d rows", n)
            return 0
        sent = 0
        # Chunk inside flush in case the buffer overshot
        while self._buffer:
            batch = self._buffer[: self.upsert_batch_size]
            self._buffer = self._buffer[self.upsert_batch_size :]
            try:
                n = self.supabase.upsert_recent_sales(batch)
                sent += n
                self.rows_upserted += n
            except Exception as e:
                self.errors_count += 1
                log.exception(
                    "recent-sales: upsert failed for %d rows: %s", len(batch), e
                )
        log.info("recent-sales: upserted %d rows", sent)
        return sent

    def finish(self, status: str = "success") -> None:
        """Flush remaining buffer + close out the market_import_runs row.

        ``status`` must be one of the Block 4B-W-1 CHECK-allowed values
        ('running','success','partial','failed'). Anything else is coerced
        to 'failed' with a warning rather than passed through to PostgREST
        (which would 23514).
        """
        if status not in _DB_STATUSES:
            log.warning(
                "recent-sales: unknown finish status=%r; coercing to 'failed'",
                status,
            )
            status = "failed"
        try:
            self.flush()
        finally:
            self._finalize(status)

    def _finalize(self, status: str) -> None:
        log.info(
            "recent-sales: ingestion summary status=%s cards_seen=%d "
            "cards_allowlisted=%d cards_parsed=%d rows_ok=%d "
            "rows_quarantined=%d rows_rejected=%d rows_upserted=%d errors=%d",
            status, self.cards_seen, self.cards_allowlisted, self.cards_parsed,
            self.rows_ok, self.rows_quarantined, self.rows_rejected,
            self.rows_upserted, self.errors_count,
        )
        if self.dry_run or self.supabase is None or not self.run_id:
            return
        duration_ms: int | None = None
        if self._started_monotonic is not None:
            duration_ms = int((time.monotonic() - self._started_monotonic) * 1000)
        # Schema-aligned per Block 4B-W-1:
        #   completed_at  (NOT finished_at)
        #   pages_processed (closest analogue to cards_seen)
        #   rows_duplicate is required on insert with default 0; we set it
        #     explicitly to 0 here because v1 does not yet split insert vs
        #     update via PostgREST.
        #   cards_allowlisted / cards_parsed / rows_upserted / errors_count
        #     have no column — recorded inside the schema's ``notes`` text
        #     column as a JSON blob for audit.
        payload = {
            "status": status,
            "completed_at": _iso_now(),
            "pages_processed": self.cards_seen,
            "rows_ok": self.rows_ok,
            "rows_quarantined": self.rows_quarantined,
            "rows_rejected": self.rows_rejected,
            "rows_duplicate": 0,
            "duration_ms": duration_ms,
            # Notes JSON: start from runner-supplied extras then overlay
            # the built-in audit keys so they always win on collision.
            "notes": json.dumps({
                **self._extra_notes,
                # Preserve the descriptive logical label for audit; the DB
                # ``source`` column carries only the schema-allowed token
                # (e.g. 'pilot'), not 'recent_sales_pilot'.
                "import_type": self.import_type,
                "cards_allowlisted": self.cards_allowlisted,
                "cards_parsed": self.cards_parsed,
                "rows_upserted": self.rows_upserted,
                "errors_count": self.errors_count,
            }, sort_keys=True),
        }
        try:
            self.supabase.update_market_run(self.run_id, payload)
        except Exception as e:
            log.exception(
                "recent-sales: failed to finalize market_import_runs row %s: %s",
                self.run_id, e,
            )

    # — context manager sugar ————————————————————————————————————————————

    def __enter__(self) -> "RecentSalesIngestion":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.finish(status="failed" if exc_type is not None else "success")


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sale_row_to_db_row(row: SaleRow) -> dict:
    """
    Convert a parser SaleRow into the dict we POST to recent_sales.

    Drops parser-internal fields the remote schema is not assumed to carry,
    then aligns enum-valued columns to the Block 4B-W-1 CHECK vocabularies.
    Unknown values become 'unknown' rather than failing the write — the
    parser is the source of truth and this layer never raises on a row.
    """
    d = row.to_dict()
    for f in _PARSER_INTERNAL_FIELDS:
        d.pop(f, None)

    # best_offer_status: parser may emit 'not_best_offer'; DB requires
    # one of 'none' / 'accepted' / 'unknown'.
    bo = d.get("best_offer_status")
    if bo is not None:
        d["best_offer_status"] = _DB_BEST_OFFER_STATUS_MAP.get(bo, "unknown")

    # condition_bucket: parser emits five values absent from the DB list.
    cb = d.get("condition_bucket")
    if cb is not None:
        d["condition_bucket"] = _DB_CONDITION_BUCKET_MAP.get(cb, "unknown")

    # raw_or_graded: nullable in schema; deterministically derivable from
    # observed_section. We populate when the parser left it blank so the
    # column is usable for analytics without breaking the CHECK.
    if not d.get("raw_or_graded"):
        sect = d.get("observed_section") or ""
        if sect == "completed-auctions-used":
            d["raw_or_graded"] = "raw"
        elif sect.startswith("completed-auctions-"):
            d["raw_or_graded"] = "graded"

    return d


_CARD_NUMBER_RE = re.compile(r"#(\d+)[A-Za-z]*\s*$")


def parse_expected_card_number(product_name: str | None) -> str | None:
    """
    Best-effort card-number extraction from a CSV ``product-name``.
    Returns the bare number string ("4", "85") or None. Trailing letter
    suffixes are dropped (`#54a` -> "54"); the parser's wrong-card-number
    check is leading-zero tolerant so this is fine.
    """
    if not product_name:
        return None
    m = _CARD_NUMBER_RE.search(product_name)
    if not m:
        return None
    return m.group(1)


# ────────────────────────────────────────────────────────────────────────────
# Scraper integration entry point
# ────────────────────────────────────────────────────────────────────────────

def init_for_scraper_run(*, import_type: str = DEFAULT_IMPORT_TYPE) -> RecentSalesIngestion | None:
    """
    Convenience initialiser used by pokeprices_scraper_v8 inside its main().

    Returns a fully-started ``RecentSalesIngestion`` controller when the
    feature flag is on AND a Supabase client could be built AND the allow-list
    could be read. Returns ``None`` in every failure mode — callers must treat
    that as "ingestion disabled" and proceed with the existing price scrape.
    """
    if not is_ingestion_enabled():
        log.info("recent-sales: ingestion disabled (RECENT_SALES_INGESTION_ENABLED != 'true')")
        return None

    dry_run = is_dry_run()
    supabase = build_default_supabase_client()
    if supabase is None:
        return None

    try:
        allow_list = supabase.get_allow_list()
    except Exception as e:
        log.exception("recent-sales: failed to load allow-list: %s", e)
        return None

    log.info(
        "recent-sales: allow-list loaded count=%d dry_run=%s", len(allow_list), dry_run,
    )
    if not allow_list:
        log.warning("recent-sales: allow-list is empty; no cards will be ingested")

    ig = RecentSalesIngestion(supabase, allow_list, dry_run=dry_run, import_type=import_type)
    ig.start()
    return ig
