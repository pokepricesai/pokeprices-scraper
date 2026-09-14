"""
scryfall_ingestion.py — Block MTG-1B / Scryfall catalogue + game data.

Fresh Scryfall catalogue and game-data ingestion for MTGPrices. Populates
the eight production tables created by the web-repo migration
``migrations/2026-09-14-mtg-target-schema-and-backfill.sql``:

    mtg_sets
      └── mtg_printings          (via set_id + scryfall_id upsert keys)
            └── mtg_printing_finishes
    mtg_oracle_cards             (from Scryfall's Oracle Cards bulk feed)
    mtg_oracle_legalities        (expanded from oracle-card legalities map)
    mtg_rulings                  (from Scryfall's Rulings bulk feed)

Does NOT populate ``mtg_external_identifiers`` (Stage 1C owns MTGJSON
reconciliation) or ``mtg_price_observations`` (Stage 1C owns pricing).

Sources
-------
Scryfall's bulk-data metadata endpoint
    https://api.scryfall.com/bulk-data
which returns objects with ``type``, ``download_uri``, ``updated_at``,
``content_type``, ``content_encoding``, ``compressed_size``, etc. The
concrete download URLs are DISCOVERED at run time — never hard-coded.
We consume the entries with type in::

    oracle_cards        # one row per oracle_id (game-rules identity)
    default_cards       # one row per Scryfall printing (English pref.)
    rulings             # every ruling row Scryfall knows about

Sets come from the paged ``/sets`` REST endpoint (small, ~1k rows).

Safety belt
-----------
Ingestion refuses to write unless BOTH conditions are aligned:

  * ``--dry-run`` NOT set
  * ``MTG_CATALOGUE_INGESTION_ENABLED`` env var == "true"

If either is not satisfied, the run parses + validates but writes
nothing. In dry-run mode no ``market_import_runs`` row is created either.

Idempotency
-----------
Every upsert uses ON CONFLICT / merge-duplicates:

    mtg_sets              on_conflict=code
    mtg_oracle_cards      on_conflict=oracle_id
    mtg_printings         on_conflict=scryfall_id
    mtg_printing_finishes on_conflict=printing_id,finish
    mtg_oracle_legalities on_conflict=oracle_card_id,format
    mtg_rulings           on_conflict=oracle_card_id,source,published_at,comment_hash

Re-running the identical Scryfall snapshot produces zero duplicates.

Non-destructive
---------------
A card / set that disappears from the Scryfall bulk feed is NOT deleted
from the DB. Retirement policy is a later Stage 1D decision.

Legacy tables
-------------
The four ``*_legacy_20260327`` archive tables and every Pokemon table
are UNTOUCHED by this module. All writes are namespaced under the new
production ``mtg_*`` tables.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

import requests


log = logging.getLogger("scryfall_ingestion")

PARSER_VERSION = "scryfall_catalogue@v1"

# ─── Constants ──────────────────────────────────────────────────────────────

SCRYFALL_BULK_METADATA_URL = "https://api.scryfall.com/bulk-data"
SCRYFALL_SETS_URL = "https://api.scryfall.com/sets"
SCRYFALL_USER_AGENT = (
    "MTGPricesCatalogueIngest/1.0 (+https://www.pokeprices.io; "
    "contact@pokeprices.io)"
)

DEFAULT_HTTP_TIMEOUT = 60.0
DEFAULT_HTTP_MAX_RETRIES = 3
DEFAULT_HTTP_RETRY_BACKOFF = 5.0

DEFAULT_UPSERT_BATCH_SIZE = 500

# Bulk-data types we consume.
BULK_TYPE_ORACLE = "oracle_cards"
BULK_TYPE_DEFAULT = "default_cards"
BULK_TYPE_RULINGS = "rulings"

# Table names.
T_SETS = "mtg_sets"
T_ORACLE_CARDS = "mtg_oracle_cards"
T_PRINTINGS = "mtg_printings"
T_FINISHES = "mtg_printing_finishes"
T_LEGALITIES = "mtg_oracle_legalities"
T_RULINGS = "mtg_rulings"
T_MARKET_IMPORT_RUNS = "market_import_runs"

# market_import_runs.source vocabulary. This module uses admin_manual for
# manual runs and scraper_nightly when the future scheduler invokes us.
DB_STATUSES = frozenset({"running", "success", "partial", "failed"})


class MarketImportRunAbort(RuntimeError):
    """Raised when a non-dry-run ingestion cannot open a
    ``market_import_runs`` row. The importer must NOT write any
    catalogue rows without a tracked run — this is the fail-closed
    guard added after the first Stage 1B live attempt exposed a
    partially-written state.
    """
    pass


# ─── Safety-belt helpers ────────────────────────────────────────────────────


def is_ingestion_enabled(environ: Any = None) -> bool:
    env = environ if environ is not None else os.environ
    return (env.get("MTG_CATALOGUE_INGESTION_ENABLED") or "").strip().lower() == "true"


def is_dry_run(environ: Any = None) -> bool:
    env = environ if environ is not None else os.environ
    return (env.get("MTG_CATALOGUE_DRY_RUN") or "").strip().lower() == "true"


def _iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class _PrependedRawIO(io.RawIOBase):
    """A tiny RawIOBase adapter that yields ``prefix`` bytes first, then
    the ``underlying`` byte stream. Used so we can peek a couple of
    bytes for gzip-magic detection and still pass a complete stream to
    ``gzip.GzipFile``. Not thread-safe; single-consumer only.
    """

    def __init__(self, prefix: bytes, underlying: Any) -> None:
        super().__init__()
        self._prefix = memoryview(bytes(prefix))
        self._underlying = underlying

    def readable(self) -> bool:
        return True

    def readinto(self, buf) -> int:
        n = len(buf)
        if n == 0:
            return 0
        if len(self._prefix) > 0:
            take = min(n, len(self._prefix))
            buf[:take] = self._prefix[:take]
            self._prefix = self._prefix[take:]
            return take
        # Delegate to the underlying stream. urllib3 HTTPResponse has
        # ``read(n)``; wrap it into a readinto contract.
        data = self._underlying.read(n)
        if not data:
            return 0
        buf[:len(data)] = data
        return len(data)


# ─── Scryfall HTTP client ───────────────────────────────────────────────────


class ScryfallClient:
    """HTTP wrapper for Scryfall's bulk-data and /sets endpoints.

    Never makes one call per card — bulk downloads only.
    """

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout: float = DEFAULT_HTTP_TIMEOUT,
        max_retries: int = DEFAULT_HTTP_MAX_RETRIES,
        retry_backoff: float = DEFAULT_HTTP_RETRY_BACKOFF,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": SCRYFALL_USER_AGENT,
            "Accept": "application/json;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
        })
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff

    def _get(self, url: str, *, stream: bool = False) -> requests.Response:
        attempts = max(1, self.max_retries + 1)
        for attempt in range(1, attempts + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout, stream=stream)
            except requests.RequestException as e:
                if attempt >= attempts:
                    raise
                wait = self.retry_backoff * attempt
                log.warning("scryfall GET %s failed: %s — backing off %.1fs", url, e, wait)
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 502, 503, 504):
                if attempt >= attempts:
                    raise RuntimeError(
                        f"scryfall GET {url} failed after {attempts} attempts: "
                        f"HTTP {resp.status_code}"
                    )
                wait = self.retry_backoff * attempt
                log.warning(
                    "scryfall GET %s → HTTP %d (attempt %d/%d) — backing off %.1fs",
                    url, resp.status_code, attempt, attempts, wait,
                )
                time.sleep(wait)
                continue
            raise RuntimeError(
                f"scryfall GET {url} → HTTP {resp.status_code} {resp.text[:200]}"
            )
        raise RuntimeError(f"scryfall GET {url} exhausted retries")

    def get_bulk_metadata(self) -> dict[str, dict]:
        """Return the bulk-data metadata objects keyed by ``type``."""
        resp = self._get(SCRYFALL_BULK_METADATA_URL)
        body = resp.json()
        items = body.get("data") or []
        out: dict[str, dict] = {}
        for item in items:
            t = item.get("type")
            if t:
                out[t] = item
        return out

    def iter_bulk_records(self, download_uri: str, content_encoding: str | None = None) -> Iterator[dict]:
        """Stream JSON records from a Scryfall bulk file.

        As of 2026-09 Scryfall serves bulk feeds as gzip-encoded JSONL
        (one JSON object per newline) — see ``jsonl_download_uri`` and
        ``content_type: application/gzip`` in the bulk-data metadata.
        This function is defensive about that transition: it detects
        gzip on the payload via the magic bytes (``1f 8b``) and falls
        back gracefully to plain JSONL or a legacy JSON array wrapper.

        Streaming behaviour: we wrap the underlying socket in a
        BufferedReader → GzipFile chain and iterate lines, so the entire
        bulk file does NOT need to fit in memory. Only one line is
        buffered + parsed at a time.
        """
        resp = self._get(download_uri, stream=True)
        # Prevent requests / urllib3 from transparently decoding gzip
        # under us — we handle it ourselves based on payload sniffing.
        try:
            resp.raw.decode_content = False
        except Exception:
            pass

        # Peek first 2 bytes so we can spot the gzip magic before the
        # rest of the socket is consumed. The peek needs re-prepending
        # so downstream sees the whole file.
        peek = resp.raw.read(2)
        looks_gzip = peek[:2] == b'\x1f\x8b' or (
            content_encoding and content_encoding.lower() == "gzip"
        )

        composed = _PrependedRawIO(peek, resp.raw)
        stream = gzip.GzipFile(fileobj=composed) if looks_gzip else composed
        text_reader = io.TextIOWrapper(
            io.BufferedReader(stream, buffer_size=1 << 20),
            encoding="utf-8",
            errors="strict",
        )

        # Iterate. Tolerate:
        #   * legacy JSON-array wrappers where the whole file begins
        #     ``[`` and ends ``]`` with commas between rows,
        #   * blank lines,
        #   * trailing commas.
        for raw_line in text_reader:
            s = raw_line.strip()
            if not s:
                continue
            if s in ("[", "]"):
                continue
            if s.endswith(","):
                s = s[:-1]
            try:
                yield json.loads(s)
            except json.JSONDecodeError as e:
                log.warning(
                    "scryfall: skipping malformed JSONL line (%d chars): %s",
                    len(s), e,
                )

    def iter_sets(self) -> Iterator[dict]:
        """Iterate every set object from the paged /sets endpoint."""
        url = SCRYFALL_SETS_URL
        while url:
            resp = self._get(url)
            body = resp.json()
            for item in body.get("data") or []:
                yield item
            url = body.get("next_page")


# ─── Supabase PostgREST client ──────────────────────────────────────────────


class SupabaseClient:
    """Thin PostgREST wrapper for MTGPrices catalogue writes.

    Deliberately mirrors the shape of the one in
    ``recent_sales_ingestion.py`` so the two ingestion pipelines can
    coexist without duplicating an entire framework.
    """

    def __init__(
        self,
        url: str,
        key: str,
        *,
        session: requests.Session | None = None,
        timeout: float = 60.0,
    ) -> None:
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

    # — Generic upsert ————————————————————————————————————————————————————
    def upsert(
        self,
        table: str,
        rows: list[dict],
        *,
        on_conflict: str,
        return_representation: bool = False,
    ) -> int:
        """POST /rest/v1/<table>?on_conflict=<cols> with merge-duplicates.

        Returns the number of rows submitted (not necessarily the number
        actually written). PostgREST does not return an affected count
        when ``Prefer: return=minimal``.
        """
        if not rows:
            return 0
        url = f"{self.url}/rest/v1/{table}?on_conflict={on_conflict}"
        prefer = "resolution=merge-duplicates,return=" + (
            "representation" if return_representation else "minimal"
        )
        headers = {**self._headers, "Prefer": prefer}
        resp = self.session.post(url, json=rows, headers=headers, timeout=self.timeout)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(
                f"{table} upsert failed: HTTP {resp.status_code} {resp.text[:400]}"
            )
        return len(rows)

    # — Lookup helpers ————————————————————————————————————————————————————
    def lookup_id_map(
        self,
        table: str,
        *,
        natural_key: str,
        id_col: str = "id",
        page_size: int = 1000,
    ) -> dict[str, str]:
        """Return a dict mapping ``<natural_key>`` value → ``<id_col>`` value.

        Pages through the table 1,000 rows at a time (PostgREST's default
        upper bound). Order is stable via ``order=<natural_key>``.
        """
        if page_size <= 0:
            raise ValueError("page_size must be > 0")
        out: dict[str, str] = {}
        offset = 0
        while True:
            url = (
                f"{self.url}/rest/v1/{table}"
                f"?select={natural_key},{id_col}"
                f"&order={natural_key}"
                f"&limit={page_size}"
                f"&offset={offset}"
            )
            resp = self.session.get(url, headers=self._headers, timeout=self.timeout)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"{table} lookup_id_map failed: HTTP {resp.status_code} "
                    f"{resp.text[:200]}"
                )
            rows = resp.json() if resp.text else []
            if not isinstance(rows, list) or not rows:
                break
            for r in rows:
                nk = r.get(natural_key)
                ident = r.get(id_col)
                if nk is not None and ident is not None:
                    out[str(nk)] = str(ident)
            if len(rows) < page_size:
                break
            offset += page_size
        return out

    # — market_import_runs ————————————————————————————————————————————————
    def insert_market_run(self, payload: dict) -> str | None:
        url = f"{self.url}/rest/v1/{T_MARKET_IMPORT_RUNS}"
        headers = {**self._headers, "Prefer": "return=representation"}
        resp = self.session.post(url, json=[payload], headers=headers, timeout=self.timeout)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"market_import_runs insert failed: HTTP {resp.status_code} {resp.text[:400]}"
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
        url = f"{self.url}/rest/v1/{T_MARKET_IMPORT_RUNS}?id=eq.{run_id}"
        headers = {**self._headers, "Prefer": "return=minimal"}
        resp = self.session.patch(url, json=payload, headers=headers, timeout=self.timeout)
        if resp.status_code not in (200, 204):
            raise RuntimeError(
                f"market_import_runs update failed: HTTP {resp.status_code} {resp.text[:300]}"
            )
        return True

    # — Row count (validation) ————————————————————————————————————————————
    def count(self, table: str, *, filter_qs: str = "") -> int:
        suffix = f"&{filter_qs}" if filter_qs else ""
        url = f"{self.url}/rest/v1/{table}?select=*&limit=0{suffix}"
        headers = {**self._headers, "Prefer": "count=exact", "Range": "0-0"}
        resp = self.session.get(url, headers=headers, timeout=self.timeout)
        cr = resp.headers.get("content-range", "")
        return int(cr.split("/")[-1] or 0)


def build_default_supabase_client() -> SupabaseClient | None:
    url = os.environ.get("SUPABASE_URL")
    key = (
        os.environ.get("SUPABASE_SERVICE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
    )
    if not url or not key:
        log.warning(
            "SUPABASE_URL / SUPABASE_SERVICE_KEY missing — Supabase client unavailable",
        )
        return None
    return SupabaseClient(url, key)


# ─── Transform functions (pure) ─────────────────────────────────────────────

# Scryfall's set object → our mtg_sets row.
def transform_set(scryfall_set: dict) -> dict | None:
    code = (scryfall_set.get("code") or "").strip().lower()
    name = scryfall_set.get("name")
    if not code or not name:
        return None
    scryfall_id = scryfall_set.get("id")
    return {
        "scryfall_id":     scryfall_id,
        "code":            code,
        "name":            name,
        "set_type":        scryfall_set.get("set_type"),
        "released_at":     scryfall_set.get("released_at"),
        "card_count":      scryfall_set.get("card_count"),
        "digital":         scryfall_set.get("digital"),
        "foil_only":       scryfall_set.get("foil_only"),
        "nonfoil_only":    scryfall_set.get("nonfoil_only"),
        "icon_svg_uri":    scryfall_set.get("icon_svg_uri"),
        "scryfall_uri":    scryfall_set.get("scryfall_uri"),
        "parent_set_code": scryfall_set.get("parent_set_code"),
        "block":           scryfall_set.get("block"),
        "block_code":      scryfall_set.get("block_code"),
        # created_at / updated_at come from column defaults; if we want
        # explicit updated_at bumping we could pass it here.
    }


def _parse_numeric_cmc(cmc: Any) -> float | None:
    if cmc is None:
        return None
    try:
        return float(cmc)
    except (TypeError, ValueError):
        return None


# Scryfall's Oracle Card → our mtg_oracle_cards row.
def transform_oracle_card(card: dict) -> dict | None:
    oid = card.get("oracle_id")
    if not oid:
        return None
    name = card.get("name")
    if not name:
        return None
    return {
        "oracle_id":       oid,
        "name":            name,
        "mana_cost":       card.get("mana_cost"),
        "mana_value":      _parse_numeric_cmc(card.get("cmc")),
        "type_line":       card.get("type_line"),
        "oracle_text":     card.get("oracle_text"),
        "power":           card.get("power"),
        "toughness":       card.get("toughness"),
        "loyalty":         card.get("loyalty"),
        "defense":         card.get("defense"),
        "colors":          card.get("colors"),
        "color_identity":  card.get("color_identity"),
        "keywords":        card.get("keywords"),
        "produced_mana":   card.get("produced_mana"),
        "reserved":        card.get("reserved"),
        "layout":          card.get("layout"),
        "game_changer":    card.get("game_changer"),
        "card_faces":      card.get("card_faces"),
    }


# Scryfall's Default Cards printing → our mtg_printings row.
# NOTE: does NOT include oracle_card_id / set_id — those are resolved by
# the orchestrator against maps loaded from the DB after Oracle + Sets
# have been upserted.
def transform_printing_body(card: dict) -> dict | None:
    sid = card.get("id")
    oid = card.get("oracle_id")
    set_code = (card.get("set") or "").strip().lower()
    name = card.get("name")
    if not sid or not oid or not set_code or not name:
        return None
    return {
        "scryfall_id":       sid,
        "set_code":          set_code,
        "collector_number":  card.get("collector_number"),
        "lang":              card.get("lang"),
        "name":              name,
        "layout":            card.get("layout"),
        "rarity":            card.get("rarity"),
        "artist":            card.get("artist"),
        "illustration_id":   card.get("illustration_id"),
        "image_uri":         (card.get("image_uris") or {}).get("normal"),
        "image_uri_small":   (card.get("image_uris") or {}).get("small"),
        "art_crop_uri":      (card.get("image_uris") or {}).get("art_crop"),
        "promo":             card.get("promo"),
        "reprint":           card.get("reprint"),
        "variation":         card.get("variation"),
        "full_art":          card.get("full_art"),
        "borderless":        card.get("border_color") == "borderless",
        "textless":          card.get("textless"),
        "digital":           card.get("digital"),
        "released_at":       card.get("released_at"),
        "scryfall_uri":      card.get("scryfall_uri"),
    }


def _oracle_id_of(card: dict) -> str | None:
    return card.get("oracle_id")


# Scryfall's Default Cards printing → list of {printing_id, finish}. The
# printing_id must be provided by the caller — we do not embed it in the
# transform because Default Cards does not know our internal UUID.
def transform_finishes(card: dict) -> list[str]:
    finishes = card.get("finishes")
    if isinstance(finishes, list):
        return [str(f).strip() for f in finishes if f]
    # Legacy fallback: boolean flags. Modern Scryfall always emits the
    # ``finishes`` array so this path should not fire.
    out = []
    if card.get("nonfoil"): out.append("nonfoil")
    if card.get("foil"):    out.append("foil")
    return out


# Oracle-card → list of legality rows. Caller passes the oracle_card_id.
def transform_legalities(card: dict) -> list[tuple[str, str]]:
    """Return list of (format, legality) pairs from the card's legalities map."""
    m = card.get("legalities") or {}
    if not isinstance(m, dict):
        return []
    out = []
    for fmt, leg in m.items():
        if fmt and leg:
            out.append((str(fmt), str(leg)))
    return out


# Scryfall ruling object → our mtg_rulings row body.
# NOTE: does NOT include oracle_card_id — caller resolves via oracle_id.
def transform_ruling_body(rl: dict) -> dict | None:
    oid = rl.get("oracle_id")
    comment = rl.get("comment")
    source = rl.get("source")
    published_at = rl.get("published_at")
    if not oid or not comment or not source:
        return None
    return {
        "oracle_id":    oid,          # resolved to oracle_card_id by orchestrator
        "source":       source,
        "published_at": published_at,
        "comment":      comment,
    }


# ─── Orchestrator ───────────────────────────────────────────────────────────


@dataclass
class IngestionStats:
    sets_read:             int = 0
    sets_upserted:         int = 0
    oracle_read:           int = 0
    oracle_upserted:       int = 0
    printings_read:        int = 0
    printings_upserted:    int = 0
    printings_no_oracle:   int = 0
    printings_no_set:      int = 0
    finishes_upserted:     int = 0
    finishes_by_type:      dict[str, int] = field(default_factory=dict)
    legalities_upserted:   int = 0
    legalities_duplicate:  int = 0
    rulings_read:          int = 0
    rulings_upserted:      int = 0
    rulings_no_oracle:     int = 0
    rulings_duplicate:     int = 0
    errors:                int = 0


@dataclass
class BulkSourceMeta:
    oracle_updated_at:  str | None = None
    oracle_download:    str | None = None
    default_updated_at: str | None = None
    default_download:   str | None = None
    rulings_updated_at: str | None = None
    rulings_download:   str | None = None
    sets_fetched_at:    str | None = None


class ScryfallCatalogueIngestion:
    """One run of the Scryfall catalogue ingestion pipeline.

    Lifecycle::

        ing = ScryfallCatalogueIngestion(supabase, scryfall, dry_run=False)
        ing.start()
        ing.run()
        ing.finish(status="success")
    """

    def __init__(
        self,
        supabase: SupabaseClient | None,
        scryfall: ScryfallClient,
        *,
        dry_run: bool = False,
        import_type: str = "admin_manual",
        upsert_batch_size: int = DEFAULT_UPSERT_BATCH_SIZE,
        run_sets: bool = True,
        run_oracle: bool = True,
        run_legalities: bool = True,
        run_printings: bool = True,
        run_finishes: bool = True,
        run_rulings: bool = True,
    ) -> None:
        self.supabase = supabase
        self.scryfall = scryfall
        self.dry_run = dry_run
        self.import_type = import_type
        self.upsert_batch_size = upsert_batch_size
        self.stats = IngestionStats()
        self.source_meta = BulkSourceMeta()
        self.run_id: str | None = None
        self._started_monotonic: float | None = None
        # Which phases to run. Useful for smoke tests.
        self._phases = {
            "sets":       run_sets,
            "oracle":     run_oracle,
            "legalities": run_legalities,
            "printings":  run_printings,
            "finishes":   run_finishes,
            "rulings":    run_rulings,
        }
        # Populated after Oracle / Sets phases; used to resolve FKs.
        self._oracle_id_to_internal: dict[str, str] = {}
        self._set_code_to_internal: dict[str, str] = {}
        self._scryfall_id_to_printing_internal: dict[str, str] = {}

    # — Lifecycle ————————————————————————————————————————————————————————

    def start(self) -> str | None:
        """Open the import run.

        Fail-closed policy (fixed after the first live 1B attempt): in
        a NON-DRY-RUN real ingestion, if the ``market_import_runs`` row
        cannot be opened the import must abort BEFORE any production
        catalogue write. A missing Supabase client, a network error, a
        CHECK-constraint rejection or an unexpected non-201 all raise
        ``MarketImportRunAbort``. The runner catches this and exits
        non-zero without invoking ``run()``.

        Dry-run mode continues to skip market_import_runs entirely and
        return None — writes are already suppressed by
        ``_flush_batch``.
        """
        self._started_monotonic = time.monotonic()
        if self.dry_run:
            log.info("scryfall catalogue: DRY-RUN — skipping market_import_runs insert")
            return None
        if self.supabase is None:
            raise MarketImportRunAbort(
                "market_import_runs cannot be opened: no Supabase client available. "
                "Refusing to write catalogue rows without a tracked run."
            )
        payload = {
            "provider":       "scryfall",
            "source":         self.import_type,
            "status":         "running",
            "started_at":     _iso_now(),
            "parser_version": PARSER_VERSION,
        }
        try:
            self.run_id = self.supabase.insert_market_run(payload)
        except Exception as e:
            # Log first so the underlying error text survives even if
            # the outer runner's traceback formatter loses it.
            log.exception("scryfall catalogue: failed to create market_import_runs row: %s", e)
            raise MarketImportRunAbort(
                f"market_import_runs insert failed: {e}. "
                "Refusing to write catalogue rows without a tracked run."
            ) from e
        if not self.run_id:
            raise MarketImportRunAbort(
                "market_import_runs insert returned no run_id. "
                "Refusing to write catalogue rows without a tracked run."
            )
        log.info("scryfall catalogue: market_import_runs run_id=%s", self.run_id)
        return self.run_id

    def finish(self, status: str = "success") -> None:
        if status not in DB_STATUSES:
            log.warning("scryfall catalogue: unknown status=%r → coercing to 'failed'", status)
            status = "failed"
        self._log_summary(status)
        if self.dry_run or self.supabase is None or not self.run_id:
            return
        duration_ms: int | None = None
        if self._started_monotonic is not None:
            duration_ms = int((time.monotonic() - self._started_monotonic) * 1000)
        # Fit the stats into market_import_runs' actual columns; extras
        # go into the ``notes`` jsonb.
        payload = {
            "status":           status,
            "completed_at":     _iso_now(),
            # pages_processed: rough count of records read across all
            # phases (Scryfall doesn't paginate our bulk downloads; this
            # is the informative closest analogue).
            "pages_processed":  (
                self.stats.sets_read + self.stats.oracle_read
                + self.stats.printings_read + self.stats.rulings_read
            ),
            "rows_ok":          (
                self.stats.sets_upserted + self.stats.oracle_upserted
                + self.stats.printings_upserted + self.stats.finishes_upserted
                + self.stats.legalities_upserted + self.stats.rulings_upserted
            ),
            "rows_quarantined": (
                self.stats.printings_no_oracle + self.stats.printings_no_set
                + self.stats.rulings_no_oracle
            ),
            "rows_rejected":    0,
            "rows_duplicate":   0,
            "duration_ms":      duration_ms,
            "layout_signature": "scryfall_v1",
            "notes": json.dumps({
                "import_type":            self.import_type,
                "parser_version":         PARSER_VERSION,
                "oracle_updated_at":      self.source_meta.oracle_updated_at,
                "oracle_download":        self.source_meta.oracle_download,
                "default_updated_at":     self.source_meta.default_updated_at,
                "default_download":       self.source_meta.default_download,
                "rulings_updated_at":     self.source_meta.rulings_updated_at,
                "rulings_download":       self.source_meta.rulings_download,
                "sets_fetched_at":        self.source_meta.sets_fetched_at,
                "sets_read":              self.stats.sets_read,
                "sets_upserted":          self.stats.sets_upserted,
                "oracle_read":            self.stats.oracle_read,
                "oracle_upserted":        self.stats.oracle_upserted,
                "printings_read":         self.stats.printings_read,
                "printings_upserted":     self.stats.printings_upserted,
                "printings_no_oracle":    self.stats.printings_no_oracle,
                "printings_no_set":       self.stats.printings_no_set,
                "finishes_upserted":      self.stats.finishes_upserted,
                "finishes_by_type":       self.stats.finishes_by_type,
                "legalities_upserted":    self.stats.legalities_upserted,
                "legalities_duplicate":   self.stats.legalities_duplicate,
                "rulings_read":           self.stats.rulings_read,
                "rulings_upserted":       self.stats.rulings_upserted,
                "rulings_no_oracle":      self.stats.rulings_no_oracle,
                "rulings_duplicate":      self.stats.rulings_duplicate,
                "errors":                 self.stats.errors,
            }, sort_keys=True),
        }
        try:
            self.supabase.update_market_run(self.run_id, payload)
        except Exception as e:
            log.exception("scryfall catalogue: finalize market_import_runs failed: %s", e)

    def _log_summary(self, status: str) -> None:
        s = self.stats
        log.info(
            "scryfall catalogue done. status=%s "
            "sets=%d/%d oracle=%d/%d printings=%d/%d "
            "printings_no_oracle=%d printings_no_set=%d "
            "finishes=%d legalities=%d legalities_duplicate=%d "
            "rulings=%d/%d rulings_no_oracle=%d rulings_duplicate=%d "
            "errors=%d",
            status,
            s.sets_upserted, s.sets_read,
            s.oracle_upserted, s.oracle_read,
            s.printings_upserted, s.printings_read,
            s.printings_no_oracle, s.printings_no_set,
            s.finishes_upserted, s.legalities_upserted, s.legalities_duplicate,
            s.rulings_upserted, s.rulings_read,
            s.rulings_no_oracle, s.rulings_duplicate,
            s.errors,
        )

    # — Bulk metadata discovery ———————————————————————————————————————————

    def _discover_sources(self) -> None:
        """Populate self.source_meta from Scryfall's bulk-data metadata.

        As of 2026-09 Scryfall's metadata objects expose the current
        download URL under ``jsonl_download_uri``. The legacy
        ``download_uri`` key returns None. We prefer the jsonl variant
        and fall back defensively.
        """
        meta = self.scryfall.get_bulk_metadata()
        for typ, meta_key in [
            (BULK_TYPE_ORACLE,  "oracle"),
            (BULK_TYPE_DEFAULT, "default"),
            (BULK_TYPE_RULINGS, "rulings"),
        ]:
            item = meta.get(typ)
            if item is None:
                log.warning("scryfall catalogue: bulk metadata missing type=%s", typ)
                continue
            uri = item.get("jsonl_download_uri") or item.get("download_uri")
            setattr(self.source_meta, f"{meta_key}_updated_at", item.get("updated_at"))
            setattr(self.source_meta, f"{meta_key}_download",   uri)
        self.source_meta.sets_fetched_at = _iso_now()

    # — Batching helper ————————————————————————————————————————————————————

    def _flush_batch(
        self,
        table: str,
        rows: list[dict],
        on_conflict: str,
    ) -> int:
        """Write a batch through SupabaseClient.upsert. Dry-run counts only."""
        if not rows:
            return 0
        if self.dry_run or self.supabase is None:
            return len(rows)
        try:
            return self.supabase.upsert(table, rows, on_conflict=on_conflict)
        except Exception as e:
            log.exception("scryfall catalogue: %s upsert batch failed: %s", table, e)
            self.stats.errors += 1
            return 0

    # — Phase 1: sets —————————————————————————————————————————————————————

    def _ingest_sets(self) -> None:
        log.info("scryfall catalogue: phase 1 sets — starting")
        batch: list[dict] = []
        for scryfall_set in self.scryfall.iter_sets():
            self.stats.sets_read += 1
            row = transform_set(scryfall_set)
            if row is None:
                self.stats.errors += 1
                continue
            batch.append(row)
            if len(batch) >= self.upsert_batch_size:
                sent = self._flush_batch(T_SETS, batch, on_conflict="code")
                self.stats.sets_upserted += sent
                batch = []
        if batch:
            sent = self._flush_batch(T_SETS, batch, on_conflict="code")
            self.stats.sets_upserted += sent
        log.info("scryfall catalogue: phase 1 sets — read=%d upserted=%d",
                 self.stats.sets_read, self.stats.sets_upserted)

    # — Phase 2: oracle cards + legalities ———————————————————————————————

    def _ingest_oracle_and_legalities(self) -> None:
        log.info("scryfall catalogue: phase 2 oracle — starting")
        oracle_uri = self.source_meta.oracle_download
        if not oracle_uri:
            log.error("scryfall catalogue: no oracle_cards download URI — skipping")
            return
        # Two batches: oracle rows (upsert on oracle_id), and legality
        # rows (upsert on (oracle_card_id, format)). Legalities require
        # the internal oracle_card_id which is only known AFTER the
        # oracle upsert commits and we can look up the internal UUIDs.
        # To keep this streaming we do it in two passes:
        #   Pass 1 — stream, upsert oracle rows, remember (oracle_id →
        #            legality map) in memory.
        #   Pass 2 — lookup_id_map(mtg_oracle_cards), then flush all
        #            legality rows in batches.

        # Pass 1
        oracle_batch: list[dict] = []
        legality_source: list[tuple[str, str, str]] = []  # (oracle_id, format, legality)
        oracle_meta = self.scryfall.get_bulk_metadata().get(BULK_TYPE_ORACLE, {})
        content_encoding = oracle_meta.get("content_encoding")
        for card in self.scryfall.iter_bulk_records(oracle_uri, content_encoding=content_encoding):
            self.stats.oracle_read += 1
            row = transform_oracle_card(card)
            if row is None:
                self.stats.errors += 1
                continue
            oracle_batch.append(row)
            if self._phases["legalities"]:
                oid = row["oracle_id"]
                for fmt, leg in transform_legalities(card):
                    legality_source.append((oid, fmt, leg))
            if len(oracle_batch) >= self.upsert_batch_size:
                sent = self._flush_batch(T_ORACLE_CARDS, oracle_batch, on_conflict="oracle_id")
                self.stats.oracle_upserted += sent
                oracle_batch = []
        if oracle_batch:
            sent = self._flush_batch(T_ORACLE_CARDS, oracle_batch, on_conflict="oracle_id")
            self.stats.oracle_upserted += sent
        log.info("scryfall catalogue: phase 2 oracle — read=%d upserted=%d",
                 self.stats.oracle_read, self.stats.oracle_upserted)

        if not self._phases["legalities"]:
            return

        # Pass 2 — resolve oracle_id → internal UUID + flush legalities.
        log.info("scryfall catalogue: phase 2 legalities — resolving %d source rows",
                 len(legality_source))
        oracle_map = self._load_oracle_map()
        legality_batch: list[dict] = []
        # Run-scoped dedup keyed on the DB unique constraint
        # (oracle_card_id, format). Postgres refuses to touch the same
        # conflict target twice within one INSERT ... ON CONFLICT, so we
        # must collapse duplicates before the batch reaches the DB. Scryfall
        # currently does not emit duplicates here, but we do this
        # defensively so a future feed change cannot break the run.
        seen_legalities: set[tuple[str, str]] = set()
        for oid, fmt, leg in legality_source:
            internal = oracle_map.get(oid)
            if not internal:
                self.stats.errors += 1
                continue
            key = (internal, fmt)
            if key in seen_legalities:
                self.stats.legalities_duplicate += 1
                continue
            seen_legalities.add(key)
            legality_batch.append({
                "oracle_card_id": internal,
                "format":         fmt,
                "legality":       leg,
            })
            if len(legality_batch) >= self.upsert_batch_size:
                sent = self._flush_batch(
                    T_LEGALITIES, legality_batch,
                    on_conflict="oracle_card_id,format",
                )
                self.stats.legalities_upserted += sent
                legality_batch = []
        if legality_batch:
            sent = self._flush_batch(
                T_LEGALITIES, legality_batch,
                on_conflict="oracle_card_id,format",
            )
            self.stats.legalities_upserted += sent
        log.info("scryfall catalogue: phase 2 legalities — upserted=%d duplicate=%d",
                 self.stats.legalities_upserted, self.stats.legalities_duplicate)

    # — Phase 3: printings ————————————————————————————————————————————————

    def _ingest_printings_and_finishes(self) -> None:
        log.info("scryfall catalogue: phase 3 printings — starting")
        default_uri = self.source_meta.default_download
        if not default_uri:
            log.error("scryfall catalogue: no default_cards download URI — skipping")
            return
        # Resolve maps.
        oracle_map = self._load_oracle_map()
        set_map = self._load_set_map()

        default_meta = self.scryfall.get_bulk_metadata().get(BULK_TYPE_DEFAULT, {})
        content_encoding = default_meta.get("content_encoding")
        printing_batch: list[dict] = []
        # Finishes are keyed by printing_id, which we only learn AFTER
        # the printing upsert. We buffer (scryfall_id, [finishes]) tuples
        # here and resolve to printing_id via a second lookup at the end.
        finishes_source: list[tuple[str, list[str]]] = []

        for card in self.scryfall.iter_bulk_records(default_uri, content_encoding=content_encoding):
            self.stats.printings_read += 1
            body = transform_printing_body(card)
            if body is None:
                self.stats.printings_no_oracle += 1
                continue
            set_code = body["set_code"]
            set_id = set_map.get(set_code)
            if not set_id:
                self.stats.printings_no_set += 1
                continue
            oid = _oracle_id_of(card)
            oc_id = oracle_map.get(oid) if oid else None
            if not oc_id:
                self.stats.printings_no_oracle += 1
                continue
            body["oracle_card_id"] = oc_id
            body["set_id"]         = set_id
            printing_batch.append(body)
            if self._phases["finishes"]:
                finishes_source.append((body["scryfall_id"], transform_finishes(card)))
            if len(printing_batch) >= self.upsert_batch_size:
                sent = self._flush_batch(T_PRINTINGS, printing_batch, on_conflict="scryfall_id")
                self.stats.printings_upserted += sent
                printing_batch = []
        if printing_batch:
            sent = self._flush_batch(T_PRINTINGS, printing_batch, on_conflict="scryfall_id")
            self.stats.printings_upserted += sent
        log.info(
            "scryfall catalogue: phase 3 printings — read=%d upserted=%d "
            "no_oracle=%d no_set=%d",
            self.stats.printings_read, self.stats.printings_upserted,
            self.stats.printings_no_oracle, self.stats.printings_no_set,
        )

        if not self._phases["finishes"]:
            return

        # Phase 3b — finishes.
        log.info("scryfall catalogue: phase 3 finishes — resolving %d printings",
                 len(finishes_source))
        printing_map = self._load_printing_map()
        finish_batch: list[dict] = []
        for scryfall_id, finishes in finishes_source:
            internal = printing_map.get(scryfall_id)
            if not internal:
                self.stats.errors += 1
                continue
            for f in finishes:
                self.stats.finishes_by_type[f] = self.stats.finishes_by_type.get(f, 0) + 1
                finish_batch.append({"printing_id": internal, "finish": f})
                if len(finish_batch) >= self.upsert_batch_size:
                    sent = self._flush_batch(
                        T_FINISHES, finish_batch,
                        on_conflict="printing_id,finish",
                    )
                    self.stats.finishes_upserted += sent
                    finish_batch = []
        if finish_batch:
            sent = self._flush_batch(
                T_FINISHES, finish_batch,
                on_conflict="printing_id,finish",
            )
            self.stats.finishes_upserted += sent
        log.info("scryfall catalogue: phase 3 finishes — upserted=%d",
                 self.stats.finishes_upserted)

    # — Phase 4: rulings ————————————————————————————————————————————————

    def _ingest_rulings(self) -> None:
        log.info("scryfall catalogue: phase 4 rulings — starting")
        rulings_uri = self.source_meta.rulings_download
        if not rulings_uri:
            log.error("scryfall catalogue: no rulings download URI — skipping")
            return
        oracle_map = self._load_oracle_map()
        rulings_meta = self.scryfall.get_bulk_metadata().get(BULK_TYPE_RULINGS, {})
        content_encoding = rulings_meta.get("content_encoding")
        ruling_batch: list[dict] = []
        # Run-scoped dedup keyed on the effective DB identity
        # (oracle_card_id, source, published_at, comment). The DB unique
        # constraint uses (oracle_card_id, source, published_at,
        # comment_hash) where comment_hash is a GENERATED column derived
        # from comment, so this key is equivalent. Postgres refuses to
        # touch the same conflict target twice inside one INSERT ... ON
        # CONFLICT — Scryfall's rulings feed contains repeats (often
        # across faces of DFCs) that would otherwise blow up a batch.
        seen_rulings: set[tuple[str, str, str | None, str]] = set()
        for rl in self.scryfall.iter_bulk_records(rulings_uri, content_encoding=content_encoding):
            self.stats.rulings_read += 1
            body = transform_ruling_body(rl)
            if body is None:
                self.stats.errors += 1
                continue
            oid = body.pop("oracle_id")
            internal = oracle_map.get(oid)
            if not internal:
                self.stats.rulings_no_oracle += 1
                continue
            body["oracle_card_id"] = internal
            key = (internal, body["source"], body.get("published_at"), body["comment"])
            if key in seen_rulings:
                self.stats.rulings_duplicate += 1
                continue
            seen_rulings.add(key)
            ruling_batch.append(body)
            if len(ruling_batch) >= self.upsert_batch_size:
                sent = self._flush_batch(
                    T_RULINGS, ruling_batch,
                    # comment_hash is a GENERATED column; the DB derives
                    # it from `comment`. The uniqueness constraint spans
                    # (oracle_card_id, source, published_at, comment_hash).
                    on_conflict="oracle_card_id,source,published_at,comment_hash",
                )
                self.stats.rulings_upserted += sent
                ruling_batch = []
        if ruling_batch:
            sent = self._flush_batch(
                T_RULINGS, ruling_batch,
                on_conflict="oracle_card_id,source,published_at,comment_hash",
            )
            self.stats.rulings_upserted += sent
        log.info(
            "scryfall catalogue: phase 4 rulings — read=%d upserted=%d no_oracle=%d duplicate=%d",
            self.stats.rulings_read, self.stats.rulings_upserted,
            self.stats.rulings_no_oracle, self.stats.rulings_duplicate,
        )

    # — Lookup-map helpers ———————————————————————————————————————————————

    def _load_oracle_map(self) -> dict[str, str]:
        if self._oracle_id_to_internal:
            return self._oracle_id_to_internal
        if self.supabase is None:
            return {}
        log.info("scryfall catalogue: loading mtg_oracle_cards lookup map")
        self._oracle_id_to_internal = self.supabase.lookup_id_map(
            T_ORACLE_CARDS, natural_key="oracle_id", id_col="id",
        )
        log.info("scryfall catalogue: oracle map size=%d", len(self._oracle_id_to_internal))
        return self._oracle_id_to_internal

    def _load_set_map(self) -> dict[str, str]:
        if self._set_code_to_internal:
            return self._set_code_to_internal
        if self.supabase is None:
            return {}
        log.info("scryfall catalogue: loading mtg_sets lookup map")
        self._set_code_to_internal = self.supabase.lookup_id_map(
            T_SETS, natural_key="code", id_col="id",
        )
        log.info("scryfall catalogue: set map size=%d", len(self._set_code_to_internal))
        return self._set_code_to_internal

    def _load_printing_map(self) -> dict[str, str]:
        if self._scryfall_id_to_printing_internal:
            return self._scryfall_id_to_printing_internal
        if self.supabase is None:
            return {}
        log.info("scryfall catalogue: loading mtg_printings lookup map (this is large)")
        self._scryfall_id_to_printing_internal = self.supabase.lookup_id_map(
            T_PRINTINGS, natural_key="scryfall_id", id_col="id",
        )
        log.info("scryfall catalogue: printing map size=%d",
                 len(self._scryfall_id_to_printing_internal))
        return self._scryfall_id_to_printing_internal

    # — Orchestrator entry-point ————————————————————————————————————————

    def run(self) -> None:
        self._discover_sources()
        log.info(
            "scryfall catalogue: sources discovered "
            "oracle_updated=%s default_updated=%s rulings_updated=%s",
            self.source_meta.oracle_updated_at,
            self.source_meta.default_updated_at,
            self.source_meta.rulings_updated_at,
        )
        if self._phases["sets"]:
            self._ingest_sets()
        if self._phases["oracle"] or self._phases["legalities"]:
            self._ingest_oracle_and_legalities()
        if self._phases["printings"] or self._phases["finishes"]:
            self._ingest_printings_and_finishes()
        if self._phases["rulings"]:
            self._ingest_rulings()

    # — context manager sugar ————————————————————————————————————————————

    def __enter__(self) -> "ScryfallCatalogueIngestion":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.finish(status="failed" if exc_type is not None else "success")
