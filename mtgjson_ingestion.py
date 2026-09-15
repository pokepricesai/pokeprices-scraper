"""MTGJSON AllPrices ingestion module — Stage 1C.

Streams MTGJSON's AllPrices.json.gz through ijson, resolves each MTGJSON
UUID to a mtg_printings row via mtg_external_identifiers, resolves the
finish to a mtg_printing_finishes row, and writes historical price
observations to mtg_price_observations under a market_import_runs
lineage row.

Design principles (inherited from Stage 1B scryfall_ingestion.py):
  * Fail-closed on market_import_runs: no catalogue write if the run
    row cannot be opened.
  * Idempotent by construction: relies on the plain composite UNIQUE
    on mtg_price_observations plus PostgREST
    ``Prefer: resolution=ignore-duplicates``. Reruns silently skip
    rows already stored on their historical identity.
  * Streams; never holds the whole flattened price set in memory.
  * Quarantines with counted reasons; never silently drops rows.
  * Historical immutability: existing observations are NEVER
    modified (ON CONFLICT DO NOTHING semantic).

Public surface:
  * class :class:`MTGJSONClient`
  * class :class:`SupabaseClient`
  * class :class:`AllPricesIngestion`
  * exception :class:`MarketImportRunAbort`
  * function :func:`transform_price_observation`
  * env helpers :func:`is_ingestion_enabled` / :func:`is_dry_run`
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import ijson

# ─── Constants ───────────────────────────────────────────────────────────────

PARSER_VERSION = "mtgjson_all_prices@v1"
INGESTION_SOURCE = "mtgjson_all_prices"

DEFAULT_UPSERT_BATCH_SIZE = 500
DEFAULT_PROGRESS_EVERY = 10000
DEFAULT_HTTP_TIMEOUT_SEC = 60.0

MTGJSON_META_URL             = "https://mtgjson.com/api/v5/Meta.json"
MTGJSON_ALLPRICES_URL        = "https://mtgjson.com/api/v5/AllPrices.json.gz"
MTGJSON_ALLPRICES_SHA256_URL = "https://mtgjson.com/api/v5/AllPrices.json.gz.sha256"

# Scryfall finish naming lives on mtg_printing_finishes.finish; MTGJSON
# emits its own finish keys under each provider block. Keep this map
# explicit so unknown values are quarantined rather than silently
# dropped or force-mapped.
FINISH_MAP: dict[str, str] = {
    "normal": "nonfoil",
    "foil":   "foil",
    "etched": "etched",
}

# Table names — matches Stage 1A schema.
T_PRICES = "mtg_price_observations"
T_RUNS   = "market_import_runs"

# Environment flags
ENV_ENABLED = "MTG_PRICING_INGESTION_ENABLED"
ENV_DRY_RUN = "MTG_PRICING_DRY_RUN"


log = logging.getLogger("mtgjson_ingestion")


# ─── Env / safety helpers ────────────────────────────────────────────────────

def is_ingestion_enabled(env: dict | None = None) -> bool:
    """Real writes only proceed when ``MTG_PRICING_INGESTION_ENABLED=true``."""
    env = env if env is not None else os.environ
    return (env.get(ENV_ENABLED, "").strip().lower() == "true")


def is_dry_run(env: dict | None = None) -> bool:
    env = env if env is not None else os.environ
    return (env.get(ENV_DRY_RUN, "").strip().lower() == "true")


# ─── HTTP helpers (stdlib only) ─────────────────────────────────────────────

def _urlopen(req: Request, timeout: float = DEFAULT_HTTP_TIMEOUT_SEC):
    return urlopen(req, timeout=timeout)


# ─── MTGJSON client ─────────────────────────────────────────────────────────

class MTGJSONClient:
    """Downloads + SHA-256-verifies MTGJSON bulk files.

    Downloads are single-file; not streamed from the network (we're going
    to stream from the on-disk gzip anyway). Files are cached under a
    workspace directory and re-verified.
    """

    def __init__(self, workspace: Path):
        self.workspace = workspace
        self.workspace.mkdir(parents=True, exist_ok=True)

    def download_meta(self) -> dict:
        with _urlopen(Request(MTGJSON_META_URL)) as r:
            data = r.read()
        (self.workspace / "Meta.json").write_bytes(data)
        return json.loads(data.decode("utf-8"))

    def download_allprices(self) -> tuple[Path, str, str]:
        """Return (local_path, expected_sha256, actual_sha256).

        The caller is responsible for asserting expected==actual before
        proceeding.
        """
        local_gz = self.workspace / "AllPrices.json.gz"
        local_sha_path = self.workspace / "AllPrices.json.gz.sha256"

        with _urlopen(Request(MTGJSON_ALLPRICES_SHA256_URL)) as r:
            expected = r.read().decode("utf-8").strip()
        local_sha_path.write_text(expected, encoding="utf-8")

        with _urlopen(Request(MTGJSON_ALLPRICES_URL), timeout=1800.0) as r:
            data = r.read()
        local_gz.write_bytes(data)

        actual = hashlib.sha256(data).hexdigest()
        return local_gz, expected, actual

    @staticmethod
    def verify_sha256(path: Path, expected: str) -> str:
        """Compute SHA-256 of an existing file and return the hex digest."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()


# ─── Supabase client (PostgREST) ────────────────────────────────────────────

class SupabaseClient:
    """Minimal PostgREST client. Only implements what Stage 1C needs."""

    def __init__(self, url: str, key: str, timeout: float = DEFAULT_HTTP_TIMEOUT_SEC):
        self.url = url.rstrip("/")
        self.key = key
        self.timeout = timeout

    def _req(self, path: str, *, method: str = "GET",
             body: Any = None, prefer: str | None = None,
             max_retries: int = 5) -> tuple[int, str]:
        """PostgREST request with bounded retry on transient failures.

        Network drops (WinError 10054, connection reset, DNS blip) and
        Supabase 5xx responses are retried with exponential backoff.
        HTTP 4xx responses are returned immediately (they represent
        deterministic errors that retries cannot fix).
        """
        import socket
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if prefer:
            headers["Prefer"] = prefer

        payload = None
        if body is not None:
            payload = json.dumps(body, default=self._json_default).encode("utf-8")

        attempt = 0
        while True:
            req = Request(f"{self.url}{path}", method=method, headers=headers)
            if payload is not None:
                req.data = payload
            try:
                with _urlopen(req, timeout=self.timeout) as r:
                    return r.getcode(), r.read().decode("utf-8")
            except HTTPError as e:
                # 5xx = transient server-side — worth retrying.
                # 4xx = deterministic — return immediately.
                if e.code >= 500 and attempt < max_retries:
                    delay = 2 ** attempt
                    log.warning("HTTP %d on %s; retrying in %ds (attempt %d/%d)",
                                e.code, path, delay, attempt + 1, max_retries)
                    time.sleep(delay)
                    attempt += 1
                    continue
                return e.code, e.read().decode("utf-8")
            except (OSError, socket.timeout, ConnectionError) as e:
                # WinError 10054, ConnectionResetError, ETIMEDOUT, DNS blips.
                if attempt < max_retries:
                    delay = 2 ** attempt
                    log.warning("network error on %s: %s; retrying in %ds (attempt %d/%d)",
                                path, e, delay, attempt + 1, max_retries)
                    time.sleep(delay)
                    attempt += 1
                    continue
                raise

    @staticmethod
    def _json_default(obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    def open_run(self, payload: dict) -> str | None:
        code, resp = self._req(
            f"/rest/v1/{T_RUNS}", method="POST",
            body=[payload], prefer="return=representation",
        )
        if code >= 400:
            raise RuntimeError(f"{T_RUNS} insert failed: HTTP {code} {resp[:400]}")
        rows = json.loads(resp) if resp else []
        return rows[0]["id"] if rows else None

    def patch_run(self, run_id: str, payload: dict) -> None:
        code, resp = self._req(
            f"/rest/v1/{T_RUNS}?id=eq.{run_id}", method="PATCH", body=payload,
        )
        if code >= 400:
            log.error("finalize %s failed: HTTP %s %s", T_RUNS, code, resp[:400])

    def upsert_prices(self, rows: list[dict]) -> tuple[int, int]:
        """POST /mtg_price_observations with resolution=ignore-duplicates.

        Returns (inserted_new, batch_size). Under the plain-column UNIQUE
        (printing_finish_id, observed_on, provider, market, currency,
         price_type, condition) PostgREST returns the freshly inserted
        rows only; duplicates are silently omitted from the response.
        """
        if not rows:
            return 0, 0
        on_conflict = "printing_finish_id,observed_on,provider,market,currency,price_type,condition"
        code, resp = self._req(
            f"/rest/v1/{T_PRICES}?on_conflict={on_conflict}",
            method="POST", body=rows,
            prefer="resolution=ignore-duplicates,return=representation",
        )
        if code >= 400:
            raise RuntimeError(f"{T_PRICES} upsert failed: HTTP {code} {resp[:400]}")
        try:
            inserted = json.loads(resp) if resp else []
        except json.JSONDecodeError:
            inserted = []
        return len(inserted), len(rows)


def build_default_supabase_client() -> SupabaseClient | None:
    url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = (os.environ.get("SUPABASE_SERVICE_KEY")
           or os.environ.get("SUPABASE_SERVICE_ROLE_KEY"))
    if not url or not key:
        return None
    return SupabaseClient(url, key)


# ─── Reconciliation loader ──────────────────────────────────────────────────

@dataclass
class ReconciliationIndex:
    """Ephemeral in-memory index built from mtgjson_data/reconciliation.json.

    - uuid_to_printing:  MTGJSON UUID → mtg_printings.id
    - printing_finishes: mtg_printings.id → {mtgjson_finish → mtg_printing_finishes.id}
    - meta_by_uuid:      MTGJSON UUID → subset of card metadata (for source_reference lookups)
    - mtgjson_meta:      passthrough of MTGJSON's Meta.json build info
    """
    uuid_to_printing: dict[str, str]
    printing_finishes: dict[str, dict[str, str]]
    meta_by_uuid: dict[str, dict]
    mtgjson_meta: dict


def load_reconciliation(path: Path, finish_lookup: dict[tuple[str, str], str]) -> ReconciliationIndex:
    """Load reconciliation.json and cross-link with the mtg_printing_finishes
    lookup map (printing_id, mtgjson_finish) -> finish_id.
    """
    recon = json.loads(path.read_text(encoding="utf-8"))
    uuids = recon["uuids"]
    uuid_to_printing = {u: v["printing_id"] for u, v in uuids.items()}
    meta_by_uuid = uuids
    # finish_lookup is (printing_id, scryfall_finish) -> finish_id. We
    # invert the FINISH_MAP to translate MTGJSON's terms to Scryfall's
    # at resolution time.
    printing_finishes: dict[str, dict[str, str]] = {}
    for (pid, scryfall_finish), fid in finish_lookup.items():
        printing_finishes.setdefault(pid, {})[scryfall_finish] = fid
    return ReconciliationIndex(
        uuid_to_printing=uuid_to_printing,
        printing_finishes=printing_finishes,
        meta_by_uuid=meta_by_uuid,
        mtgjson_meta=recon.get("mtgjson_meta", {}),
    )


def load_finish_lookup(supabase: SupabaseClient) -> dict[tuple[str, str], str]:
    """Read mtg_printing_finishes and return (printing_id, finish) -> id."""
    lookup: dict[tuple[str, str], str] = {}
    offset = 0
    page = 1000
    while True:
        code, resp = supabase._req(
            f"/rest/v1/mtg_printing_finishes?select=id,printing_id,finish"
            f"&limit={page}&offset={offset}"
        )
        if code >= 400:
            raise RuntimeError(f"finish lookup failed: HTTP {code} {resp[:400]}")
        rows = json.loads(resp) if resp else []
        for r in rows:
            lookup[(r["printing_id"], r["finish"])] = r["id"]
        offset += page
        if len(rows) < page:
            break
    return lookup


# ─── Transform ──────────────────────────────────────────────────────────────

def transform_price_observation(
    *,
    printing_finish_id: str,
    observed_on: str,
    provider: str,
    market: str | None,
    currency: str,
    price_type: str | None,
    price: Any,
    import_run_id: str | None,
    source_reference: str,
    ingestion_source: str = INGESTION_SOURCE,
    condition: str | None = None,
) -> dict:
    """Materialise one mtg_price_observations row.

    Applies the ' '-fill normaliser for the NOT NULL default columns so
    the plain UNIQUE constraint arbitrates correctly.
    """
    return {
        "printing_finish_id": printing_finish_id,
        "observed_on":        observed_on,
        "provider":           provider,
        "ingestion_source":   ingestion_source,
        "market":             market or "",
        "currency":           currency,
        "price_type":         price_type or "",
        "condition":          condition or "",
        "price":              float(price) if isinstance(price, Decimal) else float(price),
        "import_run_id":      import_run_id,
        "source_reference":   source_reference,
        "is_anomalous":       False,
    }


def _is_valid_price(price: Any) -> bool:
    if price is None:
        return False
    if not isinstance(price, (int, float, Decimal)):
        return False
    try:
        f = float(price)
    except (TypeError, ValueError):
        return False
    if f != f:  # NaN
        return False
    if f < 0:
        return False
    return True


# ─── Stats + orchestrator ──────────────────────────────────────────────────

@dataclass
class IngestionStats:
    uuids_read:            int = 0
    uuids_priced:          int = 0
    uuids_mapped:          int = 0
    uuids_unmapped:        int = 0
    obs_total:             int = 0
    obs_mapped:            int = 0
    obs_unmapped_uuid:     int = 0
    obs_missing_finish:    int = 0
    obs_unknown_finish:    int = 0
    obs_invalid_price:     int = 0
    obs_out_of_date_range: int = 0
    inserted_new:          int = 0
    conflict_skipped:      int = 0
    errors:                int = 0
    # Date-scoped source-observation count: number of observations in
    # AllPrices whose date matches only_date / only_month (or all when
    # neither filter is set). Includes observations under UNMAPPED
    # UUIDs — this is the correct signal for gap-repair
    # source-side confirmation.
    obs_source_for_date:   int = 0
    provider_counts:       dict[str, int] = field(default_factory=dict)
    market_counts:         dict[str, int] = field(default_factory=dict)
    finish_counts:         dict[str, int] = field(default_factory=dict)
    currency_counts:       dict[str, int] = field(default_factory=dict)
    price_type_counts:     dict[str, int] = field(default_factory=dict)
    quarantine_missing_finish_by_provider: dict[str, int] = field(default_factory=dict)
    quarantine_missing_finish_by_source_finish: dict[str, int] = field(default_factory=dict)
    date_min: str = "9999-12-31"
    date_max: str = "0000-00-00"

    def bump_counter(self, d: dict[str, int], key: str, n: int = 1):
        d[key] = d.get(key, 0) + n

    def note_date(self, date_str: str):
        if date_str < self.date_min: self.date_min = date_str
        if date_str > self.date_max: self.date_max = date_str


class MarketImportRunAbort(RuntimeError):
    """Raised by :meth:`AllPricesIngestion.start` when the run row cannot
    be opened. Callers MUST NOT proceed to catalogue writes on this
    exception — historical price ingest is fail-closed on the run
    lineage row.
    """


@dataclass
class AllPricesIngestion:
    supabase: SupabaseClient | None
    allprices_path: Path
    reconciliation: ReconciliationIndex
    dry_run: bool = False
    import_type: str = "admin_manual"
    upsert_batch_size: int = DEFAULT_UPSERT_BATCH_SIZE
    only_date: str | None = None
    only_month: str | None = None
    progress_every: int = DEFAULT_PROGRESS_EVERY
    sha256_expected: str | None = None
    sha256_actual: str | None = None
    stats: IngestionStats = field(default_factory=IngestionStats)
    run_id: str | None = None
    started_wall_time: float = 0.0

    def start(self) -> str | None:
        payload = {
            "provider":         "mtgjson",
            "source":           self.import_type,
            "status":           "running",
            "started_at":       time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "parser_version":   PARSER_VERSION,
            "layout_signature": "mtgjson_v5",
            "notes": json.dumps({
                "phase":              "prices",
                "mtgjson_meta":       self.reconciliation.mtgjson_meta,
                "sha256_expected":    self.sha256_expected,
                "sha256_actual":      self.sha256_actual,
                "only_date":          self.only_date,
                "only_month":         self.only_month,
                "upsert_batch_size":  self.upsert_batch_size,
            }, sort_keys=True),
        }
        if self.dry_run:
            log.info("dry-run: not opening market_import_runs row")
            return None
        if self.supabase is None:
            raise MarketImportRunAbort(
                "no Supabase client — cannot open market_import_runs in a real run"
            )
        try:
            self.run_id = self.supabase.open_run(payload)
        except Exception as e:
            raise MarketImportRunAbort(f"open market_import_runs failed: {e}") from e
        if not self.run_id:
            raise MarketImportRunAbort(
                "open market_import_runs returned no id — refusing to proceed"
            )
        log.info("mtgjson prices: market_import_runs run_id=%s", self.run_id)
        return self.run_id

    def finish(self, status: str) -> None:
        if self.dry_run or self.supabase is None or self.run_id is None:
            self._log_summary(status)
            return
        s = self.stats
        completed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        duration_ms = int((time.time() - self.started_wall_time) * 1000)
        notes = {
            "phase":              "prices",
            "parser_version":     PARSER_VERSION,
            "mtgjson_meta":       self.reconciliation.mtgjson_meta,
            "sha256_expected":    self.sha256_expected,
            "sha256_actual":      self.sha256_actual,
            "only_date":          self.only_date,
            "only_month":         self.only_month,
            "uuids_read":         s.uuids_read,
            "uuids_priced":       s.uuids_priced,
            "uuids_mapped":       s.uuids_mapped,
            "uuids_unmapped":     s.uuids_unmapped,
            "obs_total":          s.obs_total,
            "obs_mapped":         s.obs_mapped,
            "obs_unmapped_uuid":  s.obs_unmapped_uuid,
            "obs_missing_finish": s.obs_missing_finish,
            "obs_unknown_finish": s.obs_unknown_finish,
            "obs_invalid_price":  s.obs_invalid_price,
            "obs_out_of_date_range": s.obs_out_of_date_range,
            "obs_source_for_date": s.obs_source_for_date,
            "inserted_new":       s.inserted_new,
            "conflict_skipped":   s.conflict_skipped,
            "errors":             s.errors,
            "date_min":           s.date_min if s.date_min != "9999-12-31" else None,
            "date_max":           s.date_max if s.date_max != "0000-00-00" else None,
            "provider_counts":    s.provider_counts,
            "market_counts":      s.market_counts,
            "finish_counts":      s.finish_counts,
            "currency_counts":    s.currency_counts,
            "price_type_counts":  s.price_type_counts,
            "quarantine_missing_finish_by_provider":     s.quarantine_missing_finish_by_provider,
            "quarantine_missing_finish_by_source_finish": s.quarantine_missing_finish_by_source_finish,
        }
        payload = {
            "status":       status,
            "completed_at": completed_at,
            "duration_ms":  duration_ms,
            "rows_ok":      s.inserted_new,
            "rows_quarantined": s.obs_missing_finish + s.obs_unmapped_uuid + s.obs_invalid_price,
            "rows_duplicate":   s.conflict_skipped,
            "notes":        json.dumps(notes, sort_keys=True),
        }
        self.supabase.patch_run(self.run_id, payload)
        self._log_summary(status)

    def _log_summary(self, status: str) -> None:
        s = self.stats
        log.info(
            "mtgjson prices done. status=%s "
            "uuids=%d/%d mapped=%d unmapped=%d "
            "obs_total=%d mapped=%d inserted=%d skipped=%d "
            "missing_finish=%d unknown_finish=%d invalid_price=%d unmapped_uuid=%d errors=%d "
            "date_range=%s..%s",
            status,
            s.uuids_priced, s.uuids_read, s.uuids_mapped, s.uuids_unmapped,
            s.obs_total, s.obs_mapped, s.inserted_new, s.conflict_skipped,
            s.obs_missing_finish, s.obs_unknown_finish, s.obs_invalid_price,
            s.obs_unmapped_uuid, s.errors,
            s.date_min if s.date_min != "9999-12-31" else "-",
            s.date_max if s.date_max != "0000-00-00" else "-",
        )

    # ─── Streaming ──────────────────────────────────────────────────────

    def run(self) -> None:
        self.started_wall_time = time.time()
        recon = self.reconciliation

        # Per-batch dedup: batch is a dict keyed by target unique-key
        # tuple. Postgres refuses to touch the same conflict target twice
        # inside one INSERT ... ON CONFLICT statement, so batches must
        # contain unique target keys. Cross-batch collisions are handled
        # by DB-side `Prefer: resolution=ignore-duplicates` at row level.
        #
        # Structure: dict[target_key -> row]. The first row seen for a
        # given key wins for that batch; subsequent same-key rows are
        # counted as within-run duplicates and dropped locally. This
        # bounds our peak memory to O(batch_size) instead of O(run) and
        # keeps the full 63 M-row bootstrap within a small RSS.
        batch: dict[tuple[str, str, str, str, str, str, str], dict] = {}

        # Filter helpers
        def in_scope(date_str: str) -> bool:
            if self.only_date and date_str != self.only_date:
                return False
            if self.only_month and not date_str.startswith(self.only_month):
                return False
            return True

        with gzip.open(self.allprices_path, "rb") as raw:
            for uuid_key, price_data in ijson.kvitems(raw, "data"):
                self.stats.uuids_read += 1
                if self.stats.uuids_read % self.progress_every == 0:
                    elapsed = max(time.time() - self.started_wall_time, 0.001)
                    log.info(
                        "progress uuids=%d obs_mapped=%d inserted=%d skipped=%d "
                        "unmapped=%d missing_finish=%d errors=%d rate=%.0f rows/s",
                        self.stats.uuids_read, self.stats.obs_mapped,
                        self.stats.inserted_new, self.stats.conflict_skipped,
                        self.stats.uuids_unmapped, self.stats.obs_missing_finish,
                        self.stats.errors, self.stats.inserted_new / elapsed,
                    )

                if not isinstance(price_data, dict) or not price_data:
                    continue
                self.stats.uuids_priced += 1

                printing_id = recon.uuid_to_printing.get(uuid_key)
                if not printing_id:
                    self.stats.uuids_unmapped += 1
                    # count observations under this unmapped UUID (all quarantined)
                    for market, providers in price_data.items():
                        if not isinstance(providers, dict): continue
                        for _prov, kinds in providers.items():
                            if not isinstance(kinds, dict): continue
                            for kind in ("buylist", "retail"):
                                block = kinds.get(kind)
                                if not isinstance(block, dict): continue
                                for _finish, dated in block.items():
                                    if isinstance(dated, dict):
                                        self.stats.obs_total += len(dated)
                                        self.stats.obs_unmapped_uuid += len(dated)
                                        # Date-scoped source count. Even
                                        # though the UUID is unmapped, if
                                        # the source has a row for the
                                        # target date under it, that IS
                                        # evidence the source has data
                                        # for the date.
                                        if self.only_date is not None:
                                            if self.only_date in dated:
                                                self.stats.obs_source_for_date += 1
                                        elif self.only_month is not None:
                                            for d in dated:
                                                if isinstance(d, str) and d.startswith(self.only_month):
                                                    self.stats.obs_source_for_date += 1
                                        else:
                                            self.stats.obs_source_for_date += len(dated)
                    continue

                self.stats.uuids_mapped += 1
                finish_map = recon.printing_finishes.get(printing_id, {})

                for market, providers in price_data.items():
                    if not isinstance(providers, dict): continue
                    for provider, kinds in providers.items():
                        if not isinstance(kinds, dict): continue
                        currency = kinds.get("currency") or ""
                        for price_type in ("buylist", "retail"):
                            block = kinds.get(price_type)
                            if not isinstance(block, dict): continue
                            for mtgjson_finish, dated in block.items():
                                if not isinstance(dated, dict): continue
                                scryfall_finish = FINISH_MAP.get(mtgjson_finish)
                                if scryfall_finish is None:
                                    self.stats.obs_total += len(dated)
                                    self.stats.obs_unknown_finish += len(dated)
                                    continue
                                finish_id = finish_map.get(scryfall_finish)
                                for date_str, price in dated.items():
                                    self.stats.obs_total += 1
                                    if not in_scope(date_str):
                                        self.stats.obs_out_of_date_range += 1
                                        continue
                                    # In-scope observation exists in source
                                    # for this date — increment date-scoped
                                    # source counter BEFORE any downstream
                                    # rejection (missing finish, invalid
                                    # price). This is the authoritative
                                    # signal for gap-repair source-side
                                    # confirmation.
                                    self.stats.obs_source_for_date += 1
                                    if finish_id is None:
                                        self.stats.obs_missing_finish += 1
                                        self.stats.bump_counter(
                                            self.stats.quarantine_missing_finish_by_provider,
                                            provider,
                                        )
                                        self.stats.bump_counter(
                                            self.stats.quarantine_missing_finish_by_source_finish,
                                            mtgjson_finish,
                                        )
                                        continue
                                    if not _is_valid_price(price):
                                        self.stats.obs_invalid_price += 1
                                        continue

                                    # Per-batch dedup on the DB unique
                                    # key — Postgres cannot touch the
                                    # same conflict target twice in one
                                    # statement. Cross-batch duplicates
                                    # are handled by DB DO NOTHING.
                                    key = (
                                        finish_id, date_str, provider,
                                        market or "", currency,
                                        price_type or "", "",
                                    )
                                    if key in batch:
                                        self.stats.conflict_skipped += 1
                                        continue

                                    self.stats.obs_mapped += 1
                                    self.stats.note_date(date_str)
                                    self.stats.bump_counter(self.stats.provider_counts, provider)
                                    self.stats.bump_counter(self.stats.market_counts, market or "")
                                    self.stats.bump_counter(self.stats.finish_counts, scryfall_finish)
                                    self.stats.bump_counter(self.stats.currency_counts, currency)
                                    self.stats.bump_counter(self.stats.price_type_counts, price_type)

                                    batch[key] = transform_price_observation(
                                        printing_finish_id=finish_id,
                                        observed_on=date_str,
                                        provider=provider,
                                        market=market,
                                        currency=currency,
                                        price_type=price_type,
                                        price=price,
                                        import_run_id=self.run_id,
                                        source_reference=uuid_key,
                                    )
                                    if len(batch) >= self.upsert_batch_size:
                                        self._flush(list(batch.values()))
                                        batch = {}

        # Final flush
        if batch:
            self._flush(list(batch.values()))

    def _flush(self, rows: list[dict]) -> None:
        if self.dry_run or self.supabase is None:
            # count everything as would-have-been-inserted for reporting
            self.stats.inserted_new += len(rows)
            return
        try:
            inserted, batch_size = self.supabase.upsert_prices(rows)
        except Exception as e:
            self.stats.errors += len(rows)
            log.error("batch upsert failed (%d rows): %s", len(rows), e)
            return
        self.stats.inserted_new += inserted
        self.stats.conflict_skipped += (batch_size - inserted)


# ─── Convenience: reconciliation JSON path ──────────────────────────────────

def default_reconciliation_path(repo_root: Path | None = None) -> Path:
    if repo_root is None:
        repo_root = Path(__file__).resolve().parent
    return repo_root / "mtgjson_data" / "reconciliation.json"


def default_allprices_path(repo_root: Path | None = None) -> Path:
    if repo_root is None:
        repo_root = Path(__file__).resolve().parent
    return repo_root / "mtgjson_data" / "AllPrices.json.gz"
