"""
onboard_new_set.py
==================
Automates the five manual steps previously required to onboard a new
PriceCharting Pokémon set:

  1. Fetch the console listing from PriceCharting and parse every row
     into (id, console-name, product-name, loose-price).
  2. Write a CSV in the exact format of the existing files in pc_csvs/.
  3. Validate and classify the rows (singles vs sealed, missing #NN,
     card-number spread, bracket variants) and print a human report.
  4. Cross-check the generated CSV against the DB (read-only) for a
     collision with an existing set / tcg_set_code — only when the
     Supabase env vars are already set in the caller's shell.
  5. Run seed_set_cards.py in --dry-run mode and capture its output.

The --apply path additionally writes the CSV into pc_csvs/, runs the
seeder for real, and appends the console name as a new line to the
batch file. --apply is intentionally refused unless both Supabase env
vars are present AND the caller is on a CI runner (the workflow sets
GITHUB_ACTIONS=true).

Nothing in this script ever prints an environment value. Secrets are
read via os.environ.get and used only inside the seeder subprocess
(which inherits the parent's environment).

Design invariants (verified against the production code):

  * URL slugs use pokeprices_scraper_v8.build_url rules so the listing
    URL this script fetches round-trips to the same slug the nightly
    scraper will use on every row.
  * The PriceCharting request session (headers, retry/backoff, timeout)
    is reused verbatim via pokeprices_scraper_v8.fetch_card_page.
  * CSV writer uses csv.writer so load_cards_from_pc_csvs can read it
    back via csv.DictReader with no surprises.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import io
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Iterable

import requests


REPO_ROOT = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------
# Scraper / seeder imports — load by path so a crash in this file can
# never accidentally change what either of them sees.
# ---------------------------------------------------------------------

def _load_module(module_name: str, filename: str):
    path = os.path.join(REPO_ROOT, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_scraper = _load_module("pokeprices_scraper_v8", "pokeprices_scraper_v8.py")
_seeder = _load_module("seed_set_cards", "seed_set_cards.py")

from set_classifier import (  # noqa: E402  — imported after _load_module runs
    extract_card_number,
    has_bracket_variant,
    is_sealed_by_name,
)


# ---------------------------------------------------------------------
# PriceCharting listing URL / fetch / parse
# ---------------------------------------------------------------------

# Polite minimum between listing fetches (seconds). Higher than the
# scraper's REQUEST_DELAY because console pages are much larger than
# product pages and we only need a handful of them.
LISTING_REQUEST_DELAY = 1.5

# Hard cap on fetches per run so a buggy pagination loop cannot hammer
# PriceCharting.
MAX_LISTING_FETCHES = 40


_LISTING_ROW_RE = re.compile(
    r"<tr\s+id=\"product-(?P<pc_id>\d+)\"[^>]*>(?P<body>.*?)</tr>",
    re.IGNORECASE | re.DOTALL,
)
_LISTING_NAME_RE = re.compile(
    r"<td\s+class=\"title\"[^>]*>\s*<a[^>]*>(?P<name>[^<]+)</a>",
    re.IGNORECASE | re.DOTALL,
)
_LISTING_LOOSE_TD_RE = re.compile(
    r"<td\s+class=\"price\s+numeric\s+used_price\"[^>]*>(?P<inner>.*?)</td>",
    re.IGNORECASE | re.DOTALL,
)
_LISTING_JS_PRICE_RE = re.compile(
    r"<span\s+class=\"js-price\"[^>]*>(?P<price>.*?)</span>",
    re.IGNORECASE | re.DOTALL,
)
_LISTING_HEADING_RE = re.compile(
    r"<title>(?P<title>[^<]+)</title>",
    re.IGNORECASE,
)
_NEXT_PAGE_FORM_RE = re.compile(
    r'<form[^>]*class="[^"]*\bnext_page\b[^"]*"[^>]*>(?P<inner>.*?)</form>',
    re.IGNORECASE | re.DOTALL,
)
_HIDDEN_INPUT_RE = re.compile(
    r'<input[^>]*name="(?P<name>[^"]+)"[^>]*value="(?P<value>[^"]*)"',
    re.IGNORECASE,
)


def build_console_url(console_name: str) -> str:
    """Build the listing URL using the same console-slug rules as
    pokeprices_scraper_v8.build_url. Calls build_url with an empty
    product and strips the trailing slug so we stay on the exact slug
    the nightly scraper will use."""
    sentinel = _scraper.build_url(console_name, "x")
    # Everything up to /game/<console-slug>/ is what we want; swap the
    # "/game/" prefix for "/console/" and drop the trailing product.
    m = re.match(r"https://www\.pricecharting\.com/game/([^/]+)/", sentinel)
    if not m:
        raise RuntimeError(f"Unexpected URL shape from build_url: {sentinel!r}")
    return f"https://www.pricecharting.com/console/{m.group(1)}"


def _looks_like_right_console(html: str, console_name: str) -> bool:
    """Heuristic: PriceCharting console pages carry the console name in
    their <title>. A 404 or a wrong-slug redirect would not."""
    m = _LISTING_HEADING_RE.search(html)
    if not m:
        return False
    title = m.group("title").strip().lower()
    want = console_name.lower().strip()
    return want in title


def parse_listing_html(html: str) -> list[dict]:
    """Extract (pc_id, product_name, loose_price) tuples from a single
    PriceCharting listing page's HTML.

    Returns a list of dicts in document order with keys:
      - 'pc_id'          (str, digits only)
      - 'product_name'   (str, HTML-entity-decoded, whitespace-trimmed)
      - 'loose_price'    (str, e.g. "$47.70", or "" if missing)
    """
    rows: list[dict] = []
    for match in _LISTING_ROW_RE.finditer(html):
        body = match.group("body")
        name_m = _LISTING_NAME_RE.search(body)
        if not name_m:
            continue
        name = _decode_entities(name_m.group("name")).strip()
        if not name:
            continue
        price = ""
        td_m = _LISTING_LOOSE_TD_RE.search(body)
        if td_m:
            span_m = _LISTING_JS_PRICE_RE.search(td_m.group("inner"))
            if span_m:
                raw = span_m.group("price").strip()
                # PriceCharting uses "&nbsp;" / nothing for missing prices.
                if raw and raw not in ("-", "—", "N/A"):
                    price = _decode_entities(raw)
        rows.append({
            "pc_id": match.group("pc_id"),
            "product_name": name,
            "loose_price": price,
        })
    return rows


_HTML_ENTITIES = {
    "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&#39;": "'",
    "&apos;": "'", "&nbsp;": " ",
}


def _decode_entities(s: str) -> str:
    """Decode the small set of HTML entities PriceCharting emits.
    Avoids importing html just to keep the dependency surface identical
    to pokeprices_scraper_v8 (which only uses requests + bs4-less regex)."""
    for k, v in _HTML_ENTITIES.items():
        s = s.replace(k, v)
    # &#NNNN; numeric entities — rare but possible for é etc.
    s = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), s)
    return s


def parse_next_page_form(html: str) -> dict[str, str] | None:
    """Return the hidden-input field dict of PriceCharting's "more
    results" form, or None if the form is absent.

    The form sits directly below the games_table. Its presence signals
    that further rows exist; its absence signals the listing is complete.
    Fields observed on 2026-11-26 against pokemon-delta-reign:
      sort, when, release-date, cursor
    `cursor` is the number of rows the server has already served — the
    client POSTs it back verbatim to request the next chunk."""
    m = _NEXT_PAGE_FORM_RE.search(html)
    if not m:
        return None
    return {
        im.group("name"): im.group("value")
        for im in _HIDDEN_INPUT_RE.finditer(m.group("inner"))
    }


def _http_get(url: str) -> str | None:
    """Thin wrapper over pokeprices_scraper_v8.fetch_card_page so tests
    can substitute without reaching into the scraper module."""
    return _scraper.fetch_card_page(url, retries=2, backoff_seconds=1.5)


def _http_post(url: str, fields: dict[str, str]) -> str | None:
    """POST the next-page form with the same retry style as
    pokeprices_scraper_v8.fetch_card_page: retries on 429 / 5xx /
    timeout, never retries on 404, 10-second per-request timeout.

    Reuses _scraper.session so the User-Agent, Accept and
    Accept-Language headers match the nightly scraper byte-for-byte."""
    max_attempts = 3  # 1 initial + 2 retries, matches fetch_card_page
    for attempt in range(max_attempts):
        try:
            resp = _scraper.session.post(url, data=fields, timeout=10)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            return None
        except requests.exceptions.Timeout:
            pass
        except requests.exceptions.RequestException:
            pass
        if attempt < max_attempts - 1:
            time.sleep(1.5 * (attempt + 1))
    return None


def fetch_listing(console_name: str, verbose_log: list[str]) -> list[dict]:
    """Fetch every listing page for the given console and return the
    deduped, document-ordered list of row dicts.

    Mechanism (verified against pokemon-delta-reign on 2026-11-26):
      1. GET /console/<slug> for the first 150 rows.
      2. Below the games_table the server emits a POST form with
         class="next_page js-next-page" and hidden inputs {sort, when,
         release-date, cursor}. The cursor is the row count served so
         far.
      3. POSTing those fields to the same URL returns the next chunk —
         a full HTML page whose games_table holds only the new rows
         (and may itself carry a next_page form). The loop repeats
         until the response has no next_page form, adds no new ids,
         or we hit MAX_LISTING_FETCHES.

    `verbose_log` is mutated with one line per request + a count so the
    report can show exactly what was fetched. No DB side effects."""
    console_url = build_console_url(console_name)

    verbose_log.append(f"GET {console_url}")
    html = _http_get(console_url)
    if html is None:
        raise RuntimeError(
            f"PriceCharting listing fetch failed for {console_name!r}. "
            f"Tried: {console_url}. Verify the console name against "
            "https://www.pricecharting.com/category/pokemon-cards"
        )
    if not _looks_like_right_console(html, console_name):
        raise RuntimeError(
            f"Fetched page title does not match console name "
            f"{console_name!r}. URL: {console_url}. First 160 chars: "
            f"{html[:160]!r}"
        )

    all_rows: dict[str, dict] = {}
    for row in parse_listing_html(html):
        all_rows[row["pc_id"]] = row
    verbose_log.append(f"  -> {len(all_rows)} rows (total {len(all_rows)})")

    form = parse_next_page_form(html)
    fetches = 1
    while form is not None and fetches < MAX_LISTING_FETCHES:
        time.sleep(LISTING_REQUEST_DELAY)
        cursor = form.get("cursor", "?")
        verbose_log.append(f"POST {console_url}  cursor={cursor}")
        body = _http_post(console_url, form)
        fetches += 1
        if body is None:
            verbose_log.append("  -> no response (retries exhausted); stopping")
            break
        added = 0
        for row in parse_listing_html(body):
            if row["pc_id"] not in all_rows:
                all_rows[row["pc_id"]] = row
                added += 1
        verbose_log.append(f"  -> +{added} rows (total {len(all_rows)})")
        if added == 0:
            verbose_log.append("  -> no new ids; stopping")
            break
        form = parse_next_page_form(body)
        if form is None:
            verbose_log.append("  -> no next_page form; listing complete")

    if form is not None and fetches >= MAX_LISTING_FETCHES:
        verbose_log.append(
            f"  WARNING: hit MAX_LISTING_FETCHES={MAX_LISTING_FETCHES} with a "
            "next_page form still present; returning partial list."
        )

    return list(all_rows.values())


# ---------------------------------------------------------------------
# CSV writer — matches pc_csvs/ format exactly
# ---------------------------------------------------------------------

CSV_HEADER = ["id", "console-name", "product-name", "loose-price"]


def write_pc_csv(rows: Iterable[dict], console_name: str, path: str) -> int:
    """Write rows in the exact format of pc_csvs/*.csv. Returns row
    count. Opens the file in text mode with newline="" so csv.writer's
    LF line endings are preserved on every platform (the existing CSVs
    are LF-terminated — see pc_csvs/Pokemon 30th Celebration.csv)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    written = 0
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(CSV_HEADER)
        for r in rows:
            writer.writerow([
                r["pc_id"],
                console_name,
                r["product_name"],
                r["loose_price"],
            ])
            written += 1
    return written


# ---------------------------------------------------------------------
# Classifier / validator
# ---------------------------------------------------------------------

def classify_rows(rows: Iterable[dict]) -> dict:
    """Pure classifier (no I/O). Returns a dict with:
      - 'total':              int
      - 'duplicate_ids':      list[str]
      - 'singles':            list[row]   (has #NN)
      - 'sealed':             list[row]
      - 'unnumbered':         list[row]
      - 'bracket_variants':   list[row]
      - 'card_numbers':       list[int]   (parseable numerics only)
      - 'highest_number':     int | None
      - 'missing_numbers':    list[int]   (1..highest, minus seen)
      - 'distinct_numbers':   int         (len(unique parseable numerics))
    """
    rows = list(rows)
    seen_ids: dict[str, int] = {}
    for r in rows:
        seen_ids[r["pc_id"]] = seen_ids.get(r["pc_id"], 0) + 1

    duplicate_ids = sorted([pid for pid, n in seen_ids.items() if n > 1])

    singles: list[dict] = []
    sealed: list[dict] = []
    unnumbered: list[dict] = []
    bracket_variants: list[dict] = []
    card_numbers: list[int] = []
    distinct_set: set[int] = set()

    for r in rows:
        name = r["product_name"]
        number_token = extract_card_number(name)
        if number_token is not None:
            singles.append(r)
            m = re.match(r"^(\d+)", number_token)
            if m:
                n = int(m.group(1))
                card_numbers.append(n)
                distinct_set.add(n)
        elif is_sealed_by_name(name):
            sealed.append(r)
        else:
            unnumbered.append(r)
        if has_bracket_variant(name):
            bracket_variants.append(r)

    highest = max(card_numbers) if card_numbers else None
    missing: list[int] = []
    if highest is not None:
        missing = sorted(set(range(1, highest + 1)) - distinct_set)

    return {
        "total": len(rows),
        "duplicate_ids": duplicate_ids,
        "singles": singles,
        "sealed": sealed,
        "unnumbered": unnumbered,
        "bracket_variants": bracket_variants,
        "card_numbers": card_numbers,
        "highest_number": highest,
        "missing_numbers": missing,
        "distinct_numbers": len(distinct_set),
    }


# ---------------------------------------------------------------------
# Batch-file append — idempotent, newline-style preserving
# ---------------------------------------------------------------------

def append_console_to_batch(batch_path: str, console_name: str) -> str:
    """Append `console_name` as a new last line to `batch_path` iff the
    file does not already contain an identical line. Preserves the
    file's existing newline style.

    Returns one of:
      * "already-present"  — file already contained the console
      * "appended"         — a new line was written
      * "created"          — the batch file did not exist
    """
    console_line = console_name.strip()
    if not os.path.exists(batch_path):
        with open(batch_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(console_line + "\n")
        return "created"

    # Preserve the existing newline style (\n vs \r\n). csv files in
    # this repo are LF but batch6.txt could in principle be CRLF on a
    # Windows developer checkout.
    with open(batch_path, "rb") as f:
        raw = f.read()
    newline = b"\r\n" if b"\r\n" in raw else b"\n"
    text = raw.decode("utf-8")
    existing = [line.rstrip("\r\n") for line in text.splitlines()]
    if console_line in existing:
        return "already-present"

    # Ensure the file ends with its newline before we append the new line.
    suffix = b""
    if not raw.endswith(newline):
        suffix += newline
    suffix += console_line.encode("utf-8") + newline
    with open(batch_path, "ab") as f:
        f.write(suffix)
    return "appended"


# ---------------------------------------------------------------------
# Optional read-only DB cross-check
# ---------------------------------------------------------------------

def db_env_present() -> bool:
    return bool(os.environ.get("SUPABASE_URL")) and bool(
        os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY")
    )


def db_cross_check(set_name: str, tcg_set_code: str | None) -> dict:
    """Read-only GET counts for the two tables we can cheaply check.
    Returns {'cards_count': int, 'tcg_cards_count': int | None,
             'errors': list[str]}."""
    import requests
    from urllib.parse import quote

    url = os.environ["SUPABASE_URL"]
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_KEY"]
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Prefer": "count=exact",
        "Range": "0-0",
    }

    out: dict = {"cards_count": None, "tcg_cards_count": None, "errors": []}

    try:
        r = requests.get(
            f"{url}/rest/v1/cards?set_name=eq.{quote(set_name)}&select=card_slug",
            headers=headers, timeout=30,
        )
        if r.status_code in (200, 206):
            cr = r.headers.get("Content-Range", "0/0")
            out["cards_count"] = int(cr.rsplit("/", 1)[-1] or 0)
        else:
            out["errors"].append(f"cards count HTTP {r.status_code}: {r.text[:120]}")
    except Exception as e:
        out["errors"].append(f"cards count: {e}")

    if tcg_set_code:
        try:
            r = requests.get(
                f"{url}/rest/v1/pokemon_tcg_provider_cards?tcg_set_code=eq."
                f"{quote(tcg_set_code)}&select=tcg_card_id",
                headers=headers, timeout=30,
            )
            if r.status_code in (200, 206):
                cr = r.headers.get("Content-Range", "0/0")
                out["tcg_cards_count"] = int(cr.rsplit("/", 1)[-1] or 0)
            elif r.status_code == 404:
                out["errors"].append(
                    "pokemon_tcg_provider_cards table not found — skipped"
                )
            else:
                out["errors"].append(
                    f"tcg cards count HTTP {r.status_code}: {r.text[:120]}"
                )
        except Exception as e:
            out["errors"].append(f"tcg cards count: {e}")

    return out


# ---------------------------------------------------------------------
# Seeder subprocess runner
# ---------------------------------------------------------------------

def run_seeder(csv_path: str, args, dry_run: bool) -> tuple[int, str]:
    """Invoke seed_set_cards.py as a subprocess. Always passes
    --require-console so a mislabelled CSV cannot leak into the DB
    even if --apply is ever hand-invoked. --dry-run is used here only
    when `dry_run` is True."""
    cmd = [
        sys.executable,
        os.path.join(REPO_ROOT, "seed_set_cards.py"),
        "--csv", csv_path,
        "--set-name", args.set_name,
        "--require-console", args.console,
        "--language", args.language,
    ]
    if args.release_date:
        cmd += ["--release-date", args.release_date]
    if args.printed_total:
        cmd += ["--printed-total", str(args.printed_total)]
    if dry_run:
        cmd.append("--dry-run")
    proc = subprocess.run(
        cmd, capture_output=True, text=True, cwd=REPO_ROOT,
    )
    stdout = (proc.stdout or "") + (("\n[STDERR]\n" + proc.stderr) if proc.stderr else "")
    return proc.returncode, stdout


# ---------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------

def write_report(path: str, lines: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for line in lines:
            f.write(line.rstrip() + "\n")


def evaluate_completeness(cls: dict, expected_cards: int | None) -> tuple[bool, list[str]]:
    """Return (is_incomplete, reasons). A listing is flagged incomplete
    when either:
      * --expected-cards was supplied and the count of distinct numeric
        card numbers is MORE THAN 10% BELOW it, or
      * there are MORE THAN 10 missing numbers between 1 and the highest
        card number seen.

    Both checks are side-effect free so the main flow can decide whether
    to prepend a banner, exit non-zero in --apply, or let
    --allow-incomplete override."""
    reasons: list[str] = []
    distinct = cls.get("distinct_numbers", 0)
    highest = cls.get("highest_number")
    missing = cls.get("missing_numbers", [])

    if expected_cards is not None and expected_cards > 0:
        shortfall_ratio = (expected_cards - distinct) / expected_cards
        if shortfall_ratio > 0.10:
            reasons.append(
                f"distinct card numbers ({distinct}) is {shortfall_ratio*100:.1f}% "
                f"below --expected-cards ({expected_cards})"
            )

    if len(missing) > 10:
        reasons.append(
            f"{len(missing)} card numbers are missing between 1 and the highest "
            f"observed number (#{highest})"
        )

    return (len(reasons) > 0), reasons


def _format_classification(cls: dict, printed_total: int | None,
                           expected_cards: int | None) -> list[str]:
    out = []
    out.append(f"Total rows:               {cls['total']}")
    out.append(f"Duplicate PriceCharting ids: {len(cls['duplicate_ids'])}"
               + (f" ({cls['duplicate_ids']!r})" if cls['duplicate_ids'] else ""))
    out.append(f"Singles (name has #NN):   {len(cls['singles'])}")
    out.append(f"Sealed products:          {len(cls['sealed'])}")
    out.append(f"Unnumbered (neither):     {len(cls['unnumbered'])}")
    out.append(f"Bracket-variant rows:     {len(cls['bracket_variants'])}")
    out.append(f"Distinct numeric #NN:     {cls['distinct_numbers']}")
    out.append(f"Highest card number seen: {cls['highest_number']}")
    if printed_total is not None:
        drift = cls['distinct_numbers'] - printed_total
        out.append(f"--printed-total:          {printed_total} (distinct - printed = {drift})")
    if expected_cards is not None:
        drift = cls['distinct_numbers'] - expected_cards
        pct = (abs(drift) / expected_cards * 100) if expected_cards else 0
        out.append(f"--expected-cards:         {expected_cards} (distinct - expected = {drift}; {pct:.1f}%)")
    out.append(f"Missing numbers 1..highest ({len(cls['missing_numbers'])}):")
    if cls["missing_numbers"]:
        out.append("  " + ", ".join(str(n) for n in cls["missing_numbers"][:200]))
        if len(cls["missing_numbers"]) > 200:
            out.append(f"  …and {len(cls['missing_numbers']) - 200} more")
    else:
        out.append("  (none)")
    return out


# ---------------------------------------------------------------------
# main
# ---------------------------------------------------------------------

def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Onboard a new PriceCharting Pokémon set.",
    )
    p.add_argument("--console", required=True,
                   help='PriceCharting console name, e.g. "Pokemon Delta Reign".')
    p.add_argument("--set-name", required=True,
                   help='DB set_name (no "Pokemon " prefix), e.g. "Delta Reign".')
    p.add_argument("--release-date", required=True,
                   help="YYYY-MM-DD.")
    p.add_argument("--printed-total", type=int, required=True,
                   help="Printed denominator (what appears on the cards).")
    p.add_argument("--language", choices=("en", "jp"), default="en")
    p.add_argument("--expected-cards", type=int, default=None,
                   help="Optional full card count including secret rares.")
    p.add_argument("--tcg-set-code", default=None,
                   help="Optional TCG set code for the DB cross-check.")
    p.add_argument("--batch-file", default=None,
                   help="Batch file to append to with --apply. When omitted, "
                        "the choice depends on --language: 'en' uses "
                        "batches/batch6.txt; 'jp' picks the "
                        "batches/batch-japanese-*.txt with the fewest "
                        "non-empty lines (tie: lowest numeric suffix). An "
                        "explicit value always wins.")
    p.add_argument("--out-dir", default="pc_csvs",
                   help="Where to write the generated CSV. In dry-run mode "
                        "a git-ignored temp folder is used instead.")
    p.add_argument("--report", required=True,
                   help="Path to the plaintext report file.")
    p.add_argument("--apply", action="store_true",
                   help="Write to pc_csvs/, run the seeder for real, and "
                        "append to the batch file. Default off = dry run.")
    p.add_argument("--allow-incomplete", action="store_true",
                   help="Proceed even when the completeness guard would "
                        "otherwise flag this listing as truncated. In dry "
                        "run the banner is still shown; with --apply this "
                        "flag is the only way to seed a listing whose "
                        "coverage falls short of --expected-cards or has "
                        "more than 10 gap numbers.")
    p.add_argument("--min-rows", type=int, default=50,
                   help="Hard-fail threshold for the row-count sanity "
                        "check. Default 50 catches partial PriceCharting "
                        "responses for standard sets; drop to ~10 for "
                        "small official promo sets (e.g. McDonald's). "
                        "Must be at least 5.")
    return p.parse_args(argv)


def validate_language_name_consistency(language: str, console: str,
                                        set_name: str) -> None:
    """Refuse a mislabelled onboarding before any PriceCharting request:
      * --language jp requires console starting with "Pokemon Japanese "
        AND --set-name starting with "Japanese ".
      * --language en forbids both prefixes.

    Raises ValueError with a clear message; callers map that to a
    non-zero exit. This is the first guard the nightly scraper relies
    on for language segregation — see the W48B doc-block in
    pokeprices_scraper_v8.py and seed_set_cards.py."""
    if language == "jp":
        if not console.startswith("Pokemon Japanese "):
            raise ValueError(
                f"--language jp requires --console to start with "
                f"'Pokemon Japanese '. Got {console!r}."
            )
        if not set_name.startswith("Japanese "):
            raise ValueError(
                f"--language jp requires --set-name to start with "
                f"'Japanese '. Got {set_name!r}."
            )
    else:  # en
        if console.startswith("Pokemon Japanese "):
            raise ValueError(
                f"--language en forbids --console starting with "
                f"'Pokemon Japanese '. Got {console!r}."
            )
        if set_name.startswith("Japanese "):
            raise ValueError(
                f"--language en forbids --set-name starting with "
                f"'Japanese '. Got {set_name!r}."
            )


_JP_BATCH_RE = re.compile(r"^batch-japanese-(\d+)\.txt$", re.IGNORECASE)

# Same shape the workflow commit step enforces. Any batch path the
# script emits — whether auto-chosen or passed in via --batch-file —
# has to match this before the script will do any real work.
BATCH_PATH_RE = re.compile(r"^batches/[A-Za-z0-9._-]+\.txt$")


def validate_repo_relative_batch_path(path: str) -> None:
    """Raise ValueError when `path` is not a repo-relative
    batches/<name>.txt. The workflow's commit step enforces the same
    regex — we enforce it in-process too so a buggy auto-selection or
    a hand-typed --batch-file cannot survive past validation and into
    the seeder step (which would write cards to the DB and leave the
    commit step to fail)."""
    if not isinstance(path, str) or not BATCH_PATH_RE.match(path):
        raise ValueError(
            f"Batch file path must be repo-relative and match "
            f"^batches/[A-Za-z0-9._-]+\\.txt$. Got {path!r}."
        )


def validate_explicit_batch_matches_language(language: str,
                                              explicit_path: str) -> None:
    """When an operator passes --batch-file explicitly, require that
    the filename line up with --language:
      * --language en forbids batches/batch-japanese-*.txt
      * --language jp requires batches/batch-japanese-*.txt
    Preserves the language segregation the nightly scraper's language
    tagging depends on (see W48B commentary in seed_set_cards.py)."""
    basename = os.path.basename(explicit_path.replace("\\", "/"))
    is_jp_batch = _JP_BATCH_RE.match(basename) is not None
    if language == "en" and is_jp_batch:
        raise ValueError(
            f"--language en forbids a batch file whose name starts "
            f"'batch-japanese-'. Got {explicit_path!r}."
        )
    if language == "jp" and not is_jp_batch:
        raise ValueError(
            f"--language jp requires a batch file whose name starts "
            f"'batch-japanese-'. Got {explicit_path!r}."
        )


def _repo_relative(path: str, repo_root: str) -> str:
    """Return `path` as 'batches/<name>.txt' (forward slashes, relative
    to repo_root). Idempotent when already relative. Backslashes are
    collapsed to forward slashes so Windows local runs emit the same
    shape as CI."""
    p = path.replace("\\", "/")
    if os.path.isabs(p):
        root = repo_root.replace("\\", "/").rstrip("/") + "/"
        if p.startswith(root):
            p = p[len(root):]
        else:
            # Fall back to just the tail; validate_repo_relative_batch_path
            # will reject anything suspicious.
            p = os.path.relpath(p, repo_root).replace("\\", "/")
    return p


def choose_batch_file(language: str, repo_root: str,
                      explicit: str | None,
                      batches_subdir: str = "batches") -> str:
    """Pick the batch file the --apply path should append to. Returns
    a repo-relative forward-slash path (e.g. 'batches/batch6.txt') so
    the workflow commit step and the GITHUB_OUTPUT emission agree on
    exactly one path shape.

      * `explicit` always wins, but is normalised to repo-relative.
      * `language == 'en'` → batches/batch6.txt.
      * `language == 'jp'` → the batch-japanese-N.txt with the fewest
        non-empty lines under `repo_root/batches_subdir`. Ties break
        on the smaller N.

    Raises FileNotFoundError when `language == 'jp'` and no JP batch
    files exist — we would otherwise silently fall back to batch6.txt,
    which would flow a Japanese set into the English pipeline."""
    if explicit is not None:
        return _repo_relative(explicit, repo_root)
    if language == "en":
        return f"{batches_subdir}/batch6.txt"

    batches_dir = os.path.join(repo_root, batches_subdir)
    if not os.path.isdir(batches_dir):
        raise FileNotFoundError(
            f"batches directory not found: {batches_dir}"
        )
    candidates: list[tuple[int, int, str]] = []  # (line_count, N, filename)
    for name in sorted(os.listdir(batches_dir)):
        m = _JP_BATCH_RE.match(name)
        if not m:
            continue
        with open(os.path.join(batches_dir, name), "r", encoding="utf-8") as f:
            count = sum(1 for line in f if line.strip())
        candidates.append((count, int(m.group(1)), name))
    if not candidates:
        raise FileNotFoundError(
            f"No batch-japanese-*.txt files under {batches_dir}; "
            "cannot route a Japanese set."
        )
    candidates.sort(key=lambda c: (c[0], c[1]))
    return f"{batches_subdir}/{candidates[0][2]}"


def _guard_apply(args) -> None:
    """--apply is for CI only, and only when the DB env vars are set."""
    if not args.apply:
        return
    if os.environ.get("GITHUB_ACTIONS") != "true":
        print("--apply is refused outside GitHub Actions (set GITHUB_ACTIONS=true "
              "in CI only). Local operators should run without --apply.", file=sys.stderr)
        sys.exit(2)
    if not db_env_present():
        print("--apply requires SUPABASE_URL and SUPABASE_SERVICE_KEY to be set.",
              file=sys.stderr)
        sys.exit(2)


def main(argv=None) -> int:
    args = _parse_args(argv)

    # --min-rows floor. 5 is the smallest we will accept — below that
    # a legitimate PriceCharting result is almost certainly a mistake.
    if args.min_rows < 5:
        print(f"ERROR: --min-rows must be at least 5. Got {args.min_rows}.",
              file=sys.stderr)
        return 7

    # Language / name-prefix consistency. Caught before --apply guard
    # so an invalid dry run fails fast too — the same mistake would
    # otherwise burn a Supabase round-trip in CI and (worse) leave a
    # polluted batch file behind.
    try:
        validate_language_name_consistency(args.language, args.console, args.set_name)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 7

    # If --batch-file was given explicitly, require it to line up with
    # --language. Caught before resolution so --apply cannot slip a
    # Japanese set past the English pipeline (or vice versa).
    explicit_batch = args.batch_file
    if explicit_batch is not None:
        try:
            validate_explicit_batch_matches_language(args.language, explicit_batch)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 7

    # Resolve the batch file BEFORE we start work so the report (and
    # the CI commit step) agree on the target path. On apply mode we
    # need the JP batches directory to exist; on dry-run we tolerate
    # its absence — print rather than crash.
    chosen_batch: str | None
    try:
        chosen_batch = choose_batch_file(args.language, REPO_ROOT, explicit_batch)
    except FileNotFoundError as e:
        if args.apply:
            print(f"ERROR: {e}", file=sys.stderr)
            return 7
        chosen_batch = None  # dry-run: report-only
    args.batch_file = chosen_batch if chosen_batch is not None \
        else "batches/batch6.txt"

    # Validate the path matches the repo-relative shape the workflow
    # commit step enforces. Auto-selected paths are always validated;
    # an explicit path is validated too whenever --apply is on (a
    # stricter check than needed for dry-run but matches the spec).
    must_validate = (explicit_batch is None) or args.apply
    if must_validate:
        try:
            validate_repo_relative_batch_path(args.batch_file)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 7

    # GitHub Actions step output so the commit step can stage the
    # batch file this run actually chose rather than hard-coding
    # batch6.txt. choose_batch_file already returned a repo-relative
    # forward-slash path so this is a straight write.
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if gh_output:
        with open(gh_output, "a", encoding="utf-8") as f:
            f.write(f"batch_file={args.batch_file}\n")

    _guard_apply(args)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    # The banner slot is prepended once classification has run — placed
    # above every other section so a reviewer scanning only the top of
    # the report cannot miss an incomplete listing.
    banner: list[str] = []
    report: list[str] = []
    report.append(f"# onboard_new_set.py report")
    report.append(f"Generated:         {now}")
    report.append(f"Mode:              {'APPLY' if args.apply else 'DRY RUN'}")
    report.append(f"--console:         {args.console!r}")
    report.append(f"--set-name:        {args.set_name!r}")
    report.append(f"--release-date:    {args.release_date}")
    report.append(f"--printed-total:   {args.printed_total}")
    report.append(f"--language:        {args.language}")
    report.append(f"--expected-cards:  {args.expected_cards}")
    report.append(f"--tcg-set-code:    {args.tcg_set_code}")
    report.append(f"--batch-file:      {args.batch_file}")
    report.append("")

    # 1) Fetch listing
    fetch_log: list[str] = []
    try:
        rows = fetch_listing(args.console, fetch_log)
    except Exception as e:
        report.append("FETCH FAILED:")
        for l in fetch_log:
            report.append(f"  {l}")
        report.append(f"  ERROR: {e}")
        write_report(args.report, banner + report)
        print(f"ERROR: {e}", file=sys.stderr)
        return 3

    report.append("Fetch log:")
    for l in fetch_log:
        report.append(f"  {l}")
    report.append("")

    # 2) Write CSV
    csv_basename = f"Pokemon {args.set_name}.csv" if not args.console.lower().startswith("pokemon") else f"{args.console}.csv"
    # Prefer the user-supplied console name verbatim for the file name
    # — that's how the existing pc_csvs/ files are named
    # (e.g. "Pokemon 30th Celebration.csv").
    csv_basename = f"{args.console}.csv"
    if args.apply:
        out_dir = args.out_dir
    else:
        out_dir = os.path.join(REPO_ROOT, ".onboard_tmp")
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, csv_basename)

    written = write_pc_csv(rows, args.console, csv_path)
    report.append(f"CSV written:       {csv_path} ({written} rows)")
    report.append("")

    # 3) Classify
    cls = classify_rows(rows)
    report.append("Classification summary")
    report.append("----------------------")
    report.extend(_format_classification(cls, args.printed_total, args.expected_cards))
    report.append("")

    # 4) Cross-checks
    warnings: list[str] = []
    if cls["total"] < args.min_rows:
        msg = (f"HARD FAIL: only {cls['total']} rows returned. "
               f"Below --min-rows threshold ({args.min_rows}). Pass a lower "
               "--min-rows for small official promo sets (e.g. McDonald's, "
               "typically ~15 rows). Aborting before seeder.")
        warnings.append(msg)
        report.append(msg)
    if cls["duplicate_ids"]:
        warnings.append(f"HARD FAIL: duplicate PriceCharting ids: {cls['duplicate_ids']}")

    # Completeness guard. Prepend an INCOMPLETE banner to the report
    # when either --expected-cards is more than 10% short or more than
    # 10 card numbers are missing between 1 and the highest seen. In
    # --apply mode this aborts before the seeder unless the operator
    # has explicitly passed --allow-incomplete.
    incomplete, incomplete_reasons = evaluate_completeness(cls, args.expected_cards)
    if incomplete:
        banner.append("!" * 72)
        banner.append("!! INCOMPLETE LISTING — PriceCharting did not return a full set of")
        banner.append("!! rows. Seeding now would create a partially-populated set with")
        banner.append("!! gaps that the nightly scraper cannot fill on its own (rows must")
        banner.append("!! exist in pc_csvs/ before the scraper will touch them).")
        banner.append("!!")
        for r in incomplete_reasons:
            banner.append(f"!!   * {r}")
        banner.append("!!")
        if args.apply and not args.allow_incomplete:
            banner.append("!! --apply aborted. Re-run with --allow-incomplete to force seed.")
        elif args.allow_incomplete:
            banner.append("!! --allow-incomplete was supplied; proceeding anyway.")
        else:
            banner.append("!! Dry-run only: no seeder called with --apply.")
        banner.append("!" * 72)
        banner.append("")
        for r in incomplete_reasons:
            warnings.append(f"INCOMPLETE: {r}")

    if db_env_present():
        report.append("DB cross-check (read-only)")
        report.append("--------------------------")
        xc = db_cross_check(args.set_name, args.tcg_set_code)
        report.append(f"cards table rows with set_name={args.set_name!r}: {xc['cards_count']}")
        if args.tcg_set_code:
            report.append(f"pokemon_tcg_provider_cards rows with "
                          f"tcg_set_code={args.tcg_set_code!r}: {xc['tcg_cards_count']}")
        if xc["errors"]:
            for err in xc["errors"]:
                report.append(f"  note: {err}")
        if xc["cards_count"] and xc["cards_count"] > 0:
            warnings.append(
                f"WARN: cards table already has {xc['cards_count']} row(s) for "
                f"set_name={args.set_name!r}. A brand-new set should have 0."
            )
        report.append("")
    else:
        report.append("DB cross-check: SKIPPED — SUPABASE_URL / SUPABASE_SERVICE_KEY not set.")
        report.append("")

    if warnings:
        report.append("Warnings")
        report.append("--------")
        for w in warnings:
            report.append(f"  * {w}")
        report.append("")

    hard_fail = cls["total"] < args.min_rows or bool(cls["duplicate_ids"])
    if hard_fail:
        report.append("Aborting before seeder due to HARD FAIL above.")
        write_report(args.report, banner + report)
        print("HARD FAIL — see report:", args.report, file=sys.stderr)
        return 4

    # Completeness-guard enforcement for --apply.
    if args.apply and incomplete and not args.allow_incomplete:
        report.append("Aborting before seeder due to INCOMPLETE listing (see banner).")
        report.append("Re-run with --allow-incomplete to force seed, or investigate "
                      "the listing on PriceCharting first.")
        write_report(args.report, banner + report)
        print("INCOMPLETE — aborted before seeder. See report:",
              args.report, file=sys.stderr)
        return 6

    # 5) Seeder dry-run — run it even in apply mode first, for the log
    report.append("seed_set_cards.py --dry-run output")
    report.append("----------------------------------")
    if not db_env_present():
        report.append("SKIPPED — SUPABASE_URL / SUPABASE_SERVICE_KEY not set. "
                      "The seeder refuses to run without them. "
                      "This is expected on a local workstation.")
    else:
        rc, out = run_seeder(csv_path, args, dry_run=True)
        report.append(f"(exit code {rc})")
        report.append(out)
    report.append("")

    # 6) --apply tail
    # args.batch_file is a repo-relative forward-slash path; resolve
    # it against REPO_ROOT for file operations so callers can run from
    # any CWD.
    batch_abs = args.batch_file if os.path.isabs(args.batch_file) \
        else os.path.join(REPO_ROOT, args.batch_file.replace("/", os.sep))
    if args.apply:
        # Write CSV to pc_csvs/ (we already wrote to out_dir which == pc_csvs/)
        # Append batch file
        append_status = append_console_to_batch(batch_abs, args.console)
        report.append(f"Batch file:        {args.batch_file}  [{append_status}]")
        # Run seeder for real
        rc, out = run_seeder(csv_path, args, dry_run=False)
        report.append("seed_set_cards.py (apply) output")
        report.append("--------------------------------")
        report.append(f"(exit code {rc})")
        report.append(out)
        write_report(args.report, banner + report)
        if rc != 0:
            print(f"Seeder failed with exit {rc}. See {args.report}.", file=sys.stderr)
            return 5
    else:
        # Describe what --apply would do
        report.append("What --apply would do")
        report.append("---------------------")
        report.append(f"  * Move the CSV into {args.out_dir}/{csv_basename}")
        report.append(f"  * Run seed_set_cards.py --require-console {args.console!r} for real")
        existing_batch = ""
        if os.path.exists(batch_abs):
            with open(batch_abs, "r", encoding="utf-8") as f:
                existing_batch = f.read()
        would_append = args.console not in existing_batch.splitlines()
        report.append(f"  * Append {args.console!r} to {args.batch_file} "
                      f"(would-append={would_append})")
        report.append("")
        report.append("Nightly behaviour for a newly seeded set")
        report.append("----------------------------------------")
        report.append(
            "  * cadence.py puts every brand-new card_slug into the "
            "DAILY_DISCOVERY cohort on night 1 (no prior successful raw "
            "observation, no prior page_reached_no_price streak). It "
            "then promotes to DAILY_VALUE / WEEKLY_LOW / WEEKLY_UNPRICED "
            "based on the first few observations — nothing additional "
            "needs to be done after this script's --apply."
        )
        report.append(
            "  * pokeprices_scraper_v8 PATCHes image_url / pc_url on "
            "existing cards rows whenever it finds them empty, so the "
            "card images for the new set populate automatically on the "
            "first night the set is in the batch file."
        )
        write_report(args.report, banner + report)

    print(f"Report written to {args.report}")
    print(f"CSV written to {csv_path} ({written} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
