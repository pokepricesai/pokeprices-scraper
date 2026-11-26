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


def fetch_listing(console_name: str, verbose_log: list[str]) -> list[dict]:
    """Fetch every listing page for the given console and return the
    deduped, document-ordered list of row dicts.

    `verbose_log` is mutated with a line per URL tried so the eventual
    report can show exactly what was fetched. No DB side effects.
    """
    console_url = build_console_url(console_name)

    def _get(url: str) -> str | None:
        verbose_log.append(f"GET {url}")
        return _scraper.fetch_card_page(url, retries=2, backoff_seconds=1.5)

    html = _get(console_url)
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

    # Follow pagination if the site ever paginates this listing. PC
    # currently serves all rows on page 1 and ignores ?page=; we still
    # probe a couple of extra pages so a future change here is handled
    # automatically. The loop stops the moment we see no new ids.
    fetches = 1
    page = 2
    prev_count = -1
    while fetches < MAX_LISTING_FETCHES and prev_count != len(all_rows):
        prev_count = len(all_rows)
        time.sleep(LISTING_REQUEST_DELAY)
        paged_url = f"{console_url}?page={page}"
        html_page = _get(paged_url)
        fetches += 1
        if html_page is None:
            break
        added = 0
        for row in parse_listing_html(html_page):
            if row["pc_id"] not in all_rows:
                all_rows[row["pc_id"]] = row
                added += 1
        if added == 0:
            break
        page += 1

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
    p.add_argument("--batch-file", default=os.path.join("batches", "batch6.txt"))
    p.add_argument("--out-dir", default="pc_csvs",
                   help="Where to write the generated CSV. In dry-run mode "
                        "a git-ignored temp folder is used instead.")
    p.add_argument("--report", required=True,
                   help="Path to the plaintext report file.")
    p.add_argument("--apply", action="store_true",
                   help="Write to pc_csvs/, run the seeder for real, and "
                        "append to the batch file. Default off = dry run.")
    return p.parse_args(argv)


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
    _guard_apply(args)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
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
        write_report(args.report, report)
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
    if cls["total"] < 50:
        msg = (f"HARD FAIL: only {cls['total']} rows returned. "
               "PriceCharting listings for a brand-new Pokémon set normally "
               "have >= 50 rows. Aborting before seeder.")
        warnings.append(msg)
        report.append(msg)
    if cls["duplicate_ids"]:
        warnings.append(f"HARD FAIL: duplicate PriceCharting ids: {cls['duplicate_ids']}")
    if args.expected_cards is not None and cls["distinct_numbers"]:
        drift = abs(cls["distinct_numbers"] - args.expected_cards)
        if drift / args.expected_cards > 0.10:
            warnings.append(
                f"WARN: distinct card numbers ({cls['distinct_numbers']}) "
                f"differ from --expected-cards ({args.expected_cards}) by more than 10%."
            )

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

    hard_fail = cls["total"] < 50 or bool(cls["duplicate_ids"])
    if hard_fail:
        report.append("Aborting before seeder due to HARD FAIL above.")
        write_report(args.report, report)
        print("HARD FAIL — see report:", args.report, file=sys.stderr)
        return 4

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
    if args.apply:
        # Write CSV to pc_csvs/ (we already wrote to out_dir which == pc_csvs/)
        # Append batch file
        append_status = append_console_to_batch(args.batch_file, args.console)
        report.append(f"Batch file:        {args.batch_file}  [{append_status}]")
        # Run seeder for real
        rc, out = run_seeder(csv_path, args, dry_run=False)
        report.append("seed_set_cards.py (apply) output")
        report.append("--------------------------------")
        report.append(f"(exit code {rc})")
        report.append(out)
        write_report(args.report, report)
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
        if os.path.exists(args.batch_file):
            with open(args.batch_file, "r", encoding="utf-8") as f:
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
        write_report(args.report, report)

    print(f"Report written to {args.report}")
    print(f"CSV written to {csv_path} ({written} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
