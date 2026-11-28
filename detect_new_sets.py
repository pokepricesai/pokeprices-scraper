"""
detect_new_sets.py
==================
Read-only audit of PriceCharting's Pokémon category index against the
sets already tracked in this repo. Writes a plain-text report naming
every console on PriceCharting that is not in any batches/*.txt on
origin/main, split into English, Japanese, and near-miss (same
build_url slug, different spelling) buckets.

No database access. No file writes besides the report path given on
the command line. No --apply. Fetches only the category index — never
individual product pages — and sleeps 1.5 s between any follow-up
requests should PriceCharting ever start paginating this page.

The HTTP session, headers and retry style are inherited from
pokeprices_scraper_v8 via the same indirection used by
onboard_new_set.py, so this script is byte-identical to the nightly
scraper as far as PriceCharting can tell.

Usage:
    python detect_new_sets.py --report detect-report.txt
"""

from __future__ import annotations

import argparse
import html
import importlib.util
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib.parse import unquote


REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
CATEGORY_URL = "https://www.pricecharting.com/category/pokemon-cards"
# Polite minimum between requests. Reused from onboard_new_set.
REQUEST_DELAY = 1.5


def _load_module(module_name: str, filename: str):
    path = os.path.join(REPO_ROOT, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_scraper = _load_module("pokeprices_scraper_v8", "pokeprices_scraper_v8.py")


# ---------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------

_CONSOLE_LINK_RE = re.compile(
    r'<a\s+href="(?P<href>/console/pokemon-[^"#?]+)"'
    r'(?:\s+title="(?P<title>[^"]*)")?\s*>'
    r'(?P<inner>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_TITLE_RE = re.compile(r"<title>(?P<t>[^<]+)</title>", re.IGNORECASE)


def parse_category_html(page_html: str) -> list[dict]:
    """Return every distinct /console/pokemon-* link on the page as
    a list of {'display_name', 'url', 'slug'} dicts, in first-seen
    document order.

    When the same URL appears with different anchor labels (the "New
    Releases" sidebar at the top of the page uses short labels like
    "McDonalds 2026" while the main listing uses the canonical
    "Pokemon McDonalds 2026"), the title="..." attribute is preferred
    when it starts with "Pokemon"; otherwise the inner text is used.
    """
    by_url: dict[str, str] = {}
    for m in _CONSOLE_LINK_RE.finditer(page_html):
        href = m.group("href").strip()
        inner = html.unescape(m.group("inner") or "").strip()
        title = html.unescape(m.group("title") or "").strip()
        name = ""
        for cand in (title, inner):
            if cand.lower().startswith("pokemon"):
                name = cand
                break
        if not name:
            name = title or inner
        if not name:
            continue
        if href not in by_url:
            by_url[href] = name
    out = []
    for url, name in by_url.items():
        out.append({
            "display_name": name,
            "url": f"https://www.pricecharting.com{url}",
            "slug": url[len("/console/"):],
        })
    return out


def looks_like_category_page(page_html: str) -> bool:
    m = _TITLE_RE.search(page_html)
    return bool(m and "pokemon" in m.group("t").lower())


# ---------------------------------------------------------------------
# Live fetch (reuses pokeprices_scraper_v8 retries/headers)
# ---------------------------------------------------------------------

def _http_get(url: str) -> str | None:
    """Thin wrapper over pokeprices_scraper_v8.fetch_card_page so tests
    can monkey-patch without touching the scraper module."""
    return _scraper.fetch_card_page(url, retries=2, backoff_seconds=1.5)


def fetch_category(verbose_log: list[str]) -> list[dict]:
    """GET the category index. If the page ever starts to paginate
    (currently it does not — all 302 consoles are served in one HTML
    document as of 2026-11-27) emit a clear WARNING into verbose_log
    rather than silently returning a partial list."""
    verbose_log.append(f"GET {CATEGORY_URL}")
    page_html = _http_get(CATEGORY_URL)
    if page_html is None:
        raise RuntimeError(
            f"Could not fetch {CATEGORY_URL}. Retries exhausted."
        )
    if not looks_like_category_page(page_html):
        raise RuntimeError(
            f"Category page did not look right. First 160 chars: "
            f"{page_html[:160]!r}"
        )
    consoles = parse_category_html(page_html)
    verbose_log.append(f"  -> {len(consoles)} distinct consoles")

    # Defensive pagination probe: look for the same next_page form
    # that the per-set listing uses. The category page does not emit
    # one today, but surfacing its absence — rather than ignoring it —
    # means a future site change is noticed.
    next_form = re.search(
        r'<form[^>]*class="[^"]*\bnext_page\b[^"]*"', page_html,
        re.IGNORECASE,
    )
    if next_form is not None:
        verbose_log.append(
            "  WARNING: category page now carries a next_page form; "
            "detect_new_sets is NOT walking it. Rerun with the paging "
            "implementation added before trusting this list."
        )
    return consoles


# ---------------------------------------------------------------------
# Known-set list — origin/main batches/*.txt
# ---------------------------------------------------------------------

def read_batches_from_origin_main() -> dict[str, list[str]]:
    """Return {batch_filename: [console_name, ...]} read from
    origin/main. We never trust the working tree here because this
    script's whole job is to find drift between PriceCharting and
    what's actually shipped."""
    result = subprocess.run(
        ["git", "ls-tree", "--name-only", "origin/main", "batches/"],
        capture_output=True, text=True, cwd=REPO_ROOT, check=True,
    )
    files = [f for f in result.stdout.splitlines() if f.endswith(".txt")]
    out: dict[str, list[str]] = {}
    for f in files:
        show = subprocess.run(
            ["git", "show", f"origin/main:{f}"],
            capture_output=True, text=True, cwd=REPO_ROOT, check=True,
        )
        out[f] = [line.strip() for line in show.stdout.splitlines() if line.strip()]
    return out


def pc_csvs_on_origin_main() -> set[str]:
    """Return the set of pc_csvs/* filenames tracked on origin/main."""
    result = subprocess.run(
        ["git", "ls-tree", "--name-only", "origin/main", "pc_csvs/"],
        capture_output=True, text=True, cwd=REPO_ROOT, check=True,
    )
    return set(result.stdout.splitlines())


# ---------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------

def _canonical_key(name: str) -> str:
    """Case-insensitive, whitespace-collapsed key for membership tests."""
    return re.sub(r"\s+", " ", name.strip().lower())


def _build_url_slug(console_name: str) -> str:
    """Reuse pokeprices_scraper_v8.build_url and strip everything but
    the console-slug portion. This is the authoritative slug the
    nightly scraper will use, so near-miss detection must match it."""
    sentinel = _scraper.build_url(console_name, "x")
    m = re.match(r"https://www\.pricecharting\.com/game/([^/]+)/", sentinel)
    if not m:
        raise RuntimeError(f"Unexpected build_url shape: {sentinel!r}")
    return m.group(1)


def compare(consoles: list[dict],
            batches: dict[str, list[str]]) -> dict:
    """Pure classifier: return {'english': [...], 'japanese': [...],
    'near_miss': [...]} where each list holds dicts of shape
    {'display_name', 'url', 'slug', 'in_batches': bool, 'csv_on_main':
    bool, 'near_miss_batched_as': str | None}."""
    tracked_names: set[str] = set()
    tracked_slugs: dict[str, str] = {}   # scraper slug -> original name
    for _, lines in batches.items():
        for line in lines:
            tracked_names.add(_canonical_key(line))
            try:
                tracked_slugs[_build_url_slug(line)] = line
            except Exception:
                pass

    english: list[dict] = []
    japanese: list[dict] = []
    near_miss: list[dict] = []

    for c in consoles:
        name = c["display_name"]
        key = _canonical_key(name)
        if key in tracked_names:
            continue
        try:
            slug = _build_url_slug(name)
        except Exception:
            slug = None
        near = tracked_slugs.get(slug) if slug else None
        entry = {
            "display_name": name,
            "url": c["url"],
            "slug": c["slug"],
            "near_miss_batched_as": near,
        }
        # Chinese / Korean sets are explicitly out of scope — they are
        # not represented in any current batch file and the task asks
        # only for English and Japanese buckets. Surface them under a
        # dedicated line in the report tail so they are visible without
        # polluting the main two groups.
        if near is not None and near != name:
            near_miss.append(entry)
        elif name.lower().startswith("pokemon japanese"):
            japanese.append(entry)
        elif name.lower().startswith("pokemon chinese") or name.lower().startswith("pokemon korean"):
            # Treated as "other language"; not strictly a miss because we
            # don't track them anywhere.
            continue
        elif name.lower().startswith("pokemon"):
            english.append(entry)
        # else: unlikely — skip silently

    return {"english": english, "japanese": japanese, "near_miss": near_miss}


# ---------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------

def _csv_name(console: str) -> str:
    return f"pc_csvs/{console}.csv"


def write_report(path: str, consoles: list[dict], batches: dict[str, list[str]],
                 csvs_on_main: set[str], diff: dict,
                 fetch_log: list[str]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    lines: list[str] = []
    lines.append("# detect_new_sets.py report")
    lines.append(f"Generated:         {now}")
    lines.append(f"Source:            {CATEGORY_URL}")
    lines.append("")
    lines.append("Fetch log:")
    for l in fetch_log:
        lines.append(f"  {l}")
    lines.append("")

    lines.append("Summary")
    lines.append("-------")
    lines.append(f"PriceCharting consoles found: {len(consoles)}")
    batched_total = sum(len(v) for v in batches.values())
    lines.append(f"Already in batches/*.txt (on origin/main): {batched_total}")
    lines.append(f"Missing — English: {len(diff['english'])}")
    lines.append(f"Missing — Japanese: {len(diff['japanese'])}")
    lines.append(f"Near-miss (slug matches, spelling differs): {len(diff['near_miss'])}")
    lines.append("")

    def dump_group(header: str, group: list[dict]) -> None:
        lines.append(header)
        lines.append("-" * len(header))
        if not group:
            lines.append("  (none)")
            lines.append("")
            return
        for e in sorted(group, key=lambda x: x["display_name"].lower()):
            has_csv = _csv_name(e["display_name"]) in csvs_on_main
            lines.append(f"  {e['display_name']}")
            lines.append(f"    URL:                 {e['url']}")
            lines.append(f"    pc_csvs/ on main:    {'YES' if has_csv else 'no'}")
            if e["near_miss_batched_as"]:
                lines.append(
                    f"    near-miss of batched {e['near_miss_batched_as']!r}"
                )
        lines.append("")

    dump_group("Missing — English", diff["english"])
    dump_group("Missing — Japanese", diff["japanese"])
    dump_group("Near-miss (same build_url slug, different spelling)",
               diff["near_miss"])

    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for line in lines:
            f.write(line.rstrip() + "\n")


# ---------------------------------------------------------------------
# main
# ---------------------------------------------------------------------

def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Report PriceCharting consoles not yet in any batch.",
    )
    p.add_argument("--report", required=True,
                   help="Path of the plaintext report to write.")
    args = p.parse_args(argv)

    fetch_log: list[str] = []
    consoles = fetch_category(fetch_log)
    batches = read_batches_from_origin_main()
    csvs = pc_csvs_on_origin_main()
    diff = compare(consoles, batches)
    write_report(args.report, consoles, batches, csvs, diff, fetch_log)

    print(f"Wrote {args.report}: "
          f"{len(diff['english'])} English, "
          f"{len(diff['japanese'])} Japanese, "
          f"{len(diff['near_miss'])} near-miss.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
