# Recent-sales parser (Block 4A-S1)

`recent_sales_parser.py` is a standalone parser for the "recent completed sales"
tables that PriceCharting renders on every English Pokémon card page. It is
deliberately **not** wired into the production nightly scraper
(`pokeprices_scraper_v8.py`) and writes nothing to Supabase. It exists so the
selectors, identity rules, and classification can be exhaustively tested
against saved HTML fixtures before any production wiring is added in a later
block.

This document is the engineering reference for that parser. The product-level
audit that motivated it lives in the Block 4A-S report.

---

## Observed PriceCharting structure

Findings from inspecting 7 representative HTML samples
(`tests/fixtures/recent_sales/`):

* Recent sales are **server-rendered** inside the same HTML response already
  fetched by the nightly scraper for current prices. Zero additional HTTP
  requests are required.
* Each grade has its own
  `<div class="completed-auctions-XXX" style="display: …;">` that contains a
  `<table class="hoverable-rows sortable">`.
* The wrapper div's only class is the section slug; the matching tab UI lives
  in `<div id="tab-bar">` and carries the additional class `tab` — the parser
  uses this to ignore the tabs and only walk the row tables.
* Up to ~30 visible rows per section on vintage cards; modern cards can show
  more (the `_audit_samples` review found `completed-auctions-used` returning
  60 rows on Obsidian Flames Charizard ex).
* Sale-row anchors carry one of four classes (`js-ebay-completed-sale`,
  `js-tcgplayer-completed-sale`, `js-ha-completed-sale`,
  `js-pwcc-completed-sale`) and a trailing bracket label
  (`[eBay]`, `[TCGPlayer]`, `[HeritageAuctions]`, `[PWCC]`).
* eBay rows expose a strong identity in `tr id="ebay-{ITEM_ID}"`.
  HeritageAuctions and PWCC carry equivalent `ha-…` / `pwcc-…` ids.
  TCGPlayer rows have no `tr id` and must use the fallback hash.
* Best-Offer rows use `title="best offer accepted price"` for the final price
  and `title="best offer list price"` for the crossed-out list price; the
  parser is anchored on those `title` attributes, not on row position.
* Sale dates are ISO `YYYY-MM-DD`.
* Prices are USD strings (`$1,234.56`).
* **Japanese pages** (`/game/jp-pokemon-*`) use a different layout entirely:
  no `<div id="full-prices">`, no `<td id="*_price">`, no
  `VGPC.chart_data`, no `data-show-tab="completed-auctions-*"`. They are
  reported as `unsupported_layout` with zero rows.

---

## Supported section mappings

`SECTION_MAP` in `recent_sales_parser.py` is the single source of truth.
Each section class slug maps to a `SectionInfo(observed_section,
grading_company, grade, raw_or_graded, confidence)`. The confidence value
reflects how unambiguous the grade attribution is in PriceCharting's UI:
sections whose tab text literally names the company (`PSA 10`, `CGC 10`,
`BGS 10`, `BGS 10 Black`, `CGC 10 Pristine`, `SGC 10`, `TAG 10`, `ACE 10`)
get 100; sections whose tab text is only `Grade N` get 80; `Ungraded` is 100.

| Section class slug                | grade            | grading_company | raw_or_graded |
| --------------------------------- | ---------------- | --------------- | ------------- |
| `used`                            | Ungraded         | —               | raw           |
| `cib`                             | Grade 7          | —               | graded        |
| `new`                             | Grade 8          | —               | graded        |
| `graded`                          | Grade 9          | —               | graded        |
| `box-only`                        | Grade 9.5        | —               | graded        |
| `manual-only`                     | PSA 10           | PSA             | graded        |
| `loose-and-manual`                | Grade 1          | —               | graded        |
| `box-and-manual`                  | Grade 2          | —               | graded        |
| `grade-three`                     | Grade 3          | —               | graded        |
| `grade-four`                      | Grade 4          | —               | graded        |
| `grade-five`                      | Grade 5          | —               | graded        |
| `grade-six`                       | Grade 6          | —               | graded        |
| `loose-and-box`                   | BGS 10           | BGS             | graded        |
| `grade-seventeen`                 | CGC 10           | CGC             | graded        |
| `grade-eighteen`                  | SGC 10           | SGC             | graded        |
| `grade-nineteen`                  | CGC 10 Pristine  | CGC             | graded        |
| `grade-twenty`                    | BGS 10 Black     | BGS             | graded        |
| `grade-twenty-one`                | TAG 10           | TAG             | graded        |
| `grade-twenty-two`                | ACE 10           | ACE             | graded        |

Any other `completed-auctions-…` slug is **not silently mapped**; the row is
preserved with `parse_status="quarantined"`, `rejection_reason` contains
`unknown_section_class`, and the wrapper class is reported in the result
`warnings`.

---

## Marketplace mappings

Three independent signals are extracted per row:

1. **Anchor class** on `<td class="title"> > a`:
   * `js-ebay-completed-sale` → `ebay`
   * `js-tcgplayer-completed-sale` → `tcgplayer`
   * `js-ha-completed-sale` → `heritage_auctions`
   * `js-pwcc-completed-sale` → `pwcc`
2. **Trailing bracket label** following the closing `</a>` in the title
   cell — `[eBay]`, `[TCGPlayer]`, `[HeritageAuctions]`/`[Heritage Auctions]`,
   `[PWCC]`.
3. **URL host** of the listing href — `ebay.*` → `ebay`, `tcgplayer.com` →
   `tcgplayer`, `ha.com` → `heritage_auctions`,
   `pwccmarketplace.com`/`pwcc.com` → `pwcc`.

When all three agree, the row is `ok`. When they disagree, the row is
`quarantined` with `rejection_reason="conflicting_marketplace"` and the
observed signals are preserved in `raw_metadata` (only field that ever stores
the original affiliate-tagged href).

### Marketplace item IDs

* **eBay**: `tr id="ebay-(\d+)"` first, then `/itm/.../(\d{6,})` URL
  fallback. The URL fallback also matches when the tr-id is absent (modern
  eBay layouts sometimes omit it). Strong key.
* **HeritageAuctions**: `tr id="ha-([0-9a-zA-Z_-]+)"`. Strong key when present.
* **PWCC**: `tr id="pwcc-([0-9a-zA-Z_-]+)"`. Strong key when present.
* **TCGPlayer**: no row id, no public marketplace item id in the href.
  Always uses the fallback hash.

### Marketplace country

Inferred from the host only for eBay, and only when the suffix is
unambiguously a country TLD (`ebay.co.uk` → `GB`, `ebay.de` → `DE`, etc.).
A bare `ebay.com` is never recorded as `US`; it is `None`. PriceCharting
localises affiliate links by geography, so the host is not a reliable
country attestation for the international marketplace.

---

## Best Offer handling

The parser is anchored on the `title` attribute of each `<span class="js-price">`:

| `title=` value                | role             |
| ----------------------------- | ---------------- |
| `best offer accepted price`   | **final** price  |
| `best offer list price`       | crossed-out list price |
| (no title)                    | normal final price |

Spans inside `<td class="numeric">` carrying the additional class
`listed-price-inline` are treated as list prices (same as
`best offer list price`).

Result shape:

* Normal sale → `sale_price_cents=<final>`, `original_price_cents=None`,
  `best_offer_status="not_best_offer"`.
* Best Offer accepted → `sale_price_cents=<accepted>`,
  `original_price_cents=<list>`, `best_offer_status="accepted"`.

The crossed-out price is **never** treated as the final price. The highest
displayed price is never used by assumption.

---

## Identity rules

### Strong key (preferred)

Emitted when a marketplace item ID was extracted **and** the marketplace was
disambiguated. The hash is `sha256` of:

```
provider | provider_card_id | marketplace_source | marketplace_item_id | observed_section
```

`observed_section` is part of the strong key so that a provider
reclassification (the same item ID moves from one grade section to another)
is *visible* as a new row, not silently overwritten.

### Fallback key

Emitted when no marketplace item ID is available. Hashed inputs:

```
provider | provider_card_id | sale_date | marketplace_source | observed_section | sale_price_cents | normalised_title
```

`sale_price_cents` is **not** rounded — the exact observed value is preserved.

### Normalised title

Used inside the fallback key only:

* `unicodedata.normalize("NFKC", title)`
* `lower().strip()`
* Whitespace collapsed.
* Whitespace around `#` is padded, around `/` is removed.
* Card numbers like `#4`, `4/102`, and `085/108` are preserved.

The parser deliberately does **not** strip all punctuation.

### `raw_hash`

A separate `sha256` over every directly observed row field, including
prices, listing URL, title, condition text, variant text, best-offer status,
and any anomaly flags. Used for change detection: the same item correcting
its title overnight keeps the same `provider_sale_key` but emits a different
`raw_hash`.

### In-batch dedup

The parser collapses duplicates within a single response by
`provider_sale_key` (when strong) or `raw_hash` (when fallback). Per-night
upsert dedup is the consumer's responsibility — outside the scope of this
parser.

### Row position is never used as identity.

---

## Quality classification

Every row is classified as one of:

* `ok` — all required fields present, no quarantine flags.
* `quarantined` — partially recoverable; the row is kept but the reason is
  surfaced in `rejection_reason` and `anomaly_flags`. Possible reasons:
  * `missing_marketplace` — none of the three marketplace signals fired.
  * `missing_title` — title `<a>` text was empty.
  * `unknown_section_class` — wrapper div class not in `SECTION_MAP`.
  * `conflicting_marketplace` — anchor / bracket / host signals disagreed.
  * `lot_or_bundle` — title matches keywords like `lot of`, `bundle of`,
    `x10`, `x20`, ` repack`, ` mystery`, `binder full`.
  * `proxy_or_reprint` — title matches `proxy`, `replica`, `fake`,
    `custom card`, `metal card`, etc.
  * `wrong_language` — page language is `en` but the title contains non-Latin
    script (CJK).
  * `wrong_card_number` — caller passed `expected_card_number=` and the
    title's `#NN` / `NN/NNN` token disagrees (leading-zero tolerant).
* `rejected` — structurally unusable. Reasons:
  * `malformed_date` — `td.date` was not ISO `YYYY-MM-DD`.
  * `missing_final_price` — no `js-price` span found in any `td.numeric`.
  * `invalid_final_price` — span was present but value was non-numeric, zero,
    or negative.
  * `unsafe_url` — listing href used a scheme other than `http`/`https`
    (e.g. `javascript:`).

The parser **never deletes** rows silently. Rejected and quarantined rows
both come back in `RecentSalesParseResult.rows` with non-`ok` status, so
the caller can audit them.

---

## Language handling

Language is inferred from the page URL path:

| URL path begins with | language |
| -------------------- | -------- |
| `/game/jp-pokemon-`  | `ja`     |
| `/game/pokemon-`     | `en`     |
| anything else        | `unknown` |

Language is **never** inferred from titles, slugs, or page content.

### Japanese (`ja`)

The parser returns immediately with:

* `parse_status="unsupported_layout"`
* `rows=[]`
* a single warning identifying the unsupported language
* a deterministic `layout_signature` computed cheaply from a few regex
  probes (anchor presence, `id="full-prices"` presence, `VGPC.chart_data`
  presence) so selector-drift monitoring still works without full parsing.

---

## Condition and variant parsing

Conservative title-based parsing. The **section is authoritative** for
graded vs raw; the title is advisory.

* `condition_bucket` ∈ `{near_mint, lightly_played, moderately_played,
  heavily_played, damaged, graded, raw_unknown, unknown}`.
* If the section is graded, `condition_bucket` is always `graded` regardless
  of what the title says ("NM" mention in a PSA 10 row is captured in
  `condition_text` but does not downgrade the bucket).
* `first_edition_status` ∈ `{first_edition, unlimited, unknown}`.
* `variant_text` captures every `[…]` bracketed token from the title; if
  the title says `Shadowless`, it is appended even when not bracketed.
* The parser never asserts a variant when the title is ambiguous (e.g. a
  plain "Charizard" → `variant_text=None`, `first_edition_status="unknown"`).

---

## Anomaly flags

`row.anomaly_flags` is a list (may be empty). The flags this parser is
allowed to set are:

* `lot_or_bundle`
* `proxy_or_reprint`
* `wrong_language`
* `wrong_card_number`
* `conflicting_marketplace`
* `missing_title`
* `unknown_section`

Price-relative anomalies (`suspicious_low_price`, `suspicious_high_price`)
that require comparison against the card's current price live in a later
validation service. The parser does no historical or z-score lookups.

---

## Layout signature

`layout_signature` is a deterministic SHA-256 over:

* whether `<a name="completed-auctions">` is present
* whether `<div id="full-prices">` is present
* the sorted set of `completed-auctions-*` div classes that wrap actual
  tables
* the sorted set of `js-*-completed-sale` anchor classes seen
* the sorted set of `<td class>` values used inside any
  `hoverable-rows sortable` table

The signature is independent of the data shown — two consecutive nightly
captures of the same card produce the same signature even when the prices
and titles differ. Used by Stage F monitoring to alert on selector drift
without alerting on traffic changes.

---

## Known limitations

* TCGPlayer rows have no marketplace item ID; dedup must rely on the
  fallback hash. A TCGPlayer listing that PriceCharting re-emits the next
  day with the exact same title and price would NOT be a duplicate emission
  (different `sale_date`) and will not collapse to a single key.
* The condition heuristic is intentionally narrow — it only fires on
  whitespace-delimited tokens like ` nm `, ` lp `, `damaged`. Sellers'
  freeform language (`gem mint plus`, `mint condition no flaws`) is not
  classified beyond the bucket.
* Japanese coverage is **not** supported in v1. The parser returns
  `unsupported_layout` and is structurally ready for a future JP-specific
  parser to be added without breaking the API.
* The wrong-card-number heuristic only runs when the caller supplies
  `expected_card_number=`. The parser does not sniff the card number from
  `internal_card_slug` because that slug in this repo is a PriceCharting
  product id, not a card number.
* `marketplace_country` is `None` for any host this parser is not certain
  about (including bare `ebay.com`).
* `parse_status="failed"` is reserved for unhandled internal errors; the
  parser never raises, but if a future change introduces a path that does,
  the result type will surface it.

---

## Fixture-refresh process

1. Fixtures live in `tests/fixtures/recent_sales/`.
2. To refresh, set the override flag and run the standalone helper:
   ```
   $env:FORCE=1
   python tests/fixtures/recent_sales/_refresh_fixtures.py
   ```
   The helper uses the same headers as `pokeprices_scraper_v8.py` and adds
   a 1 s sleep between requests. It only overwrites when `FORCE=1`.
3. After refreshing, re-run `python -m pytest tests/test_recent_sales_parser.py`
   and `python audit_recent_sales_fixtures.py` to confirm no parser
   regressions.
4. The refresh helper is **never** invoked by CI; it is engineer-driven.

---

## Next pilot stage

Block 4A-S1 is parser + fixtures + tests + audit-script only. The next
stage (Block 4A-S2 / Stage B) introduces a **100-card recent-sales pilot**:

1. Add a CLI flag to `pokeprices_scraper_v8.py` such as
   `--pilot-sales-list <file>` that, for those cards only, calls
   `parse_recent_sales()` against the HTML already fetched and writes the
   result into a NEW staging table (e.g. `recent_sales_pilot` in
   pokeprices-web's database).
2. No production read path. No public UI. Run for ~5 consecutive nights.
3. Inspect dedup behaviour (strong vs fallback keys), quarantine rate,
   conflicting-marketplace rate, layout-signature stability.
4. Acceptance: ≥98% rows match strong key on supported marketplaces;
   quarantined rate <5%; no row count delta on a quiet day for a quiet
   card.

The full staged plan lives in the Block 4A-S audit report (Stages A→H).

---

## Production isolation statement

Block 4A-S1 is committed and production-isolated:

* `pokeprices_scraper_v8.py` is unchanged. It does not import
  `recent_sales_parser`.
* `.github/workflows/nightly-scrape.yml` is unchanged.
* Current-price extraction, `daily_prices` writes, `card_volume` writes,
  image updates, eBay scraping, deal detection, and analytics jobs are all
  unchanged.
* No Supabase migration is created in this repo.
* The parser writes nothing to Supabase and does not import
  `requests` / `supabase`. The only external surface it touches is
  `bs4.BeautifulSoup`.
* Test fixtures are static HTML on disk. The fixture-refresh script is the
  only piece of code in this block that can touch the network, and it is
  off by default (`FORCE=1` required and `FORCE!=1` is the default).
