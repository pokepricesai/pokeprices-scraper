"""Small wrappers around MTGJSON AllPricesToday.

Kept in its own module so unit tests can exercise gzip reading, ijson
parsing, meta.data.date extraction, checksum success + failure and
malformed input without relying on the network.
"""
from __future__ import annotations

import gzip
import hashlib
import logging
from pathlib import Path
from urllib.request import Request, urlopen

log = logging.getLogger("mtg_stage1d.allprices_today")

TODAY_URL     = "https://mtgjson.com/api/v5/AllPricesToday.json.gz"
TODAY_SHA_URL = "https://mtgjson.com/api/v5/AllPricesToday.json.gz.sha256"

# MTGJSON's CDN returns 403 for the default urllib UA. Provide a proper
# identifier with contact info per their operational preferences.
USER_AGENT = (
    "MTGPricesStage1D/1.0 (+https://www.pokeprices.io; contact@pokeprices.io)"
)


def _get(url: str, timeout: float):
    from urllib.request import Request, urlopen
    return urlopen(Request(url, headers={"User-Agent": USER_AGENT}), timeout=timeout)


class ChecksumMismatch(RuntimeError):
    pass


class MalformedSource(RuntimeError):
    pass


def download_and_verify(workspace: Path) -> tuple[Path, str, str]:
    """Return (local_path, expected_sha, actual_sha).

    Raises :class:`ChecksumMismatch` if they disagree.
    """
    workspace.mkdir(parents=True, exist_ok=True)
    with _get(TODAY_SHA_URL, timeout=60) as r:
        expected = r.read().decode("utf-8").strip()
    with _get(TODAY_URL, timeout=1800) as r:
        payload = r.read()
    local = workspace / "AllPricesToday.json.gz"
    local.write_bytes(payload)
    (local.parent / "AllPricesToday.json.gz.sha256").write_text(expected, encoding="utf-8")
    actual = hashlib.sha256(payload).hexdigest()
    if actual != expected:
        raise ChecksumMismatch(
            f"AllPricesToday SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return local, expected, actual


def verify_checksum(path: Path, expected_sha: str) -> str:
    """Compute SHA-256 of a local file and raise if it does not match."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    actual = h.hexdigest()
    if actual != expected_sha:
        raise ChecksumMismatch(
            f"AllPricesToday SHA-256 mismatch: expected={expected_sha} actual={actual}"
        )
    return actual


def read_meta_date(allprices_gz: Path) -> str:
    """Return the ``meta.date`` (or ``meta.data.date`` on older builds)
    from the gzip'd MTGJSON payload, WITHOUT parsing the full data
    section.

    Raises :class:`MalformedSource` if neither field is present.
    """
    import ijson
    # MTGJSON v5 emits ``{"meta": {"date": "..."}, ...}``. Some historic
    # builds nest it as ``{"meta": {"data": {"date": "..."}}, ...}``.
    # Support both.
    with gzip.open(allprices_gz, "rb") as raw:
        # Walk parse events; short-circuit as soon as we find a date.
        for prefix, event, value in ijson.parse(raw):
            if prefix == "meta.date" and event == "string" and value:
                return str(value)
            if prefix == "meta.data.date" and event == "string" and value:
                return str(value)
            # Break out once we hit the huge data object.
            if prefix.startswith("data.") or prefix == "data":
                break
    raise MalformedSource("meta.date not found in AllPricesToday file")


def read_meta_full(allprices_gz: Path) -> dict:
    """Return the full ``meta`` object from the gzip'd MTGJSON payload
    WITHOUT parsing the huge data section.

    Normalised to the shape ``{"data": {"date": ..., "version": ...},
    "meta": {"date": ..., "version": ...}}`` that mtgjson_ingestion
    stores in ``market_import_runs.notes.mtgjson_meta``.
    """
    import ijson
    with gzip.open(allprices_gz, "rb") as raw:
        for k, v in ijson.kvitems(raw, "meta"):
            if isinstance(v, dict):
                # v5: ``meta`` is ``{"date": "...", "version": "..."}``
                if "date" in v or "version" in v:
                    return {"data": v, "meta": v}
                # legacy: ``meta.data`` nests further
                if "data" in v and isinstance(v["data"], dict):
                    return {"data": v["data"], "meta": v["data"]}
    return {}
