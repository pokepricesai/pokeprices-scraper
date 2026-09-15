"""Helpers to produce a tiny structurally-faithful AllPricesToday.json.gz
fixture at test time.

We do NOT commit the ~5 MB live file. Instead each test builds a small
gzip on demand from an in-memory Python dict that matches MTGJSON's
AllPrices/AllPricesToday shape:

    {
      "meta": { "date": "YYYY-MM-DD", "version": "..." },
      "data": {
        "<mtgjson_uuid>": {
          "<market>": {
            "<provider>": {
              "currency": "USD" | "EUR" | ...,
              "buylist" | "retail": {
                 "normal" | "foil" | "etched": { "YYYY-MM-DD": <price> }
              }
            }
          }
        }
      }
    }

Two helpers:
    * :func:`build_bytes`   — return the gzip'd bytes for the given
                              payload dict.
    * :func:`build_file`    — write those bytes to a tempfile-like path
                              and return the path.
"""
from __future__ import annotations

import gzip
import io
import json
from pathlib import Path


VALID_MINIMAL_PAYLOAD = {
    "meta": {
        "date": "2026-09-14",
        "version": "5.3.0+20260914",
    },
    "data": {
        "aaaaaaaa-0000-0000-0000-000000000001": {
            "paper": {
                "tcgplayer": {
                    "currency": "USD",
                    "retail": {
                        "normal": {"2026-09-14": 4.25},
                        "foil":   {"2026-09-14": 12.90},
                    },
                    "buylist": {
                        "normal": {"2026-09-14": 2.10},
                    },
                },
                "cardmarket": {
                    "currency": "EUR",
                    "retail": {
                        "normal": {"2026-09-14": 3.99},
                    },
                },
            },
        },
        "aaaaaaaa-0000-0000-0000-000000000002": {
            "mtgo": {
                "cardhoarder": {
                    "currency": "USD",
                    "retail": {
                        "normal": {"2026-09-14": 0.55},
                    },
                },
            },
        },
    },
}


MALFORMED_MISSING_DATE = {
    "meta": {"version": "5.3.0+broken"},   # no 'date'
    "data": {},
}


def build_bytes(payload: dict) -> bytes:
    """Gzip-encode ``payload`` as JSON."""
    raw = json.dumps(payload).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(raw)
    return buf.getvalue()


def build_file(payload: dict, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(build_bytes(payload))
    return path
