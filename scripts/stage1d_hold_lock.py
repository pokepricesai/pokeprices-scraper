"""Test helper — acquire the Stage 1D run lock and hold it for N seconds.

Used by scripts/stage1d_gate9_concurrency.ps1 to guarantee the lock is
held while a second pipeline invocation attempts to acquire it.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import mtgjson_ingestion as mi          # noqa: E402
from mtg_stage1d import lock            # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hold-seconds", type=int, default=30)
    p.add_argument("--label", default="gate9_holder")
    args = p.parse_args()

    s = mi.build_default_supabase_client()
    if s is None:
        print("SUPABASE creds missing", file=sys.stderr)
        return 3
    h = lock.acquire(s, "mtg_daily_ingest", args.label,
                     metadata={"purpose": "gate9_holder"})
    print(f"HOLDER acquired lock: {h.acquired_by} at {h.acquired_at}", flush=True)
    try:
        for i in range(args.hold_seconds):
            time.sleep(1)
            if i % 5 == 0:
                lock.heartbeat(s, h)
                print(f"HOLDER heartbeat {i+1}/{args.hold_seconds}", flush=True)
    finally:
        lock.release(s, h)
        print(f"HOLDER released lock", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
