"""
Deduplicate FakeNewsNet ``crawl_failures.jsonl`` to **one line per**
``(news_source, label, news_id)``, keeping the entry with the **latest** ``ts``.

The crawler loads the log as a **set of keys** only, so this does not change which ids are
considered “known failures” for skip logic — it only removes repeat events for the same story.

Creates ``<log>.bak`` next to the log before overwriting (unless ``--dry-run``).

    python scripts/03_qa_fnn_dedupe_crawl_failures.py
    python scripts/03_qa_fnn_dedupe_crawl_failures.py --dry-run
    python scripts/03_qa_fnn_dedupe_crawl_failures.py --log path/to/crawl_failures.jsonl
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = Path("data/processed/fakenewsnet/crawl_failures.jsonl")


def _parse_ts(raw: str | None) -> datetime | None:
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    try:
        # fromisoformat handles "+00:00" and "Z" in 3.11+
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _key_from_obj(o: dict) -> tuple[str, str, str] | None:
    if o.get("event") != "failed":
        return None
    try:
        src = str(o["news_source"]).strip().lower()
        lbl = str(o["label"]).strip().lower()
        nid = str(o["news_id"]).strip()
        if src and lbl and nid:
            return (src, lbl, nid)
    except (KeyError, TypeError):
        pass
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Deduplicate FNN crawl_failures.jsonl (latest ts wins)")
    ap.add_argument(
        "--log",
        type=Path,
        default=DEFAULT_LOG,
        help=f"Path to JSONL (default: {DEFAULT_LOG})",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print counts only; do not write or backup",
    )
    args = ap.parse_args()

    log_path = args.log if args.log.is_absolute() else (PROJECT_ROOT / args.log).resolve()
    if not log_path.is_file():
        print("Missing log:", log_path, file=sys.stderr)
        return 1

    raw_lines = log_path.read_text(encoding="utf-8").splitlines()
    total_nonempty = sum(1 for ln in raw_lines if ln.strip())

    best: dict[tuple[str, str, str], tuple[datetime, int, str]] = {}
    skipped_malformed = 0
    skipped_non_failed = 0
    failed_lines_with_key = 0
    line_index = 0

    for line in raw_lines:
        line_index += 1
        s = line.strip()
        if not s:
            continue
        try:
            o = json.loads(s)
        except json.JSONDecodeError:
            skipped_malformed += 1
            continue
        if not isinstance(o, dict):
            skipped_malformed += 1
            continue
        key = _key_from_obj(o)
        if key is None:
            if o.get("event") == "failed":
                skipped_malformed += 1
            else:
                skipped_non_failed += 1
            continue

        failed_lines_with_key += 1
        ts_raw = o.get("ts")
        ts = _parse_ts(ts_raw) if isinstance(ts_raw, str) else None
        if ts is None:
            ts = datetime.min.replace(tzinfo=timezone.utc)

        prev = best.get(key)
        if prev is None or ts >= prev[0]:
            best[key] = (ts, line_index, json.dumps(o, ensure_ascii=False))

    dedup_lines = [t[2] for t in sorted(best.values(), key=lambda x: (x[0], x[1]))]

    print(f"Log: {log_path}")
    print(f"Non-empty input lines: {total_nonempty}")
    print(f"Parsed failed events with key: {failed_lines_with_key}")
    print(f"Unique failed-story keys (output lines): {len(dedup_lines)}")
    print(f"Removed duplicate events: {failed_lines_with_key - len(dedup_lines)}")
    if skipped_malformed:
        print(f"Skipped malformed / unkeyable failed lines: {skipped_malformed}")
    if skipped_non_failed:
        print(f"Skipped non-failed events: {skipped_non_failed}")

    if args.dry_run:
        print("Dry run — no file written.")
        return 0

    bak = log_path.with_suffix(log_path.suffix + ".bak")
    shutil.copy2(log_path, bak)
    print(f"Backup: {bak}")

    tmp = log_path.with_suffix(log_path.suffix + ".tmp")
    tmp.write_text("\n".join(dedup_lines) + ("\n" if dedup_lines else ""), encoding="utf-8")
    tmp.replace(log_path)
    print(f"Wrote deduplicated log ({len(dedup_lines)} lines).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
