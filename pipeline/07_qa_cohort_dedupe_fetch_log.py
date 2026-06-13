"""
Deduplicate ``cohort_image_fetch.log`` to one row per ``sample_id`` (optional QA after step 06).

Keeps the latest ``ok`` row when any succeeded; otherwise the latest ``fail``. Does not change
which ids step 06 skips on resume — only removes duplicate log lines. Creates ``.bak`` unless
``--dry-run``. Default log: ``data/processed/images/cohort_image_fetch.log``.

    python pipeline/07_qa_cohort_dedupe_fetch_log.py
    python pipeline/07_qa_cohort_dedupe_fetch_log.py --dry-run
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = Path("data/processed/images/cohort_image_fetch.log")


def _resolve(p: Path) -> Path:
    """Resolve a CLI path relative to the project root when not absolute."""
    return p.resolve() if p.is_absolute() else (PROJECT_ROOT / p).resolve()


def _parse_ts(raw: str) -> datetime:
    """Parse ``ts_utc`` from a fetch-log row (ISO-8601, optional trailing ``Z``)."""
    s = (raw or "").strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _pick_best(rows: list[dict[str, str]]) -> dict[str, str]:
    """Choose the canonical log row for one ``sample_id``.

    Prefers the latest ``ok`` row; if none, the latest ``fail``; otherwise latest by timestamp.

    Args:
        rows: All log rows for a single ``sample_id``.

    Returns:
        The row to keep after deduplication.
    """
    oks = [r for r in rows if (r.get("status") or "").strip().lower() == "ok"]
    if oks:
        return max(oks, key=lambda r: _parse_ts(r.get("ts_utc") or ""))
    fails = [r for r in rows if (r.get("status") or "").strip().lower() == "fail"]
    if fails:
        return max(fails, key=lambda r: _parse_ts(r.get("ts_utc") or ""))
    return max(rows, key=lambda r: _parse_ts(r.get("ts_utc") or ""))


def main() -> int:
    """Deduplicate the cohort image fetch log to one line per ``sample_id``.

    Args (CLI):
        ``--log``: Fetch log path (default ``data/processed/images/cohort_image_fetch.log``).
        ``--dry-run``: Print counts only; do not write or backup.

    Returns:
        ``0`` on success, ``1`` if the log is missing or has no header.
    """
    ap = argparse.ArgumentParser(description="Deduplicate cohort_image_fetch.log (one row per sample_id)")
    ap.add_argument("--log", type=Path, default=DEFAULT_LOG)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    log_path = _resolve(args.log)
    if not log_path.is_file():
        print(f"Missing: {log_path}", file=sys.stderr)
        return 1

    with log_path.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        fieldnames = reader.fieldnames
        if not fieldnames:
            print("No header", file=sys.stderr)
            return 1
        by_sid: dict[str, list[dict[str, str]]] = defaultdict(list)
        total_read = 0
        skipped_no_sid = 0
        for row in reader:
            total_read += 1
            sid = (row.get("sample_id") or "").strip()
            if not sid:
                skipped_no_sid += 1
                continue
            by_sid[sid].append(row)

    n_out = len(by_sid)
    attributed = total_read - skipped_no_sid
    dup_removed = attributed - n_out

    print(f"Log: {log_path}")
    print(f"Unique sample_id: {n_out}")
    print(f"Total data rows read: {total_read}")
    if skipped_no_sid:
        print(f"Skipped (empty sample_id): {skipped_no_sid}")
    print(f"Duplicate rows collapsed: {dup_removed}")

    if args.dry_run:
        print("Dry run — no file written.")
        return 0

    bak = log_path.with_suffix(log_path.suffix + ".bak")
    shutil.copy2(log_path, bak)
    print(f"Backup: {bak}")

    chosen = [_pick_best(rows) for rows in by_sid.values()]
    chosen.sort(key=lambda r: _parse_ts(r.get("ts_utc") or ""))

    tmp = log_path.with_suffix(log_path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for row in chosen:
            w.writerow({k: row.get(k, "") for k in fieldnames})

    tmp.replace(log_path)
    print(f"Wrote deduplicated log ({n_out} lines + header).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
