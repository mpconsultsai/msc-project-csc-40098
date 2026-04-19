"""
Merge ``cohort_image_fetch.log`` into ``fakenews.tsv`` by ``sample_id`` (reference columns).

Adds / updates:

- ``cohort_image_fetch_status`` — ``ok`` | ``fail`` if this ``sample_id`` appears in the cohort fetch log; empty otherwise.
- ``cohort_image_local_path`` — relative path from successful fetches (as logged).
- ``cohort_image_fetch_detail`` — failure ``detail`` when status is ``fail``; empty for ``ok``.
- ``cohort_multimodal_image_ok`` — ``true`` if status ``ok``, ``false`` if ``fail``, empty if never logged for this cohort run.

Duplicate ``sample_id`` lines in the log: **prefer ``ok`` over ``fail``**; if both same status, keep the **later** ``ts_utc``.

**Interpretation:** Any row with a non-empty ``image_option1_validity_score`` should also have
``cohort_multimodal_image_ok=true`` and a ``cohort_image_local_path`` (validation only scans ``ok`` log lines).
Rows with ``fail`` in the log never get a validity score. Rows absent from the log were not part of this
cohort fetch attempt.

Streams ``fakenews.tsv``; writes ``*.cohort_fetch_merge.bak`` unless ``--no-backup``.

    python scripts/13_cohort_merge_fetch_log_into_fakenews.py
    python scripts/13_cohort_merge_fetch_log_into_fakenews.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_FAKENEWS = Path("data/fakenews.tsv")
DEFAULT_FETCH_LOG = Path("data/processed/images/cohort_image_fetch.log")

COL_STATUS = "cohort_image_fetch_status"
COL_PATH = "cohort_image_local_path"
COL_DETAIL = "cohort_image_fetch_detail"
COL_OK = "cohort_multimodal_image_ok"
NEW_COLS = [COL_STATUS, COL_PATH, COL_DETAIL, COL_OK]


def _resolve(root: Path, p: Path) -> Path:
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def _ts_sort_key(raw: str) -> str:
    """ISO-ish timestamps sort lexicographically for same format."""
    return (raw or "").strip()


def _load_fetch_log(path: Path) -> dict[str, dict[str, str]]:
    """sample_id -> {status, local_path, detail, ts_utc}."""
    best: dict[str, dict[str, str]] = {}
    with path.open(encoding="utf-8", newline="") as fp:
        r = csv.DictReader(fp, delimiter="\t")
        for row in r:
            sid = (row.get("sample_id") or "").strip()
            if not sid:
                continue
            st = (row.get("status") or "").strip().lower()
            ts = _ts_sort_key(row.get("ts_utc") or "")
            lp = (row.get("local_path") or "").strip().replace("\\", "/")
            det = (row.get("detail") or "").strip()
            rec = {"status": st, "local_path": lp, "detail": det, "ts_utc": ts}
            if sid not in best:
                best[sid] = rec
                continue
            old = best[sid]
            old_ok = old["status"] == "ok"
            new_ok = st == "ok"
            if new_ok and not old_ok:
                best[sid] = rec
            elif old_ok and not new_ok:
                pass
            elif ts > old["ts_utc"]:
                best[sid] = rec
    return best


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge cohort_image_fetch.log into fakenews.tsv")
    ap.add_argument("--fakenews", type=Path, default=DEFAULT_FAKENEWS)
    ap.add_argument("--fetch-log", type=Path, default=DEFAULT_FETCH_LOG)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-backup", action="store_true")
    args = ap.parse_args()

    fn_path = _resolve(PROJECT_ROOT, args.fakenews)
    log_path = _resolve(PROJECT_ROOT, args.fetch_log)

    if not fn_path.is_file():
        print(f"Missing fakenews TSV: {fn_path}", file=sys.stderr)
        return 1
    if not log_path.is_file():
        print(f"Missing fetch log: {log_path}", file=sys.stderr)
        return 1

    fetch_map = _load_fetch_log(log_path)
    n_ok = sum(1 for v in fetch_map.values() if v["status"] == "ok")
    n_fail = sum(1 for v in fetch_map.values() if v["status"] == "fail")
    print(
        f"Loaded fetch log: {len(fetch_map):,} unique sample_id(s) ({n_ok:,} ok, {n_fail:,} fail).",
        file=sys.stderr,
    )

    tmp_path = fn_path.with_suffix(fn_path.suffix + ".fetch_merge_tmp")
    matched = 0

    with fn_path.open(encoding="utf-8", newline="") as inf:
        reader = csv.DictReader(inf, delimiter="\t")
        if not reader.fieldnames:
            print("Empty or invalid fakenews header.", file=sys.stderr)
            return 1
        fieldnames = list(reader.fieldnames)
        for c in NEW_COLS:
            if c not in fieldnames:
                fieldnames.append(c)

        if args.dry_run:
            for row in reader:
                sid = (row.get("sample_id") or "").strip()
                if sid in fetch_map:
                    matched += 1
            print(f"dry-run: would set fetch columns on {matched:,} row(s).", file=sys.stderr)
            return 0

        with tmp_path.open("w", encoding="utf-8", newline="") as outf:
            w = csv.DictWriter(outf, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
            w.writeheader()
            for row in reader:
                sid = (row.get("sample_id") or "").strip()
                if sid in fetch_map:
                    fr = fetch_map[sid]
                    st = fr["status"]
                    row[COL_STATUS] = st
                    row[COL_PATH] = fr["local_path"] if st == "ok" else ""
                    row[COL_DETAIL] = fr["detail"] if st == "fail" else ""
                    row[COL_OK] = "true" if st == "ok" else "false"
                    matched += 1
                else:
                    row[COL_STATUS] = ""
                    row[COL_PATH] = ""
                    row[COL_DETAIL] = ""
                    row[COL_OK] = ""
                w.writerow({k: row.get(k, "") for k in fieldnames})

    if not args.no_backup:
        bak = fn_path.with_suffix(fn_path.suffix + ".cohort_fetch_merge.bak")
        shutil.copy2(fn_path, bak)
        print(f"Backup: {bak}", file=sys.stderr)

    tmp_path.replace(fn_path)
    print(f"Wrote {fn_path} (fetch merge on {matched:,} row(s); columns {NEW_COLS}).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
