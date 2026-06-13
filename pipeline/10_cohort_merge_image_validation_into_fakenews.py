"""
Merge step 09 validation scores into ``fakenews.tsv`` by ``sample_id``.

Adds ``image_option1_validity_score``, ``image_option1_qc_flags``, and
``image_option1_training_eligible`` (``true`` when score >= ``--min-score``). Streams the main
TSV and writes a ``*.image_validation_merge.bak`` backup unless ``--no-backup``. Close
``fakenews.tsv`` in the IDE before running on large files. Paths resolve from the project root.

    python pipeline/10_cohort_merge_image_validation_into_fakenews.py
    python pipeline/10_cohort_merge_image_validation_into_fakenews.py --dry-run

``--min-score``, backup, and progress interval: ``--help``.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_FAKENEWS = Path("data/fakenews.tsv")
DEFAULT_VALIDATION = Path("data/processed/cohorts/image_validation/cohort_image_validation.tsv")

COL_SCORE = "image_option1_validity_score"
COL_FLAGS = "image_option1_qc_flags"
COL_ELIGIBLE = "image_option1_training_eligible"
NEW_COLS = [COL_SCORE, COL_FLAGS, COL_ELIGIBLE]


def _resolve(root: Path, p: Path) -> Path:
    """Resolve a CLI path relative to the project root when not absolute."""
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def _load_validation(path: Path) -> dict[str, tuple[str, str]]:
    """Load validation TSV keyed by ``sample_id``.

    Args:
        path: ``cohort_image_validation.tsv`` from step 09.

    Returns:
        Map ``sample_id -> (validity_score, flags)``; later rows overwrite earlier ones.
    """
    out: dict[str, tuple[str, str]] = {}
    with path.open(encoding="utf-8", newline="") as fp:
        r = csv.DictReader(fp, delimiter="\t")
        for row in r:
            sid = (row.get("sample_id") or "").strip()
            if not sid:
                continue
            sc = (row.get("validity_score") or "").strip()
            fl = (row.get("flags") or "").strip()
            out[sid] = (sc, fl)
    return out


def main() -> int:
    """Merge validation scores into ``fakenews.tsv`` and set training-eligibility columns.

    Loads the validation TSV into memory, streams ``--fakenews`` row by row, and writes a temp
    file that replaces the original after optional backup.

    Args (CLI):
        ``--fakenews``: Main table (default ``data/fakenews.tsv``).
        ``--validation``: Step 09 output (default ``cohort_image_validation.tsv``).
        ``--min-score``: ``image_option1_training_eligible=true`` when score >= N (default 75).
        ``--dry-run``: Count matches only; do not write.
        ``--no-backup``: Skip ``*.image_validation_merge.bak`` before replace.
        ``--progress-every``: Stderr progress every N rows (default 50000; 0 = quiet).

    Returns:
        ``0`` on success, ``1`` if inputs are missing or the fakenews header is invalid.
    """
    ap = argparse.ArgumentParser(description="Merge cohort image validation scores into fakenews.tsv")
    ap.add_argument("--fakenews", type=Path, default=DEFAULT_FAKENEWS)
    ap.add_argument("--validation", type=Path, default=DEFAULT_VALIDATION)
    ap.add_argument("--min-score", type=int, default=75, help="Training eligible if score >= this (default 75)")
    ap.add_argument("--dry-run", action="store_true", help="Parse and count only; do not write")
    ap.add_argument("--no-backup", action="store_true", help="Do not write *.image_validation_merge.bak before replace")
    ap.add_argument(
        "--progress-every",
        type=int,
        default=50_000,
        metavar="N",
        help="Print progress to stderr every N data rows (0 = quiet; default 50000)",
    )
    args = ap.parse_args()

    fn_path = _resolve(PROJECT_ROOT, args.fakenews)
    val_path = _resolve(PROJECT_ROOT, args.validation)

    if not fn_path.is_file():
        print(f"Missing fakenews TSV: {fn_path}", file=sys.stderr)
        return 1
    if not val_path.is_file():
        print(f"Missing validation TSV: {val_path}", file=sys.stderr)
        return 1

    val_map = _load_validation(val_path)
    print(f"Loaded {len(val_map):,} validation row(s).", file=sys.stderr)

    tmp_path = fn_path.with_suffix(fn_path.suffix + ".merge_tmp")
    matched = 0
    row_num = 0
    min_sc = args.min_score
    prog = max(0, int(args.progress_every))

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
                row_num += 1
                if prog and row_num % prog == 0:
                    print(f"dry-run: scanned {row_num:,} row(s)…", file=sys.stderr, flush=True)
                sid = (row.get("sample_id") or "").strip()
                if sid in val_map:
                    matched += 1
            print(f"dry-run: would update {matched:,} row(s) with validation data.", file=sys.stderr)
            return 0

        with tmp_path.open("w", encoding="utf-8", newline="") as outf:
            w = csv.DictWriter(outf, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
            w.writeheader()
            for row in reader:
                row_num += 1
                if prog and row_num % prog == 0:
                    print(f"merge: wrote {row_num:,} row(s), matched so far {matched:,}…", file=sys.stderr, flush=True)
                sid = (row.get("sample_id") or "").strip()
                if sid in val_map:
                    sc, fl = val_map[sid]
                    row[COL_SCORE] = sc
                    row[COL_FLAGS] = fl
                    try:
                        score_i = int(sc)
                        row[COL_ELIGIBLE] = "true" if score_i >= min_sc else "false"
                    except ValueError:
                        row[COL_ELIGIBLE] = ""
                    matched += 1
                else:
                    row[COL_SCORE] = ""
                    row[COL_FLAGS] = ""
                    row[COL_ELIGIBLE] = ""
                w.writerow({k: row.get(k, "") for k in fieldnames})

    if not args.no_backup:
        bak = fn_path.with_suffix(fn_path.suffix + ".image_validation_merge.bak")
        shutil.copy2(fn_path, bak)
        print(f"Backup: {bak}", file=sys.stderr)

    tmp_path.replace(fn_path)
    print(f"Wrote {fn_path} (merged {matched:,} row(s); columns {NEW_COLS}).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
