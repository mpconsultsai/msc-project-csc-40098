"""
Export training-gated rows from ``fakenews.tsv`` to ``fake_news_final.tsv`` (after step 10).

Keeps rows with ``image_option1_validity_score`` >= ``--min-score`` (default 75, inclusive).
Output has the same columns as the input. Paths resolve from the project root.

    python pipeline/src/11_cohort_export_final_tsv.py
    python pipeline/src/11_cohort_export_final_tsv.py --min-score 76

Custom input/output paths: ``--help``.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from _paths import PROJECT_ROOT

DEFAULT_INPUT = Path("data/fakenews.tsv")
DEFAULT_OUTPUT = Path("data/fake_news_final.tsv")
SCORE_COL = "image_option1_validity_score"


def _resolve(root: Path, p: Path) -> Path:
    """Resolve a CLI path relative to the project root when not absolute."""
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def main() -> int:
    """Filter ``fakenews.tsv`` by validity score and write ``fake_news_final.tsv``.

    Scans ``--input`` row by row; keeps rows with a numeric
    ``image_option1_validity_score`` >= ``--min-score``. Rows with empty or non-numeric scores
    are skipped.

    Args (CLI):
        ``--input``: Main table with merged validation columns (default ``data/fakenews.tsv``).
        ``--output``: Gated export path (default ``data/fake_news_final.tsv``).
        ``--min-score``: Inclusive threshold (default 75; use 76 for strictly above 75).

    Returns:
        ``0`` on success, ``1`` if input is missing, has no header, or lacks ``image_option1_validity_score``.
    """
    ap = argparse.ArgumentParser(description="Export fakenews rows with validity score >= min-score")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    ap.add_argument(
        "--min-score",
        type=int,
        default=75,
        metavar="N",
        help="Keep rows with validity score >= N (default 75; use 76 for strictly above 75)",
    )
    args = ap.parse_args()

    inp = _resolve(PROJECT_ROOT, args.input)
    out = _resolve(PROJECT_ROOT, args.output)

    if not inp.is_file():
        print(f"Missing input: {inp}", file=sys.stderr)
        return 1

    out.parent.mkdir(parents=True, exist_ok=True)
    min_sc = args.min_score
    kept = 0
    scanned = 0

    with inp.open(encoding="utf-8", newline="") as inf, out.open("w", encoding="utf-8", newline="") as outf:
        reader = csv.DictReader(inf, delimiter="\t")
        if not reader.fieldnames:
            print("Missing header in input.", file=sys.stderr)
            return 1
        if SCORE_COL not in reader.fieldnames:
            print(
                f"Input has no column {SCORE_COL!r}; run 10_cohort_merge_image_validation_into_fakenews.py first.",
                file=sys.stderr,
            )
            return 1
        w = csv.DictWriter(outf, fieldnames=reader.fieldnames, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for row in reader:
            scanned += 1
            raw = (row.get(SCORE_COL) or "").strip()
            if not raw:
                continue
            try:
                sc = int(raw)
            except ValueError:
                continue
            if sc >= min_sc:
                w.writerow({k: row.get(k, "") for k in reader.fieldnames})
                kept += 1

    print(
        f"Wrote {out} with {kept:,} row(s) (score >= {min_sc}; scanned {scanned:,} input row(s)).",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
