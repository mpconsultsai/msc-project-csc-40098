"""
Export rows from ``fakenews.tsv`` that pass the option-1 training gate (default: score >= 75).

Writes ``data/fake_news_final.tsv`` with the **same columns** as the input (typically the 50k cohort
subset that is both successfully fetched and QC-eligible).

Default threshold is **inclusive** (>= 75), matching ``image_option1_training_eligible=true``.
Use ``--min-score 76`` if you require strictly greater than 75.

    python scripts/14_cohort_export_final_tsv.py
    python scripts/14_cohort_export_final_tsv.py --min-score 76
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_INPUT = Path("data/fakenews.tsv")
DEFAULT_OUTPUT = Path("data/fake_news_final.tsv")
SCORE_COL = "image_option1_validity_score"


def _resolve(root: Path, p: Path) -> Path:
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def main() -> int:
    ap = argparse.ArgumentParser(description="Export fakenews rows with option-1 score >= min-score")
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
                f"Input has no column {SCORE_COL!r}; run 12_cohort_merge_option1_into_fakenews.py first.",
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
