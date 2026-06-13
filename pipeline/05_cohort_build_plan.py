"""
Build a fixed, seeded, stratified multimodal cohort plan from ``data/fakenews.tsv``.

Reads rows with ``has_image_ref=true``, splits them by ``(dataset, label_binary)``, allocates
``--n`` primary slots proportionally per stratum, and adds reserve rows for image-fetch backfill.
Writes a plan TSV for ``06_cohort_fetch_images.py``. Paths resolve from the project root.

    python pipeline/05_cohort_build_plan.py
    python pipeline/05_cohort_build_plan.py --n 50000 --seed 42

Fakeddit split filtering, reserve sizing, and output shuffle: ``--help`` or
``pipeline/DATASETS_OVERVIEW.md`` §7.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _largest_remainder_allocation(counts: dict[str, int], total: int) -> dict[str, int]:
    """Allocate ``total`` integer slots across strata in proportion to ``counts``.

    Args:
        counts: Eligible row count per stratum key.
        total: Target number of primary slots to distribute.

    Returns:
        Per-key integer allocation summing exactly to ``min(total, sum(counts))`` (or zeros if empty).
    """
    keys = list(counts.keys())
    n_eligible = sum(counts.values())
    if n_eligible == 0 or total <= 0:
        return {k: 0 for k in keys}
    raw = {k: counts[k] * total / n_eligible for k in keys}
    floors = {k: int(raw[k]) for k in keys}
    rem = total - sum(floors.values())
    frac = sorted(keys, key=lambda k: raw[k] - floors[k], reverse=True)
    out = dict(floors)
    for i in range(rem):
        out[frac[i % len(frac)]] += 1
    return out


def main() -> int:
    """Build stratified cohort plan TSV from ``fakenews.tsv``.

    Filters to multimodal-eligible rows, applies Fakeddit split rules, assigns primary and reserve
    roles per stratum, optionally shuffles output order, and writes the plan for step 06.

    Args (CLI):
        ``--input-tsv``: Consolidated table (default ``data/fakenews.tsv``).
        ``--n`` / ``--seed``: Target cohort size and RNG seed (default 50000 / 42).
        ``--reserve-multiplier``: Extra reserve rows per stratum after primary allocation.
        ``--fakeddit-splits`` / ``--include-fakeddit-test``: Which Fakeddit official splits to include.
        ``--no-shuffle-output``: Keep stratum-block order instead of interleaved shuffle.

    Returns:
        ``0`` on success, ``1`` if input is missing or ``--fakeddit-splits`` is empty.
    """
    ap = argparse.ArgumentParser(description="Build stratified multimodal cohort plan (primary + reserves)")
    ap.add_argument(
        "--input-tsv",
        type=Path,
        default=Path("data/fakenews.tsv"),
        help="Canonical consolidated TSV (default: data/fakenews.tsv)",
    )
    ap.add_argument("--n", type=int, default=50_000, help="Target multimodal successes (default: 50000)")
    ap.add_argument("--seed", type=int, default=42, help="RNG seed (default: 42)")
    ap.add_argument(
        "--reserve-multiplier",
        type=float,
        default=3.0,
        help="Per stratum, queue at least this many extra rows after primary (capped by stratum size).",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output plan TSV (default: data/processed/cohorts/multimodal_plan_n{N}_seed{S}.tsv)",
    )
    ap.add_argument(
        "--fakeddit-splits",
        type=str,
        default="train,validation",
        help="Comma-separated split_official values allowed for dataset=fakeddit (default: train,validation).",
    )
    ap.add_argument(
        "--include-fakeddit-test",
        action="store_true",
        help="Include Fakeddit official test rows in the eligible pool (overrides --fakeddit-splits).",
    )
    ap.add_argument(
        "--no-shuffle-output",
        action="store_true",
        help="Write rows in stratum iteration order (legacy). Default: shuffle all plan rows with --seed so image fetch interleaves corpora.",
    )
    args = ap.parse_args()

    if args.include_fakeddit_test:
        fd_allowed = {"train", "validation", "test"}
    else:
        fd_allowed = {s.strip().lower() for s in args.fakeddit_splits.split(",") if s.strip()}
        if not fd_allowed:
            print("Empty --fakeddit-splits", file=sys.stderr)
            return 1

    inp = (PROJECT_ROOT / args.input_tsv).resolve() if not args.input_tsv.is_absolute() else args.input_tsv
    if not inp.is_file():
        print("Missing input:", inp, file=sys.stderr)
        return 1

    rng = random.Random(args.seed)
    strata: dict[str, list[dict[str, str]]] = defaultdict(list)

    with inp.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        for row in reader:
            if (row.get("has_image_ref") or "").strip().lower() != "true":
                continue
            url = (row.get("image_ref") or "").strip()
            if not url:
                continue
            ds = (row.get("dataset") or "").strip()
            lb = (row.get("label_binary") or "").strip()
            if not ds:
                continue
            sid = (row.get("sample_id") or "").strip()
            if not sid:
                continue
            split_o = (row.get("split_official") or "").strip().lower()
            if ds.lower() == "fakeddit":
                if split_o not in fd_allowed:
                    continue
            key = f"{ds}\t{lb}"
            strata[key].append(
                {
                    "dataset": ds,
                    "label_binary": lb,
                    "split_official": split_o,
                    "sample_id": sid,
                    "image_ref": url,
                    "stratum_key": key,
                }
            )

    counts = {k: len(v) for k, v in strata.items()}
    n_eligible = sum(counts.values())
    if n_eligible < args.n:
        print(
            f"Warning: only {n_eligible} eligible rows (has_image_ref + image_ref); target {args.n}",
            file=sys.stderr,
        )
    targets = _largest_remainder_allocation(counts, min(args.n, n_eligible))

    out_rows: list[dict[str, str]] = []
    for key, rows in strata.items():
        rows_copy = list(rows)
        rng.shuffle(rows_copy)
        t_h = targets.get(key, 0)
        primary = rows_copy[:t_h]
        rest = rows_copy[t_h:]
        max_reserve = min(len(rest), max(int(t_h * args.reserve_multiplier), t_h * 2))
        reserve = rest[:max_reserve]
        for i, r in enumerate(primary):
            out_rows.append(
                {
                    **r,
                    "plan_role": "primary",
                    "stratum_order": str(i),
                }
            )
        for j, r in enumerate(reserve):
            out_rows.append(
                {
                    **r,
                    "plan_role": "reserve",
                    "stratum_order": str(t_h + j),
                }
            )

    if not args.no_shuffle_output:
        rng.shuffle(out_rows)

    out_path = args.out
    if out_path is None:
        out_dir = PROJECT_ROOT / "data" / "processed" / "cohorts"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"multimodal_plan_n{args.n}_seed{args.seed}.tsv"
    else:
        out_path = (PROJECT_ROOT / out_path).resolve() if not out_path.is_absolute() else out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "dataset",
        "label_binary",
        "split_official",
        "sample_id",
        "image_ref",
        "stratum_key",
        "plan_role",
        "stratum_order",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in out_rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    print("Eligible rows:", n_eligible)
    print("Strata:", len(strata))
    print("Target total (primary allocation):", sum(targets.values()))
    print("Plan rows (primary+reserve):", len(out_rows))
    print("Wrote", out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
