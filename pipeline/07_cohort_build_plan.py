"""
Build a **fixed, seeded, stratified** multimodal cohort plan (primary + per-stratum reserves).

Rows are stratified by ``(dataset, label_binary)`` and allocated proportionally to ``--n`` (default
50000). Within each stratum, a deterministic shuffle picks **primary** rows first, then **reserve**
rows for backfill when image download fails.

**Fakeddit official split (default):** only ``split_official`` in ``train`` and ``validation`` are
eligible — **``test`` is excluded** so the training cohort does not leak the public test set. Use
``--include-fakeddit-test`` to include all three splits (not recommended for benchmark-aligned training).
**FakeNewsNet:** all eligible rows are kept (``split_official`` is often blank in the consolidated TSV).

Output TSV (default ``data/processed/cohorts/multimodal_plan_n{N}_seed{SEED}.tsv``) columns::

    dataset, label_binary, split_official, sample_id, image_ref, stratum_key, plan_role, stratum_order

**Next step:** run ``08_cohort_fetch_images.py`` on this plan.

**Row order:** the full plan is **shuffled** with the same ``--seed`` after rows are built. That avoids
listing one entire corpus (e.g. all Fakeddit primary+reserve) before another when ``fakenews.tsv`` is
ordered by ``dataset``, which would otherwise starve downstream fetches for the second corpus until
tens of thousands of rows are processed.

Paths resolve from the project root (parent of ``pipeline/``).
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
    """Proportional integer allocation summing exactly to ``total``."""
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
