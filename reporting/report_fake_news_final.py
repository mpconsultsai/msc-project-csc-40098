"""
Summarise ``data/fake_news_final.tsv`` (validity-score–gated multimodal cohort) for reports / thesis.

Writes Markdown + JSON under ``outputs/fake_news_final_report/`` by default.

**Inputs:** existing ``fake_news_final.tsv`` (from ``pipeline/11_cohort_export_final_tsv.py``). Safe to re-run anytime.

    python reporting/report_fake_news_final.py
    python reporting/report_fake_news_final.py --input data/fake_news_final.tsv --out-dir outputs/fake_news_final_report
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

IMAGE_VALIDATION_SCRIPT = "pipeline/08_cohort_image_validation.py"


def _markdown_image_validation_methodology() -> str:
    """Static description aligned with ``08_cohort_image_validation.py`` (keep in sync manually)."""
    return "\n".join(
        [
            "## How `image_option1_validity_score` is computed (heuristic image QC)\n",
            "Scores are produced by **`"
            + IMAGE_VALIDATION_SCRIPT
            + "`** (post-download sweep). The value is an integer **1–100** heuristic: **higher ≈ more suitable "
            "as a static raster for a vision encoder**, not semantic correctness, aesthetic quality, or label "
            "veracity.\n",
            "### Measured from each image file",
            "- **Decode** with Pillow; record **format**, **width**, **height**.",
            "- **Animation:** `n_frames > 1` → treated as animated (multi-frame GIF/WebP).",
            "- **Aspect ratio:** `max(w,h)/min(w,h)`; flag **extreme_aspect** if ≥ 6.",
            "- **Tiny image:** flag **tiny_image** if shorter side **&lt; 48** px.",
            "- **Greyscale stats** (image resized so longest side ≤ `--max-side-stats`, default **512**):",
            "  - **Histogram entropy** (256-bin, Shannon bits) on grey levels.",
            "  - **Pixel variance**; flags **very_low_entropy** (&lt; 2.0) and **low_variance** (&lt; 80).",
            "",
            "### Score assembly (then clamp 1–100)",
            "Sum of five parts, then **caps**, then clamp:",
            "1. **Resolution** (up to **24** pts): points from **shorter side** thresholds (e.g. ≥256, ≥224, …).",
            "2. **Aspect** (up to **18** pts): narrower aspect ratios score higher.",
            "3. **Static vs animated** (**18** if static, **0** if animated).",
            "4. **Format** (up to **12** pts): JPEG/WebP highest; PNG/GIF lower; other/unknown lower still.",
            "5. **Texture** (up to **30** pts, capped in code): entropy band + variance penalties.",
            "**Caps (after the sum):** if **extreme_aspect** → score ≤ **55**; **tiny_image** → ≤ **35**; **animated** → ≤ **45**. Decode failure → **1**.",
            "",
            "### What this validation does *not* do",
            "- No OCR or text-on-image detection; no CLIP/semantic model; **`phash`** (if installed) is **not** part of the score.",
            "- This cohort file keeps rows with **score ≥ 75** (`image_option1_training_eligible=true`).\n",
        ]
    )


def _references_payload() -> dict:
    """Structured refs for JSON; aligns with repo docs (no single external paper for the composite score)."""
    return {
        "internal_project_documents": [
            {
                "path": "pipeline/DATA_PIPELINE_FILES_REFERENCE.md",
                "note": "Tracked inventory of scripts and canonical output paths (always in git).",
            },
            {
                "path": "pipeline/DATASETS_OVERVIEW.md",
                "note": "Unified TSV conventions: separate has_image_ref from image_download_ok / training-ready; multimodal cohort filtering expectations.",
            },
            {
                "path": "documents/msc_decisions_log.md",
                "note": "Optional local notes (gitignored by default): cohort fetch scope, validity-score threshold, training eligibility decisions.",
            },
            {
                "path": "documents/msc_proposal.tex",
                "note": "Optional local thesis/proposal (gitignored by default): SMART goals; risks on broken/missing images.",
            },
            {
                "path": "pipeline/08_cohort_image_validation.py",
                "note": "Authoritative implementation of the heuristic validity score and buckets.",
            },
            {
                "path": "data/processed/cohorts/image_validation/cohort_image_validation.tsv",
                "note": "Per-image audit trail (score, flags, entropy, variance, optional phash).",
            },
        ],
        "dataset_sources_thesis": [
            {
                "corpus": "FakeNewsNet (GossipCop, PolitiFact)",
                "cite_in_proposal": "Shu et al. (as in msc_proposal.tex / project bibliography)",
            },
            {
                "corpus": "Fakeddit",
                "cite_in_proposal": "Nakamura et al. (as in msc_proposal.tex / project bibliography)",
            },
        ],
        "literature_context_note": (
            "Multimodal misinformation papers often report filtering missing text/image pairs, broken URLs, "
            "or obviously unusable media when building social benchmarks; they do not standardise this "
            "project's composite entropy/aspect/resolution score. Treat this heuristic as an explicit, reproducible "
            "engineering rule for this dissertation pipeline."
        ),
        "illustrative_related_work": (
            "Recent work sometimes emphasises noisy or incomplete social-media multimodal inputs (e.g. "
            "Srivastava et al., 2025, CLIP-based detection in noisy environments — ICONAT); that line of "
            "work motivates rigorous data handling but is not the source of the validity-score formula."
        ),
        "external_paper_for_exact_validity_score": None,
    }


def _markdown_references_section() -> str:
    p = _references_payload()
    lines = [
        "## Documentation and literature context\n",
        "### Internal project references",
        "These artefacts record or implement the pipeline; **none** are a substitute for citing primary datasets in the thesis.\n",
    ]
    for item in p["internal_project_documents"]:
        lines.append(f"- **`{item['path']}`** — {item['note']}")
    lines.append("")
    lines.append("### Primary corpora (cite via your thesis bibliography)")
    for ds in p["dataset_sources_thesis"]:
        lines.append(f"- **{ds['corpus']}** — {ds['cite_in_proposal']}.")
    lines.append("")
    lines.append("### How this relates to the wider literature")
    lines.append(p["literature_context_note"])
    lines.append("")
    lines.append("### Illustrative related work (not the source of the validity-score formula)")
    lines.append(p["illustrative_related_work"])
    lines.append("")
    lines.append(
        "*There is **no single paper** that defines this project's composite `validity_score`; describe it as your "
        "documented data-quality gate and point readers to the decision log + validation script.*\n"
    )
    return "\n".join(lines)


DEFAULT_INPUT = Path("data/fake_news_final.tsv")
DEFAULT_OUT_DIR = Path("outputs/fake_news_final_report")


def _resolve(root: Path, p: Path) -> Path:
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def _fnn_news_source(sample_id: str) -> str:
    parts = (sample_id or "").strip().split(":")
    if len(parts) >= 2 and parts[0].strip().lower() == "fnn":
        return parts[1].strip().lower() or "unknown"
    return "unknown"


def _pct(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 2) if whole else 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarise fake_news_final.tsv")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--quiet", action="store_true", help="Do not print Markdown to stdout")
    args = ap.parse_args()

    inp = _resolve(PROJECT_ROOT, args.input)
    out_dir = _resolve(PROJECT_ROOT, args.out_dir)

    if not inp.is_file():
        print(f"Missing input: {inp}", file=sys.stderr)
        return 1

    by_ds: Counter = Counter()
    by_label: Counter = Counter()
    by_ds_label: Counter = Counter()
    fd_split: Counter = Counter()
    fnn_src: Counter = Counter()
    scores: list[int] = []
    score_counter: Counter = Counter()
    scores_by_ds: dict[str, list[int]] = defaultdict(list)
    flag_tokens: Counter = Counter()
    cohort_ok: Counter = Counter()

    with inp.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        for row in reader:
            ds = (row.get("dataset") or "").strip().lower() or "unknown"
            by_ds[ds] += 1
            lb = (row.get("label_binary") or "").strip()
            by_label[lb] += 1
            by_ds_label[(ds, lb)] += 1
            if ds == "fakeddit":
                sp = (row.get("split_official") or "").strip().lower() or "(empty)"
                fd_split[sp] += 1
            if ds == "fakenewsnet":
                fnn_src[_fnn_news_source(row.get("sample_id") or "")] += 1
            raw_sc = (row.get("image_option1_validity_score") or "").strip()
            if raw_sc:
                try:
                    sc = int(raw_sc)
                    scores.append(sc)
                    score_counter[sc] += 1
                    scores_by_ds[ds].append(sc)
                except ValueError:
                    pass
            fl = (row.get("image_option1_qc_flags") or "").strip()
            if fl:
                for t in fl.split(","):
                    t = t.strip()
                    if t:
                        flag_tokens[t] += 1
            ck = (row.get("cohort_multimodal_image_ok") or "").strip().lower() or "(empty)"
            cohort_ok[ck] += 1

    n = sum(by_ds.values())
    if n == 0:
        print("No data rows in input.", file=sys.stderr)
        return 1

    scores_sorted = sorted(scores)

    def _pctile(p: float) -> float:
        if not scores_sorted:
            return float("nan")
        k = max(0, min(len(scores_sorted) - 1, int(math.ceil(p * len(scores_sorted)) - 1)))
        return float(scores_sorted[k])

    summary = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_path": str(inp),
        "row_count": n,
        "label_binary_convention": "0 = real, 1 = fake (project consolidation)",
        "by_dataset": dict(by_ds),
        "by_dataset_pct": {ds: _pct(by_ds[ds], n) for ds in sorted(by_ds.keys())},
        "by_label_binary": dict(by_label),
        "by_label_binary_pct": {lb: _pct(by_label[lb], n) for lb in sorted(by_label.keys())},
        "by_dataset_label_binary": {
            f"{ds}|{lb}": by_ds_label[(ds, lb)]
            for ds, lb in sorted(by_ds_label.keys())
        },
        "fakeddit_split_official_counts": dict(fd_split),
        "fakenewsnet_by_news_source": dict(fnn_src),
        "validity_score_stats": {
            "min": min(scores) if scores else None,
            "max": max(scores) if scores else None,
            "mean": round(sum(scores) / len(scores), 2) if scores else None,
            "median": scores_sorted[len(scores_sorted) // 2] if scores else None,
            "p10": round(_pctile(0.10), 1) if scores else None,
            "p90": round(_pctile(0.90), 1) if scores else None,
        },
        "validity_score_value_counts": {str(k): score_counter[k] for k in sorted(score_counter.keys())},
        "validity_score_by_dataset_stats": {
            ds: {
                "n": len(v),
                "min": min(v),
                "max": max(v),
                "mean": round(sum(v) / len(v), 2),
                "median": sorted(v)[len(v) // 2],
            }
            for ds, v in sorted(scores_by_ds.items())
        },
        "image_validation_methodology_reference": IMAGE_VALIDATION_SCRIPT,
        "references": _references_payload(),
        "non_empty_qc_flags_token_counts": dict(flag_tokens.most_common(50)),
        "cohort_multimodal_image_ok_counts": dict(cohort_ok),
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "fake_news_final_summary.json"
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Fake news final cohort — breakdown summary\n")
    lines.append(f"*Generated (UTC): `{summary['generated_utc']}`*\n")
    lines.append("## Source\n")
    lines.append(f"- **Input:** `{inp}`")
    lines.append(f"- **Rows:** **{n:,}** (validity-score gate: score ≥ 75, successful cohort fetch + QC)")
    lines.append(f"- **Score implementation:** `{IMAGE_VALIDATION_SCRIPT}`")
    lines.append("")
    lines.append(_markdown_image_validation_methodology())
    lines.append(_markdown_references_section())
    lines.append("## Totals\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|------:|")
    lines.append(f"| Rows | {n:,} |")
    if scores:
        lines.append(f"| `validity_score` min | {min(scores)} |")
        lines.append(f"| `validity_score` max | {max(scores)} |")
        lines.append(f"| `validity_score` mean | {summary['validity_score_stats']['mean']} |")
        lines.append(f"| `validity_score` median | {summary['validity_score_stats']['median']} |")
        lines.append(f"| `validity_score` p10 / p90 | {summary['validity_score_stats']['p10']} / {summary['validity_score_stats']['p90']} |")
    lines.append("")
    if score_counter:
        lines.append("## `validity_score` breakdown (this file)\n")
        lines.append("*Distribution of `image_option1_validity_score` across the **gated** rows (all values here are ≥ 75).* \n")
        lines.append("| Score | Count | % of rows |")
        lines.append("|------:|------:|----------:|")
        for sc in sorted(score_counter.keys()):
            c = score_counter[sc]
            lines.append(f"| {sc} | {c:,} | **{_pct(c, n):.2f}%** |")
        lines.append("")
        lines.append("### Score bands (this file)\n")
        bands = [(75, 79), (80, 84), (85, 89), (90, 94), (95, 99), (100, 100)]
        lines.append("| Band (inclusive) | Count | % of rows |")
        lines.append("|-------------------|------:|----------:|")
        for lo, hi in bands:
            cnt = sum(score_counter[s] for s in range(lo, hi + 1) if s in score_counter)
            if cnt:
                label = f"{lo}–{hi}" if lo != hi else str(lo)
                lines.append(f"| {label} | {cnt:,} | **{_pct(cnt, n):.2f}%** |")
        lines.append("")
        lines.append("### By dataset (score summary)\n")
        lines.append("| Dataset | n | min | max | mean | median |")
        lines.append("|---------|--:|----:|----:|-----:|-------:|")
        for ds in sorted(scores_by_ds.keys()):
            v = sorted(scores_by_ds[ds])
            ln = len(v)
            lines.append(
                f"| {ds} | {ln:,} | {min(v)} | {max(v)} | "
                f"{round(sum(v) / ln, 2)} | {v[ln // 2]} |"
            )
        lines.append("")
    lines.append("## By dataset\n")
    lines.append("| Dataset | Count | % of rows |")
    lines.append("|---------|------:|----------:|")
    for ds in sorted(by_ds.keys()):
        lines.append(f"| {ds} | {by_ds[ds]:,} | **{_pct(by_ds[ds], n):.2f}%** |")
    lines.append("")
    lines.append("## By `label_binary` (all rows)\n")
    lines.append("*Convention: **0 = real**, **1 = fake**.*\n")
    lines.append("| `label_binary` | Count | % of rows |")
    lines.append("|----------------|------:|----------:|")
    for lb in sorted(by_label.keys(), key=lambda x: (x == "", x)):
        lines.append(f"| `{lb or '—'}` | {by_label[lb]:,} | **{_pct(by_label[lb], n):.2f}%** |")
    lines.append("")
    lines.append("## By dataset × `label_binary`\n")
    lines.append("| Dataset | `0` (real) | % within dataset | `1` (fake) | % within dataset | Total |")
    lines.append("|---------|------------:|-----------------:|-----------:|-----------------:|------:|")
    for ds in sorted(set(d for d, _ in by_ds_label.keys())):
        c0 = by_ds_label.get((ds, "0"), 0)
        c1 = by_ds_label.get((ds, "1"), 0)
        t = c0 + c1
        p0 = _pct(c0, t)
        p1 = _pct(c1, t)
        lines.append(f"| {ds} | {c0:,} | **{p0:.2f}%** | {c1:,} | **{p1:.2f}%** | {t:,} |")
    lines.append("")
    lines.append("## Fakeddit: `split_official`\n")
    lines.append("| split_official | Count | % of Fakeddit rows |")
    lines.append("|----------------|------:|-------------------:|")
    fd_n = by_ds.get("fakeddit", 0)
    for sp in sorted(fd_split.keys()):
        c = fd_split[sp]
        lines.append(f"| `{sp}` | {c:,} | **{_pct(c, fd_n):.2f}%** |")
    if fd_n:
        lines.append("")
        lines.append(f"*Fakeddit row total: {fd_n:,}.*")
    lines.append("")
    lines.append("## FakeNewsNet: news source (from `sample_id`)\n")
    lines.append("Pattern `fnn:<source>:…` on `dataset=fakenewsnet` rows only.\n")
    lines.append("| Source | Count | % of FNN rows |")
    lines.append("|--------|------:|--------------:|")
    fnn_n = by_ds.get("fakenewsnet", 0)
    for src in sorted(fnn_src.keys(), key=lambda s: (-fnn_src[s], s)):
        lines.append(f"| {src} | {fnn_src[src]:,} | **{_pct(fnn_src[src], fnn_n):.2f}%** |")
    lines.append("")
    if flag_tokens:
        lines.append("## Non-empty `image_option1_qc_flags` tokens\n")
        lines.append("*Most gated rows have empty flags; below are remaining token counts.*\n")
        lines.append("| Token | Count |")
        lines.append("|-------|------:|")
        for tok, c in flag_tokens.most_common(20):
            lines.append(f"| `{tok}` | {c:,} |")
        lines.append("")
    lines.append("## Cohort fetch sanity (`cohort_multimodal_image_ok`)\n")
    lines.append("| Value | Count |")
    lines.append("|-------|------:|")
    for k in sorted(cohort_ok.keys()):
        lines.append(f"| `{k}` | {cohort_ok[k]:,} |")
    lines.append("")

    md_text = "\n".join(lines)
    md_path = out_dir / "fake_news_final_summary.md"
    md_path.write_text(md_text, encoding="utf-8")

    if not args.quiet:
        print(md_text)
    print(f"Wrote {md_path}", file=sys.stderr)
    print(f"Wrote {json_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
