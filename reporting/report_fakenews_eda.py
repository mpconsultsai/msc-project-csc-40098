"""
Build summary tables and charts from ``data/fakenews.tsv`` (Fakeddit + FakeNewsNet image-ref export).

For interactive preprocessing stats (FNN failure log, Fakeddit TSV quality, full column profiling),
use **`notebooks/fakenews_preprocessing_eda.ipynb`** instead.

Requires: pandas, matplotlib (see ``requirements.txt``).

**Inputs:** existing ``fakenews.tsv`` (from consolidation). Safe to re-run anytime.

    python reporting/report_fakenews_eda.py
    python reporting/report_fakenews_eda.py --input data/fakenews.tsv --out-dir outputs/fakenews_viz

Paths are resolved from the **project root** (parent of ``reporting/``).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import cast

PROJECT_ROOT = Path(__file__).resolve().parents[1]

COLS = ["dataset", "sample_id", "split_official", "domain", "label_binary", "has_image_ref"]


def _resolve(p: Path) -> Path:
    p = p.expanduser()
    return p.resolve() if p.is_absolute() else (PROJECT_ROOT / p).resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description="Visualise fakenews.tsv (counts + PNG charts + HTML).")
    parser.add_argument("--input", type=Path, default=Path("data/fakenews.tsv"))
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("outputs/fakenews_viz"),
        help="Folder for PNGs, summary.json, and index.html (default under outputs/).",
    )
    parser.add_argument("--top-domains", type=int, default=20, help="Top Fakeddit subreddits to chart")
    args = parser.parse_args()

    src = _resolve(args.input)
    out_dir = _resolve(args.out_dir)
    if not src.is_file():
        print(f"Missing input: {src}", file=sys.stderr)
        return 1

    try:
        import matplotlib.pyplot as plt
        import pandas as pd
        import seaborn as sns
    except ImportError as e:
        print(f"Install dependencies: pip install pandas matplotlib seaborn ({e})", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", context="talk")

    df = pd.read_csv(src, sep="\t", usecols=lambda c: c in COLS, dtype=str, low_memory=False)
    n = len(df)
    if n == 0:
        print("No rows in TSV.", file=sys.stderr)
        return 1

    df["has_image_ref"] = df["has_image_ref"].fillna("").str.lower().eq("true")
    df["dataset"] = df["dataset"].fillna("unknown")

    summary: dict = {
        "input": str(src),
        "rows_total": int(n),
        "by_dataset": df["dataset"].value_counts().to_dict(),
        "has_image_ref_by_dataset": df.groupby("dataset")["has_image_ref"].sum().astype(int).to_dict(),
        "missing_image_ref_by_dataset": df.groupby("dataset")["has_image_ref"].apply(lambda s: int((~s).sum())).to_dict(),
    }

    # --- Figures ---
    def save_bar(series: pd.Series, title: str, fname: str, rotate: bool = False) -> None:
        fig, ax = plt.subplots(figsize=(10, 5))
        s = series.sort_values(ascending=False)
        sns.barplot(x=s.index.astype(str), y=s.values, ax=ax, hue=s.index.astype(str), palette="viridis", legend=False)
        ax.set_title(title)
        ax.set_ylabel("Count")
        ax.set_xlabel("")
        if rotate:
            plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        fig.tight_layout()
        fig.savefig(out_dir / fname, dpi=150, bbox_inches="tight")
        plt.close(fig)

    save_bar(df["dataset"].value_counts(), "Rows by dataset", "01_rows_by_dataset.png")

    mean_by_ds = cast(pd.Series, df.groupby("dataset")["has_image_ref"].mean())
    img_rate = mean_by_ds * 100
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(x=img_rate.index.astype(str), y=img_rate.values, ax=ax, hue=img_rate.index.astype(str), palette="crest", legend=False)
    ax.set_title("Share of rows with image_ref (%)")
    ax.set_ylabel("Percent")
    ax.set_ylim(0, 105)
    fig.tight_layout()
    fig.savefig(out_dir / "02_image_ref_rate_by_dataset.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    fd = df[df["dataset"] == "fakeddit"]
    if len(fd) > 0:
        split_c = cast(pd.Series, fd["split_official"]).fillna("(blank)").value_counts()
        save_bar(split_c, "Fakeddit: rows by split_official", "03_fakeddit_split_official.png", rotate=True)

    fnn = df[df["dataset"] == "fakenewsnet"]
    if len(fnn) > 0:
        dom = cast(pd.Series, fnn["domain"]).fillna("(blank)").value_counts()
        save_bar(dom, "FakeNewsNet: rows by domain", "04_fnn_domain.png")

    # Label distribution (string labels as stored)
    lab = df.groupby(["dataset", "label_binary"]).size().unstack(fill_value=0)
    if lab.shape[1] > 0:
        fig, ax = plt.subplots(figsize=(10, 5))
        lab.plot(kind="bar", ax=ax, rot=0)
        ax.set_title("Rows by dataset and label_binary (as in TSV)")
        ax.set_xlabel("dataset")
        ax.legend(title="label_binary", bbox_to_anchor=(1.02, 1), loc="upper left")
        fig.tight_layout()
        fig.savefig(out_dir / "05_label_binary_by_dataset.png", dpi=150, bbox_inches="tight")
        plt.close(fig)

    fd_dom = cast(pd.Series, fd["domain"]).fillna("(blank)")
    top = fd_dom.value_counts().head(max(1, args.top_domains))
    if len(top) > 0:
        fig, ax = plt.subplots(figsize=(11, 6))
        sns.barplot(x=top.values, y=top.index.astype(str), ax=ax, hue=top.index.astype(str), palette="mako", legend=False, orient="h")
        ax.set_title(f"Fakeddit: top {len(top)} subreddits (domain)")
        ax.set_xlabel("Rows")
        fig.tight_layout()
        fig.savefig(out_dir / "06_fakeddit_top_subreddits.png", dpi=150, bbox_inches="tight")
        plt.close(fig)

    summary["fakeddit_top_subreddits"] = top.head(10).to_dict() if len(top) else {}

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    # Simple HTML dashboard (opens locally; images are relative)
    imgs = sorted(p.name for p in out_dir.glob("*.png"))
    img_html = "\n".join(
        f'<section><h2>{name.replace("_", " ").replace(".png", "")}</h2><img src="{name}" alt="{name}" style="max-width:100%;height:auto;border:1px solid #ddd"/></section>'
        for name in imgs
    )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>fakenews.tsv summary</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 960px; margin: 2rem auto; padding: 0 1rem; }}
    h1 {{ font-size: 1.4rem; }}
    section {{ margin-bottom: 2.5rem; }}
    pre {{ background: #f4f4f4; padding: 1rem; overflow: auto; }}
  </style>
</head>
<body>
  <h1>fakenews.tsv visual summary</h1>
  <p>Source: <code>{src.name}</code> — {n:,} rows. Generated by <code>reporting/report_fakenews_eda.py</code>.</p>
  <h2>Key counts (JSON)</h2>
  <pre>{json.dumps(summary, indent=2)}</pre>
  <h2>Figures</h2>
  {img_html}
</body>
</html>
"""
    (out_dir / "index.html").write_text(html, encoding="utf-8")

    print(f"Wrote: {out_dir / 'index.html'} and {len(imgs)} PNG(s)", file=sys.stderr)
    print(f"Open in browser: file:///{str(out_dir / 'index.html').replace(chr(92), '/')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
