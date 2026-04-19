"""
Pie / bar charts from deduplicated ``crawl_failures.jsonl`` (FakeNewsNet).

Writes PNGs under ``outputs/fnn_failure_viz/`` by default.

    python scripts/04_qa_fnn_visualize_crawl_failures.py
    python scripts/04_qa_fnn_visualize_crawl_failures.py --log data/processed/fakenewsnet/crawl_failures.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = Path("data/processed/fakenewsnet/crawl_failures.jsonl")
DEFAULT_OUT = Path("outputs/fnn_failure_viz")


def _resolve(p: Path) -> Path:
    p = p.expanduser()
    return p.resolve() if p.is_absolute() else (PROJECT_ROOT / p).resolve()


def _fmt_source(s: str) -> str:
    m = {"gossipcop": "GossipCop", "politifact": "PolitiFact"}
    return m.get(s.strip().lower(), s)


def _load_counts(log_path: Path) -> tuple[Counter, Counter, Counter, int]:
    by_source: Counter = Counter()
    by_reason: Counter = Counter()
    by_stratum: Counter = Counter()
    n = 0
    for line in log_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        if o.get("event") != "failed":
            continue
        src = (o.get("news_source") or "").strip().lower()
        lbl = (o.get("label") or "").strip().lower()
        r = (o.get("reason") or "unknown").strip()
        by_source[src] += 1
        by_reason[r] += 1
        by_stratum[f"{src}/{lbl}"] += 1
        n += 1
    return by_source, by_reason, by_stratum, n


def main() -> int:
    ap = argparse.ArgumentParser(description="Visualise FNN crawl_failures.jsonl (pie / bar charts)")
    ap.add_argument("--log", type=Path, default=DEFAULT_LOG)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    log_path = _resolve(args.log)
    out_dir = _resolve(args.out_dir)
    if not log_path.is_file():
        print(f"Missing: {log_path}", file=sys.stderr)
        return 1

    try:
        import matplotlib.pyplot as plt
        from matplotlib.colors import to_rgba
    except ImportError as e:
        print(f"Install matplotlib: pip install matplotlib ({e})", file=sys.stderr)
        return 1

    by_source, by_reason, by_stratum, n = _load_counts(log_path)
    if n == 0:
        print("No failed events in log.", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"figure.facecolor": "white", "axes.facecolor": "white"})

    def pie(counter: Counter, labels_map: dict[str, str], title: str, fname: str) -> None:
        keys = list(counter.keys())
        vals = [counter[k] for k in keys]
        labels: list[str] = [str(labels_map.get(k, k)) for k in keys]
        fig, ax = plt.subplots(figsize=(7, 7))
        cmap = plt.get_cmap("Set3")
        n_keys = len(keys)
        colours = [
            to_rgba(cmap(float(t)))
            for t in (np.linspace(0, 1, n_keys, endpoint=False) if n_keys else [])
        ]
        pie_out = ax.pie(
            vals,
            labels=labels,
            autopct=lambda p: f"{p:.1f}%\n({int(round(p * sum(vals) / 100))})",
            colors=colours,
            pctdistance=0.72,
            labeldistance=1.05,
            startangle=90,
            textprops={"fontsize": 11},
        )
        autotexts = pie_out[2] if len(pie_out) > 2 else []
        for t in autotexts:
            t.set_fontsize(10)
        ax.set_title(f"{title}\n(n = {n_keys:,} unique stories)", fontsize=13, pad=16)
        fig.tight_layout()
        fig.savefig(out_dir / fname, dpi=150, bbox_inches="tight")
        plt.close(fig)

    # --- By news source ---
    src_labels = {k: _fmt_source(k) for k in by_source}
    pie(by_source, src_labels, "FakeNewsNet crawl failures by news source", "fnn_failures_by_news_source.png")

    # --- By reason ---
    reason_labels = {
        "no_article": "no_article\n(no usable article)",
        "empty_body": "empty_body\n(empty body text)",
        "exception": "exception",
        "unknown": "unknown",
    }
    pie(by_reason, reason_labels, "FakeNewsNet crawl failures by reason", "fnn_failures_by_reason.png")

    # --- By stratum (horizontal bar — clearer than a 4-slice pie) ---
    strat_order = sorted(by_stratum.keys(), key=lambda k: -by_stratum[k])
    strat_labels = [f"{_fmt_source(k.split('/')[0])} / {k.split('/')[1].capitalize()}" for k in strat_order]
    strat_vals = [by_stratum[k] for k in strat_order]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    cmap2 = plt.get_cmap("Set2")
    n_s = len(strat_vals)
    bar_colours = [to_rgba(cmap2(float(t))) for t in (np.linspace(0, 1, n_s, endpoint=False) if n_s else [])]
    bars = ax.barh(strat_labels[::-1], strat_vals[::-1], color=bar_colours)
    ax.set_xlabel("Unique failing stories")
    ax.set_title(f"FakeNewsNet crawl failures by source × label\n(n = {n:,})", fontsize=13)
    ax.bar_label(bars, padding=4, fmt="%d")
    ax.set_xlim(0, max(strat_vals) * 1.15)
    fig.tight_layout()
    fig.savefig(out_dir / "fnn_failures_by_stratum.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    summary = {
        "log": str(log_path),
        "unique_failed_stories": n,
        "by_news_source": dict(by_source),
        "by_reason": dict(by_reason),
        "by_stratum": dict(by_stratum),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Wrote charts and summary.json under {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
