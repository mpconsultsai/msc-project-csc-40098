"""
Summarise ``cohort_image_fetch.log`` for documentation (thesis / Confluence).

Optionally joins the cohort plan TSV to break down **primary vs reserve** and strata.

Writes **Markdown** + **JSON** under ``--out-dir`` (default ``outputs/cohort_fetch_report``).

    python pipeline/10_cohort_summarize_fetch.py
    python pipeline/10_cohort_summarize_fetch.py --log data/processed/images/cohort_image_fetch.log
    python pipeline/10_cohort_summarize_fetch.py --plan data/processed/cohorts/multimodal_plan_n50000_seed42.tsv --target-primary 50000
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_LOG = Path("data/processed/images/cohort_image_fetch.log")
DEFAULT_PLAN = Path("data/processed/cohorts/multimodal_plan_n50000_seed42.tsv")
DEFAULT_OUT_DIR = Path("outputs/cohort_fetch_report")


def _resolve(root: Path, p: Path) -> Path:
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def _parse_ts(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _fnn_news_source(sample_id: str) -> str:
    """Parse ``fnn:gossipcop:fake:...`` / ``fnn:politifact:real:...`` → ``gossipcop`` | ``politifact`` | ``unknown``."""
    parts = (sample_id or "").strip().split(":")
    if len(parts) >= 2 and parts[0].strip().lower() == "fnn":
        return parts[1].strip().lower() or "unknown"
    return "unknown"


def _load_plan_index(plan_path: Path) -> dict[str, dict[str, str]]:
    """sample_id -> row fields from plan."""
    out: dict[str, dict[str, str]] = {}
    with plan_path.open(encoding="utf-8", newline="") as fp:
        r = csv.DictReader(fp, delimiter="\t")
        for row in r:
            sid = (row.get("sample_id") or "").strip()
            if sid:
                out[sid] = row
    return out


# Align with ``pipeline/08_cohort_fetch_images.py`` ``_download_one`` (literal ``detail`` values + ``type(e).__name__``).
_FAILURE_DETAIL_GLOSSARY: dict[str, str] = {
    "(empty)": "No text was stored in the log’s `detail` column (unexpected for normal runs).",
    "reddit_placeholder_sha256": (
        "Downloaded bytes matched a SHA-256 on the project blocklist (typically Reddit/i.redd.it placeholder "
        "or CDN “missing image” payloads). The URL returned *something*, but it is treated as unusable for training."
    ),
    "too_small": (
        "HTTP response body was smaller than 512 bytes (`08_cohort_fetch_images.py`). Often an error page, "
        "empty payload, or truncated response rather than a real image."
    ),
    "pil_verify_failed": (
        "Bytes did not pass a strict PIL open+verify check—corrupt file, wrong content type (HTML/JSON masquerading "
        "as an image), or unsupported/broken image data."
    ),
    "HTTPError": (
        "The `requests` library raised after `raise_for_status()`—typically HTTP 4xx/5xx (removed post, paywall, "
        "forbidden, server error). The log stores the exception class name only, not the status line."
    ),
    "ConnectionError": (
        "Network-level failure connecting to the host (DNS, refused connection, reset, proxy issues, offline client, "
        "or remote server not accepting the connection)."
    ),
    "SSLError": (
        "TLS/SSL handshake or certificate validation failed between client and server."
    ),
    "Timeout": (
        "HTTP request exceeded the configured timeout (connect or read phase; see the `requests` `Timeout` hierarchy)."
    ),
    "ConnectTimeout": "Timed out while trying to establish a TCP connection to the server.",
    "ReadTimeout": "Connection opened, but the server did not send a complete response within the read timeout.",
    "TooManyRedirects": (
        "Redirect loop or excessive redirects when following the image URL (`allow_redirects=True` in the fetcher)."
    ),
    "InvalidURL": "URL string was malformed or not usable as an HTTP URL.",
    "MissingSchema": "URL lacked `http://` or `https://` (or similar), so `requests` could not fetch it.",
    "ChunkedEncodingError": (
        "Transfer-Encoding/chunked stream ended inconsistently (server or intermediary closed the connection early)."
    ),
    "ContentDecodingError": "Response declared a compression encoding but bytes could not be decoded.",
    "InvalidSchema": "URL scheme is not supported for this request configuration.",
}


def _failure_detail_explanation(detail: str) -> str:
    """Human-readable meaning for a log ``detail`` string (exact or truncated)."""
    d = (detail or "").strip()
    if d in _FAILURE_DETAIL_GLOSSARY:
        return _FAILURE_DETAIL_GLOSSARY[d]
    if d.endswith("..."):
        base = d[:-3].rstrip()
        if base in _FAILURE_DETAIL_GLOSSARY:
            return _FAILURE_DETAIL_GLOSSARY[base] + " *(log line may be truncated in the summary table.)*"
        token = base.split()[0] if base else ""
        if token in _FAILURE_DETAIL_GLOSSARY:
            return _FAILURE_DETAIL_GLOSSARY[token] + " *(log line may be truncated in the summary table.)*"
    if d.isidentifier() and d[:1].isupper():
        return (
            f"Python exception type name recorded by the fetcher (`type(e).__name__` from `requests` / urllib3). "
            f"Look up `{d}` in the Requests/urllib3 docs for precise semantics."
        )
    return (
        "Free-text or uncommon marker. Inspect the raw log line for the full `detail` string, or search "
        "`08_cohort_fetch_images.py` for where `detail` is set."
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarise cohort_image_fetch.log (+ optional plan join)")
    ap.add_argument("--log", type=Path, default=DEFAULT_LOG)
    ap.add_argument(
        "--plan",
        type=Path,
        default=DEFAULT_PLAN,
        help="Cohort plan TSV for primary/reserve/stratum breakdown (skip if missing)",
    )
    ap.add_argument("--target-primary", type=int, default=50_000, help="Nominal primary cohort size (default 50000)")
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--quiet", action="store_true", help="Do not print Markdown to stdout")
    args = ap.parse_args()

    log_path = _resolve(PROJECT_ROOT, args.log)
    plan_path = _resolve(PROJECT_ROOT, args.plan)
    out_dir = _resolve(PROJECT_ROOT, args.out_dir)

    if not log_path.is_file():
        print(f"Missing log: {log_path}", file=sys.stderr)
        return 1

    plan_index: dict[str, dict[str, str]] = {}
    plan_n_primary = 0
    plan_n_reserve = 0
    if plan_path.is_file():
        plan_index = _load_plan_index(plan_path)
        for r in plan_index.values():
            role = (r.get("plan_role") or "").strip().lower()
            if role == "primary":
                plan_n_primary += 1
            elif role == "reserve":
                plan_n_reserve += 1
    else:
        print(f"Note: plan not found ({plan_path}); primary/reserve sections omitted.", file=sys.stderr)

    ok_by_ds: Counter = Counter()
    fail_by_ds: Counter = Counter()
    fnn_src_ok: Counter = Counter()
    fnn_src_fail: Counter = Counter()
    fail_detail: Counter = Counter()
    ok_ts: list[datetime] = []
    fail_ts: list[datetime] = []

    ok_sids: set[str] = set()
    fail_sids: set[str] = set()

    ok_by_role: Counter = Counter()
    fail_by_role: Counter = Counter()
    ok_primary_in_plan = 0
    ok_reserve_in_plan = 0
    fail_primary_in_plan = 0
    fail_reserve_in_plan = 0
    not_in_plan_ok = 0
    not_in_plan_fail = 0
    ok_by_label_binary: Counter = Counter()
    ok_by_dataset_label_binary: Counter = Counter()

    with log_path.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        for row in reader:
            st = (row.get("status") or "").strip().lower()
            ds = (row.get("dataset") or "").strip().lower() or "unknown"
            sid = (row.get("sample_id") or "").strip()
            ts = _parse_ts(row.get("ts_utc") or "")

            if st == "ok":
                ok_by_ds[ds] += 1
                if ds == "fakenewsnet":
                    fnn_src_ok[_fnn_news_source(sid)] += 1
                if sid:
                    ok_sids.add(sid)
                if ts:
                    ok_ts.append(ts)
                if plan_index:
                    if sid in plan_index:
                        role = (plan_index[sid].get("plan_role") or "").strip().lower()
                        ok_by_role[role or "unknown"] += 1
                        if role == "primary":
                            ok_primary_in_plan += 1
                        elif role == "reserve":
                            ok_reserve_in_plan += 1
                    else:
                        not_in_plan_ok += 1
                if plan_index and sid in plan_index:
                    pr = plan_index[sid]
                    lb = (pr.get("label_binary") or "").strip()
                    ok_by_label_binary[lb] += 1
                    pds = (pr.get("dataset") or "").strip().lower() or "unknown"
                    ok_by_dataset_label_binary[(pds, lb)] += 1
            elif st == "fail":
                fail_by_ds[ds] += 1
                if ds == "fakenewsnet":
                    fnn_src_fail[_fnn_news_source(sid)] += 1
                if sid:
                    fail_sids.add(sid)
                d = (row.get("detail") or "").strip() or "(empty)"
                if len(d) > 120:
                    d = d[:117] + "..."
                fail_detail[d] += 1
                if ts:
                    fail_ts.append(ts)
                if plan_index:
                    if sid in plan_index:
                        role = (plan_index[sid].get("plan_role") or "").strip().lower()
                        fail_by_role[role or "unknown"] += 1
                        if role == "primary":
                            fail_primary_in_plan += 1
                        elif role == "reserve":
                            fail_reserve_in_plan += 1
                    else:
                        not_in_plan_fail += 1

    n_ok = sum(ok_by_ds.values())
    n_fail = sum(fail_by_ds.values())
    n_total = n_ok + n_fail

    fnn_ok_total = int(ok_by_ds["fakenewsnet"])
    fnn_fail_total = int(fail_by_ds["fakenewsnet"])
    fnn_src_keys = sorted(
        set(fnn_src_ok.keys()) | set(fnn_src_fail.keys()),
        key=lambda s: (-(fnn_src_ok[s] + fnn_src_fail[s]), s),
    )
    fnn_by_source: dict[str, dict] = {}
    for src in fnn_src_keys:
        o = int(fnn_src_ok[src])
        f = int(fnn_src_fail[src])
        att = o + f
        fnn_by_source[src] = {
            "ok": o,
            "fail": f,
            "attempts": att,
            "success_rate_pct": round(100.0 * o / att, 2) if att else 0.0,
            "pct_of_fnn_ok": round(100.0 * o / fnn_ok_total, 2) if fnn_ok_total else 0.0,
            "pct_of_fnn_fail": round(100.0 * f / fnn_fail_total, 2) if fnn_fail_total else 0.0,
        }

    tmin = None
    tmax = None
    all_ts = ok_ts + fail_ts
    if all_ts:
        tmin = min(all_ts)
        tmax = max(all_ts)

    primary_target = args.target_primary
    pct_primary_ok = (100.0 * ok_primary_in_plan / primary_target) if primary_target else 0.0

    lbl0 = int(ok_by_label_binary.get("0", 0))
    lbl1 = int(ok_by_label_binary.get("1", 0))
    pct_lbl0 = (100.0 * lbl0 / n_ok) if n_ok else 0.0
    pct_lbl1 = (100.0 * lbl1 / n_ok) if n_ok else 0.0
    lbl_empty = int(ok_by_label_binary.get("", 0))
    n_ok_plan_join = sum(int(v) for v in ok_by_label_binary.values())

    def _ds_lb(ds: str, lb: str) -> int:
        return int(ok_by_dataset_label_binary.get((ds, lb), 0))

    fd_ok = int(ok_by_ds["fakeddit"])
    fd_fail = int(fail_by_ds["fakeddit"])
    fd_att = fd_ok + fd_fail
    fd_sr = (100.0 * fd_ok / fd_att) if fd_att else 0.0
    fnn_att = fnn_ok_total + fnn_fail_total
    fnn_sr = (100.0 * fnn_ok_total / fnn_att) if fnn_att else 0.0
    reddit_ph = int(fail_detail.get("reddit_placeholder_sha256", 0))
    gc_share_fnn_ok = fnn_by_source.get("gossipcop", {}).get("pct_of_fnn_ok")

    summary: dict = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "log_path": str(log_path),
        "plan_path": str(plan_path) if plan_path.is_file() else None,
        "time_range_utc": {
            "first": tmin.isoformat() if tmin else None,
            "last": tmax.isoformat() if tmax else None,
        },
        "rows": {"ok": n_ok, "fail": n_fail, "total": n_total},
        "unique_sample_id": {"ok": len(ok_sids), "fail": len(fail_sids)},
        "ok_by_dataset": dict(ok_by_ds),
        "fail_by_dataset": dict(fail_by_ds),
        "ok_split_pct_of_all_ok": (
            {ds: round(100.0 * ok_by_ds[ds] / n_ok, 2) for ds in ok_by_ds} if n_ok else {}
        ),
        "fakenewsnet_by_news_source": fnn_by_source,
        "failure_top_details": fail_detail.most_common(25),
        "failure_top_details_explained": [
            {"detail": d, "count": c, "explanation": _failure_detail_explanation(d)}
            for d, c in fail_detail.most_common(25)
        ],
        "failure_detail_glossary": dict(sorted(_FAILURE_DETAIL_GLOSSARY.items())),
        "target_primary_n": primary_target,
        "label_binary_convention": "0 = real, 1 = fake (project consolidation; verify in fakenews.tsv build)",
        "ok_successes_by_label_binary": {
            "0_real": lbl0,
            "1_fake": lbl1,
            "empty_label_binary": lbl_empty,
            "ok_rows_with_plan_join": n_ok_plan_join,
            "ok_rows_not_in_plan": not_in_plan_ok,
            "pct_of_all_ok_0": round(pct_lbl0, 2),
            "pct_of_all_ok_1": round(pct_lbl1, 2),
        },
        "ok_successes_by_dataset_label_binary": {
            "fakeddit": {"0_real": _ds_lb("fakeddit", "0"), "1_fake": _ds_lb("fakeddit", "1")},
            "fakenewsnet": {"0_real": _ds_lb("fakenewsnet", "0"), "1_fake": _ds_lb("fakenewsnet", "1")},
        },
        "key_facts_metrics": {
            "fakeddit_attempt_success_rate_pct": round(fd_sr, 2),
            "fakenewsnet_attempt_success_rate_pct": round(fnn_sr, 2),
            "fakeddit_fail_reddit_placeholder_sha256": reddit_ph,
            "gossipcop_pct_of_fnn_ok": gc_share_fnn_ok,
            "ok_on_primary_rows": ok_primary_in_plan,
            "ok_on_reserve_rows": ok_reserve_in_plan,
        },
    }
    if plan_index:
        summary["plan_join"] = {
            "plan_rows_primary": plan_n_primary,
            "plan_rows_reserve": plan_n_reserve,
            "ok_primary": ok_primary_in_plan,
            "ok_reserve": ok_reserve_in_plan,
            "fail_primary": fail_primary_in_plan,
            "fail_reserve": fail_reserve_in_plan,
            "ok_not_in_plan": not_in_plan_ok,
            "fail_not_in_plan": not_in_plan_fail,
            "pct_of_primary_target_with_ok": round(pct_primary_ok, 2),
        }

    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "cohort_image_fetch_summary.json"
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Cohort image fetch — summary\n")
    lines.append(f"*Generated (UTC): `{summary['generated_utc']}`*\n")
    lines.append("## Sources\n")
    lines.append(f"- **Log:** `{log_path}`")
    lines.append(f"- **Plan:** `{plan_path}`" if plan_path.is_file() else "- **Plan:** *(not found — primary/reserve breakdown omitted)*")
    lines.append("")
    lines.append("## Time range (log timestamps)\n")
    lines.append(f"- **First:** {summary['time_range_utc']['first'] or '—'}")
    lines.append(f"- **Last:** {summary['time_range_utc']['last'] or '—'}")
    lines.append("")
    lines.append("## Totals\n")
    lines.append("| Metric | Count |")
    lines.append("|--------|------:|")
    lines.append(f"| `ok` rows | {n_ok:,} |")
    lines.append(f"| `fail` rows | {n_fail:,} |")
    lines.append(f"| **Total** | {n_total:,} |")
    lines.append(f"| Unique `sample_id` with ok | {len(ok_sids):,} |")
    lines.append(f"| Unique `sample_id` with fail | {len(fail_sids):,} |")
    if n_total:
        lines.append(f"| Success rate (ok / all attempts) | {100.0 * n_ok / n_total:.2f}% |")
    lines.append("")

    lines.append("## Key facts (summary)\n")
    lines.append(
        "- **Multimodal volume:** the run logged **{:,}** successful image saves (`ok`) from **{:,}** attempts "
        "(**{:.2f}%** overall success rate).".format(n_ok, n_total, 100.0 * n_ok / n_total if n_total else 0.0)
    )
    lines.append(
        "- **Corpus mix (successes):** **{:.2f}%** of all `ok` rows are Fakeddit and **{:.2f}%** are "
        "FakeNewsNet — consistent with a much larger eligible Fakeddit pool in the consolidated table.".format(
            (100.0 * fd_ok / n_ok) if n_ok else 0.0,
            (100.0 * fnn_ok_total / n_ok) if n_ok else 0.0,
        )
    )
    lines.append(
        "- **Per-attempt difficulty:** Fakeddit image fetch succeeded **{:.2f}%** of the time vs "
        "**{:.2f}%** for FakeNewsNet (each corpus’s own attempts), reflecting Reddit/CDN behaviour "
        "and older news URLs.".format(fd_sr, fnn_sr)
    )
    lines.append(
        "- **Placeholder / stub traffic (Fakeddit):** **{:,}** failures were classified as "
        "`reddit_placeholder_sha256` (known hash blocklist), i.e. many URLs still return *an* image "
        "but not a usable post image.".format(reddit_ph)
    )
    if gc_share_fnn_ok is not None:
        lines.append(
            f"- **FakeNewsNet source skew:** among FNN `ok` rows, about **{gc_share_fnn_ok:.2f}%** "
            "map to **GossipCop** ids (`fnn:gossipcop:…`); PolitiFact is a small slice — state this "
            "when generalising to “news” benchmarks."
        )
    lines.append(
        "- **Primary vs reserve:** **{:,}** successes joined to **`plan_role=primary`** and **{:,}** to "
        "**`reserve`** — reaching 50k `ok` relied heavily on **reserve** backfill, not only primary "
        "slots.".format(ok_primary_in_plan, ok_reserve_in_plan)
    )
    lines.append(
        "- **Class balance (successes):** **`label_binary=1` (fake)** is **{:.2f}%** of all **`ok`** rows and "
        "**`0` (real)** **{:.2f}%** (denominator = total `ok` in the log; see table below). "
        "*(Convention: **0 = real**, **1 = fake**; confirm against your `fakenews.tsv` build.)*".format(
            pct_lbl1, pct_lbl0
        )
    )
    lines.append(
        "- **Manual QC:** automated checks do not verify *semantic* image–text alignment; consider a "
        "**small stratified spot sample** (dataset × label × outcome) for the write-up."
    )
    lines.append("")

    lines.append("## OK successes: real vs fake (`label_binary`)\n")
    lines.append(
        "Joined **`ok`** log lines to the cohort plan on **`sample_id`**. Convention: **`0` = real**, "
        "**`1` = fake** (same as consolidated `fakenews.tsv`; verify if you changed mapping).\n"
    )
    lines.append("| Class | `label_binary` | Count | % of all `ok` |")
    lines.append("|-------|----------------|------:|--------------:|")
    lines.append(f"| Real | `0` | {lbl0:,} | **{pct_lbl0:.2f}%** |")
    lines.append(f"| Fake | `1` | {lbl1:,} | **{pct_lbl1:.2f}%** |")
    if lbl_empty:
        pct_e = (100.0 * lbl_empty / n_ok) if n_ok else 0.0
        lines.append(f"| *(empty `label_binary`)* | — | {lbl_empty:,} | **{pct_e:.2f}%** |")
    lines.append(f"| **Total `ok`** | | {n_ok:,} | **100%** |")
    lines.append("")
    lines.append(
        f"*Reconciliation: **`ok`** with plan join (any `label_binary` field): **{n_ok_plan_join:,}**; "
        f"`ok` **not** in plan file: **{not_in_plan_ok:,}**.*"
    )
    lines.append("")
    lines.append("| Dataset | `0` (real) | % within corpus `ok` | `1` (fake) | % within corpus `ok` | Total `ok` |")
    lines.append("|---------|------------:|-------------------------:|-----------:|-------------------------:|-----------:|")
    for ds in ("fakeddit", "fakenewsnet"):
        c0 = _ds_lb(ds, "0")
        c1 = _ds_lb(ds, "1")
        t = c0 + c1
        p0 = (100.0 * c0 / t) if t else 0.0
        p1 = (100.0 * c1 / t) if t else 0.0
        lines.append(
            f"| {ds} | {c0:,} | **{p0:.2f}%** | {c1:,} | **{p1:.2f}%** | {t:,} |"
        )
    lines.append("")
    other_lbl = {k: v for k, v in ok_by_label_binary.items() if k not in ("0", "1", "")}
    if other_lbl:
        lines.append("*Other `label_binary` values (unexpected):* `" + str(other_lbl) + "`")
        lines.append("")

    lines.append("## By dataset\n")
    lines.append("| Dataset | `ok` | % of all `ok` | `fail` |")
    lines.append("|---------|-----:|----------------:|-------:|")
    all_ds = sorted(set(ok_by_ds.keys()) | set(fail_by_ds.keys()))
    for ds in all_ds:
        pct_of_ok = (100.0 * ok_by_ds[ds] / n_ok) if n_ok else 0.0
        lines.append(f"| {ds} | {ok_by_ds[ds]:,} | **{pct_of_ok:.2f}%** | {fail_by_ds[ds]:,} |")
    lines.append("")
    lines.append("The **% of all `ok`** column is the corpus share of every successful image save (sums to 100%).")
    lines.append("")

    _fnn_label = {"gossipcop": "GossipCop", "politifact": "PolitiFact", "unknown": "Unknown"}
    if fnn_ok_total or fnn_fail_total:
        lines.append("## FakeNewsNet by news source (PolitiFact vs GossipCop)\n")
        lines.append(
            "Breakdown uses the `sample_id` pattern `fnn:<source>:<label>:…` (only `dataset=fakenewsnet`).\n"
        )
        lines.append(
            "| Source | `ok` | % of FNN `ok` | `fail` | % of FNN `fail` | attempts | Success rate |"
        )
        lines.append("|--------|-----:|----------------:|-------:|------------------:|---------:|-------------:|")
        for src in fnn_src_keys:
            o = int(fnn_src_ok[src])
            f = int(fnn_src_fail[src])
            att = o + f
            lbl = _fnn_label.get(src, src)
            p_ok = (100.0 * o / fnn_ok_total) if fnn_ok_total else 0.0
            p_f = (100.0 * f / fnn_fail_total) if fnn_fail_total else 0.0
            sr = (100.0 * o / att) if att else 0.0
            lines.append(
                f"| {lbl} | {o:,} | **{p_ok:.2f}%** | {f:,} | **{p_f:.2f}%** | {att:,} | {sr:.2f}% |"
            )
        lines.append("")
        lines.append(
            "**% of FNN `ok` / `fail`:** share within FakeNewsNet attempts only (each pair sums to ~100% across sources if no `unknown`)."
        )
        lines.append("")

    if plan_index:
        lines.append("## Vs cohort plan (join on `sample_id`)\n")
        lines.append(f"- **Rows in plan file:** primary {plan_n_primary:,} · reserve {plan_n_reserve:,}")
        lines.append(f"- **Nominal primary target *N* (for %):** {primary_target:,}")
        lines.append(f"- **`ok` on primary rows:** {ok_primary_in_plan:,} ({pct_primary_ok:.2f}% of *N*)")
        lines.append(f"- **`ok` on reserve rows:** {ok_reserve_in_plan:,}")
        lines.append(f"- **`fail` on primary rows:** {fail_primary_in_plan:,}")
        lines.append(f"- **`fail` on reserve rows:** {fail_reserve_in_plan:,}")
        lines.append(f"- **`ok` not found in plan:** {not_in_plan_ok:,}")
        lines.append(f"- **`fail` not found in plan:** {not_in_plan_fail:,}")
        lines.append("")

    lines.append("## Failure `detail` field\n")
    lines.append(
        "The cohort fetcher (`pipeline/08_cohort_fetch_images.py`, `_download_one`) writes either a **fixed string** "
        "after local checks (size, SHA blocklist, PIL verify) or **`type(e).__name__`** from the HTTP stack "
        "(typically `requests` / `urllib3`). The log does not store full stack traces or HTTP status lines for "
        "most errors.\n"
    )
    lines.append("### Observed in this log (top 20)\n")
    lines.append("| Count | Code / `detail` | Meaning |")
    lines.append("|------:|------------------|---------|")
    for detail, cnt in fail_detail.most_common(20):
        safe_detail = detail.replace("|", "\\|").replace("\n", " ")
        expl = _failure_detail_explanation(detail).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {cnt:,} | `{safe_detail}` | {expl} |")
    lines.append("")
    lines.append("### Reference: codes used by the fetcher\n")
    lines.append("| Code | Meaning |")
    lines.append("|------|---------|")
    ref_keys = sorted(k for k in _FAILURE_DETAIL_GLOSSARY if k != "(empty)")
    for code in ref_keys:
        expl = _FAILURE_DETAIL_GLOSSARY[code].replace("|", "\\|").replace("\n", " ")
        lines.append(f"| `{code}` | {expl} |")
    if "(empty)" in _FAILURE_DETAIL_GLOSSARY:
        c = "(empty)"
        expl = _FAILURE_DETAIL_GLOSSARY[c].replace("|", "\\|").replace("\n", " ")
        lines.append(f"| `{c}` | {expl} |")
    lines.append("")
    lines.append(
        "*Other `detail` values that look like Python class names (`ConnectError`, `OSError`, …) are passed "
        "through from the same generic `except` path; check the Requests/urllib3 documentation for that type.*"
    )
    lines.append("")

    md_text = "\n".join(lines)
    md_path = out_dir / "cohort_image_fetch_summary.md"
    md_path.write_text(md_text, encoding="utf-8")

    if not args.quiet:
        print(md_text)
    print(f"Wrote {md_path}", file=sys.stderr)
    print(f"Wrote {json_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
