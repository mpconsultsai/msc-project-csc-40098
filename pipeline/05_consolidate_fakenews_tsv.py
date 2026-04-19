"""
Build ``data/fakenews.tsv`` from Fakeddit multimodal TSVs + FakeNewsNet ``news content.json`` trees.

**Where this fits:** run after **01** (FNN crawl) and **02** (Fakeddit v2 download), or whenever you need
to refresh the unified table. **01** can invoke ``all`` automatically when this file exists (see
``01_acquire_fakenewsnet_crawl.py``).

**Schema:** see ``pipeline/DATASETS_OVERVIEW.md`` §4. ``label_binary`` is ``0``/``1`` strings (1 = fake).

    python pipeline/05_consolidate_fakenews_tsv.py all \\
        --input-root data/processed/fakeddit/v2_text_metadata \\
        --collected data/processed/fakenewsnet \\
        --failure-log data/processed/fakenewsnet/crawl_failures.jsonl \\
        --out data/fakenews.tsv

    python pipeline/05_consolidate_fakenews_tsv.py fakeddit --out data/fakenews.tsv
    python pipeline/05_consolidate_fakenews_tsv.py fakenewsnet --collected data/processed/fakenewsnet --out data/fakenews.tsv

Paths are relative to the **project root** (parent of ``pipeline/``).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Fakeddit multimodal filenames → split_official (matches ``07_cohort_build_plan`` expectations).
_FAKEDDIT_SPLIT: dict[str, str] = {
    "multimodal_train.tsv": "train",
    "multimodal_validate.tsv": "validation",
    "multimodal_test_public.tsv": "test",
}

# Output columns (§4); empty optional fields included for downstream scripts.
_OUT_FIELDS = [
    "dataset",
    "sample_id",
    "split_official",
    "split_study",
    "label_binary",
    "label_fine",
    "text",
    "title_raw",
    "image_ref",
    "has_image_ref",
    "image_local_path",
    "image_download_ok",
    "image_preprocessed_path",
    "image_training_ready",
    "article_url",
    "domain",
    "provenance",
]

_TRACKER_HOST_FRAGMENTS = (
    "google-analytics",
    "doubleclick",
    "facebook.com/tr",
    "pixel",
    "scorecardresearch",
)


def _resolve(root: Path, p: Path) -> Path:
    p = p.expanduser()
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def _load_failure_keys(path: Path | None) -> set[tuple[str, str, str]]:
    """(news_source, label, news_id) for event=failed lines."""
    keys: set[tuple[str, str, str]] = set()
    if path is None or not path.is_file():
        return keys
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            if o.get("event") != "failed":
                continue
            try:
                src = str(o["news_source"]).strip().lower()
                lbl = str(o["label"]).strip().lower()
                nid = str(o["news_id"]).strip()
                if src and lbl and nid:
                    keys.add((src, lbl, nid))
            except KeyError:
                continue
    except OSError:
        pass
    return keys


def _url_ok_for_ref(url: str) -> bool:
    u = url.strip()
    if not u or not u.startswith(("http://", "https://")):
        return False
    low = u.lower()
    if any(x in low for x in _TRACKER_HOST_FRAGMENTS):
        return False
    return True


def _pick_image_ref(article: dict[str, Any]) -> str:
    top = (article.get("top_img") or "").strip()
    if top and _url_ok_for_ref(top):
        return top
    for raw in article.get("images") or []:
        u = raw.strip() if isinstance(raw, str) else ""
        if u and _url_ok_for_ref(u):
            return u
    return ""


def _load_fnn_index(dataset_dir: Path) -> dict[tuple[str, str, str], dict[str, str]]:
    """(source, label, id) -> {news_url, title} from official CSVs."""
    out: dict[tuple[str, str, str], dict[str, str]] = {}
    if not dataset_dir.is_dir():
        return out
    for name in (
        "politifact_fake.csv",
        "politifact_real.csv",
        "gossipcop_fake.csv",
        "gossipcop_real.csv",
    ):
        p = dataset_dir / name
        if not p.is_file():
            continue
        parts = name.replace(".csv", "").split("_")
        if len(parts) < 2:
            continue
        src = parts[0].lower()
        lbl = parts[1].lower()
        with p.open(encoding="utf-8", newline="") as fp:
            r = csv.DictReader(fp)
            for row in r:
                nid = str(row.get("id") or "").strip()
                if not nid:
                    continue
                out[(src, lbl, nid)] = {
                    "news_url": (row.get("news_url") or "").strip(),
                    "title": (row.get("title") or "").strip(),
                }
    return out


def _iter_fakeddit_rows(input_root: Path) -> Iterable[dict[str, str]]:
    if not input_root.is_dir():
        return
    for tsv_path in sorted(input_root.rglob("*.tsv")):
        split_off = _FAKEDDIT_SPLIT.get(tsv_path.name)
        if split_off is None:
            continue
        prov = str(tsv_path.relative_to(PROJECT_ROOT))
        with tsv_path.open(encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp, delimiter="\t")
            for row in reader:
                rid = (row.get("id") or "").strip()
                if not rid:
                    continue
                image_url = (row.get("image_url") or "").strip()
                clean_title = (row.get("clean_title") or row.get("title") or "").strip()
                title_raw = (row.get("title") or "").strip()
                v_lb = row.get("2_way_label")
                s_lb = str(v_lb).strip() if v_lb is not None and v_lb != "" else ""
                lb = s_lb if s_lb in ("0", "1") else ""
                fine = (row.get("6_way_label") or "").strip()
                sub = (row.get("subreddit") or row.get("domain") or "").strip()
                has_ref = "true" if image_url else "false"
                yield {
                    "dataset": "fakeddit",
                    "sample_id": f"fd:{rid}",
                    "split_official": split_off,
                    "split_study": "",
                    "label_binary": lb if lb in ("0", "1") else "",
                    "label_fine": fine,
                    "text": clean_title,
                    "title_raw": title_raw,
                    "image_ref": image_url,
                    "has_image_ref": has_ref,
                    "image_local_path": "",
                    "image_download_ok": "",
                    "image_preprocessed_path": "",
                    "image_training_ready": "",
                    "article_url": "",
                    "domain": sub,
                    "provenance": prov,
                }


def _iter_fnn_rows(
    collected: Path,
    index: dict[tuple[str, str, str], dict[str, str]],
    failure_keys: set[tuple[str, str, str]],
) -> Iterable[dict[str, str]]:
    if not collected.is_dir():
        return
    for json_path in sorted(collected.rglob("news content.json")):
        try:
            rel = json_path.relative_to(collected)
        except ValueError:
            continue
        parts = rel.parts
        if len(parts) != 4 or parts[3] != "news content.json":
            continue
        source, label, nid, _ = parts
        source = source.strip().lower()
        label = label.strip().lower()
        nid = nid.strip()
        key = (source, label, nid)
        if key in failure_keys:
            continue
        try:
            article = json.loads(json_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(article, dict):
            continue
        text = (article.get("text") or "").strip()
        idx = index.get(key, {})
        title_csv = (idx.get("title") or "").strip()
        if not text:
            text = title_csv
        title_raw = (article.get("title") or title_csv or "").strip()
        url_json = (article.get("url") or "").strip()
        news_url = (idx.get("news_url") or "").strip() or url_json
        img = _pick_image_ref(article)
        has_ref = "true" if img else "false"
        lb = "1" if label == "fake" else "0" if label == "real" else ""
        prov = str(json_path.relative_to(PROJECT_ROOT))
        yield {
            "dataset": "fakenewsnet",
            "sample_id": f"fnn:{source}:{label}:{nid}",
            "split_official": "",
            "split_study": "",
            "label_binary": lb,
            "label_fine": "",
            "text": text,
            "title_raw": title_raw,
            "image_ref": img,
            "has_image_ref": has_ref,
            "image_local_path": "",
            "image_download_ok": "",
            "image_preprocessed_path": "",
            "image_training_ready": "",
            "article_url": news_url,
            "domain": source,
            "provenance": prov,
        }


def _write_tsv(path: Path, rows: Iterable[dict[str, str]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=_OUT_FIELDS, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in _OUT_FIELDS})
            n += 1
    return n


def _run_fakeddit(args: argparse.Namespace) -> int:
    root = _resolve(PROJECT_ROOT, Path(args.input_root))
    out = _resolve(PROJECT_ROOT, Path(args.out))
    n = _write_tsv(out, _iter_fakeddit_rows(root))
    print(f"Wrote {n} fakeddit rows -> {out}", file=sys.stderr)
    return 0


def _run_fakenewsnet(args: argparse.Namespace) -> int:
    collected = _resolve(PROJECT_ROOT, Path(args.collected))
    dataset_dir = _resolve(PROJECT_ROOT, Path(args.dataset_dir))
    fl = _resolve(PROJECT_ROOT, Path(args.failure_log)) if args.failure_log else None
    keys = _load_failure_keys(fl)
    index = _load_fnn_index(dataset_dir)
    out = _resolve(PROJECT_ROOT, Path(args.out))
    n = _write_tsv(out, _iter_fnn_rows(collected, index, keys))
    print(
        f"Wrote {n} fakenewsnet rows -> {out} (failure keys skipped: {len(keys)})",
        file=sys.stderr,
    )
    return 0


def _run_all(args: argparse.Namespace) -> int:
    input_root = _resolve(PROJECT_ROOT, Path(args.input_root))
    collected = _resolve(PROJECT_ROOT, Path(args.collected))
    failure_log = _resolve(PROJECT_ROOT, Path(args.failure_log)) if args.failure_log else None
    dataset_dir = _resolve(PROJECT_ROOT, Path(args.dataset_dir))
    out = _resolve(PROJECT_ROOT, Path(args.out))

    keys = _load_failure_keys(failure_log)
    index = _load_fnn_index(dataset_dir)

    def merged() -> Iterable[dict[str, str]]:
        yield from _iter_fakeddit_rows(input_root)
        yield from _iter_fnn_rows(collected, index, keys)

    n = _write_tsv(out, merged())
    print(f"Wrote {n} total rows -> {out}", file=sys.stderr)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Consolidate Fakeddit + FakeNewsNet into fakenews.tsv")
    sub = ap.add_subparsers(dest="cmd", required=True)

    common_out = argparse.ArgumentParser(add_help=False)
    common_out.add_argument(
        "--out",
        type=Path,
        default=Path("data/fakenews.tsv"),
        help="Output TSV (default: data/fakenews.tsv)",
    )

    p_all = sub.add_parser("all", parents=[common_out], help="Fakeddit multimodal + FNN crawled JSON")
    p_all.add_argument(
        "--input-root",
        type=Path,
        default=Path("data/processed/fakeddit/v2_text_metadata"),
        help="Fakeddit v2 root containing multimodal_*.tsv (default: data/processed/fakeddit/v2_text_metadata)",
    )
    p_all.add_argument(
        "--collected",
        type=Path,
        default=Path("data/processed/fakenewsnet"),
        help="FNN crawl root with news content.json (default: data/processed/fakenewsnet)",
    )
    p_all.add_argument(
        "--failure-log",
        type=Path,
        default=None,
        help="FNN crawl_failures.jsonl — rows for these (source,label,id) are omitted for FNN (default: <collected>/crawl_failures.jsonl if present)",
    )
    p_all.add_argument(
        "--dataset-dir",
        type=Path,
        default=Path("pipeline/fakenewsnet/dataset"),
        help="FakeNewsNet index CSVs for URLs/titles (default: pipeline/fakenewsnet/dataset)",
    )
    p_all.set_defaults(func=_run_all)

    p_fd = sub.add_parser("fakeddit", parents=[common_out], help="Fakeddit multimodal TSVs only")
    p_fd.add_argument(
        "--input-root",
        type=Path,
        default=Path("data/processed/fakeddit/v2_text_metadata"),
        help="Fakeddit v2 root (default: data/processed/fakeddit/v2_text_metadata)",
    )
    p_fd.set_defaults(func=_run_fakeddit)

    p_fn = sub.add_parser("fakenewsnet", parents=[common_out], help="FNN news content.json only")
    p_fn.add_argument(
        "--collected",
        type=Path,
        default=Path("data/processed/fakenewsnet"),
        help="FNN crawl root (default: data/processed/fakenewsnet)",
    )
    p_fn.add_argument(
        "--failure-log",
        type=Path,
        default=None,
        help="Omit FNN rows in this log (default: <collected>/crawl_failures.jsonl if present)",
    )
    p_fn.add_argument(
        "--dataset-dir",
        type=Path,
        default=Path("pipeline/fakenewsnet/dataset"),
        help="Index CSVs (default: pipeline/fakenewsnet/dataset)",
    )
    p_fn.set_defaults(func=_run_fakenewsnet)

    args = ap.parse_args()

    if args.cmd == "all" and args.failure_log is None:
        default_fl = _resolve(PROJECT_ROOT, Path(args.collected)) / "crawl_failures.jsonl"
        if default_fl.is_file():
            args.failure_log = default_fl

    if args.cmd == "fakenewsnet" and args.failure_log is None:
        default_fl = _resolve(PROJECT_ROOT, Path(args.collected)) / "crawl_failures.jsonl"
        if default_fl.is_file():
            args.failure_log = default_fl

    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
