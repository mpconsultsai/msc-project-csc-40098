"""
Crawl FakeNewsNet article text and image URLs from news-site links in the official CSVs.

Uses the upstream FakeNewsNet news crawler (``pipeline/sources/fakenewsnet/code``; newspaper3k + optional
Wayback fallback). News articles only — not FNN's optional social/Twitter layer. Each story is saved as ``news content.json``
under ``<out>/<source>/<label>/<id>/``; failed fetches are appended to ``<out>/crawl_failures.jsonl``.

Re-runs are point-in-time snapshots — exact replication of a prior crawl is not expected (link rot,
site changes, Wayback differences). Compare runs by story id, not identical JSON or failure counts.

Relative ``--out`` and ``--dataset-dir`` paths resolve from the project root (parent of ``pipeline/``).

    pip install -r pipeline/requirements.txt
    python pipeline/src/01_acquire_fakenewsnet_crawl.py --out data/processed/fakenewsnet --resume

Resume, failure-log skip logic, performance flags, and optional post-crawl consolidation: see
``pipeline/README.md`` or ``--help``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from _paths import PROJECT_ROOT


def _utc_iso() -> str:
    """Return the current UTC time as an ISO-8601 string (seconds, no microseconds)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _append_jsonl(path: Path, row: dict, lock: threading.Lock | None = None) -> None:
    """Append one JSON object as a line to a JSONL file.

    Args:
        path: Output log path; parent directories are created if missing.
        row: JSON-serializable mapping written as a single line.
        lock: Optional lock for thread-safe appends when ``--workers`` > 1.
    """
    line = json.dumps(row, ensure_ascii=False) + "\n"
    if lock:
        with lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(line)


def _silence_upstream_crawl_logs() -> None:
    """Suppress noisy newspaper/urllib logging from upstream crawl failures.

    Upstream calls ``logging.exception()`` on every failed HTTP parse. Silence unless
    ``--verbose-crawl`` is set.
    """
    import logging

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(logging.NullHandler())
    root.setLevel(logging.CRITICAL)
    for name in ("urllib3", "requests", "newspaper"):
        logging.getLogger(name).setLevel(logging.CRITICAL)


def _load_known_failure_keys(path: Path) -> set[tuple[str, str, str]]:
    """Load unique failed story keys from a crawl failure JSONL log.

    Args:
        path: ``crawl_failures.jsonl`` (or equivalent append-only log).

    Returns:
        Set of ``(news_source, label, news_id)`` tuples with at least one ``event=failed`` row.
        Empty if the file is missing or unreadable.
    """
    keys: set[tuple[str, str, str]] = set()
    if not path.is_file():
        return keys
    try:
        with path.open(encoding="utf-8") as f:
            for line in f:
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
                    keys.add((o["news_source"], o["label"], o["news_id"]))
                except KeyError:
                    continue
    except OSError:
        pass
    return keys


def _load_existing_article(path: Path) -> dict | None:
    """Read an existing ``news content.json`` file if present and valid JSON.

    Args:
        path: Path to ``news content.json`` for one story.

    Returns:
        Parsed JSON object, or ``None`` if the file is missing or corrupt.
    """
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _resolve_project_path(path: Path, project_root: Path) -> Path:
    """Resolve a CLI path relative to the project root, not the shell cwd.

    Args:
        path: User-supplied path (may be relative or absolute).
        project_root: Repository root (parent of ``pipeline/``).

    Returns:
        Absolute resolved path.
    """
    path = path.expanduser()
    if path.is_absolute():
        return path.resolve()
    return (project_root / path).resolve()


def _patch_upstream_post_download_sleep(news_content_module: Any, cap_seconds: float) -> None:
    """Cap upstream ``time.sleep`` after each newspaper download.

    Args:
        news_content_module: Imported ``news_content_collection`` module (monkey-patched in place).
        cap_seconds: Maximum sleep seconds per attempt (upstream default is 2s).
    """
    cap = max(0.0, float(cap_seconds))
    real_sleep = news_content_module.time.sleep

    def bounded_sleep(requested: float) -> None:
        try:
            req = float(requested)
        except (TypeError, ValueError):
            req = cap
        real_sleep(min(req, cap))

    news_content_module.time.sleep = bounded_sleep


def _process_single_item(
    news: Any,
    save_dir: Path,
    crawl_fn: Any,
) -> dict[str, Any]:
    """Fetch one story and classify the outcome for logging or write.

    Args:
        news: Row from upstream ``DataCollector.load_news_file`` (``news_id``, ``news_url``).
        save_dir: ``<out>/<source>/<label>/`` directory for this split.
        crawl_fn: Callable accepting a URL and returning article JSON or ``None``.

    Returns:
        Dict with ``kind`` ``"ok"`` or ``"failed"``. On success includes ``out_json`` and
        ``payload``; on failure includes ``reason``, ``detail``, ``news_id``, and ``news_url``.
    """
    article_dir = save_dir / news.news_id
    out_json = article_dir / "news content.json"
    article_dir.mkdir(parents=True, exist_ok=True)
    try:
        news_article = crawl_fn(news.news_url)
    except Exception as e:
        return {
            "kind": "failed",
            "reason": "exception",
            "detail": f"{type(e).__name__}: {e}",
            "news_id": news.news_id,
            "news_url": news.news_url,
            "out_json": out_json,
        }
    if not news_article:
        return {
            "kind": "failed",
            "reason": "no_article",
            "detail": "crawl_news_article returned None (live URL and Wayback both unavailable or parse failed)",
            "news_id": news.news_id,
            "news_url": news.news_url,
            "out_json": out_json,
        }
    body = (news_article.get("text") or "").strip()
    if not body:
        return {
            "kind": "failed",
            "reason": "empty_body",
            "detail": "Article parsed but text field is empty; JSON not written",
            "news_id": news.news_id,
            "news_url": news.news_url,
            "out_json": out_json,
        }
    return {"kind": "ok", "out_json": out_json, "payload": news_article}


def main() -> int:
    """Run the FakeNewsNet article crawl from CLI arguments.

    Loads index CSVs, fetches articles via upstream ``crawl_news_article``, writes
    ``news content.json`` and ``crawl_failures.jsonl``, then optionally invokes
    ``04_consolidate_fakenews_tsv.py all``.

    Returns:
        ``0`` on success, ``1`` on invalid args or missing upstream clone/dataset paths.
    """
    root = PROJECT_ROOT
    parser = argparse.ArgumentParser(
        description="Crawl FakeNewsNet article JSON from index CSVs (newspaper3k; news articles only)."
    )
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=root / "pipeline" / "sources" / "fakenewsnet" / "dataset",
        help="Directory with politifact_*.csv and gossipcop_*.csv",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=root / "data" / "processed" / "fakenewsnet",
        help="Output tree: <out>/<source>/<label>/<id>/news content.json",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip items that already have a readable news content.json (re-run safely later).",
    )
    parser.add_argument(
        "--max-articles",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Queue at most N articles for crawl in this run, counted after resume skips and failure-log skips; "
            "applies across all news_source/label buckets (stops scanning further CSV rows once the cap is reached)."
        ),
    )
    parser.add_argument(
        "--retry-empty",
        action="store_true",
        help="With --resume, still re-fetch when existing JSON has no non-whitespace text (old partial saves).",
    )
    parser.add_argument(
        "--failure-log",
        type=Path,
        default=None,
        help="Append-only JSONL of failed fetches (default: <out>/crawl_failures.jsonl).",
    )
    parser.add_argument(
        "--log-skipped",
        action="store_true",
        help="With --resume, append skips to <out>/crawl_skipped_resume.jsonl.",
    )
    parser.add_argument(
        "--verbose-crawl",
        action="store_true",
        help="Show upstream newspaper/logging tracebacks for each failed URL (very noisy).",
    )
    parser.add_argument(
        "--post-download-sleep",
        type=float,
        default=0.2,
        metavar="SEC",
        help=(
            "Cap upstream's post-download sleep (FakeNewsNet uses 2s per attempt). "
            "Default: 0.2. Use 2 to mimic upstream."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Parallel fetch threads (default 1). Try 4–8 for speed; may trigger more 429/403.",
    )
    parser.add_argument(
        "--no-wayback",
        action="store_true",
        help="Skip Internet Archive fallback after a failed live fetch (faster for dead URLs; fewer recoveries).",
    )
    parser.add_argument(
        "--retry-known-failures",
        action="store_true",
        help="Re-fetch rows listed in the failure log; default is to skip them (faster re-runs).",
    )
    parser.add_argument(
        "--no-consolidate-image-refs",
        action="store_true",
        help="Do not regenerate data/fakenews.tsv (combined image-ref TSV) when the run finishes.",
    )
    parser.add_argument(
        "--image-refs-out",
        type=Path,
        default=Path("data/fakenews.tsv"),
        help="Output path for 04_consolidate_fakenews_tsv.py all (project-relative; default data/fakenews.tsv).",
    )
    parser.add_argument(
        "--consolidate-fakeddit-root",
        type=Path,
        default=Path("data/processed/fakeddit/v2_text_metadata"),
        help="Fakeddit root passed to 04_consolidate_fakenews_tsv.py all as --input-root.",
    )
    args = parser.parse_args()

    if args.max_articles is not None and args.max_articles < 1:
        print("--max-articles must be >= 1", file=sys.stderr)
        return 1

    if not args.verbose_crawl:
        _silence_upstream_crawl_logs()

    out = _resolve_project_path(args.out, root)
    dataset_dir = _resolve_project_path(args.dataset_dir, root)
    failure_log = (
        _resolve_project_path(args.failure_log, root) if args.failure_log else (out / "crawl_failures.jsonl")
    )
    skipped_log = out / "crawl_skipped_resume.jsonl"

    known_failures: set[tuple[str, str, str]] = set()
    if not args.retry_known_failures:
        known_failures = _load_known_failure_keys(failure_log)

    w = max(1, int(args.workers))
    max_art = args.max_articles
    print(
        f"Output: {out}\nDataset: {dataset_dir}\nResume: {args.resume}\n"
        f"Max articles queued: {max_art if max_art is not None else '(no cap)'}\n"
        f"Post-download sleep cap: {args.post_download_sleep}s\nWorkers: {w}\n"
        f"Skip ids in failure log: {not args.retry_known_failures} "
        f"({len(known_failures)} unique keys loaded)",
        file=sys.stderr,
    )

    code_dir = root / "pipeline" / "sources" / "fakenewsnet" / "code"
    if not code_dir.is_dir():
        print("Missing FakeNewsNet clone at pipeline/sources/fakenewsnet/code", file=sys.stderr)
        return 1
    if not dataset_dir.is_dir():
        print(f"Dataset directory not found: {dataset_dir}", file=sys.stderr)
        return 1

    sys.path.insert(0, str(code_dir))

    import news_content_collection as ncc  # noqa: E402
    from news_content_collection import crawl_news_article, create_dir  # noqa: E402
    from util.util import DataCollector  # noqa: E402

    _patch_upstream_post_download_sleep(ncc, args.post_download_sleep)
    if args.no_wayback:
        ncc.get_website_url_from_arhieve = lambda _url: None  # type: ignore[method-assign]

    class NewsOnlyConfig:
        """Minimal config for ``DataCollector`` without upstream Twitter API wiring."""

        __slots__ = ("dataset_dir", "dump_location", "num_process")

        def __init__(self, dataset_dir: str, dump_location: str, num_process: int = 4) -> None:
            self.dataset_dir = dataset_dir
            self.dump_location = dump_location
            self.num_process = num_process

    class Loader(DataCollector):
        """Upstream CSV loader; subclass satisfies upstream typing only."""

        pass

    choices = [
        {"news_source": "politifact", "label": "fake"},
        {"news_source": "politifact", "label": "real"},
        {"news_source": "gossipcop", "label": "fake"},
        {"news_source": "gossipcop", "label": "real"},
    ]

    cfg = NewsOnlyConfig(
        dataset_dir=str(dataset_dir),
        dump_location=str(out),
        num_process=4,
    )
    loader = Loader(cfg)

    try:
        from tqdm import tqdm  # noqa: E402  # pyright: ignore[reportMissingModuleSource]
    except ImportError:
        tqdm = None  # type: ignore[misc, assignment]

    total_attempted = 0
    total_ok = 0
    total_failed = 0
    total_skipped = 0
    total_skipped_known_failure = 0
    queued_for_crawl = 0
    max_q = args.max_articles

    for choice in choices:
        if max_q is not None and queued_for_crawl >= max_q:
            break
        news_list = loader.load_news_file(choice)
        news_source = choice["news_source"]
        label = choice["label"]
        create_dir(str(out))
        create_dir(str(out / news_source))
        create_dir(str(out / news_source / label))
        save_dir = out / news_source / label

        pending: list[Any] = []
        for news in news_list:
            if max_q is not None and queued_for_crawl >= max_q:
                break
            total_attempted += 1
            out_json = save_dir / news.news_id / "news content.json"
            if args.resume:
                existing = _load_existing_article(out_json)
                if existing is not None:
                    empty_text = not (existing.get("text") or "").strip()
                    if not (args.retry_empty and empty_text):
                        total_skipped += 1
                        if args.log_skipped:
                            _append_jsonl(
                                skipped_log,
                                {
                                    "ts": _utc_iso(),
                                    "event": "skipped_resume",
                                    "news_source": news_source,
                                    "label": label,
                                    "news_id": news.news_id,
                                    "news_url": news.news_url,
                                    "has_text": not empty_text,
                                },
                            )
                        continue
            key = (news_source, label, news.news_id)
            if known_failures and key in known_failures:
                total_skipped_known_failure += 1
                continue
            pending.append(news)
            queued_for_crawl += 1

        def apply_result(r: dict[str, Any]) -> None:
            """Write a successful crawl to disk or append a failure row to the JSONL log."""
            nonlocal total_ok, total_failed
            if r["kind"] == "ok":
                r["out_json"].write_text(json.dumps(r["payload"], ensure_ascii=False), encoding="utf-8")
                total_ok += 1
                return
            total_failed += 1
            _append_jsonl(
                failure_log,
                {
                    "ts": _utc_iso(),
                    "event": "failed",
                    "reason": r["reason"],
                    "detail": r["detail"],
                    "news_source": news_source,
                    "label": label,
                    "news_id": r["news_id"],
                    "news_url": r["news_url"],
                },
            )

        if w <= 1:
            iterator = tqdm(pending, desc=f"{news_source}/{label}") if tqdm else pending
            for news in iterator:
                apply_result(_process_single_item(news, save_dir, crawl_news_article))
        else:
            with ThreadPoolExecutor(max_workers=w) as ex:
                futures = [
                    ex.submit(_process_single_item, n, save_dir, crawl_news_article) for n in pending
                ]
                pbar = tqdm(total=len(futures), desc=f"{news_source}/{label}") if tqdm else None
                for fut in as_completed(futures):
                    apply_result(fut.result())
                    if pbar is not None:
                        pbar.update(1)
                if pbar is not None:
                    pbar.close()

    meta = {
        "dataset_dir": str(dataset_dir),
        "dump_location": str(out),
        "mode": "news_articles_only",
        "max_articles": max_art,
        "queued_for_crawl": queued_for_crawl,
        "resume": args.resume,
        "retry_empty": args.retry_empty,
        "failure_log": str(failure_log),
        "post_download_sleep": args.post_download_sleep,
        "workers": w,
        "no_wayback": args.no_wayback,
        "retry_known_failures": args.retry_known_failures,
        "last_run_ts": _utc_iso(),
        "counts": {
            "attempted": total_attempted,
            "written_ok": total_ok,
            "failed": total_failed,
            "skipped_resume": total_skipped,
            "skipped_known_failure": total_skipped_known_failure,
        },
        "note": "FNN's optional social/Twitter collection uses upstream main.py + API keys (see pipeline/sources/fakenewsnet/README.md).",
    }
    (out / "_manifest.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(
        f"Done. ok={total_ok} failed={total_failed} skipped_resume={total_skipped} "
        f"skipped_known_failure={total_skipped_known_failure} "
        f"(attempted loop entries={total_attempted}). Failures: {failure_log}"
    )

    if not args.no_consolidate_image_refs:
        tsv_out = _resolve_project_path(args.image_refs_out, root)
        fakeddit_root = _resolve_project_path(args.consolidate_fakeddit_root, root)
        script = root / "pipeline" / "src" / "04_consolidate_fakenews_tsv.py"
        if not script.is_file():
            print(
                "Skipping image-ref consolidation: pipeline/src/04_consolidate_fakenews_tsv.py not found. "
                "Use --no-consolidate-image-refs to silence this, or add that script.",
                file=sys.stderr,
            )
        else:
            cmd = [
                sys.executable,
                str(script),
                "all",
                "--input-root",
                str(fakeddit_root),
                "--collected",
                str(out),
                "--failure-log",
                str(failure_log),
                "--out",
                str(tsv_out),
            ]
            print(f"Combined image-ref TSV: running {' '.join(cmd)}", file=sys.stderr)
            proc = subprocess.run(cmd, cwd=str(root))
            if proc.returncode != 0:
                print(
                    f"Warning: 04_consolidate_fakenews_tsv.py all exited {proc.returncode}",
                    file=sys.stderr,
                )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
