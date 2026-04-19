"""
Download images from the **working** ``fakenews.tsv`` under ``data/processed/images/``, keeping the
repo-root **canonical** ``data/fakenews.tsv`` read-only.

**Layout (single folder, default ``data/processed/images/``):**

- **Flat image files** — ``<dataset>_<sample_id>.<ext>`` (e.g. ``fakeddit_fd_awxhir.jpg``); no subfolders.
- **``fakenews.tsv``** — working copy of the table (created from canonical on first run; updated here).
- **``image_fetch.log``** — one tab-separated line per **HTTP fetch attempt** (ok / fail + detail).

Canonical **``data/fakenews.tsv``** is never written. If the working TSV is missing, it is copied from
the canonical file. Use ``--refresh-from-canonical`` to replace the working copy from canonical before
a run (drops prior enrichment in that file).

Paths resolve from the **project root** (parent of ``pipeline/``).

    python -u pipeline/06_images_full_table_fetch.py
    python -u pipeline/06_images_full_table_fetch.py --retry-failed --retry-geo-flagged
    python -u pipeline/06_images_full_table_fetch.py --refresh-from-canonical

**Geo / placeholder stubs:** small-image rejection (``--min-short-edge``, ``--min-total-pixels``),
``image_geo_blocked`` column, optional ``--block-sha256-file``.

Use **unbuffered** Python (``-u`` or ``PYTHONUNBUFFERED=1``) when redirecting stderr to a file.
Progress lines use ``--log-interval-batches``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import re
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_PROCESS_DIR = Path("data/processed/images")
DEFAULT_CANONICAL_TSV = Path("data/fakenews.tsv")

# Download errors that indicate CDN/geo-style placeholder payloads (flag for end-of-pipeline review).
_GEO_REVIEW_ERRORS = frozenset({"likely_placeholder_small", "blocked_sha256"})

_FILENAME_BAD = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

_EXTRA_COLS = ("image_local_path", "image_download_ok", "image_geo_blocked")

_CONTENT_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
}

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _resolve_project_path(p: Path, root: Path) -> Path:
    if p.is_absolute():
        return p
    return (root / p).resolve()


def _sanitize(s: str) -> str:
    t = _FILENAME_BAD.sub("_", (s or "").strip())
    return t if t else "unknown"


def _guess_ext_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
        lower = path.lower()
        for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"):
            if lower.endswith(ext):
                return ".jpg" if ext == ".jpeg" else ext
    except Exception:
        pass
    return ".jpg"


def _ext_from_content_type(ct: str | None) -> str | None:
    if not ct:
        return None
    key = ct.split(";")[0].strip().lower()
    return _CONTENT_EXT.get(key)


def _load_sha256_blocklist(path: Path | None) -> frozenset[str]:
    if path is None or not path.is_file():
        return frozenset()
    out: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip().split("#", 1)[0].strip().lower()
        if len(s) == 64 and all(c in "0123456789abcdef" for c in s):
            out.add(s)
    return frozenset(out)


def _append_item_log(
    log_path: Path,
    lock: threading.Lock,
    *,
    dataset: str,
    sample_id: str,
    status: str,
    detail: str,
    geo_flag: str,
) -> None:
    """One TSV line per fetch attempt (thread-safe append)."""
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    d = detail.replace("\t", " ").replace("\n", " ")[:500]
    line = f"{ts}\t{dataset}\t{sample_id}\t{status}\t{d}\t{geo_flag}\n"
    with lock:
        with log_path.open("a", encoding="utf-8") as fp:
            fp.write(line)


def _ensure_item_log_header(log_path: Path, lock: threading.Lock) -> None:
    with lock:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if log_path.exists() and log_path.stat().st_size > 0:
            return
        with log_path.open("w", encoding="utf-8") as fp:
            fp.write("ts_utc\tdataset\tsample_id\tstatus\tdetail\timage_geo_blocked\n")


def _download_one(
    url: str,
    dest: Path,
    timeout: float,
    session: Any,
    *,
    min_short_edge: int,
    min_total_pixels: int,
    reject_sha256: frozenset[str],
    skip_dimension_check: bool,
) -> tuple[bool, str]:
    from PIL import Image

    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = session.get(
            url,
            timeout=timeout,
            headers={"User-Agent": _USER_AGENT},
            allow_redirects=True,
        )
        r.raise_for_status()
        ext = _ext_from_content_type(r.headers.get("Content-Type"))
        if ext and dest.suffix.lower() not in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"):
            dest = dest.with_suffix(ext)
        data = r.content
        if len(data) < 512:
            return False, "too_small"
        if reject_sha256:
            h = hashlib.sha256(data).hexdigest()
            if h in reject_sha256:
                return False, "blocked_sha256"
        try:
            bio = io.BytesIO(data)
            img = Image.open(bio)
            img.verify()
        except Exception:
            return False, "pil_verify_failed"
        if not skip_dimension_check:
            bio2 = io.BytesIO(data)
            img2 = Image.open(bio2)
            img2.load()
            w, h = img2.size
            short = min(w, h)
            area = w * h
            if short < min_short_edge or area < min_total_pixels:
                return False, "likely_placeholder_small"
        dest.write_bytes(data)
        return True, ""
    except Exception as e:
        return False, type(e).__name__


def _row_needs_download(
    row: dict[str, str],
    *,
    retry_failed: bool,
    retry_geo_flagged: bool,
    images_root: Path,
) -> bool:
    if (row.get("has_image_ref") or "").strip().lower() != "true":
        return False
    url = (row.get("image_ref") or "").strip()
    if not url:
        return False
    ok = (row.get("image_download_ok") or "").strip().lower()
    rel = (row.get("image_local_path") or "").strip()
    geo = (row.get("image_geo_blocked") or "").strip().lower()
    if ok == "true" and rel:
        p = PROJECT_ROOT / rel if not Path(rel).is_absolute() else Path(rel)
        try:
            if p.is_file() and p.stat().st_size >= 512:
                return False
        except OSError:
            pass
    if geo == "true" and retry_geo_flagged:
        return True
    if ok == "false" and not retry_failed:
        return False
    return True


def _build_flat_image_path(images_root: Path, dataset: str, sample_id: str, url: str) -> Path:
    """Single flat directory: ``<dataset>_<sample_id>.<ext>``."""
    stem = f"{_sanitize(dataset)}_{_sanitize(sample_id)}"
    return images_root / f"{stem}{_guess_ext_from_url(url)}"


def _process_batch_rows(
    rows: list[dict[str, str]],
    *,
    images_root: Path,
    retry_failed: bool,
    retry_geo_flagged: bool,
    timeout: float,
    workers: int,
    session_factory: Any,
    attempts_left: list[int | None],
    min_short_edge: int,
    min_total_pixels: int,
    reject_sha256: frozenset[str],
    skip_dimension_check: bool,
    item_log: Path,
    log_lock: threading.Lock,
) -> None:
    jobs: list[tuple[int, str, Path]] = []
    for i, row in enumerate(rows):
        if not _row_needs_download(
            row,
            retry_failed=retry_failed,
            retry_geo_flagged=retry_geo_flagged,
            images_root=images_root,
        ):
            continue
        if attempts_left[0] is not None and attempts_left[0] <= 0:
            break
        url = (row.get("image_ref") or "").strip()
        ds = (row.get("dataset") or "").strip()
        sid = (row.get("sample_id") or "").strip()
        dest = _build_flat_image_path(images_root, ds, sid, url)
        jobs.append((i, url, dest))
        if attempts_left[0] is not None:
            attempts_left[0] -= 1

    if not jobs:
        return

    results: dict[int, tuple[bool, Path | None, str]] = {}

    def work(item: tuple[int, str, Path]) -> tuple[int, bool, Path | None, str]:
        idx, url, dest = item
        session = session_factory()
        ok, err = _download_one(
            url,
            dest,
            timeout,
            session,
            min_short_edge=min_short_edge,
            min_total_pixels=min_total_pixels,
            reject_sha256=reject_sha256,
            skip_dimension_check=skip_dimension_check,
        )
        if ok:
            return idx, True, dest.resolve(), ""
        return idx, False, None, err

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = {ex.submit(work, j): j[0] for j in jobs}
        for fut in as_completed(futs):
            idx, ok, path, err = fut.result()
            results[idx] = (ok, path, err)

    for idx, (ok, path, err) in results.items():
        row = rows[idx]
        ds = (row.get("dataset") or "").strip()
        sid = (row.get("sample_id") or "").strip()
        geo_s = ""
        if ok and path:
            try:
                rel = path.relative_to(PROJECT_ROOT)
                rel_s = str(rel).replace("\\", "/")
                row["image_local_path"] = rel_s
                row["image_download_ok"] = "true"
                row["image_geo_blocked"] = "false"
                _append_item_log(
                    item_log,
                    log_lock,
                    dataset=ds,
                    sample_id=sid,
                    status="ok",
                    detail=rel_s,
                    geo_flag="false",
                )
            except ValueError:
                row["image_local_path"] = str(path)
                row["image_download_ok"] = "true"
                row["image_geo_blocked"] = "false"
                _append_item_log(
                    item_log,
                    log_lock,
                    dataset=ds,
                    sample_id=sid,
                    status="ok",
                    detail=str(path),
                    geo_flag="false",
                )
        else:
            row["image_local_path"] = row.get("image_local_path") or ""
            row["image_download_ok"] = "false"
            geo_s = "true" if err in _GEO_REVIEW_ERRORS else ""
            row["image_geo_blocked"] = geo_s
            if err:
                row["_download_error"] = err
            _append_item_log(
                item_log,
                log_lock,
                dataset=ds,
                sample_id=sid,
                status="fail",
                detail=err or "unknown",
                geo_flag=geo_s or "",
            )


def _write_row(writer: csv.DictWriter, row: dict[str, str], fieldnames: list[str]) -> None:
    if (row.get("has_image_ref") or "").strip().lower() != "true":
        row["image_local_path"] = ""
        row["image_download_ok"] = "false"
        row["image_geo_blocked"] = ""
    out = {k: row.get(k, "") for k in fieldnames}
    writer.writerow(out)


def main() -> int:
    root = PROJECT_ROOT
    parser = argparse.ArgumentParser(
        description="Download images into data/processed/images/ (flat) and update working fakenews.tsv.",
    )
    parser.add_argument(
        "--process-dir",
        type=Path,
        default=DEFAULT_PROCESS_DIR,
        help="Folder for flat images + working fakenews.tsv + image_fetch.log (default: data/processed/images)",
    )
    parser.add_argument(
        "--canonical-tsv",
        type=Path,
        default=DEFAULT_CANONICAL_TSV,
        help="Read-only source used to create/refresh working fakenews.tsv (default: data/fakenews.tsv)",
    )
    parser.add_argument(
        "--work-tsv",
        type=Path,
        default=None,
        help="Working TSV to read/update (default: <process-dir>/fakenews.tsv)",
    )
    parser.add_argument(
        "--item-log",
        type=Path,
        default=None,
        help="Append one line per fetch attempt (default: <process-dir>/image_fetch.log)",
    )
    parser.add_argument(
        "--refresh-from-canonical",
        action="store_true",
        help="Overwrite working fakenews.tsv from canonical before run (loses prior enrichment in work file).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Rows per batch (default: 200)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        metavar="N",
        help="Concurrent download threads per batch (default: 6)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=45.0,
        help="HTTP timeout seconds (default: 45)",
    )
    parser.add_argument(
        "--max-download-attempts",
        type=int,
        default=0,
        metavar="N",
        help="Cap HTTP attempts (0 = no cap). All rows still written.",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-attempt rows with image_download_ok=false",
    )
    parser.add_argument(
        "--retry-geo-flagged",
        action="store_true",
        help="Re-attempt rows with image_geo_blocked=true",
    )
    parser.add_argument(
        "--sleep-between-batches",
        type=float,
        default=0.0,
        metavar="SEC",
        help="Optional pause between batches",
    )
    parser.add_argument(
        "--min-short-edge",
        type=int,
        default=100,
        metavar="PX",
        help="Min shorter image side in px (default: 100)",
    )
    parser.add_argument(
        "--min-total-pixels",
        type=int,
        default=18000,
        metavar="N",
        help="Min width*height (default: 18000)",
    )
    parser.add_argument(
        "--no-dimension-filter",
        action="store_true",
        help="Skip min-size rejection after PIL verify",
    )
    parser.add_argument(
        "--block-sha256-file",
        type=Path,
        default=None,
        metavar="PATH",
        help="Hashes of payloads to reject as placeholders",
    )
    parser.add_argument(
        "--log-interval-batches",
        type=int,
        default=5,
        metavar="N",
        help="Progress line every N batches (batch 1 always logs)",
    )
    args = parser.parse_args()

    stderr = sys.stderr
    if hasattr(stderr, "reconfigure"):
        try:
            stderr.reconfigure(line_buffering=True)  # type: ignore[union-attr]
        except (OSError, ValueError):
            pass

    process_dir = _resolve_project_path(args.process_dir, root)
    canonical = _resolve_project_path(args.canonical_tsv, root)
    work_tsv = (
        _resolve_project_path(args.work_tsv, root)
        if args.work_tsv
        else (process_dir / "fakenews.tsv")
    )
    item_log = (
        _resolve_project_path(args.item_log, root) if args.item_log else (process_dir / "image_fetch.log")
    )
    images_root = process_dir
    block_path = _resolve_project_path(args.block_sha256_file, root) if args.block_sha256_file else None
    reject_sha256 = _load_sha256_blocklist(block_path)

    process_dir.mkdir(parents=True, exist_ok=True)

    if not canonical.is_file():
        _log(f"Missing canonical TSV (needed to seed working copy): {canonical}")
        return 1

    if args.refresh_from_canonical:
        try:
            shutil.copy2(canonical, work_tsv)
            _log(f"Refreshed working TSV from canonical -> {work_tsv}")
        except OSError as e:
            _log(f"Could not copy canonical -> work: {e}")
            return 1
    elif not work_tsv.is_file():
        try:
            shutil.copy2(canonical, work_tsv)
            _log(f"Created working TSV from canonical -> {work_tsv}")
        except OSError as e:
            _log(f"Could not create working TSV: {e}")
            return 1

    log_lock = threading.Lock()
    _ensure_item_log_header(item_log, log_lock)

    try:
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
    except ImportError:
        _log("Install requests: pip install requests")
        return 1

    try:
        from PIL import Image  # noqa: F401
    except ImportError:
        _log("Install Pillow: pip install Pillow")
        return 1

    def make_session():
        s = requests.Session()
        retries = Retry(total=2, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503))
        s.mount("http://", HTTPAdapter(max_retries=retries))
        s.mount("https://", HTTPAdapter(max_retries=retries))
        return s

    tmp_fd: int | None = None
    tmp_path: Path | None = None
    try:
        tmp_fd, tmp_name = tempfile.mkstemp(
            suffix=".tsv.tmp",
            prefix="fakenews_",
            dir=str(work_tsv.parent),
        )
        tmp_path = Path(tmp_name)
    except OSError as e:
        _log(f"Could not create temp file: {e}")
        return 1

    attempts_left: list[int | None] = [
        None if args.max_download_attempts <= 0 else args.max_download_attempts
    ]
    rows_written = 0
    try:
        with open(work_tsv, newline="", encoding="utf-8") as fin, open(
            tmp_fd, "w", newline="", encoding="utf-8", closefd=True
        ) as fout:
            reader = csv.DictReader(fin, delimiter="\t")
            if reader.fieldnames is None:
                _log("Working TSV has no header")
                return 1
            base_fields = list(reader.fieldnames)
            for c in _EXTRA_COLS:
                if c not in base_fields:
                    base_fields.append(c)
            fieldnames = base_fields
            writer = csv.DictWriter(fout, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
            writer.writeheader()

            t0 = time.monotonic()
            batch_num = 0
            iv = max(1, int(args.log_interval_batches))

            _log(
                f"start: process_dir={process_dir} work_tsv={work_tsv} canonical={canonical} "
                f"item_log={item_log} batch_size={args.batch_size} workers={args.workers}"
            )

            batch: list[dict[str, str]] = []

            def flush_batch() -> None:
                nonlocal rows_written, batch_num
                if not batch:
                    return
                _process_batch_rows(
                    batch,
                    images_root=images_root,
                    retry_failed=args.retry_failed,
                    retry_geo_flagged=args.retry_geo_flagged,
                    timeout=args.timeout,
                    workers=args.workers,
                    session_factory=make_session,
                    attempts_left=attempts_left,
                    min_short_edge=max(1, args.min_short_edge),
                    min_total_pixels=max(1, args.min_total_pixels),
                    reject_sha256=reject_sha256,
                    skip_dimension_check=args.no_dimension_filter,
                    item_log=item_log,
                    log_lock=log_lock,
                )
                for r in batch:
                    r.pop("_download_error", None)
                    _write_row(writer, r, fieldnames)
                rows_written += len(batch)
                batch_num += 1
                al = attempts_left[0]
                al_s = "unlimited" if al is None else str(al)
                if batch_num == 1 or batch_num % iv == 0:
                    _log(
                        f"progress: batch={batch_num} rows_written={rows_written} "
                        f"elapsed_s={time.monotonic() - t0:.1f} download_attempts_left={al_s}"
                    )
                batch.clear()
                if args.sleep_between_batches > 0:
                    time.sleep(args.sleep_between_batches)

            for row in reader:
                row.pop("_download_error", None)
                for c in _EXTRA_COLS:
                    row.setdefault(c, "")
                batch.append(row)
                if len(batch) >= args.batch_size:
                    flush_batch()

            flush_batch()

        if work_tsv.exists():
            work_tsv.unlink()
        tmp_path.replace(work_tsv)
        _log(
            f"Done. rows_written={rows_written} work_tsv={work_tsv} "
            f"images_flat={images_root} log={item_log}"
        )
        return 0
    except KeyboardInterrupt:
        _log("Interrupted.")
        if tmp_path and tmp_path.exists():
            _log(f"Partial temp file: {tmp_path}")
        return 130
    except Exception as e:
        _log(f"Error: {e}")
        if tmp_path and tmp_path.exists():
            _log(f"Temp file (may be partial): {tmp_path}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
