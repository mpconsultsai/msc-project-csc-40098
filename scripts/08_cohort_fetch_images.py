"""
Download images listed in a cohort plan TSV (from ``07_cohort_build_plan.py``).

Writes one file per row under ``data/processed/images/`` using a **sanitised** ``sample_id`` as the
stem (e.g. ``fd_9b85vf.jpg``). **Single-threaded**, append-only log, idempotent **resume**: any
``sample_id`` that already appears in ``cohort_image_fetch.log`` (**ok or fail**) is skipped; the log
is the only resume signal (no scanning the image folder). Stale URLs are assumed to keep failing, so
``fail`` rows are not retried unless you use ``--force``, a fresh ``--log``, or edit the log.

Paths resolve from the project root (parent of ``scripts/``).

    python -u scripts/08_cohort_fetch_images.py --plan-tsv data/processed/cohorts/multimodal_plan_n50000_seed42.tsv
    python -u scripts/08_cohort_fetch_images.py --plan-tsv ... --limit 100
    python -u scripts/08_cohort_fetch_images.py --plan-tsv ... --stop-after-ok 50000
    python -u scripts/08_cohort_fetch_images.py --plan-tsv ... --force
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_OUT_DIR = Path("data/processed/images")
DEFAULT_PLAN = Path("data/processed/cohorts/multimodal_plan_n50000_seed42.tsv")
DEFAULT_BLOCKLIST = Path("scripts/08_cohort_reddit_placeholder_sha256.txt")

_FILENAME_BAD = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

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


def _resolve(root: Path, p: Path) -> Path:
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def _sanitize_sample_id(sample_id: str) -> str:
    t = _FILENAME_BAD.sub("_", (sample_id or "").strip())
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


def _load_sha256_blocklist(path: Path) -> frozenset[str]:
    if not path.is_file():
        return frozenset()
    out: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip().split("#", 1)[0].strip().lower()
        if len(s) == 64 and all(c in "0123456789abcdef" for c in s):
            out.add(s)
    return frozenset(out)


def _append_log(log_path: Path, fields: list[str]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    line = "\t".join(f.replace("\t", " ").replace("\n", " ") for f in fields) + "\n"
    with log_path.open("a", encoding="utf-8") as fp:
        fp.write(line)


def _ensure_log_header(log_path: Path, header: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if log_path.exists() and log_path.stat().st_size > 0:
        return
    with log_path.open("w", encoding="utf-8") as fp:
        fp.write(header)


def _load_logged_sample_ids(log_path: Path) -> set[str]:
    """Every ``sample_id`` that appears in the log (``ok`` or ``fail``): treat as already handled."""
    if not log_path.is_file():
        return set()
    out: set[str] = set()
    try:
        with log_path.open(encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp, delimiter="\t")
            if not reader.fieldnames:
                return set()
            for row in reader:
                st = (row.get("status") or "").strip().lower()
                if st not in ("ok", "fail"):
                    continue
                sid = (row.get("sample_id") or "").strip()
                if sid:
                    out.add(sid)
    except OSError:
        return set()
    return out


def _count_ok_lines_in_log(log_path: Path) -> int:
    n = 0
    if not log_path.is_file():
        return 0
    try:
        with log_path.open(encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp, delimiter="\t")
            for row in reader:
                if (row.get("status") or "").strip().lower() == "ok":
                    n += 1
    except OSError:
        return 0
    return n


def _download_one(
    url: str,
    dest: Path,
    timeout: float,
    reject_sha256: frozenset[str],
) -> tuple[bool, str, Path | None]:
    import requests
    from PIL import Image

    dest.parent.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    try:
        r = session.get(
            url,
            timeout=timeout,
            headers={"User-Agent": _USER_AGENT},
            allow_redirects=True,
        )
        r.raise_for_status()
        data = r.content
        if len(data) < 512:
            return False, "too_small", None

        h = hashlib.sha256(data).hexdigest()
        if h in reject_sha256:
            return False, "reddit_placeholder_sha256", None

        try:
            bio = io.BytesIO(data)
            img = Image.open(bio)
            img.verify()
        except Exception:
            return False, "pil_verify_failed", None

        ext = _ext_from_content_type(r.headers.get("Content-Type"))
        if ext:
            out = dest.with_suffix(ext)
        elif dest.suffix.lower() not in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"):
            out = dest.with_suffix(_guess_ext_from_url(url))
        else:
            out = dest

        out.write_bytes(data)
        return True, "", out
    except Exception as e:
        return False, type(e).__name__, None
    finally:
        session.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch cohort plan images (sanitised sample_id filenames, resume-friendly)")
    ap.add_argument(
        "--plan-tsv",
        type=Path,
        default=DEFAULT_PLAN,
        help=f"Cohort plan TSV (default: {DEFAULT_PLAN})",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Image output directory (default: {DEFAULT_OUT_DIR})",
    )
    ap.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Append fetch log TSV (default: <out-dir>/cohort_image_fetch.log)",
    )
    ap.add_argument(
        "--placeholder-sha256-file",
        type=Path,
        default=DEFAULT_BLOCKLIST,
        help=f"Hashes to reject as Reddit/CDN placeholders (default: {DEFAULT_BLOCKLIST})",
    )
    ap.add_argument("--timeout", type=float, default=45.0, help="HTTP timeout seconds (default: 45)")
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after this many download attempts in this run (skips do not count; not the same as ok count)",
    )
    ap.add_argument(
        "--stop-after-ok",
        type=int,
        default=None,
        metavar="N",
        help="Exit once total ok lines in log (existing + this run) reach N; default runs through whole plan",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Ignore log skip rules; attempt every plan row (still appends to log)",
    )
    ap.add_argument("--log-every", type=int, default=500, help="Progress stderr every N attempts (default: 500)")
    args = ap.parse_args()

    plan_path = _resolve(PROJECT_ROOT, args.plan_tsv)
    out_dir = _resolve(PROJECT_ROOT, args.out_dir)
    block_path = _resolve(PROJECT_ROOT, args.placeholder_sha256_file)
    log_path = _resolve(PROJECT_ROOT, args.log) if args.log else (out_dir / "cohort_image_fetch.log")

    if not plan_path.is_file():
        _log(f"Missing plan TSV: {plan_path}")
        return 1

    reject = _load_sha256_blocklist(block_path)
    header = "ts_utc\tstatus\tdataset\tsample_id\timage_ref\tlocal_path\tdetail\n"
    _ensure_log_header(log_path, header)

    logged_sids: set[str] = _load_logged_sample_ids(log_path)
    if logged_sids:
        _log(f"Resume: {len(logged_sids)} sample_id(s) already in log (ok or fail; skipped)")

    baseline_ok = _count_ok_lines_in_log(log_path)
    if args.stop_after_ok is not None and baseline_ok >= args.stop_after_ok:
        _log(f"Already at {baseline_ok} ok line(s) in log (>= --stop-after-ok {args.stop_after_ok}); nothing to do.")
        return 0
    if args.stop_after_ok is not None:
        _log(f"--stop-after-ok {args.stop_after_ok}: starting from {baseline_ok} ok line(s) in log")

    processed = 0
    skipped_resume = 0
    ok_n = 0
    fail_n = 0

    with plan_path.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        for row_i, row in enumerate(reader):
            if args.limit is not None and processed >= args.limit:
                break
            url = (row.get("image_ref") or "").strip()
            sid = (row.get("sample_id") or "").strip()
            ds = (row.get("dataset") or "").strip()
            if not url or not sid:
                continue

            stem = _sanitize_sample_id(sid)
            dest = out_dir / f"{stem}{_guess_ext_from_url(url)}"

            if not args.force and sid in logged_sids:
                skipped_resume += 1
                if args.log_every and (row_i + 1) % args.log_every == 0:
                    _log(
                        f"row ~{row_i + 1}: resume_skipped={skipped_resume} "
                        f"processed={processed} ok={ok_n} fail={fail_n}"
                    )
                continue

            processed += 1
            ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            ok, detail, written = _download_one(url, dest, args.timeout, reject)
            if ok and written is not None:
                ok_n += 1
                logged_sids.add(sid)
                rel = written.relative_to(PROJECT_ROOT)
                _append_log(
                    log_path,
                    [ts, "ok", ds, sid, url, str(rel).replace("\\", "/"), ""],
                )
                if args.stop_after_ok is not None and baseline_ok + ok_n >= args.stop_after_ok:
                    _log(
                        f"Stopped: reached --stop-after-ok {args.stop_after_ok} "
                        f"(log now has {baseline_ok + ok_n} ok line(s))."
                    )
                    break
            elif not ok:
                fail_n += 1
                logged_sids.add(sid)
                _append_log(
                    log_path,
                    [ts, "fail", ds, sid, url, "", detail],
                )

            if args.log_every and processed % args.log_every == 0:
                _log(
                    f"progress: rows_seen~{row_i + 1} processed={processed} "
                    f"resume_skipped={skipped_resume} ok={ok_n} fail={fail_n}"
                )

    _log(
        f"Done. processed_attempts={processed} resume_skipped={skipped_resume} ok={ok_n} fail={fail_n}. "
        f"Images: {out_dir} Log: {log_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
