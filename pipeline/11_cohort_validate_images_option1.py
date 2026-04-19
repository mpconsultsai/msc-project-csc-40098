"""
Option-1 validation sweep: cheap file/pixel heuristics + a 1–100 **validity score** (experimental).

Reads ``ok`` rows from ``cohort_image_fetch.log``, opens each ``local_path``, and writes:

- ``outputs/cohort_image_validation/option1_validation.tsv`` — one row per image; after each run the file is
  **rewritten sorted** by ``validity_score`` **ascending** (lowest first), then ``sample_id``.
- ``outputs/cohort_image_validation/option1_validation_summary.log`` — run header + score quantiles + flag counts.

The score is a **heuristic** (“how photo-like / training-friendly under these rules”), not ground truth.
Tune buckets in ``_score_from_metrics`` after you compare samples.

    python pipeline/11_cohort_validate_images_option1.py
    python pipeline/11_cohort_validate_images_option1.py --limit 500
    python pipeline/11_cohort_validate_images_option1.py --resume
    python pipeline/11_cohort_validate_images_option1.py --sort-only

Optional: ``pip install imagehash`` for a perceptual hash column (near-duplicate work later).
"""

from __future__ import annotations

import argparse
import csv
import importlib
import math
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_FETCH_LOG = Path("data/processed/images/cohort_image_fetch.log")
DEFAULT_OUT_DIR = Path("outputs/cohort_image_validation")

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore

try:
    imagehash = importlib.import_module("imagehash")
except ImportError:
    imagehash = None


def _resolve(root: Path, p: Path) -> Path:
    return p.resolve() if p.is_absolute() else (root / p).resolve()


@dataclass
class Metrics:
    ok: bool = False
    error: str = ""
    width: int = 0
    height: int = 0
    format: str = ""
    animated: bool = False
    file_bytes: int = 0
    aspect_ratio: float = 0.0
    gray_entropy: float = 0.0
    gray_variance: float = 0.0
    flags: list[str] = field(default_factory=list)


def _analyze_image(path: Path, max_side_stats: int) -> Metrics:
    m = Metrics()
    try:
        m.file_bytes = path.stat().st_size
    except OSError as e:
        m.error = f"stat:{e}"
        m.flags.append("missing_or_unreadable")
        return m

    try:
        from PIL import Image, ImageOps
    except ImportError:
        m.error = "PIL_missing"
        m.flags.append("pil_missing")
        return m

    try:
        with Image.open(path) as im:
            m.format = (im.format or "").upper()
            m.width, m.height = im.size
            n_frames = getattr(im, "n_frames", 1)
            m.animated = n_frames > 1
            if m.animated:
                m.flags.append("animated")

            ar = max(m.width, m.height) / max(min(m.width, m.height), 1)
            m.aspect_ratio = round(ar, 4)
            if ar >= 6.0:
                m.flags.append("extreme_aspect")
            if min(m.width, m.height) < 48:
                m.flags.append("tiny_image")

            gray = ImageOps.grayscale(im)
            if max(gray.size) > max_side_stats:
                gray = gray.copy()
                gray.thumbnail((max_side_stats, max_side_stats), Image.Resampling.LANCZOS)

            if np is None:
                m.gray_entropy = 0.0
                m.gray_variance = 0.0
                m.flags.append("numpy_missing_stats")
            else:
                arr = np.asarray(gray, dtype=np.uint8)
                m.gray_variance = float(np.var(arr))
                hist, _ = np.histogram(arr.flatten(), bins=256, range=(0, 256))
                total = float(hist.sum())
                if total <= 0:
                    m.gray_entropy = 0.0
                else:
                    p = hist.astype(np.float64) / total
                    p = p[p > 0]
                    m.gray_entropy = float(-(p * np.log2(p)).sum())

                if m.gray_entropy < 2.0:
                    m.flags.append("very_low_entropy")
                if m.gray_variance < 80.0:
                    m.flags.append("low_variance")

            m.ok = True
    except Exception as e:  # noqa: BLE001
        m.error = type(e).__name__
        m.flags.append("decode_error")

    return m


def _format_points(fmt: str) -> int:
    f = (fmt or "").upper()
    if f in ("JPEG", "JPG", "WEBP"):
        return 12
    if f == "PNG":
        return 10
    if f == "GIF":
        return 7
    if f in ("BMP", "TIFF", "TIF"):
        return 9
    if f:
        return 6
    return 4


def _resolution_points(w: int, h: int) -> int:
    s = min(w, h)
    if s >= 256:
        return 24
    if s >= 224:
        return 22
    if s >= 160:
        return 18
    if s >= 128:
        return 15
    if s >= 96:
        return 11
    if s >= 64:
        return 7
    if s >= 48:
        return 4
    return 0


def _aspect_points(ar: float) -> int:
    if ar <= 1.5:
        return 18
    if ar <= 2.0:
        return 16
    if ar <= 2.5:
        return 14
    if ar <= 3.5:
        return 11
    if ar <= 5.0:
        return 7
    if ar <= 8.0:
        return 4
    return 0


def _texture_points(entropy: float, variance: float) -> int:
    """Grayscale entropy (0..~8); variance of uint8 pixels."""
    if entropy < 1.5:
        base = 4
    elif entropy < 2.5:
        base = 9
    elif entropy < 4.0:
        base = 14
    elif entropy < 5.5:
        base = 19
    elif entropy < 7.0:
        base = 24
    else:
        base = 28

    if variance < 40:
        base = max(0, base - 10)
    elif variance < 120:
        base = max(0, base - 4)
    return min(30, base)


def _score_from_metrics(m: Metrics) -> int:
    """Return integer 1..100 (heuristic training-facing validity)."""
    if not m.ok:
        return 1

    s = 0
    s += _resolution_points(m.width, m.height)
    s += _aspect_points(m.aspect_ratio)
    s += 0 if m.animated else 18
    s += _format_points(m.format)
    s += _texture_points(m.gray_entropy, m.gray_variance)

    if "extreme_aspect" in m.flags:
        s = min(s, 55)
    if "tiny_image" in m.flags:
        s = min(s, 35)
    if m.animated:
        s = min(s, 45)

    return max(1, min(100, int(round(s))))


def _phash(path: Path) -> str:
    if imagehash is None:
        return ""
    try:
        from PIL import Image

        with Image.open(path) as im:
            h = imagehash.phash(im)
        return str(h)
    except Exception:
        return ""


VALIDATION_FIELDNAMES = [
    "ts_utc",
    "sample_id",
    "dataset",
    "local_path",
    "validity_score",
    "flags",
    "error",
    "width",
    "height",
    "format",
    "animated",
    "aspect_ratio",
    "gray_entropy",
    "gray_variance",
    "file_bytes",
    "phash",
]


def _rewrite_tsv_sorted_by_score(tsv_path: Path) -> int:
    """Sort all data rows by validity_score ascending, then sample_id. Returns row count or 0 if skipped."""
    if not tsv_path.is_file() or tsv_path.stat().st_size == 0:
        return 0
    with tsv_path.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        rows = list(reader)
    if not rows:
        return 0

    def _sort_key(row: dict[str, str]) -> tuple[int, str]:
        raw = (row.get("validity_score") or "").strip()
        try:
            sc = int(raw)
        except ValueError:
            sc = 0
        return (sc, (row.get("sample_id") or "").strip())

    rows.sort(key=_sort_key)
    tmp = tsv_path.with_suffix(".tsv.sort.tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="") as fp:
            w = csv.DictWriter(
                fp,
                fieldnames=VALIDATION_FIELDNAMES,
                delimiter="\t",
                extrasaction="ignore",
            )
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in VALIDATION_FIELDNAMES})
        tmp.replace(tsv_path)
    except OSError:
        if tmp.is_file():
            tmp.unlink(missing_ok=True)
        raise
    return len(rows)


def _load_done_sample_ids(tsv_path: Path) -> set[str]:
    if not tsv_path.is_file():
        return set()
    out: set[str] = set()
    with tsv_path.open(encoding="utf-8", newline="") as fp:
        r = csv.DictReader(fp, delimiter="\t")
        for row in r:
            sid = (row.get("sample_id") or "").strip()
            if sid:
                out.add(sid)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Option-1 cohort image validation + 1–100 validity score")
    ap.add_argument("--fetch-log", type=Path, default=DEFAULT_FETCH_LOG)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--limit", type=int, default=None, help="Process at most N new ok rows (after resume)")
    ap.add_argument("--resume", action="store_true", help="Skip sample_id already present in the TSV")
    ap.add_argument("--max-side-stats", type=int, default=512, help="Max side for entropy/variance (speed)")
    ap.add_argument("--no-phash", action="store_true", help="Skip perceptual hash even if imagehash is installed")
    ap.add_argument(
        "--sort-only",
        action="store_true",
        help="Only rewrite option1_validation.tsv sorted by validity_score (lowest first); no image IO",
    )
    args = ap.parse_args()

    log_path = _resolve(PROJECT_ROOT, args.fetch_log)
    out_dir = _resolve(PROJECT_ROOT, args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tsv_path = out_dir / "option1_validation.tsv"
    summ_path = out_dir / "option1_validation_summary.log"

    if args.sort_only:
        n = _rewrite_tsv_sorted_by_score(tsv_path)
        print(f"Sorted {n} data rows in {tsv_path} by validity_score (ascending).", file=sys.stderr)
        return 0 if n or tsv_path.is_file() else 1

    if not log_path.is_file():
        print(f"Missing fetch log: {log_path}", file=sys.stderr)
        return 1

    if np is None:
        print("Warning: numpy not installed; entropy/variance will be 0. Install numpy for metrics.", file=sys.stderr)

    tsv_exists = tsv_path.is_file()
    done = _load_done_sample_ids(tsv_path) if (args.resume and tsv_exists) else set()
    if args.resume and tsv_exists:
        out_mode = "a"
        write_header = False
    else:
        out_mode = "w"
        write_header = True

    fieldnames = VALIDATION_FIELDNAMES

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    scores: list[int] = []
    flag_counter: Counter = Counter()
    processed_new = 0
    skipped_resume = 0
    skipped_missing = 0

    with tsv_path.open(out_mode, encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        if write_header:
            w.writeheader()

        with log_path.open(encoding="utf-8", newline="") as log_fp:
            reader = csv.DictReader(log_fp, delimiter="\t")
            for row in reader:
                if (row.get("status") or "").strip().lower() != "ok":
                    continue
                sid = (row.get("sample_id") or "").strip()
                if not sid:
                    continue
                if args.resume and sid in done:
                    skipped_resume += 1
                    continue
                if args.limit is not None and processed_new >= args.limit:
                    break

                rel = (row.get("local_path") or "").strip().replace("/", "\\")
                if not rel:
                    skipped_missing += 1
                    continue
                img_path = _resolve(PROJECT_ROOT, Path(rel))
                if not img_path.is_file():
                    skipped_missing += 1
                    m = Metrics(ok=False, error="file_missing", flags=["file_missing"])
                    score = 1
                    rec = {
                        "ts_utc": run_ts,
                        "sample_id": sid,
                        "dataset": (row.get("dataset") or "").strip(),
                        "local_path": rel.replace("\\", "/"),
                        "validity_score": score,
                        "flags": ",".join(m.flags),
                        "error": m.error,
                        "width": 0,
                        "height": 0,
                        "format": "",
                        "animated": "false",
                        "aspect_ratio": "",
                        "gray_entropy": "",
                        "gray_variance": "",
                        "file_bytes": 0,
                        "phash": "",
                    }
                    w.writerow(rec)
                    scores.append(score)
                    for f in m.flags:
                        flag_counter[f] += 1
                    processed_new += 1
                    done.add(sid)
                    continue

                m = _analyze_image(img_path, args.max_side_stats)
                score = _score_from_metrics(m)
                ph = "" if args.no_phash else _phash(img_path)

                rec = {
                    "ts_utc": run_ts,
                    "sample_id": sid,
                    "dataset": (row.get("dataset") or "").strip(),
                    "local_path": rel.replace("\\", "/"),
                    "validity_score": score,
                    "flags": ",".join(sorted(set(m.flags))),
                    "error": m.error,
                    "width": m.width,
                    "height": m.height,
                    "format": m.format,
                    "animated": "true" if m.animated else "false",
                    "aspect_ratio": f"{m.aspect_ratio:.4f}" if m.aspect_ratio else "",
                    "gray_entropy": f"{m.gray_entropy:.4f}" if m.ok else "",
                    "gray_variance": f"{m.gray_variance:.2f}" if m.ok else "",
                    "file_bytes": m.file_bytes,
                    "phash": ph,
                }
                w.writerow(rec)
                scores.append(score)
                for f in m.flags:
                    flag_counter[f] += 1
                processed_new += 1
                done.add(sid)

    n_sorted = _rewrite_tsv_sorted_by_score(tsv_path)
    if n_sorted:
        print(
            f"Rewrote {tsv_path} sorted by validity_score ascending ({n_sorted} rows).",
            file=sys.stderr,
        )

    # Summary log (append for this run)
    lines = [
        f"run_ts_utc={run_ts}",
        f"fetch_log={log_path}",
        f"tsv_out={tsv_path}",
        f"processed_new={processed_new}",
        f"skipped_already_in_tsv={skipped_resume}",
        f"skipped_missing_path={skipped_missing}",
        f"numpy_available={bool(np)}",
        f"imagehash_available={bool(imagehash and not args.no_phash)}",
        "",
        "validity_score is a heuristic 1-100 (higher ~ more photo-like under option-1 rules).",
        "",
    ]
    if scores:
        arr = sorted(scores)
        n = len(arr)

        def pct(p: float) -> float:
            if not arr:
                return float("nan")
            k = max(0, min(n - 1, int(math.ceil(p * n) - 1)))
            return float(arr[k])

        lines.append(f"score_min={arr[0]}")
        lines.append(f"score_max={arr[-1]}")
        lines.append(f"score_mean={sum(arr) / n:.2f}")
        lines.append(f"score_median={arr[n // 2]}")
        lines.append(f"score_p10={pct(0.10):.1f}")
        lines.append(f"score_p90={pct(0.90):.1f}")
        lines.append("")
        lines.append("flag_counts (this run rows only):")
        for k, v in flag_counter.most_common():
            lines.append(f"  {k}={v}")
    else:
        lines.append("no new rows processed")

    summ_path.parent.mkdir(parents=True, exist_ok=True)
    with summ_path.open("a", encoding="utf-8") as sf:
        sf.write("\n---\n")
        sf.write("\n".join(lines) + "\n")

    print(f"Wrote {tsv_path}", file=sys.stderr)
    print(f"Appended summary {summ_path}", file=sys.stderr)
    print("\n".join(lines), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
