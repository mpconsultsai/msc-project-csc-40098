"""
Build text-only and image-only training views from ``fake_news_final.tsv`` (after step 11).

Restores ``text`` / ``title_raw`` / ``article_url`` from each row's ``provenance`` file, then writes
``fake_news_final_text.tsv`` (all gated rows) and ``fake_news_final_image.tsv`` (image-eligible rows
with a local path). Used for Goal 2 single-modality baselines and by ``training/``. Paths resolve
from the project root.

    python pipeline/12_cohort_export_modality_views.py

Custom input/output paths: ``--help``.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_INPUT = Path("data/fake_news_final.tsv")
DEFAULT_TEXT_OUTPUT = Path("data/fake_news_final_text.tsv")
DEFAULT_IMAGE_OUTPUT = Path("data/fake_news_final_image.tsv")

TEXT_FIELDS = [
    "sample_id",
    "dataset",
    "split_official",
    "domain",
    "label_binary",
    "label_fine",
    "text",
    "title_raw",
    "article_url",
    "provenance",
]

IMAGE_FIELDS = [
    "sample_id",
    "dataset",
    "split_official",
    "domain",
    "label_binary",
    "label_fine",
    "cohort_image_local_path",
    "image_option1_validity_score",
    "image_option1_training_eligible",
    "cohort_multimodal_image_ok",
    "provenance",
]


def _resolve(root: Path, p: Path) -> Path:
    """Resolve a CLI path relative to the project root when not absolute."""
    p = p.expanduser()
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def _read_final_rows(inp: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Load all rows from the gated final cohort TSV.

    Args:
        inp: ``fake_news_final.tsv`` or equivalent.

    Returns:
        ``(fieldnames, rows)`` from the file header and body.

    Raises:
        ValueError: If the file has no header row.
    """
    with inp.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        if not reader.fieldnames:
            raise ValueError("Input TSV has no header.")
        rows = list(reader)
        return list(reader.fieldnames), rows


def _extract_fakeddit_text(tsv_path: Path, wanted_ids: set[str]) -> Dict[str, tuple[str, str, str]]:
    """Read title/text for Fakeddit rows referenced by ``sample_id``.

    Args:
        tsv_path: Fakeddit multimodal TSV from ``provenance``.
        wanted_ids: ``fd:…`` sample IDs to load from this file.

    Returns:
        Map ``sample_id -> (text, title_raw, article_url)``; URL is always empty for Fakeddit.
    """
    out: Dict[str, tuple[str, str, str]] = {}
    if not tsv_path.is_file():
        return out

    with tsv_path.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        for row in reader:
            rid = (row.get("id") or "").strip()
            if not rid:
                continue
            sid = f"fd:{rid}"
            if sid not in wanted_ids:
                continue
            text = (row.get("clean_title") or row.get("title") or "").strip()
            title_raw = (row.get("title") or "").strip()
            out[sid] = (text, title_raw, "")
    return out


def _extract_fnn_text(json_path: Path) -> tuple[str, str, str]:
    """Read text fields from one FakeNewsNet ``news content.json`` file.

    Args:
        json_path: Crawled article JSON from ``provenance``.

    Returns:
        ``(text, title_raw, article_url)``; empty strings if missing or unreadable.
    """
    if not json_path.is_file():
        return ("", "", "")
    try:
        raw = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ("", "", "")
    if not isinstance(raw, dict):
        return ("", "", "")
    text = (raw.get("text") or "").strip()
    title = (raw.get("title") or "").strip()
    url = (raw.get("url") or "").strip()
    return (text, title, url)


def _truthy(v: str) -> bool:
    """Return True for common truthy string values (``true``, ``1``, ``yes``)."""
    return str(v).strip().lower() in {"true", "1", "yes"}


def _write_tsv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    """Write one tab-separated file with header and rows.

    Args:
        path: Output TSV path (parent dirs created if needed).
        fieldnames: Column order for the header and each row.
        rows: Row dicts; missing keys become empty strings.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def main() -> int:
    """Export text-only and image-only training TSVs from the gated final cohort.

    Reads ``--input``, restores text from ``provenance`` (Fakeddit TSV or FNN JSON), writes
    ``--text-out`` with one row per input row and ``--image-out`` with rows that pass image
    fetch + training-eligibility gates and have a ``cohort_image_local_path``.

    Args (CLI):
        ``--input``: Gated cohort from step 11 (default ``data/fake_news_final.tsv``).
        ``--text-out``: Text baseline export (default ``data/fake_news_final_text.tsv``).
        ``--image-out``: Image baseline export (default ``data/fake_news_final_image.tsv``).

    Returns:
        ``0`` on success, ``1`` if input is missing, invalid, or lacks required columns.
    """
    ap = argparse.ArgumentParser(description="Build text-only and image-only final cohort views")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--text-out", type=Path, default=DEFAULT_TEXT_OUTPUT)
    ap.add_argument("--image-out", type=Path, default=DEFAULT_IMAGE_OUTPUT)
    args = ap.parse_args()

    inp = _resolve(PROJECT_ROOT, args.input)
    text_out = _resolve(PROJECT_ROOT, args.text_out)
    image_out = _resolve(PROJECT_ROOT, args.image_out)

    if not inp.is_file():
        print(f"Missing input: {inp}", file=sys.stderr)
        return 1

    try:
        fields, rows = _read_final_rows(inp)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    required = {"sample_id", "dataset", "provenance"}
    missing = sorted(required - set(fields))
    if missing:
        print(f"Input is missing required column(s): {', '.join(missing)}", file=sys.stderr)
        return 1

    # Group wanted IDs by provenance for efficient file scans.
    wanted_by_prov: dict[tuple[str, str], set[str]] = {}
    for r in rows:
        ds = (r.get("dataset") or "").strip().lower()
        sid = (r.get("sample_id") or "").strip()
        prov = (r.get("provenance") or "").strip()
        if not ds or not sid or not prov:
            continue
        key = (ds, prov)
        wanted_by_prov.setdefault(key, set()).add(sid)

    text_map: Dict[str, tuple[str, str, str]] = {}
    missing_prov_files = 0

    for (dataset, prov), wanted_ids in wanted_by_prov.items():
        src = _resolve(PROJECT_ROOT, Path(prov))
        if not src.is_file():
            missing_prov_files += 1
            continue
        if dataset == "fakeddit":
            text_map.update(_extract_fakeddit_text(src, wanted_ids))
        elif dataset == "fakenewsnet":
            # One JSON per row provenance; each file corresponds to one sample.
            # Keep robust by reading once and assigning all matching IDs.
            t, tr, u = _extract_fnn_text(src)
            for sid in wanted_ids:
                text_map[sid] = (t, tr, u)

    text_rows: list[dict[str, str]] = []
    image_rows: list[dict[str, str]] = []
    text_non_empty = 0
    title_non_empty = 0
    url_non_empty = 0

    for r in rows:
        sid = (r.get("sample_id") or "").strip()
        text, title_raw, article_url = text_map.get(sid, ("", "", ""))

        base = dict(r)
        base["text"] = text
        base["title_raw"] = title_raw
        base["article_url"] = article_url

        if text:
            text_non_empty += 1
        if title_raw:
            title_non_empty += 1
        if article_url:
            url_non_empty += 1

        text_rows.append({k: base.get(k, "") for k in TEXT_FIELDS})

        # Image-only view: keep only rows that are training-eligible and fetched/ok.
        if _truthy(base.get("cohort_multimodal_image_ok", "")) and _truthy(
            base.get("image_option1_training_eligible", "")
        ):
            if (base.get("cohort_image_local_path") or "").strip():
                image_rows.append({k: base.get(k, "") for k in IMAGE_FIELDS})

    _write_tsv(text_out, TEXT_FIELDS, text_rows)
    _write_tsv(image_out, IMAGE_FIELDS, image_rows)

    print(f"Wrote {text_out} with {len(text_rows):,} row(s).", file=sys.stderr)
    print(f"Wrote {image_out} with {len(image_rows):,} row(s).", file=sys.stderr)
    print(
        "Recovered non-empty fields "
        f"(text/title_raw/article_url): {text_non_empty:,} / {title_non_empty:,} / {url_non_empty:,}",
        file=sys.stderr,
    )
    if missing_prov_files:
        print(
            f"Warning: {missing_prov_files} provenance file(s) were missing and could not be used.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
