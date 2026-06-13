"""
Download Fakeddit v2.0 text/metadata TSVs from Google Drive.

Writes multimodal train/validation/test TSVs under ``<out>/v2_text_metadata/`` (labels, text fields,
``image_url``, etc.). This is the usual acquire step before ``04_consolidate_fakenews_tsv.py``.

Requires: ``pip install gdown``. Official links: ``pipeline/fakeddit/README.md``.

    python pipeline/02_acquire_fakeddit_metadata.py --out data/processed/fakeddit
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path


V2_FOLDER = "https://drive.google.com/drive/folders/1jU7qgDqU1je9Y0PMKJ_f31yXRo5uWGFm"


def main() -> int:
    """Download Fakeddit v2 text/metadata from Google Drive (gdown).

    Fetches the official v2 folder into ``<out>/v2_text_metadata/``. That output feeds the
    consolidated ``data/fakenews.tsv`` build; images are fetched later from ``image_url`` in the
    cohort pipeline, not from this step.

    Args (CLI):
        ``--out``: Download root (default ``data/processed/fakeddit``).
        ``--skip-text``: Skip the metadata folder (unusual).
        ``--remaining-ok``: Pass through to gdown when the Drive folder fetch is partial.

    Returns:
        ``0`` on success, ``1`` if gdown is not installed.
    """
    parser = argparse.ArgumentParser(
        description="Download Fakeddit v2 text/metadata TSVs from Google Drive (gdown)."
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/processed/fakeddit"),
        help="Download root; metadata TSVs go in <out>/v2_text_metadata/ (default: data/processed/fakeddit)",
    )
    parser.add_argument("--skip-text", action="store_true", help="Skip v2.0 TSV / metadata folder")
    parser.add_argument(
        "--images",
        action="store_true",
        help="Also download the bundled image archive from Drive (large)",
    )
    parser.add_argument(
        "--comments",
        action="store_true",
        help="Also download the comment-data folder from Drive",
    )
    parser.add_argument("--remaining-ok", action="store_true", help="Pass remaining_ok=True to gdown folder fetch")
    args = parser.parse_args()

    try:
        gdown = importlib.import_module("gdown")
    except ImportError:
        print("Install gdown: pip install gdown", file=sys.stderr)
        return 1

    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)

    if not args.skip_text:
        text_dir = out / "v2_text_metadata"
        text_dir.mkdir(parents=True, exist_ok=True)
        print("Downloading v2.0 text and metadata folder (this may take a while)…")
        gdown.download_folder(
            V2_FOLDER,
            output=str(text_dir),
            quiet=False,
            remaining_ok=args.remaining_ok,
        )

    print(f"Finished. Files under {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
