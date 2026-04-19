"""
Download Fakeddit v2.0 text/metadata from Google Drive (default).

Image archive and comment folders are **opt-in** (`--images`, `--comments`) — large and not required
for TSV-based multimodal work if you use `image_url` or skip images entirely.

Official links: data/fakeddit/README.md. Requires: pip install gdown

    python scripts/02_acquire_fakeddit_metadata.py --out data/processed/fakeddit
    python scripts/02_acquire_fakeddit_metadata.py --images --comments   # full Drive mirror
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path


V2_FOLDER = "https://drive.google.com/drive/folders/1jU7qgDqU1je9Y0PMKJ_f31yXRo5uWGFm"
IMAGE_ARCHIVE = "https://drive.google.com/uc?id=1cjY6HsHaSZuLVHywIxD5xQqng33J5S2b"
COMMENTS_FOLDER = "https://drive.google.com/drive/folders/150sL4SNi5zFK8nmllv5prWbn0LyvLzvo"


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Fakeddit assets from Google Drive (gdown).")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/processed/fakeddit"),
        help="Root for Fakeddit downloads (default: data/processed/fakeddit → v2_text_metadata/, etc.)",
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

    if args.images:
        img_path = out / "fakeddit_images_archive"
        print("Downloading image archive (large)…")
        gdown.download(IMAGE_ARCHIVE, str(img_path), quiet=False, fuzzy=True)

    if args.comments:
        cdir = out / "comments"
        cdir.mkdir(parents=True, exist_ok=True)
        print("Downloading comment data folder…")
        gdown.download_folder(
            COMMENTS_FOLDER,
            output=str(cdir),
            quiet=False,
            remaining_ok=args.remaining_ok,
        )

    print(f"Finished. Files under {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
