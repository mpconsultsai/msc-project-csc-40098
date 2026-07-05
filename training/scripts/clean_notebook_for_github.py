#!/usr/bin/env python3
"""Remove broken ipywidgets metadata so GitHub can render notebooks.

Colab / Hugging Face Trainer progress bars sometimes leave
``metadata.widgets`` without a ``state`` key. GitHub then fails with:
"the 'state' key is missing from 'metadata.widgets'".

Usage:
    python training/scripts/clean_notebook_for_github.py training/notebooks/*.ipynb
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

WIDGET_MIME = "application/vnd.jupyter.widget-view+json"
WIDGET_STATE_MIME = "application/vnd.jupyter.widget-state+json"


def clean_notebook(path: Path) -> bool:
    """Strip widget metadata and widget outputs. Returns True if anything changed."""
    nb = json.loads(path.read_text(encoding="utf-8"))
    changed = False

    meta = nb.setdefault("metadata", {})
    for key in ("widgets", WIDGET_STATE_MIME):
        if key in meta:
            del meta[key]
            changed = True

    for cell in nb.get("cells", []):
        cell_meta = cell.get("metadata", {})
        if "widgets" in cell_meta:
            del cell_meta["widgets"]
            changed = True

        if cell.get("cell_type") != "code":
            continue

        outputs = cell.get("outputs", [])
        if not outputs:
            continue

        kept = []
        for out in outputs:
            data = out.get("data", {})
            if WIDGET_MIME in data or WIDGET_STATE_MIME in data:
                changed = True
                continue
            kept.append(out)
        cell["outputs"] = kept

    if changed:
        path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return changed


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 1

    any_changed = False
    for arg in argv[1:]:
        path = Path(arg)
        if not path.is_file():
            print(f"skip (not found): {path}", file=sys.stderr)
            continue
        if clean_notebook(path):
            print(f"cleaned: {path}")
            any_changed = True
        else:
            print(f"ok (no widgets): {path}")

    return 0 if any_changed or len(argv) > 1 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
