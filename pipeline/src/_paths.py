"""Shared path resolution for the pipeline scripts.

The numbered scripts live in ``pipeline/src/`` but read inputs and write outputs
(notably under ``data/``) relative to the **repository root**. Rather than counting
parent directories (e.g. ``Path(__file__).parents[2]``), which is brittle if the
folder layout changes, this module locates the root by walking up from itself
until it finds a directory containing a repository-level marker.
"""

from __future__ import annotations

from pathlib import Path

#: Files/folders that uniquely identify the repository root in a fresh checkout.
ROOT_MARKERS: tuple[str, ...] = (".git", "pyrightconfig.json", "requirements.txt")


def find_project_root(start: Path | None = None) -> Path:
    """Locate the repository root by searching upwards for a known marker.

    Args:
        start: Directory to begin the search from; defaults to this file's
            directory (``pipeline/src``).

    Returns:
        The resolved repository-root path (the first ancestor, including
        ``start``, that contains any entry in :data:`ROOT_MARKERS`).

    Raises:
        FileNotFoundError: If no marker is found in ``start`` or any parent.
    """
    start = (start or Path(__file__).parent).resolve()
    for candidate in (start, *start.parents):
        if any((candidate / marker).exists() for marker in ROOT_MARKERS):
            return candidate
    raise FileNotFoundError(
        f"Could not locate the project root (markers={ROOT_MARKERS}) at or above {start}"
    )


#: Repository root, resolved once at import time and reused by the pipeline scripts.
PROJECT_ROOT = find_project_root()
