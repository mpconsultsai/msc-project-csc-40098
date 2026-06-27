"""Load frozen image cohort, verify paths, assign study splits (same policy as text)."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pandas as pd


from cohort_text import RANDOM_SEED, assign_split_study

EXPECTED_ROWS = 48_878
IMAGE_TSV_NAME = "fake_news_final_image.tsv"


def resolve_project_root(start: Path | None = None) -> Path:
    """Locate the project root that contains ``data/<image TSV>``.

    Args:
        start: Directory to search from; defaults to the current working
            directory.

    Returns:
        The resolved project-root path.

    Raises:
        FileNotFoundError: If the image cohort TSV cannot be found.
    """
    root = start or Path.cwd()
    if (root / "data" / IMAGE_TSV_NAME).is_file():
        return root
    if root.name == "training":
        root = root.parent
    elif (root / "training").is_dir() and (root.parent / "data").is_dir():
        root = root.parent
    if not (root / "data" / IMAGE_TSV_NAME).is_file():
        raise FileNotFoundError(
            f"Could not find data/{IMAGE_TSV_NAME}. Run from repository root."
        )
    return root


def image_path_series(df: pd.DataFrame, project_root: Path) -> pd.Series:
    """Resolve the relative image paths to absolute paths under the project root.

    Args:
        df: Frame with a ``cohort_image_local_path`` column.
        project_root: Root the relative paths are joined to.

    Returns:
        A Series of absolute :class:`~pathlib.Path` objects.
    """
    col = cast(pd.Series, df["cohort_image_local_path"])
    return col.map(lambda p: project_root / str(p))


def verify_image_files(
    df: pd.DataFrame, project_root: Path
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Drop rows whose image file is missing on disk.

    Args:
        df: Frame with a ``cohort_image_local_path`` column.
        project_root: Root the relative image paths are resolved against.

    Returns:
        A ``(filtered_df, stats)`` pair, where ``stats`` reports
        ``rows_in``, ``rows_missing_image``, and ``rows_kept``.
    """
    paths = image_path_series(df, project_root)
    exists = cast(pd.Series, paths.map(lambda p: p.is_file()))
    missing_n = int((~exists).sum())
    out = cast(pd.DataFrame, df.loc[exists].copy())
    stats = {
        "rows_in": len(df),
        "rows_missing_image": missing_n,
        "rows_kept": len(out),
    }
    return out, stats


def load_image_cohort(
    project_root: Path | None = None,
    *,
    seed: int = RANDOM_SEED,
    drop_missing_images: bool = True,
) -> pd.DataFrame:
    """Load and validate the frozen image cohort, then assign study splits.

    Args:
        project_root: Project root; resolved automatically if ``None``.
        seed: Random seed passed to :func:`assign_split_study`.
        drop_missing_images: If ``True``, drop rows whose image file is missing.

    Returns:
        The cohort frame with an integer ``label_binary`` and a ``split_study``
        column.

    Raises:
        ValueError: If the row count or labels fail validation.
    """
    root = resolve_project_root(project_root)
    tsv = root / "data" / IMAGE_TSV_NAME
    df = pd.read_csv(tsv, sep="\t", dtype=str, keep_default_na=False)

    if len(df) != EXPECTED_ROWS:
        raise ValueError(f"Expected {EXPECTED_ROWS} rows, got {len(df)}")

    if not bool(df["label_binary"].isin(["0", "1"]).all()):
        raise ValueError("label_binary must be '0' or '1'")

    df["label_binary"] = df["label_binary"].astype("int64")
    df = assign_split_study(df, seed=seed)

    if drop_missing_images:
        df, _stats = verify_image_files(df, root)
        if len(df) != EXPECTED_ROWS:
            # Document if any images missing from frozen export
            pass

    return df


def train_val_frames(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a cohort frame into its train and validation subsets.

    Args:
        df: A cohort frame with a ``split_study`` column.

    Returns:
        A ``(train_df, val_df)`` pair of copies.
    """
    train = cast(pd.DataFrame, df[df["split_study"] == "train"].copy())
    val = cast(pd.DataFrame, df[df["split_study"] == "validation"].copy())
    return train, val
