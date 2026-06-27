"""Load multimodal cohort: text + local image path, same splits as unimodal baselines."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from cohort_image import IMAGE_TSV_NAME, verify_image_files
from cohort_text import RANDOM_SEED, EXPECTED_ROWS, load_text_cohort, resolve_project_root

MULTIMODAL_COLUMNS = [
    "sample_id",
    "dataset",
    "split_study",
    "label_binary",
    "text",
    "cohort_image_local_path",
]


def load_multimodal_cohort(
    project_root: Path | None = None,
    *,
    seed: int = RANDOM_SEED,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Build the multimodal cohort by joining the text and image exports.

    Loads the text cohort, joins the local image path from the image export on
    ``sample_id``, then drops rows whose image file is missing on disk.

    Args:
        project_root: Project root; resolved automatically if ``None``.
        seed: Random seed passed through to the text cohort split.

    Returns:
        A ``(merged_df, stats)`` pair. ``stats`` reports row counts at each
        stage (text cohort, after join, missing images, kept, train, validation).

    Raises:
        FileNotFoundError: If the image TSV is missing.
        ValueError: If the image TSV row count is unexpected.
    """
    root = resolve_project_root(project_root)
    image_tsv = root / "data" / IMAGE_TSV_NAME
    if not image_tsv.is_file():
        raise FileNotFoundError(f"Could not find data/{IMAGE_TSV_NAME}")

    text_df = load_text_cohort(root, seed=seed)
    img_df = pd.read_csv(image_tsv, sep="\t", dtype=str, keep_default_na=False)
    if len(img_df) != EXPECTED_ROWS:
        raise ValueError(f"Expected {EXPECTED_ROWS} image rows, got {len(img_df)}")

    merged = text_df.merge(
        img_df[["sample_id", "cohort_image_local_path"]],
        on="sample_id",
        how="inner",
        validate="one_to_one",
    )
    rows_after_join = len(merged)
    merged, file_stats = verify_image_files(merged, root)

    stats = {
        "rows_text_cohort": int(len(text_df)),
        "rows_after_join": int(rows_after_join),
        "rows_missing_image": int(file_stats["rows_missing_image"]),
        "rows_kept": int(file_stats["rows_kept"]),
        "rows_train": int((merged["split_study"] == "train").sum()),
        "rows_validation": int((merged["split_study"] == "validation").sum()),
    }
    return merged, stats


def train_val_frames(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a cohort frame into its train and validation subsets.

    Args:
        df: A cohort frame with a ``split_study`` column.

    Returns:
        A ``(train_df, val_df)`` pair of copies.
    """
    train: pd.DataFrame = df.loc[df["split_study"] == "train"].copy()
    val: pd.DataFrame = df.loc[df["split_study"] == "validation"].copy()
    return train, val


def cohort_stats(df: pd.DataFrame) -> dict[str, object]:
    """Summarise the cohort for notebook logging and thesis footnotes.

    Args:
        df: The multimodal cohort frame.

    Returns:
        Row totals and per-split label/dataset breakdowns.
    """
    train, val = train_val_frames(df)
    return {
        "rows_total": int(len(df)),
        "rows_train": int(len(train)),
        "rows_validation": int(len(val)),
        "label_train": train["label_binary"].value_counts().sort_index().to_dict(),
        "label_val": val["label_binary"].value_counts().sort_index().to_dict(),
        "dataset_train": train.groupby("dataset").size().to_dict(),
        "dataset_val": val.groupby("dataset").size().to_dict(),
    }
