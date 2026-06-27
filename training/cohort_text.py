"""Load frozen text cohort and assign study splits (shared across training notebooks)."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pandas as pd
from sklearn.model_selection import train_test_split

EXPECTED_ROWS = 48_878
FNN_TRAIN_FRAC = 0.8
RANDOM_SEED = 42
TEXT_TSV_NAME = "fake_news_final_text.tsv"


def resolve_project_root(start: Path | None = None) -> Path:
    root = start or Path.cwd()
    if (root / "data" / TEXT_TSV_NAME).is_file():
        return root
    if root.name == "training":
        root = root.parent
    elif (root / "training").is_dir() and (root.parent / "data").is_dir():
        root = root.parent
    if (root / "data" / TEXT_TSV_NAME).is_file():
        return root
    # Colab notebooks copy data to /content/msc; cwd is often /content.
    colab_root = Path("/content/msc")
    if (colab_root / "data" / TEXT_TSV_NAME).is_file():
        return colab_root
    raise FileNotFoundError(
        f"Could not find data/{TEXT_TSV_NAME}. Run from repository root "
        "or run the Colab setup cell (copies TSV from Drive to /content/msc/data/)."
    )


def assign_split_study(df: pd.DataFrame, *, seed: int = RANDOM_SEED) -> pd.DataFrame:
    """Fakeddit: copy split_official. FakeNewsNet: stratified 80/20 on label_binary."""
    out = df.copy()
    out["split_study"] = ""

    fd_mask = out["dataset"] == "fakeddit"
    out.loc[fd_mask, "split_study"] = out.loc[fd_mask, "split_official"]

    fnn_mask = out["dataset"] == "fakenewsnet"
    fnn_idx = out.index[fnn_mask]
    fnn_labels = out.loc[fnn_mask, "label_binary"]
    train_idx, val_idx = train_test_split(
        fnn_idx,
        train_size=FNN_TRAIN_FRAC,
        random_state=seed,
        stratify=fnn_labels,
    )
    out.loc[train_idx, "split_study"] = "train"
    out.loc[val_idx, "split_study"] = "validation"

    if not bool(out["split_study"].isin(["train", "validation"]).all()):
        raise ValueError("split_study must be train or validation for all rows")
    return out


def load_text_cohort(project_root: Path | None = None, *, seed: int = RANDOM_SEED) -> pd.DataFrame:
    root = resolve_project_root(project_root)
    tsv = root / "data" / TEXT_TSV_NAME
    df = pd.read_csv(tsv, sep="\t", dtype=str, keep_default_na=False)

    if len(df) != EXPECTED_ROWS:
        raise ValueError(f"Expected {EXPECTED_ROWS} rows, got {len(df)}")

    if not bool(df["label_binary"].isin(["0", "1"]).all()):
        raise ValueError("label_binary must be '0' or '1'")

    if not bool(df["text"].fillna("").str.strip().ne("").all()):
        raise ValueError("text must be non-empty")

    df["label_binary"] = df["label_binary"].astype("int64")
    return assign_split_study(df, seed=seed)


def train_val_frames(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = cast(pd.DataFrame, df[df["split_study"] == "train"].copy())
    val = cast(pd.DataFrame, df[df["split_study"] == "validation"].copy())
    return train, val
