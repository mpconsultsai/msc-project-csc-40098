"""Late fusion: frozen unimodal scores → logistic combiner (RQ2)."""

from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from PIL import Image
from sklearn.linear_model import LogisticRegression
from torch.utils.data import DataLoader, Dataset
from torchvision import models, transforms
from torchvision.models import ResNet18_Weights
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from fusion_common import (
    FUSION_LATE_RUN_ID,
    IMAGENET_MEAN,
    IMAGENET_STD,
    MAX_LENGTH,
    RANDOM_SEED,
    evaluate_predictions,
)

FUSION_RUN_ID = FUSION_LATE_RUN_ID
evaluate_scores = evaluate_predictions


class _TextScoreDataset(Dataset):
    def __init__(self, texts: list[str], tokenizer, max_length: int):
        self.texts = texts
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self) -> int:
        return len(self.texts)

    def __getitem__(self, idx: int):
        enc = self.tokenizer(
            self.texts[idx],
            truncation=True,
            max_length=self.max_length,
            padding="max_length",
            return_tensors="pt",
        )
        return {k: v.squeeze(0) for k, v in enc.items()}


class _ImageScoreDataset(Dataset):
    def __init__(self, frame: pd.DataFrame, root: Path, transform):
        self.frame = frame.reset_index(drop=True)
        self.root = root
        self.transform = transform

    def __len__(self) -> int:
        return len(self.frame)

    def __getitem__(self, idx: int):
        row = self.frame.iloc[idx]
        path = self.root / row["cohort_image_local_path"]
        img = Image.open(path).convert("RGB")
        return self.transform(img)


@torch.inference_mode()
def score_text(
    frame: pd.DataFrame,
    model_dir: Path,
    *,
    device: torch.device,
    batch_size: int = 64,
    max_length: int = MAX_LENGTH,
) -> np.ndarray:
    """Return P(fake) per row, same order as ``frame``."""
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModelForSequenceClassification.from_pretrained(model_dir)
    model.to(device)
    model.eval()

    texts = frame["text"].tolist()
    ds = _TextScoreDataset(texts, tokenizer, max_length)
    loader = DataLoader(ds, batch_size=batch_size, shuffle=False, num_workers=0)

    scores: list[float] = []
    for batch in loader:
        batch = {k: v.to(device) for k, v in batch.items()}
        logits = model(**batch).logits
        prob_fake = torch.softmax(logits, dim=-1)[:, 1].cpu().numpy()
        scores.extend(prob_fake.tolist())
    return np.asarray(scores, dtype=np.float64)


@torch.inference_mode()
def score_image(
    frame: pd.DataFrame,
    weights_path: Path,
    *,
    project_root: Path,
    device: torch.device,
    batch_size: int = 64,
) -> np.ndarray:
    """Return P(fake) per row, same order as ``frame``."""
    val_tf = transforms.Compose(
        [
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(IMAGENET_MEAN, IMAGENET_STD),
        ]
    )
    ds = _ImageScoreDataset(frame, project_root, val_tf)
    loader = DataLoader(ds, batch_size=batch_size, shuffle=False, num_workers=0)

    model = models.resnet18(weights=ResNet18_Weights.IMAGENET1K_V1)
    model.fc = nn.Linear(model.fc.in_features, 2)
    state = torch.load(weights_path, map_location="cpu", weights_only=True)
    if isinstance(state, dict) and "model" in state:
        state = state["model"]
    model.load_state_dict(state)
    model.to(device)
    model.eval()

    scores: list[float] = []
    for batch in loader:
        batch = batch.to(device, non_blocking=True)
        logits = model(batch)
        prob_fake = torch.softmax(logits, dim=-1)[:, 1].cpu().numpy()
        scores.extend(prob_fake.tolist())
    return np.asarray(scores, dtype=np.float64)


def fit_late_fusion_combiner(
    score_text: np.ndarray,
    score_image: np.ndarray,
    y: np.ndarray,
    *,
    seed: int = RANDOM_SEED,
) -> LogisticRegression:
    """Train logistic regression on [text_score, image_score] → label."""
    x = np.column_stack([score_text, score_image])
    combiner = LogisticRegression(
        class_weight="balanced",
        random_state=seed,
        max_iter=1000,
    )
    combiner.fit(x, y)
    return combiner


def predict_late_fusion(
    combiner: LogisticRegression,
    score_text: np.ndarray,
    score_image: np.ndarray,
    *,
    threshold: float = 0.5,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (y_pred, score_fake) from the fusion combiner."""
    x = np.column_stack([score_text, score_image])
    score_fake = combiner.predict_proba(x)[:, 1]
    y_pred = (score_fake >= threshold).astype(np.int64)
    return y_pred, score_fake


def save_fusion_run(
    run_dir: Path,
    *,
    metrics: dict,
    val_predictions: pd.DataFrame,
    combiner: LogisticRegression,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    import json

    with (run_dir / "metrics.json").open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    val_predictions.to_csv(run_dir / "predictions_val.tsv", sep="\t", index=False)
    with (run_dir / "late_fusion_combiner.pkl").open("wb") as f:
        pickle.dump(combiner, f)


def load_late_combiner_checkpoint(run_dir: Path) -> LogisticRegression:
    """Load the sklearn late-fusion combiner from ``late_fusion_combiner.pkl``."""
    with (run_dir / "late_fusion_combiner.pkl").open("rb") as f:
        return pickle.load(f)
