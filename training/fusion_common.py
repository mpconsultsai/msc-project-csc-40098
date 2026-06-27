"""Shared fusion utilities: artifacts, frozen encoders, embeddings, metrics."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from PIL import Image
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    precision_recall_curve,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from torch.utils.data import DataLoader, Dataset
from torchvision import models, transforms
from torchvision.models import ResNet18_Weights
from transformers import AutoModelForSequenceClassification, AutoTokenizer

TEXT_RUN_ID = "text_distilbert_baseline"
IMAGE_RUN_ID = "image_resnet18_baseline"
FUSION_LATE_RUN_ID = "fusion_late_logistic"
FUSION_EARLY_RUN_ID = "fusion_early_concat"
FUSION_ATTENTION_RUN_ID = "fusion_attention"
MAX_LENGTH = 128
RANDOM_SEED = 42
TEXT_EMB_DIM = 768
IMAGE_EMB_DIM = 512

IMAGENET_MEAN = (0.485, 0.456, 0.406)
IMAGENET_STD = (0.229, 0.224, 0.225)


def _resolve_image_weights(project_root: Path) -> Path | None:
    run_dir = project_root / "runs" / IMAGE_RUN_ID
    primary = run_dir / "resnet18_state.pt"
    if primary.is_file():
        return primary
    epochs = sorted(run_dir.glob("resnet18_epoch*.pt"))
    if epochs:
        return epochs[-1]
    return None


def resolve_image_weights_path(project_root: Path) -> Path:
    """Return ResNet checkpoint path under ``runs/`` or raise ``FileNotFoundError``."""
    weights = _resolve_image_weights(project_root)
    if weights is None:
        raise FileNotFoundError(
            f"No ResNet weights in {project_root / 'runs' / IMAGE_RUN_ID}"
        )
    return weights


def require_unimodal_artifacts(project_root: Path) -> tuple[Path, Path]:
    """Return (distilbert_model_dir, resnet_weights_path) or raise FileNotFoundError."""
    text_model = project_root / "runs" / TEXT_RUN_ID / "model"
    image_weights = _resolve_image_weights(project_root)
    missing: list[str] = []
    if not (text_model / "config.json").is_file():
        missing.append(
            f"{text_model}/ — run DistilBERT save cell "
            f"(auto-persists to My Drive/runs/{TEXT_RUN_ID}/)"
        )
    if image_weights is None:
        missing.append(
            f"{project_root / 'runs' / IMAGE_RUN_ID}/ — run ResNet eval cell "
            f"(auto-persists to My Drive/runs/{IMAGE_RUN_ID}/)"
        )
    if missing:
        raise FileNotFoundError(
            "Missing unimodal artifacts:\n  " + "\n  ".join(missing)
        )
    assert image_weights is not None
    return text_model, image_weights


class MultimodalEmbeddingDataset(Dataset):
    """Batch text tokens + image tensor for frozen encoder forward passes."""

    def __init__(
        self,
        frame: pd.DataFrame,
        root: Path,
        tokenizer,
        max_length: int,
        image_transform,
    ):
        self.frame = frame.reset_index(drop=True)
        self.root = root
        self.tokenizer = tokenizer
        self.max_length = max_length
        self.image_transform = image_transform

    def __len__(self) -> int:
        return len(self.frame)

    def __getitem__(self, idx: int):
        row = self.frame.iloc[idx]
        enc = self.tokenizer(
            row["text"],
            truncation=True,
            max_length=self.max_length,
            padding="max_length",
            return_tensors="pt",
        )
        tokens = {k: v.squeeze(0) for k, v in enc.items()}
        path = self.root / row["cohort_image_local_path"]
        img = Image.open(path).convert("RGB")
        image = self.image_transform(img)
        label = int(row["label_binary"])
        return tokens, image, label


def load_frozen_encoders(
    text_model_dir: Path,
    image_weights_path: Path,
    device: torch.device,
) -> tuple[AutoModelForSequenceClassification, object, nn.Module]:
    """Load fine-tuned DistilBERT and ResNet-18 backbone (no classifier head)."""
    tokenizer = AutoTokenizer.from_pretrained(text_model_dir)
    text_model = AutoModelForSequenceClassification.from_pretrained(text_model_dir)
    text_model.to(device)
    text_model.eval()
    for p in text_model.parameters():
        p.requires_grad = False

    resnet = models.resnet18(weights=ResNet18_Weights.IMAGENET1K_V1)
    resnet.fc = nn.Linear(resnet.fc.in_features, 2)
    state = torch.load(image_weights_path, map_location="cpu", weights_only=True)
    if isinstance(state, dict) and "model" in state:
        state = state["model"]
    resnet.load_state_dict(state)
    image_backbone = nn.Sequential(*list(resnet.children())[:-1])
    image_backbone.to(device)
    image_backbone.eval()
    for p in image_backbone.parameters():
        p.requires_grad = False

    return text_model, tokenizer, image_backbone


def image_transform_eval():
    """ImageNet-normalised 224×224 transform for validation and PoC inference."""
    return transforms.Compose(
        [
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(IMAGENET_MEAN, IMAGENET_STD),
        ]
    )


def load_distilbert_classifier(
    model_dir: Path, device: torch.device
) -> tuple[AutoModelForSequenceClassification, object]:
    """Load fine-tuned DistilBERT tokenizer + 2-class head for inference."""
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModelForSequenceClassification.from_pretrained(model_dir)
    model.to(device)
    model.eval()
    return model, tokenizer


def load_resnet_classifier(weights_path: Path, device: torch.device) -> nn.Module:
    """Load fine-tuned ResNet-18 2-class classifier."""
    model = models.resnet18(weights=ResNet18_Weights.IMAGENET1K_V1)
    model.fc = nn.Linear(model.fc.in_features, 2)
    state = torch.load(weights_path, map_location="cpu", weights_only=True)
    if isinstance(state, dict) and "model" in state:
        state = state["model"]
    model.load_state_dict(state)
    model.to(device)
    model.eval()
    return model


@torch.inference_mode()
def score_text_sample(
    text: str,
    model: nn.Module,
    tokenizer,
    device: torch.device,
    *,
    max_length: int = MAX_LENGTH,
) -> float:
    """Return P(fake) for a single text string."""
    enc = tokenizer(
        text,
        truncation=True,
        max_length=max_length,
        padding="max_length",
        return_tensors="pt",
    )
    enc = {k: v.to(device) for k, v in enc.items()}
    logits = model(**enc).logits
    return float(torch.softmax(logits, dim=-1)[0, 1].cpu())


@torch.inference_mode()
def score_image_sample(
    image: Image.Image,
    model: nn.Module,
    device: torch.device,
) -> float:
    """Return P(fake) for a single PIL image."""
    tensor = image_transform_eval()(image.convert("RGB")).unsqueeze(0).to(device)
    logits = model(tensor)
    return float(torch.softmax(logits, dim=-1)[0, 1].cpu())


@torch.inference_mode()
def extract_sample_embeddings(
    text: str,
    image: Image.Image,
    text_model: nn.Module,
    tokenizer,
    image_backbone: nn.Module,
    device: torch.device,
    *,
    max_length: int = MAX_LENGTH,
) -> tuple[np.ndarray, np.ndarray]:
    """Return DistilBERT [CLS] (768-d) and ResNet pool (512-d) for one sample."""
    enc = tokenizer(
        text,
        truncation=True,
        max_length=max_length,
        padding="max_length",
        return_tensors="pt",
    )
    token_batch = {k: v.to(device) for k, v in enc.items()}
    text_vec = text_model.distilbert(**token_batch).last_hidden_state[:, 0].cpu().numpy()

    img_tensor = image_transform_eval()(image.convert("RGB")).unsqueeze(0).to(device)
    image_vec = image_backbone(img_tensor).flatten(1).cpu().numpy()
    return text_vec.astype(np.float32), image_vec.astype(np.float32)


@torch.inference_mode()
def extract_embeddings(
    frame: pd.DataFrame,
    text_model: nn.Module,
    tokenizer,
    image_backbone: nn.Module,
    *,
    project_root: Path,
    device: torch.device,
    batch_size: int = 32,
    max_length: int = MAX_LENGTH,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (text_emb, image_emb, labels) with shapes (N,768), (N,512), (N,)."""
    ds = MultimodalEmbeddingDataset(
        frame,
        project_root,
        tokenizer,
        max_length,
        image_transform_eval(),
    )
    loader = DataLoader(ds, batch_size=batch_size, shuffle=False, num_workers=0)

    text_out: list[np.ndarray] = []
    image_out: list[np.ndarray] = []
    labels: list[int] = []

    for tokens, images, y_batch in loader:
        token_batch = {k: v.to(device) for k, v in tokens.items()}
        hidden = text_model.distilbert(**token_batch).last_hidden_state[:, 0]
        text_out.append(hidden.cpu().numpy())

        images = images.to(device, non_blocking=True)
        feats = image_backbone(images).flatten(1)
        image_out.append(feats.cpu().numpy())
        labels.extend(int(y) for y in y_batch)

    return (
        np.vstack(text_out).astype(np.float32),
        np.vstack(image_out).astype(np.float32),
        np.asarray(labels, dtype=np.int64),
    )


def compute_class_weights(labels: np.ndarray, device: torch.device) -> torch.Tensor:
    classes, counts = np.unique(labels, return_counts=True)
    total = counts.sum()
    weights = total / (len(classes) * counts)
    ordered = np.zeros(len(classes), dtype=np.float32)
    for cls, w in zip(classes, weights):
        ordered[int(cls)] = w
    return torch.tensor(ordered, dtype=torch.float32, device=device)


def evaluate_predictions(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    score_fake: np.ndarray,
) -> dict[str, float]:
    """Threshold=0.5 metrics plus ranking scores (ROC-AUC, AP) and per-class F1/recall."""
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro")),
        "f1_real": float(f1_score(y_true, y_pred, pos_label=0)),
        "f1_fake": float(f1_score(y_true, y_pred, pos_label=1)),
        "recall_real": float(recall_score(y_true, y_pred, pos_label=0)),
        "recall_fake": float(recall_score(y_true, y_pred, pos_label=1)),
        "roc_auc": float(roc_auc_score(y_true, score_fake)),
        "average_precision": float(average_precision_score(y_true, score_fake)),
    }


def plot_validation_figures(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    score_fake: np.ndarray,
    *,
    title_prefix: str,
    roc_auc: float | None = None,
    avg_precision: float | None = None,
):
    """Return (confusion_matrix_fig, roc_pr_fig) matching unimodal notebook style."""
    import matplotlib.pyplot as plt
    from sklearn.metrics import confusion_matrix

    if roc_auc is None:
        roc_auc = float(roc_auc_score(y_true, score_fake))
    if avg_precision is None:
        avg_precision = float(average_precision_score(y_true, score_fake))

    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    fig_cm, ax_cm = plt.subplots(figsize=(4, 3.5))
    im = ax_cm.imshow(cm, cmap="Blues")
    ax_cm.set_xticks([0, 1], labels=["pred real", "pred fake"])
    ax_cm.set_yticks([0, 1], labels=["true real", "true fake"])
    for i in range(2):
        for j in range(2):
            ax_cm.text(j, i, str(int(cm[i, j])), ha="center", va="center", color="black")
    ax_cm.set_title(f"{title_prefix} — confusion matrix (validation)")
    fig_cm.colorbar(im, ax=ax_cm, fraction=0.046)

    fpr, tpr, _ = roc_curve(y_true, score_fake)
    prec, rec, _ = precision_recall_curve(y_true, score_fake)

    fig_curves, axes = plt.subplots(1, 2, figsize=(9, 3.8))
    axes[0].plot(fpr, tpr, lw=2, label=f"AUC = {roc_auc:.3f}")
    axes[0].plot([0, 1], [0, 1], "k--", lw=1, alpha=0.5)
    axes[0].set_xlabel("False positive rate")
    axes[0].set_ylabel("True positive rate")
    axes[0].set_title("ROC curve (fake = positive)")
    axes[0].legend(loc="lower right")
    axes[0].grid(alpha=0.3)

    axes[1].plot(rec, prec, lw=2, label=f"AP = {avg_precision:.3f}")
    axes[1].set_xlabel("Recall")
    axes[1].set_ylabel("Precision")
    axes[1].set_title("Precision–recall curve (fake = positive)")
    axes[1].legend(loc="upper right")
    axes[1].grid(alpha=0.3)
    fig_curves.suptitle(title_prefix, fontsize=11, y=1.02)

    fig_cm.tight_layout()
    fig_curves.tight_layout()
    return fig_cm, fig_curves


def save_validation_figures(
    run_dir: Path,
    fig_cm,
    fig_curves,
    *,
    dpi: int = 150,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    fig_cm.savefig(run_dir / "confusion_matrix.png", dpi=dpi, bbox_inches="tight")
    fig_curves.savefig(run_dir / "roc_pr_curves.png", dpi=dpi, bbox_inches="tight")


def save_run_artifacts(
    run_dir: Path,
    *,
    metrics: dict[str, object],
    val_predictions: pd.DataFrame,
    extra_paths: dict[str, Path] | None = None,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    with (run_dir / "metrics.json").open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    val_predictions.to_csv(run_dir / "predictions_val.tsv", sep="\t", index=False)
    if extra_paths:
        for name, src in extra_paths.items():
            if src.is_file():
                dst = run_dir / name
                dst.write_bytes(src.read_bytes())
