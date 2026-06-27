"""Early fusion: concat frozen CLS + pooled CNN features → linear head."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from fusion_common import (
    FUSION_EARLY_RUN_ID,
    IMAGE_EMB_DIM,
    RANDOM_SEED,
    TEXT_EMB_DIM,
    compute_class_weights,
    evaluate_predictions,
    extract_embeddings,
    load_frozen_encoders,
    require_unimodal_artifacts,
)


class EarlyFusionHead(nn.Module):
    """Single linear layer on concatenated modality embeddings (standard early fusion)."""

    def __init__(
        self,
        text_dim: int = TEXT_EMB_DIM,
        image_dim: int = IMAGE_EMB_DIM,
        num_classes: int = 2,
    ):
        super().__init__()
        self.classifier = nn.Linear(text_dim + image_dim, num_classes)

    def forward(self, text_emb: torch.Tensor, image_emb: torch.Tensor) -> torch.Tensor:
        """Concatenate the modality embeddings and return 2-class logits."""
        return self.classifier(torch.cat([text_emb, image_emb], dim=1))


@dataclass
class EarlyFusionConfig:
    """Hyperparameters for training the early-fusion head."""

    epochs: int = 8
    batch_size: int = 256
    learning_rate: float = 1e-3
    random_seed: int = RANDOM_SEED
    embed_batch_size: int = 32


def train_early_fusion_head(
    text_train: np.ndarray,
    image_train: np.ndarray,
    y_train: np.ndarray,
    *,
    config: EarlyFusionConfig,
    device: torch.device,
) -> EarlyFusionHead:
    """Train the early-fusion linear head on precomputed embeddings.

    Args:
        text_train: Text embeddings, shape ``(N, 768)``.
        image_train: Image embeddings, shape ``(N, 512)``.
        y_train: Integer labels, shape ``(N,)``.
        config: Training hyperparameters.
        device: Device to train on.

    Returns:
        The trained :class:`EarlyFusionHead` (in eval-ready state).
    """
    torch.manual_seed(config.random_seed)
    model = EarlyFusionHead().to(device)
    class_weights = compute_class_weights(y_train, device)
    criterion = nn.CrossEntropyLoss(weight=class_weights)
    optimizer = torch.optim.AdamW(model.parameters(), lr=config.learning_rate)

    ds = TensorDataset(
        torch.from_numpy(text_train),
        torch.from_numpy(image_train),
        torch.from_numpy(y_train),
    )
    loader = DataLoader(ds, batch_size=config.batch_size, shuffle=True)

    model.train()
    for epoch in range(config.epochs):
        running = 0.0
        for t_emb, i_emb, y_batch in loader:
            t_emb = t_emb.to(device)
            i_emb = i_emb.to(device)
            y_batch = y_batch.to(device).long()
            optimizer.zero_grad(set_to_none=True)
            logits = model(t_emb, i_emb)
            loss = criterion(logits, y_batch)
            loss.backward()
            optimizer.step()
            running += float(loss.item()) * len(y_batch)
        print(f"  epoch {epoch + 1}/{config.epochs} train loss: {running / len(ds):.4f}")

    return model


@torch.inference_mode()
def predict_early_fusion(
    model: EarlyFusionHead,
    text_emb: np.ndarray,
    image_emb: np.ndarray,
    device: torch.device,
    *,
    batch_size: int = 512,
) -> tuple[np.ndarray, np.ndarray]:
    """Predict labels and fake probabilities with the early-fusion head.

    Args:
        model: A trained :class:`EarlyFusionHead`.
        text_emb: Text embeddings, shape ``(N, 768)``.
        image_emb: Image embeddings, shape ``(N, 512)``.
        device: Device to run inference on.
        batch_size: Inference batch size.

    Returns:
        A ``(y_pred, score_fake)`` pair of arrays (threshold 0.5 for labels).
    """
    model.eval()
    ds = TensorDataset(
        torch.from_numpy(text_emb),
        torch.from_numpy(image_emb),
    )
    loader = DataLoader(ds, batch_size=batch_size, shuffle=False)
    preds: list[int] = []
    scores: list[float] = []
    for t_emb, i_emb in loader:
        logits = model(t_emb.to(device), i_emb.to(device))
        prob = torch.softmax(logits, dim=-1)[:, 1].cpu().numpy()
        scores.extend(prob.tolist())
        preds.extend((prob >= 0.5).astype(np.int64).tolist())
    return np.asarray(preds, dtype=np.int64), np.asarray(scores, dtype=np.float64)


def run_early_fusion(
    train_df,
    val_df,
    *,
    project_root: Path,
    device: torch.device,
    config: EarlyFusionConfig | None = None,
) -> tuple[dict, EarlyFusionHead, np.ndarray, np.ndarray]:
    """End-to-end early fusion: extract embeddings, train head, return val metrics."""
    cfg = config or EarlyFusionConfig()
    text_dir, image_weights = require_unimodal_artifacts(project_root)
    text_model, tokenizer, image_backbone = load_frozen_encoders(
        text_dir, image_weights, device
    )

    print(f"Extracting train embeddings ({len(train_df)} rows)...")
    t_train, i_train, y_train = extract_embeddings(
        train_df,
        text_model,
        tokenizer,
        image_backbone,
        project_root=project_root,
        device=device,
        batch_size=cfg.embed_batch_size,
    )
    print(f"Extracting val embeddings ({len(val_df)} rows)...")
    t_val, i_val, y_val = extract_embeddings(
        val_df,
        text_model,
        tokenizer,
        image_backbone,
        project_root=project_root,
        device=device,
        batch_size=cfg.embed_batch_size,
    )

    print("Training early fusion head...")
    head = train_early_fusion_head(
        t_train, i_train, y_train, config=cfg, device=device
    )
    y_pred, score_fake = predict_early_fusion(head, t_val, i_val, device)
    metrics: dict[str, object] = {
        **evaluate_predictions(y_val, y_pred, score_fake),
        "run_id": FUSION_EARLY_RUN_ID,
        "fusion_type": "early_concat_linear",
        "train_rows": int(len(train_df)),
        "val_rows": int(len(val_df)),
        "text_emb_dim": TEXT_EMB_DIM,
        "image_emb_dim": IMAGE_EMB_DIM,
        "epochs": cfg.epochs,
        "batch_size": cfg.batch_size,
        "learning_rate": cfg.learning_rate,
        "random_seed": cfg.random_seed,
    }
    return metrics, head, y_pred, score_fake


def load_early_head_checkpoint(run_dir: Path, device: torch.device) -> EarlyFusionHead:
    """Load a saved early-fusion head from ``early_fusion_head.pt``."""
    head = EarlyFusionHead()
    head.load_state_dict(
        torch.load(run_dir / "early_fusion_head.pt", map_location="cpu", weights_only=True)
    )
    head.to(device)
    head.eval()
    return head
