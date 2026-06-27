"""Attention fusion: learned softmax weights over projected modality embeddings."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from fusion_common import (
    FUSION_ATTENTION_RUN_ID,
    IMAGE_EMB_DIM,
    RANDOM_SEED,
    TEXT_EMB_DIM,
    compute_class_weights,
    evaluate_predictions,
    extract_embeddings,
    load_frozen_encoders,
    require_unimodal_artifacts,
)


class AttentionFusionHead(nn.Module):
    """Project each modality, softmax-gate over {text, image}, then classify.

    Standard two-modality attention: weights sum to 1 per sample (interpretable).
    Optional LayerNorm on projected vectors and softmax temperature for Stage-2 tuning.
    """

    def __init__(
        self,
        text_dim: int = TEXT_EMB_DIM,
        image_dim: int = IMAGE_EMB_DIM,
        proj_dim: int = 256,
        num_classes: int = 2,
        *,
        use_layer_norm: bool = False,
        softmax_temperature: float = 1.0,
    ):
        super().__init__()
        self.text_proj = nn.Linear(text_dim, proj_dim)
        self.image_proj = nn.Linear(image_dim, proj_dim)
        self.attn_score = nn.Linear(proj_dim, 1)
        self.classifier = nn.Linear(proj_dim, num_classes)
        self.proj_dim = proj_dim
        self.use_layer_norm = use_layer_norm
        self.softmax_temperature = softmax_temperature
        self.text_norm = nn.LayerNorm(proj_dim) if use_layer_norm else None
        self.image_norm = nn.LayerNorm(proj_dim) if use_layer_norm else None

    def forward(
        self, text_emb: torch.Tensor, image_emb: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Project, softmax-gate, and classify the two modality embeddings.

        Args:
            text_emb: Text embeddings, shape ``(B, text_dim)``.
            image_emb: Image embeddings, shape ``(B, image_dim)``.

        Returns:
            A ``(logits, weights)`` pair: 2-class logits ``(B, 2)`` and the
            per-sample modality attention weights ``(B, 2)`` (sum to 1).
        """
        text_p = torch.tanh(self.text_proj(text_emb))
        image_p = torch.tanh(self.image_proj(image_emb))
        if self.text_norm is not None and self.image_norm is not None:
            text_p = self.text_norm(text_p)
            image_p = self.image_norm(image_p)
        stacked = torch.stack([text_p, image_p], dim=1)  # (B, 2, proj)
        logits_attn = self.attn_score(stacked).squeeze(-1)  # (B, 2)
        tau = max(float(self.softmax_temperature), 1e-6)
        weights = torch.softmax(logits_attn / tau, dim=1)
        fused = (stacked * weights.unsqueeze(-1)).sum(dim=1)
        return self.classifier(fused), weights


@dataclass
class AttentionFusionConfig:
    """Hyperparameters for the attention-fusion head and its training.

    ``proj_dim``, ``softmax_temperature``, and ``use_layer_norm`` are the
    pre-registered Stage-2 tuning axes; their defaults reproduce Stage 1.
    """

    proj_dim: int = 256
    softmax_temperature: float = 1.0
    use_layer_norm: bool = False
    epochs: int = 8
    batch_size: int = 256
    learning_rate: float = 1e-3
    random_seed: int = RANDOM_SEED
    embed_batch_size: int = 32


def train_attention_fusion_head(
    text_train: np.ndarray,
    image_train: np.ndarray,
    y_train: np.ndarray,
    *,
    config: AttentionFusionConfig,
    device: torch.device,
) -> AttentionFusionHead:
    """Train the attention-fusion head on precomputed embeddings.

    Args:
        text_train: Text embeddings, shape ``(N, 768)``.
        image_train: Image embeddings, shape ``(N, 512)``.
        y_train: Integer labels, shape ``(N,)``.
        config: Training and architecture hyperparameters.
        device: Device to train on.

    Returns:
        The trained :class:`AttentionFusionHead`.
    """
    torch.manual_seed(config.random_seed)
    model = AttentionFusionHead(
        proj_dim=config.proj_dim,
        use_layer_norm=config.use_layer_norm,
        softmax_temperature=config.softmax_temperature,
    ).to(device)
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
            logits, _ = model(t_emb, i_emb)
            loss = criterion(logits, y_batch)
            loss.backward()
            optimizer.step()
            running += float(loss.item()) * len(y_batch)
        print(f"  epoch {epoch + 1}/{config.epochs} train loss: {running / len(ds):.4f}")

    return model


@torch.inference_mode()
def predict_attention_fusion(
    model: AttentionFusionHead,
    text_emb: np.ndarray,
    image_emb: np.ndarray,
    device: torch.device,
    *,
    batch_size: int = 512,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Predict labels, fake probabilities, and attention weights.

    Args:
        model: A trained :class:`AttentionFusionHead`.
        text_emb: Text embeddings, shape ``(N, 768)``.
        image_emb: Image embeddings, shape ``(N, 512)``.
        device: Device to run inference on.
        batch_size: Inference batch size.

    Returns:
        A ``(y_pred, score_fake, attn)`` triple, where ``attn`` has shape
        ``(N, 2)`` of ``[text, image]`` modality weights.
    """
    model.eval()
    ds = TensorDataset(
        torch.from_numpy(text_emb),
        torch.from_numpy(image_emb),
    )
    loader = DataLoader(ds, batch_size=batch_size, shuffle=False)
    preds: list[int] = []
    scores: list[float] = []
    weight_rows: list[list[float]] = []
    for t_emb, i_emb in loader:
        logits, weights = model(t_emb.to(device), i_emb.to(device))
        prob = torch.softmax(logits, dim=-1)[:, 1].cpu().numpy()
        w = weights.cpu().numpy()
        scores.extend(prob.tolist())
        preds.extend((prob >= 0.5).astype(np.int64).tolist())
        weight_rows.extend(w.tolist())
    attn = np.asarray(weight_rows, dtype=np.float64)
    return (
        np.asarray(preds, dtype=np.int64),
        np.asarray(scores, dtype=np.float64),
        attn,
    )


def run_attention_fusion(
    train_df,
    val_df,
    *,
    project_root: Path,
    device: torch.device,
    config: AttentionFusionConfig | None = None,
) -> tuple[dict, AttentionFusionHead, np.ndarray, np.ndarray, np.ndarray]:
    """Run end-to-end attention fusion: embed, train the head, evaluate on val.

    Args:
        train_df: Training cohort frame.
        val_df: Validation cohort frame.
        project_root: Project root holding the unimodal checkpoints.
        device: Device to run on.
        config: Hyperparameters; defaults to :class:`AttentionFusionConfig`.

    Returns:
        A ``(metrics, head, y_pred, score_fake, attn_weights)`` tuple, where
        ``metrics`` includes the mean per-modality attention weights.
    """
    cfg = config or AttentionFusionConfig()
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

    print("Training attention fusion head...")
    head = train_attention_fusion_head(
        t_train, i_train, y_train, config=cfg, device=device
    )
    y_pred, score_fake, attn_weights = predict_attention_fusion(
        head, t_val, i_val, device
    )
    metrics: dict[str, object] = {
        **evaluate_predictions(y_val, y_pred, score_fake),
        "run_id": FUSION_ATTENTION_RUN_ID,
        "fusion_type": "attention_softmax",
        "train_rows": int(len(train_df)),
        "val_rows": int(len(val_df)),
        "proj_dim": cfg.proj_dim,
        "softmax_temperature": cfg.softmax_temperature,
        "use_layer_norm": cfg.use_layer_norm,
        "mean_attn_text": float(attn_weights[:, 0].mean()),
        "mean_attn_image": float(attn_weights[:, 1].mean()),
        "epochs": cfg.epochs,
        "batch_size": cfg.batch_size,
        "learning_rate": cfg.learning_rate,
        "random_seed": cfg.random_seed,
    }
    return metrics, head, y_pred, score_fake, attn_weights


def load_attention_head_checkpoint(
    run_dir: Path, device: torch.device
) -> AttentionFusionHead:
    """Load attention head; hyperparameters come from ``metrics.json`` when present."""
    import json

    metrics_path = run_dir / "metrics.json"
    proj_dim = 256
    softmax_temperature = 1.0
    use_layer_norm = False
    if metrics_path.is_file():
        meta = json.loads(metrics_path.read_text())
        proj_dim = int(meta.get("proj_dim", 256))
        softmax_temperature = float(meta.get("softmax_temperature", 1.0))
        use_layer_norm = bool(meta.get("use_layer_norm", False))
    head = AttentionFusionHead(
        proj_dim=proj_dim,
        use_layer_norm=use_layer_norm,
        softmax_temperature=softmax_temperature,
    )
    head.load_state_dict(
        torch.load(run_dir / "attention_fusion_head.pt", map_location="cpu", weights_only=True)
    )
    head.to(device)
    head.eval()
    return head
