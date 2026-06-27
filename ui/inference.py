"""Load ``runs/`` checkpoints and score single samples for the Gradio PoC."""

from __future__ import annotations

import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import joblib
import numpy as np
import torch
from PIL import Image

# training/ fusion helpers (same logic as the notebooks)
_TRAINING = Path(__file__).resolve().parent.parent / "training"
if str(_TRAINING) not in sys.path:
    sys.path.insert(0, str(_TRAINING))

from fusion_attention import load_attention_head_checkpoint, predict_attention_fusion  # noqa: E402
from fusion_common import (  # noqa: E402
    FUSION_ATTENTION_RUN_ID,
    FUSION_EARLY_RUN_ID,
    FUSION_LATE_RUN_ID,
    extract_sample_embeddings,
    load_distilbert_classifier,
    load_frozen_encoders,
    load_resnet_classifier,
    require_unimodal_artifacts,
    resolve_image_weights_path,
    score_image_sample,
    score_text_sample,
)
from fusion_early import load_early_head_checkpoint, predict_early_fusion  # noqa: E402
from fusion_late import load_late_combiner_checkpoint, predict_late_fusion  # noqa: E402

TFIDF_PIPELINE_NAME = "tfidf_pipeline.joblib"
THRESHOLD = 0.5


def pick_device() -> torch.device:
    """Return CUDA, Apple MPS, or CPU depending on what PyTorch can use."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def label_from_score(score_fake: float) -> str:
    """Map P(fake) to a user-facing label using :data:`THRESHOLD` (0.5)."""
    return "Likely fake" if score_fake >= THRESHOLD else "Likely real"


def _result(score_fake: float, **extra: Any) -> dict[str, Any]:
    return {"label": label_from_score(score_fake), "score_fake": score_fake, **extra}


class InferenceEngine:
    """Lazy-load checkpoints from ``runs/`` and dispatch to the matching scorer."""

    def __init__(self, project_root: Path):
        self.root = project_root
        self.runs = project_root / "runs"
        self.device = pick_device()
        self._cache: dict[str, Any] = {}

    def predict(
        self,
        text: str | None,
        image: Image.Image | None,
        model_key: str,
    ) -> dict[str, Any]:
        """Score one sample for a UI ``model_key`` (see ``gradio-ui.py`` dropdown)."""
        text_clean = (text or "").strip()
        handlers: dict[str, Callable[[], dict[str, Any]]] = {
            "text_tfidf": lambda: self._predict_tfidf(text_clean),
            "text_distilbert": lambda: self._predict_distilbert(text_clean),
            "image_resnet18": lambda: self._predict_resnet(image),
            "fusion_late": lambda: self._predict_fusion_late(text_clean, image),
            "fusion_early": lambda: self._predict_fusion_early(text_clean, image),
            "fusion_attention": lambda: self._predict_fusion_attention(text_clean, image),
        }
        try:
            return handlers[model_key]()
        except KeyError as exc:
            raise ValueError(f"Unknown model_key: {model_key}") from exc

    def _cached(self, key: str, loader: Callable[[], Any]) -> Any:
        if key not in self._cache:
            self._cache[key] = loader()
        return self._cache[key]

    def _predict_tfidf(self, text: str) -> dict[str, Any]:
        def load():
            path = self.runs / "text_tfidf_baseline" / TFIDF_PIPELINE_NAME
            if not path.is_file():
                raise FileNotFoundError(
                    f"Missing {path.name} — re-run the TF-IDF notebook save cell."
                )
            return joblib.load(path)

        pipeline = self._cached("tfidf", load)
        score_fake = float(pipeline.predict_proba([text])[0, 1])
        return _result(score_fake)

    def _predict_distilbert(self, text: str) -> dict[str, Any]:
        model, tokenizer = self._cached("distilbert", self._load_distilbert)
        score_fake = score_text_sample(text, model, tokenizer, self.device)
        return _result(score_fake)

    def _predict_resnet(self, image: Image.Image | None) -> dict[str, Any]:
        if image is None:
            raise ValueError("Image required")
        model = self._cached("resnet", self._load_resnet)
        score_fake = score_image_sample(image, model, self.device)
        return _result(score_fake)

    def _predict_fusion_late(
        self, text: str, image: Image.Image | None
    ) -> dict[str, Any]:
        if image is None:
            raise ValueError("Image required for fusion")
        score_text = self._predict_distilbert(text)["score_fake"]
        score_image = self._predict_resnet(image)["score_fake"]
        combiner = self._cached(
            "late_combiner",
            lambda: load_late_combiner_checkpoint(self.runs / FUSION_LATE_RUN_ID),
        )
        _, score_fused = predict_late_fusion(
            combiner,
            np.array([score_text]),
            np.array([score_image]),
        )
        return _result(
            float(score_fused[0]),
            score_text=score_text,
            score_image=score_image,
        )

    def _predict_fusion_early(
        self, text: str, image: Image.Image | None
    ) -> dict[str, Any]:
        if image is None:
            raise ValueError("Image required for fusion")
        text_emb, image_emb = self._sample_embeddings(text, image)
        head = self._cached(
            "early_head",
            lambda: load_early_head_checkpoint(self.runs / FUSION_EARLY_RUN_ID, self.device),
        )
        _, scores = predict_early_fusion(head, text_emb, image_emb, self.device)
        return _result(float(scores[0]))

    def _predict_fusion_attention(
        self, text: str, image: Image.Image | None
    ) -> dict[str, Any]:
        if image is None:
            raise ValueError("Image required for fusion")
        text_emb, image_emb = self._sample_embeddings(text, image)
        head = self._cached(
            "attention_head",
            lambda: load_attention_head_checkpoint(
                self.runs / FUSION_ATTENTION_RUN_ID, self.device
            ),
        )
        _, scores, attn = predict_attention_fusion(head, text_emb, image_emb, self.device)
        return _result(
            float(scores[0]),
            attn_text=float(attn[0, 0]),
            attn_image=float(attn[0, 1]),
        )

    def _sample_embeddings(
        self, text: str, image: Image.Image
    ) -> tuple[np.ndarray, np.ndarray]:
        text_model, tokenizer, image_backbone = self._cached("encoders", self._load_encoders)
        return extract_sample_embeddings(
            text, image, text_model, tokenizer, image_backbone, self.device
        )

    def _load_distilbert(self):
        text_dir, _ = require_unimodal_artifacts(self.root)
        return load_distilbert_classifier(text_dir, self.device)

    def _load_resnet(self):
        weights = resolve_image_weights_path(self.root)
        return load_resnet_classifier(weights, self.device)

    def _load_encoders(self):
        text_dir, image_weights = require_unimodal_artifacts(self.root)
        return load_frozen_encoders(text_dir, image_weights, self.device)


@lru_cache(maxsize=1)
def get_engine(project_root: str | Path) -> InferenceEngine:
    """Return a process-wide :class:`InferenceEngine` (one instance per root path)."""
    return InferenceEngine(Path(project_root))
