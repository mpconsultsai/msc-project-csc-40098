"""Single-sample inference for the Gradio proof-of-concept UI.

This module wraps the trained checkpoints under ``ui/models/`` behind a single
:class:`InferenceEngine`. Given one text string and/or one PIL image, the engine
dispatches to the scorer that matches a UI ``model_key`` and returns a small,
JSON-friendly result dictionary.

Every scorer returns a dict with at least:

- ``label``: a user-facing verdict from :func:`label_from_score`.
- ``score_fake``: ``P(fake)`` as a float in ``[0, 1]``.

Fusion scorers add extra diagnostic keys (per-modality scores for late fusion,
attention weights for attention fusion).

The scoring logic is shared with the training notebooks by importing the
``training/src`` fusion helpers, so the UI and the experiments stay in step.
"""

from __future__ import annotations

import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import joblib
import numpy as np
import torch
from PIL import Image
from torch import nn

# training/src fusion helpers (same logic as the notebooks)
_TRAINING_SRC = Path(__file__).resolve().parent.parent / "training" / "src"
if str(_TRAINING_SRC) not in sys.path:
    sys.path.insert(0, str(_TRAINING_SRC))

from fusion_attention import load_attention_head_checkpoint, predict_attention_fusion  # noqa: E402
from fusion_common import (  # noqa: E402
    extract_sample_embeddings,
    load_distilbert_classifier,
    load_frozen_encoders,
    load_resnet_classifier,
    score_image_sample,
    score_text_sample,
)
from fusion_early import load_early_head_checkpoint, predict_early_fusion  # noqa: E402
from fusion_late import load_late_combiner_checkpoint, predict_late_fusion  # noqa: E402

TFIDF_PIPELINE_NAME = "tfidf_pipeline.joblib"
RESNET_WEIGHTS_NAME = "resnet18_state.pt"
LATE_FUSION_NAME = "late_fusion_combiner.pkl"
EARLY_FUSION_NAME = "early_fusion_head.pt"
ATTENTION_FUSION_NAME = "attention_fusion_head.pt"
DISTILBERT_DIR_NAME = "model"
THRESHOLD = 0.5
MODELS_DIR = Path(__file__).resolve().parent / "models"


def _distilbert_weights_present(text_dir: Path) -> bool:
    """True if a Hugging Face weight file exists alongside config."""
    return (text_dir / "model.safetensors").is_file() or (text_dir / "pytorch_model.bin").is_file()


def _text_model_dir(models_dir: Path) -> Path:
    """Return the DistilBERT Hugging Face save under ``ui/models/model/``."""
    text_dir = models_dir / DISTILBERT_DIR_NAME
    if not (text_dir / "config.json").is_file():
        raise FileNotFoundError(
            f"Missing DistilBERT model at {text_dir}/ — copy the training `model/` folder here."
        )
    if not _distilbert_weights_present(text_dir):
        raise FileNotFoundError(
            f"Missing DistilBERT weights in {text_dir}/ — copy `model.safetensors` from "
            "Colab `My Drive/runs/text_distilbert_baseline/model/` (not stored in git)."
        )
    return text_dir


def _image_weights_path(models_dir: Path) -> Path:
    """Return the ResNet checkpoint in ``ui/models/``."""
    primary = models_dir / RESNET_WEIGHTS_NAME
    if primary.is_file():
        return primary
    epochs = sorted(models_dir.glob("resnet18_epoch*.pt"))
    if epochs:
        return epochs[-1]
    raise FileNotFoundError(
        f"No ResNet weights in {models_dir}/ — copy `resnet18_state.pt` from training."
    )


def pick_device() -> torch.device:
    """Select the best available PyTorch device.

    Returns:
        The CUDA device if available, else Apple MPS, else CPU.
    """
    if torch.cuda.is_available():
        return torch.device("cuda")
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def label_from_score(score_fake: float) -> str:
    """Map a fake-probability score to a user-facing label.

    Args:
        score_fake: ``P(fake)`` in ``[0, 1]``.

    Returns:
        ``"Likely fake"`` if ``score_fake`` is at least :data:`THRESHOLD`
        (0.5), otherwise ``"Likely real"``.
    """
    return "Likely fake" if score_fake >= THRESHOLD else "Likely real"


def _result(score_fake: float, **extra: Any) -> dict[str, Any]:
    """Build the standard result dict, optionally augmented with diagnostics.

    Args:
        score_fake: ``P(fake)`` in ``[0, 1]``.
        **extra: Optional per-model diagnostics merged into the result, e.g.
            ``score_text`` / ``score_image`` for late fusion or
            ``attn_text`` / ``attn_image`` for attention fusion.

    Returns:
        A dict with ``label`` and ``score_fake`` keys plus any ``extra`` keys.
    """
    return {"label": label_from_score(score_fake), "score_fake": score_fake, **extra}


class InferenceEngine:
    """Lazy-load checkpoints from ``ui/models/`` and dispatch to the matching scorer."""

    def __init__(self, models_dir: Path | None = None):
        """Configure paths and device; checkpoints load lazily on first use.

        Args:
            models_dir: Directory containing trained checkpoints. Defaults to
                :data:`MODELS_DIR` (``ui/models/``).
        """
        self.models_dir = models_dir or MODELS_DIR
        self.device = pick_device()
        self._cache: dict[str, Any] = {}

    def predict(
        self,
        text: str | None,
        image: Image.Image | None,
        model_key: str,
    ) -> dict[str, Any]:
        """Score one sample for a UI ``model_key`` (see ``gradio-ui.py`` dropdown).

        Args:
            text: Raw post text; ``None`` or whitespace is treated as empty.
            image: A PIL image, required for the image and fusion models.
            model_key: One of the supported keys (``text_tfidf``,
                ``text_distilbert``, ``image_resnet18``, ``fusion_late``,
                ``fusion_early``, ``fusion_attention``).

        Returns:
            A result dict (see the module docstring).

        Raises:
            ValueError: If ``model_key`` is not recognised, or a required input
                (e.g. an image for an image/fusion model) is missing.
            FileNotFoundError: If the checkpoint for the model is absent.
        """
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
        """Memoise a loaded artefact so each checkpoint loads at most once.

        Args:
            key: Cache key identifying the artefact.
            loader: Zero-argument callable that loads the artefact on a cache miss.

        Returns:
            The cached artefact, loading it via ``loader`` on first access.
        """
        if key not in self._cache:
            self._cache[key] = loader()
        return self._cache[key]

    def _predict_tfidf(self, text: str) -> dict[str, Any]:
        """Score text with the TF-IDF + logistic-regression sklearn pipeline.

        Args:
            text: Cleaned post text.

        Returns:
            A standard result dict.

        Raises:
            FileNotFoundError: If the saved TF-IDF pipeline is missing.
        """
        def load() -> Any:
            path = self.models_dir / TFIDF_PIPELINE_NAME
            if not path.is_file():
                raise FileNotFoundError(
                    f"Missing {path.name} — re-run the TF-IDF notebook save cell."
                )
            return joblib.load(path)

        pipeline = self._cached("tfidf", load)
        score_fake = float(pipeline.predict_proba([text])[0, 1])
        return _result(score_fake)

    def _predict_distilbert(self, text: str) -> dict[str, Any]:
        """Score text with the fine-tuned DistilBERT classifier.

        Args:
            text: Cleaned post text.

        Returns:
            A standard result dict.
        """
        model, tokenizer = self._cached("distilbert", self._load_distilbert)
        score_fake = score_text_sample(text, model, tokenizer, self.device)
        return _result(score_fake)

    def _predict_resnet(self, image: Image.Image | None) -> dict[str, Any]:
        """Score an image with the ResNet-18 classifier.

        Args:
            image: The PIL image to score.

        Returns:
            A standard result dict.

        Raises:
            ValueError: If ``image`` is ``None``.
        """
        if image is None:
            raise ValueError("Image required")
        model = self._cached("resnet", self._load_resnet)
        score_fake = score_image_sample(image, model, self.device)
        return _result(score_fake)

    def _predict_fusion_late(
        self, text: str, image: Image.Image | None
    ) -> dict[str, Any]:
        """Late fusion: combine the DistilBERT and ResNet scores via the logistic combiner.

        Args:
            text: Cleaned post text.
            image: The PIL image to score.

        Returns:
            A standard result dict, plus the per-modality scores
            ``score_text`` and ``score_image``.

        Raises:
            ValueError: If ``image`` is ``None``.
        """
        if image is None:
            raise ValueError("Image required for fusion")
        score_text = self._predict_distilbert(text)["score_fake"]
        score_image = self._predict_resnet(image)["score_fake"]
        combiner = self._cached(
            "late_combiner",
            lambda: load_late_combiner_checkpoint(self.models_dir),
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
        """Early fusion: concatenate frozen embeddings and score with the linear head.

        Args:
            text: Cleaned post text.
            image: The PIL image to score.

        Returns:
            A standard result dict.

        Raises:
            ValueError: If ``image`` is ``None``.
        """
        if image is None:
            raise ValueError("Image required for fusion")
        text_emb, image_emb = self._sample_embeddings(text, image)
        head = self._cached(
            "early_head",
            lambda: load_early_head_checkpoint(self.models_dir, self.device),
        )
        _, scores = predict_early_fusion(head, text_emb, image_emb, self.device)
        return _result(float(scores[0]))

    def _predict_fusion_attention(
        self, text: str, image: Image.Image | None
    ) -> dict[str, Any]:
        """Attention fusion: score with the attention head and return the modality weights.

        Args:
            text: Cleaned post text.
            image: The PIL image to score.

        Returns:
            A standard result dict, plus ``attn_text`` and ``attn_image``
            (softmax modality weights that sum to 1).

        Raises:
            ValueError: If ``image`` is ``None``.
        """
        if image is None:
            raise ValueError("Image required for fusion")
        text_emb, image_emb = self._sample_embeddings(text, image)
        head = self._cached(
            "attention_head",
            lambda: load_attention_head_checkpoint(self.models_dir, self.device),
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
        """Extract the frozen embeddings shared by both feature-level fusion heads.

        Args:
            text: Cleaned post text.
            image: The PIL image to embed.

        Returns:
            A ``(text_embedding, image_embedding)`` pair of NumPy arrays.
        """
        text_model, tokenizer, image_backbone = self._cached("encoders", self._load_encoders)
        return extract_sample_embeddings(
            text, image, text_model, tokenizer, image_backbone, self.device
        )

    def _load_distilbert(self) -> tuple[nn.Module, Any]:
        """Load the fine-tuned DistilBERT classifier.

        Returns:
            A ``(model, tokenizer)`` pair.
        """
        text_dir = _text_model_dir(self.models_dir)
        return load_distilbert_classifier(text_dir, self.device)

    def _load_resnet(self) -> nn.Module:
        """Load the ResNet-18 image classifier from its saved state dict.

        Returns:
            The ResNet-18 classifier model.
        """
        weights = _image_weights_path(self.models_dir)
        return load_resnet_classifier(weights, self.device)

    def _load_encoders(self) -> tuple[nn.Module, Any, nn.Module]:
        """Load the frozen encoders used for early and attention fusion.

        Returns:
            A ``(text_model, tokenizer, image_backbone)`` tuple.
        """
        text_dir = _text_model_dir(self.models_dir)
        image_weights = _image_weights_path(self.models_dir)
        return load_frozen_encoders(text_dir, image_weights, self.device)


@lru_cache(maxsize=1)
def get_engine() -> InferenceEngine:
    """Return a process-wide :class:`InferenceEngine` using :data:`MODELS_DIR`."""
    return InferenceEngine()
