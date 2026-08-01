#!/usr/bin/env python3
"""
Gradio proof-of-concept — multimodal fake news check.

Run from project root (use the project virtualenv):

    .venv/bin/pip install -r ui/requirements.txt
    .venv/bin/python ui/gradio-ui.py

If you see ``ModuleNotFoundError: No module named 'gradio'``, you are not using
``.venv`` — activate it (``source .venv/bin/activate``) or use the paths above.

Inference is in ``ui/inference.py`` (loads checkpoints from ``ui/models/``).
Copy trained checkpoints into ``ui/models/`` (flat layout — see ``ui/README.md``).

Deploy:
  Local:     .venv/bin/python ui/gradio-ui.py
  Public:    .venv/bin/python ui/gradio-ui.py --share
  HF Space:  see ui/README.md
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import gradio as gr
from PIL import Image, UnidentifiedImageError

# ``inference`` lives alongside this file; add ui/ to sys.path before importing it.
_UI_DIR = Path(__file__).resolve().parent
if str(_UI_DIR) not in sys.path:
    sys.path.insert(0, str(_UI_DIR))

from inference import (  # noqa: E402
    ATTENTION_FUSION_NAME,
    DISTILBERT_DIR_NAME,
    EARLY_FUSION_NAME,
    LATE_FUSION_NAME,
    MODELS_DIR,
    RESNET_WEIGHTS_NAME,
    TFIDF_PIPELINE_NAME,
    _distilbert_weights_present,
    format_device_label,
    get_engine,
)
# --- Model catalogue (matches training run folders) ---

MODEL_CATALOG: dict[str, dict[str, str]] = {
    "text_tfidf": {
        "name": "TF-IDF + logistic regression",
        "paradigm": "Traditional ML",
        "modality": "Text",
        "architecture": "Bag-of-words (TF-IDF features) → logistic classifier (sklearn)",
        "inputs": "Post text",
    },
    "text_distilbert": {
        "name": "DistilBERT",
        "paradigm": "Deep learning (NLP)",
        "modality": "Text",
        "architecture": "Transformer encoder (DistilBERT), fine-tuned for classification",
        "inputs": "Post text",
    },
    "image_resnet18": {
        "name": "ResNet-18",
        "paradigm": "Deep learning (vision)",
        "modality": "Image",
        "architecture": "CNN (ResNet-18), transfer learning from ImageNet",
        "inputs": "Image",
    },
    "fusion_late": {
        "name": "Late fusion",
        "paradigm": "Multimodal (traditional combiner)",
        "modality": "Text + image",
        "architecture": "Frozen DistilBERT + ResNet-18 → P(fake) scores → logistic regression",
        "inputs": "Post text and image",
    },
    "fusion_early": {
        "name": "Early fusion",
        "paradigm": "Multimodal (deep learning head)",
        "modality": "Text + image",
        "architecture": "Frozen encoders → concat [CLS] + CNN embeddings → linear classifier",
        "inputs": "Post text and image",
    },
    "fusion_attention": {
        "name": "Attention fusion (default)",
        "paradigm": "Multimodal (deep learning head)",
        "modality": "Text + image",
        "architecture": "Frozen encoders → learned attention weights over modalities → classifier",
        "inputs": "Post text and image",
    },
}

MODEL_ORDER: list[str] = [
    "text_tfidf",
    "text_distilbert",
    "image_resnet18",
    "fusion_late",
    "fusion_early",
    "fusion_attention",
]


def model_dropdown_label(model_key: str) -> str:
    """Build the compact dropdown label for a model.

    Args:
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        A ``"modality · paradigm · name"`` label for the dropdown.
    """
    c = MODEL_CATALOG[model_key]
    return f"{c['modality']} · {c['paradigm']} · {c['name']}"


MODEL_CHOICES: list[tuple[str, str]] = [
    (model_dropdown_label(key), key) for key in MODEL_ORDER
]

DEFAULT_MODEL = "fusion_attention"

TEXT_ONLY = {"text_tfidf", "text_distilbert"}
IMAGE_ONLY = {"image_resnet18"}
FUSION = {"fusion_late", "fusion_early", "fusion_attention"}


# Flat filenames under ui/models/ (DistilBERT keeps a model/ subfolder).
def _unimodal_for_fusion(root: Path) -> bool:
    text_dir = root / DISTILBERT_DIR_NAME
    return (
        (text_dir / "config.json").is_file()
        and _distilbert_weights_present(text_dir)
        and (
            (root / RESNET_WEIGHTS_NAME).is_file() or any(root.glob("resnet18_epoch*.pt"))
        )
    )


ARTEFACT_CHECKS: dict[str, Callable[[Path], bool]] = {
    "text_tfidf": lambda root: (root / TFIDF_PIPELINE_NAME).is_file(),
    "text_distilbert": lambda root: (root / DISTILBERT_DIR_NAME / "config.json").is_file()
    and _distilbert_weights_present(root / DISTILBERT_DIR_NAME),
    "image_resnet18": lambda root: (root / RESNET_WEIGHTS_NAME).is_file()
    or any(root.glob("resnet18_epoch*.pt")),
    "fusion_late": lambda root: (root / LATE_FUSION_NAME).is_file() and _unimodal_for_fusion(root),
    "fusion_early": lambda root: (root / EARLY_FUSION_NAME).is_file() and _unimodal_for_fusion(root),
    "fusion_attention": lambda root: (root / ATTENTION_FUSION_NAME).is_file()
    and _unimodal_for_fusion(root),
}

TEXT_TSV = "data/fake_news_final_text.tsv"
IMAGE_TSV = "data/fake_news_final_image.tsv"

# Frozen cohort rows (text + image); labels are not shown in the UI.
EXAMPLE_FAKE_ID = "fnn:politifact:fake:politifact13468"
EXAMPLE_REAL_ID = "fnn:gossipcop:real:gossipcop-954027"

EXAMPLES_DIR = _UI_DIR / "assets" / "examples"
E1_IMAGE_PATH = EXAMPLES_DIR / "e1_snopes_dog_child.png"
E2_IMAGE_PATH = EXAMPLES_DIR / "e2_bbc_earth_cat.jpg"

# Phase 2 external examples (short social text; images stored under ui/assets/examples/).
E1_SNOPES_TEXT = (
    "WOW: During the recent earthquakes in Venezuela, young Mateo was reportedly "
    "trapped beneath the rubble in complete darkness. His faithful Golden Retriever, "
    "Max, stayed right by his side without moving an inch. He wrapped his warm body "
    "around the child, shielding him from the cold and fear as the world seemed to "
    "crumble around them.\n\n"
    "Hours later, rescuers' flashlights cut through the debris. They found an "
    "unforgettable scene: the boy resting peacefully, held close by a dog whose eyes "
    "showed exhaustion but also unbreakable resolve.\n\n"
    "Max had completed his duty. He never abandoned his post. In that instant, amid "
    "the dust and emotions, it was evident that the real hero possessed golden fur "
    "and a heart full of endless love.\n\n"
    "Incredible!"
)
E2_BBC_EARTH_TEXT = (
    "Happy #InternationalCatDay!\n"
    "Who knew deadly could be so cute! The black-footed cat is the deadliest wild "
    "cat in the world"
)

# Example buttons by phase tab: (key, short label). Details show above Analyse.
EXAMPLE_BUTTONS_PHASE1: list[tuple[str, str]] = [
    ("cohort_real", "GossipCop real"),
    ("cohort_fake", "PolitiFact fake"),
]
EXAMPLE_BUTTONS_PHASE2: list[tuple[str, str]] = [
    ("e1_snopes", "Snopes viral claim"),
    ("e2_bbc_earth", "BBC Earth (X)"),
]
EXAMPLE_BUTTONS: list[tuple[str, str]] = (
    EXAMPLE_BUTTONS_PHASE1 + EXAMPLE_BUTTONS_PHASE2
)

# Source blurb shown above Analyse when an example button is clicked.
EXAMPLE_INFO: dict[str, str] = {
    "cohort_real": (
        "**Phase 1** — Frozen FakeNewsNet cohort row "
        "`fnn:gossipcop:real:gossipcop-954027` (GossipCop).\n\n"
        "Cohort label is for tester smoke checks only — not shown as the model verdict. "
        "Third-party news imagery via the project cohort."
    ),
    "cohort_fake": (
        "**Phase 1** — Frozen FakeNewsNet cohort row "
        "`fnn:politifact:fake:politifact13468` (PolitiFact).\n\n"
        "Cohort label is for tester smoke checks only — not shown as the model verdict. "
        "Third-party news imagery via the project cohort."
    ),
    "e1_snopes": (
        "**Phase 2** — Viral social claim discussed by Snopes "
        "([fact check](https://www.snopes.com/fact-check/dog-child-venezuela-earthquake/); "
        "Ibrahim, 2026), rated Fake (AI-style image).\n\n"
        "Demonstrates pattern-match vs fact-check. Image is third-party; demo use only."
    ),
    "e2_bbc_earth": (
        "**Phase 2** — Official [@BBCEarth](https://x.com/BBCEarth/status/1292022742137466880) "
        "X post (International Cat Day / black-footed cat).\n\n"
        "Authentic publisher social post (non-political). Still © BBC; demo use only with citation."
    ),
}

# Remote image fetch limits for View Input → image URL → Load (_download_url_bytes).
# IMAGE_URL_TIMEOUT_S — abort slow or dead links instead of hanging the UI.
# IMAGE_URL_MAX_BYTES — cap payload size so large files do not exhaust memory.
# IMAGE_URL_USER_AGENT — identify the PoC; many CDNs and social pages reject bare urllib requests.
IMAGE_URL_TIMEOUT_S = 8
IMAGE_URL_MAX_BYTES = 8 * 1024 * 1024
IMAGE_URL_USER_AGENT = (
    "Mozilla/5.0 (compatible; MSC-FakeNews-PoC/1.0; +https://github.com/)"
)
_OG_IMAGE_RE = re.compile(
    r'(?:property|name)=["\'](?:og:image|twitter:image)["\']\s+content=["\']([^"\']+)["\']'
    r'|content=["\']([^"\']+)["\']\s+(?:property|name)=["\'](?:og:image|twitter:image)["\']',
    re.IGNORECASE,
)


def load_cohort_example(
    root: Path,
    sample_id: str,
) -> tuple[str, Image.Image | None]:
    """Load the post text and image for one frozen cohort row.

    Args:
        root: Repository root containing the cohort TSVs and images.
        sample_id: The ``sample_id`` to look up in both cohort exports.

    Returns:
        A ``(text, image)`` pair. ``text`` is empty and ``image`` is ``None``
        if the TSVs or the image file are missing. Images are returned as
        :class:`PIL.Image.Image` so Gradio does not rely on ephemeral temp paths.
    """
    text_tsv = root / TEXT_TSV
    image_tsv = root / IMAGE_TSV
    if not text_tsv.is_file() or not image_tsv.is_file():
        return "", None

    text_value = ""
    with text_tsv.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            if row.get("sample_id") == sample_id:
                text_value = (row.get("text") or row.get("title_raw") or "").strip()
                break

    image_rel = ""
    with image_tsv.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            if row.get("sample_id") == sample_id:
                image_rel = (row.get("cohort_image_local_path") or "").strip()
                break

    image_value: Image.Image | None = None
    if image_rel:
        candidate = root / image_rel
        if candidate.is_file():
            image_value = Image.open(candidate).convert("RGB")

    return text_value, image_value


def _load_local_image(path: Path) -> Image.Image | None:
    """Load an RGB PIL image from disk, or ``None`` if missing/unreadable."""
    if not path.is_file():
        return None
    try:
        return Image.open(path).convert("RGB")
    except OSError:
        return None


def load_demo_examples(
    root: Path,
) -> dict[str, tuple[str, Image.Image | None]]:
    """Preload cohort and Phase~2 demo posts.

    Args:
        root: Repository root containing the cohort TSVs and images.

    Returns:
        A dict keyed by example keys (``cohort_real``, ``cohort_fake``,
        ``e1_snopes``, ``e2_bbc_earth``), each mapping to ``(text, image)``.
    """
    return {
        "cohort_real": load_cohort_example(root, EXAMPLE_REAL_ID),
        "cohort_fake": load_cohort_example(root, EXAMPLE_FAKE_ID),
        "e1_snopes": (E1_SNOPES_TEXT, _load_local_image(E1_IMAGE_PATH)),
        "e2_bbc_earth": (E2_BBC_EARTH_TEXT, _load_local_image(E2_IMAGE_PATH)),
    }


def apply_demo_example(
    example_key: str | None,
    examples: dict[str, tuple[str, Image.Image | None]],
) -> tuple[str, Image.Image | None, str, str, str]:
    """Return text, image, cleared URL, example source info, and cleared verdict.

    Args:
        example_key: A key in ``examples``, or empty/None for no selection.
        examples: Preloaded examples from :func:`load_demo_examples`.

    Returns:
        ``(text, image, empty_url, source_info, empty_verdict)``.
    """
    key = (example_key or "").strip()
    if not key:
        return "", None, "", "", ""
    text, image = examples.get(key, ("", None))
    info = EXAMPLE_INFO.get(key, "")
    return text, image, "", info, ""


def on_example_selected(
    example_key: str | None,
    examples: dict[str, tuple[str, Image.Image | None]],
) -> tuple[Any, ...]:
    """Load the selected example into the inputs and show source details."""
    return apply_demo_example(example_key, examples)


def _normalise_http_url(url: str) -> str:
    """Return a stripped URL or raise if it is not http(s) with a host."""
    cleaned = url.strip()
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Image URL must be a valid http(s) link.")
    return cleaned


def _download_url_bytes(url: str) -> tuple[bytes, str]:
    """Fetch URL bytes with size/timeout limits.

    Args:
        url: Absolute ``http``/``https`` URL.

    Returns:
        ``(payload_bytes, content_type)``.

    Raises:
        ValueError: On network failure or oversized payload.
    """
    request = Request(url, headers={"User-Agent": IMAGE_URL_USER_AGENT})
    try:
        with urlopen(request, timeout=IMAGE_URL_TIMEOUT_S) as response:
            content_type = (response.headers.get("Content-Type") or "").lower()
            data = response.read(IMAGE_URL_MAX_BYTES + 1)
    except HTTPError as exc:
        raise ValueError(f"Could not fetch image (HTTP {exc.code}).") from exc
    except URLError as exc:
        raise ValueError("Could not fetch image (network error).") from exc
    except TimeoutError as exc:
        raise ValueError("Timed out fetching the image URL.") from exc

    if len(data) > IMAGE_URL_MAX_BYTES:
        raise ValueError("Image is too large (max 8 MB).")
    return data, content_type


def _is_html_response(content_type: str, data: bytes) -> bool:
    """Return True when the payload looks like an HTML document."""
    if "text/html" in content_type:
        return True
    prefix = data.lstrip()[:15].lower()
    return prefix.startswith((b"<!doctype html", b"<html"))


def _preview_image_url_from_html(html: str) -> str | None:
    """Extract ``og:image`` / ``twitter:image`` from an HTML page, if present."""
    match = _OG_IMAGE_RE.search(html)
    if not match:
        return None
    return (match.group(1) or match.group(2) or "").strip() or None


def _pil_from_bytes(data: bytes) -> Image.Image:
    """Decode image bytes to RGB PIL, or raise ``UnidentifiedImageError``."""
    return Image.open(BytesIO(data)).convert("RGB")


def _fetch_og_preview_image(html_bytes: bytes) -> Image.Image:
    """Resolve an image from a social/HTML page via ``og:image`` (one hop)."""
    preview_url = _preview_image_url_from_html(html_bytes.decode("utf-8", errors="ignore"))
    if not preview_url:
        raise ValueError(
            "That looks like a web page, not an image. "
            "Paste a direct image link, or an X photo page."
        )

    preview_data, _ = _download_url_bytes(_normalise_http_url(preview_url))
    try:
        return _pil_from_bytes(preview_data)
    except UnidentifiedImageError as exc:
        raise ValueError(
            "Could not load the page's preview image. Try a direct image link."
        ) from exc


def fetch_image_from_url(url: str) -> Image.Image:
    """Download a remote image and return it as RGB PIL.

    Accepts a direct image link, or an HTML page that declares ``og:image`` /
    ``twitter:image`` (one hop), which covers many social photo pages.

    Args:
        url: An ``http`` or ``https`` URL.

    Returns:
        An RGB :class:`PIL.Image.Image`.

    Raises:
        ValueError: If the URL is invalid, the download fails, the payload is too
            large, or no usable image can be resolved.
    """
    url = _normalise_http_url(url)
    data, content_type = _download_url_bytes(url)

    try:
        return _pil_from_bytes(data)
    except UnidentifiedImageError:
        if not _is_html_response(content_type, data):
            raise ValueError(
                "URL did not return a usable image (use a direct image link)."
            ) from None
        return _fetch_og_preview_image(data)


def resolve_image(
    image: Any,
    image_url: str | None,
) -> tuple[Image.Image | None, str | None]:
    """Prefer an uploaded image; otherwise fetch from a URL if provided.

    Args:
        image: Uploaded PIL image, or ``None``.
        image_url: Optional direct image URL.

    Returns:
        ``(resolved_image, error_message)``. ``error_message`` is set only when
        a URL was supplied and could not be loaded.
    """
    if image is not None:
        return image, None

    url = (image_url or "").strip()
    if not url:
        return None, None

    try:
        return fetch_image_from_url(url), None
    except ValueError as exc:
        return None, str(exc)


def load_image_url_for_ui(
    image_url: str | None,
) -> tuple[Any, str, Any]:
    """Fetch a URL into the Image preview for the Gradio PoC.

    Args:
        image_url: Direct image link or social photo page URL.

    Returns:
        ``(image_or_update, status_markdown, tabs_update)``. On success the
        image is shown, the Image tab is selected, and the status is cleared;
        on failure the preview is left unchanged.
    """
    url = (image_url or "").strip()
    if not url:
        return gr.update(), "### ⚠️ Paste an image URL first.", gr.update()

    try:
        return (
            fetch_image_from_url(url),
            "",
            gr.update(selected="image"),
        )
    except ValueError as exc:
        return gr.update(), f"### ⚠️ {exc}", gr.update()


def clear_text_input() -> str:
    """Return an empty string to clear the text input."""
    return ""


def _modality_tabs_for_model(model_key: str) -> tuple[Any, Any, Any]:
    """Return Gradio updates for Text/Image tab visibility and selection.

    Args:
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        ``(text_tab, image_tab, tabs)`` updates. Fusion shows both tabs and
        selects Text; single-modality models hide the unused tab.
    """
    show_text = model_key in TEXT_ONLY or model_key in FUSION
    show_image = model_key in IMAGE_ONLY or model_key in FUSION
    selected: str = "text" if show_text else "image"
    return (
        gr.update(visible=show_text),
        gr.update(visible=show_image),
        gr.update(selected=selected),
    )


def reset_demo() -> tuple[Any, ...]:
    """Restore the PoC to its initial empty state.

    Returns:
        Model, model info, text, image, URL, example source info, cleared
        verdict, modality tab visibility/selection for the default model, and
        Examples tabs reset to Phase~1.
    """
    text_tab, image_tab, tabs = _modality_tabs_for_model(DEFAULT_MODEL)
    return (
        DEFAULT_MODEL,
        model_info_markdown(DEFAULT_MODEL),
        "",
        None,
        "",
        "",
        "",
        text_tab,
        image_tab,
        tabs,
        gr.update(selected="phase1"),
    )


DISCLAIMER = (
    "*Model-based estimate for demonstration — not a definitive truth claim.*"
)


def project_root() -> Path:
    """Return the repository root (the parent of ``ui/``)."""
    return Path(__file__).resolve().parent.parent


def artefacts_present(model_key: str) -> bool:
    """Check whether the trained artefacts for a model exist under ``ui/models/``.

    Args:
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        ``True`` if the run directory and the model-specific checkpoint file(s)
        are present, otherwise ``False``.
    """
    check = ARTEFACT_CHECKS.get(model_key, lambda _root: False)
    return check(MODELS_DIR)


def validate_inputs(
    text: str | None,
    image: Any,
    model_key: str,
) -> str | None:
    """Validate the inputs required by the selected model.

    Args:
        text: Raw post text (may be ``None``).
        image: The uploaded or URL-resolved image, or ``None`` if none was provided.
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        A user-facing error message if a required input is missing, otherwise
        ``None``.
    """
    text_clean = (text or "").strip()
    has_image = image is not None

    if model_key in TEXT_ONLY and not text_clean:
        return "Please enter some post text for this model."
    if model_key in IMAGE_ONLY and not has_image:
        return "Please upload an image or paste an image URL for this model."
    if model_key in FUSION:
        if not text_clean:
            return "Please enter post text for multimodal fusion."
        if not has_image:
            return "Please upload an image or paste an image URL for multimodal fusion."
    return None


def model_info_markdown(model_key: str) -> str:
    """Render the descriptive helper text shown under the model selector.

    Args:
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        Markdown content (modality, paradigm, architecture, inputs), or an empty
        string if ``model_key`` is unknown.
    """
    c = MODEL_CATALOG.get(model_key, {})
    if not c:
        return ""
    return (
        f"**{c['modality']}** — {c['paradigm']}  \n"
        f"*{c['architecture']}*  \n"
        f"**Inputs:** {c['inputs']}"
    )


def model_display_name(model_key: str) -> str:
    """Return the human-readable model name used in the verdict text.

    Args:
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        A ``"modality — name (paradigm)"`` string, or ``model_key`` itself if
        the key is unknown.
    """
    c = MODEL_CATALOG.get(model_key, {})
    if not c:
        return model_key
    return f"{c['modality']} — {c['name']} ({c['paradigm']})"


def input_visibility_for_model(model_key: str) -> tuple[Any, Any, Any]:
    """Compute Text/Image tab visibility and selection for a model.

    Args:
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        Three Gradio updates for ``(text tab, image tab, tabs)``.
    """
    return _modality_tabs_for_model(model_key)


def format_verdict(
    label: str,
    score_fake: float,
    model_label: str,
    *,
    detail: dict[str, Any] | None = None,
) -> str:
    """Format the prediction as the Markdown verdict shown in the UI.

    Args:
        label: The predicted label (e.g. ``"Likely fake"``).
        score_fake: ``P(fake)`` in ``[0, 1]``.
        model_label: The display name of the model used.
        detail: Optional extras from the engine (late-fusion scores, attention
            weights, device).

    Returns:
        A Markdown string with the estimate, score, diagnostics, and disclaimer.
    """
    detail = detail or {}
    score_real = 1.0 - score_fake
    lines = [
        f"### Estimate: **{label}**",
        "",
        f"- **P(fake):** {score_fake:.3f}",
        f"- **P(real):** {score_real:.3f}",
        "- **Decision threshold:** 0.50 (fake if P(fake) >= 0.50)",
        f"- **Model:** {model_label}",
    ]

    device = detail.get("device")
    if device:
        lines.append(f"- **Device:** {format_device_label(device)}")

    if "score_text" in detail and "score_image" in detail:
        lines.append(
            f"- **Late fusion inputs:** P(fake|text)={float(detail['score_text']):.3f}, "
            f"P(fake|image)={float(detail['score_image']):.3f}"
        )

    if "attn_text" in detail and "attn_image" in detail:
        lines.append(
            f"- **Attention weights:** text={float(detail['attn_text']):.3f}, "
            f"image={float(detail['attn_image']):.3f}"
        )

    lines.extend(["", DISCLAIMER])
    return "\n".join(lines)


def run_inference(
    text: str | None,
    image: Any,
    model_key: str,
) -> dict[str, Any]:
    """Run inference by delegating to the lazy-loaded engine in ``inference.py``.

    Args:
        text: Raw post text (may be ``None``).
        image: The uploaded PIL image, or ``None``.
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        The engine's result dict (``label``, ``score_fake``, and any extras).
    """
    return get_engine().predict(text, image, model_key)


def analyse(
    text: str | None,
    image: Any,
    model_key: str,
    image_url: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Validate inputs, run inference, and format the result for the UI.

    Args:
        text: Raw post text (may be ``None``).
        image: The uploaded PIL image, or ``None``.
        model_key: A key in :data:`MODEL_CATALOG`.
        image_url: Optional direct image URL used when no upload is present.

    Returns:
        A ``(markdown, detail)`` pair: the Markdown to display, and a detail dict
        with the raw result (or an ``error`` key when validation fails, artefacts
        are missing, or inference raises).
    """
    model_label = model_display_name(model_key)

    resolved, resolve_err = resolve_image(image, image_url)
    if resolve_err:
        return f"### ⚠️ {resolve_err}", {
            "error": resolve_err,
            "model": model_key,
        }

    err = validate_inputs(text, resolved, model_key)
    if err:
        return f"### ⚠️ {err}", {
            "error": err,
            "model": model_key,
            "resolved_image": resolved,
        }

    if not artefacts_present(model_key):
        msg = (
            "### Artefacts missing\n\n"
            "Expected checkpoints in `ui/models/` (see `ui/README.md`).\n\n"
            "Copy the trained files from Colab `My Drive/runs/` into `ui/models/`."
        )

        return msg, {
            "error": "artefacts_missing",
            "model": model_key,
            "resolved_image": resolved,
        }

    engine = get_engine()
    try:
        result = engine.predict(text, resolved, model_key)
    except Exception as exc:
        return (
            f"### Runtime error\n\n`{type(exc).__name__}: {exc}`",
            {
                "error": "runtime",
                "model": model_key,
                "detail": str(exc),
                "resolved_image": resolved,
            },
        )

    label = result.get("label", "unknown")
    score_fake = float(result.get("score_fake", 0.0))
    detail = {
        **result,
        "model": model_key,
        "device": str(getattr(engine, "device", "cpu")),
        "resolved_image": resolved,
    }
    return (
        format_verdict(
            label,
            score_fake,
            model_label,
            detail=detail,
        ),
        detail,
    )


def analyse_for_ui(
    text: str | None,
    image: Any,
    model_key: str,
    image_url: str | None = None,
) -> tuple[str, Any]:
    """Gradio click handler returning the verdict and a visible image preview.

    Wraps :func:`analyse`. When an image was resolved (upload or URL), it is
    returned so the Image component shows what was analysed.

    Args:
        text: Raw post text (may be ``None``).
        image: The uploaded PIL image, or ``None``.
        model_key: A key in :data:`MODEL_CATALOG`.
        image_url: Optional image URL used when no upload is present.

    Returns:
        ``(verdict_markdown, image_or_update)``.
    """
    verdict, detail = analyse(text, image, model_key, image_url=image_url)
    preview = detail.get("resolved_image")
    if preview is not None:
        return verdict, preview
    return verdict, gr.update()


# Fixed preview height so example / URL loads do not resize the layout.
IMAGE_PREVIEW_HEIGHT = 320


@dataclass
class DemoComponents:
    """References to the interactive components, shared between builders and wiring."""

    model_in: gr.Dropdown
    model_info: gr.Markdown
    input_tabs: gr.Tabs
    text_tab: gr.Tab
    image_tab: gr.Tab
    text_in: gr.Textbox
    clear_text_btn: gr.Button
    image_in: gr.Image
    image_url_in: gr.Textbox
    load_url_btn: gr.Button
    example_btns: dict[str, gr.Button]
    example_tabs: gr.Tabs
    example_info: gr.Markdown
    analyse_btn: gr.Button
    reset_btn: gr.Button
    verdict_out: gr.Markdown


_RESET_ICON = _UI_DIR / "assets" / "reset.svg"
_STYLESHEET_PATH = _UI_DIR / "stylesheet.css"

# BEM class hooks — block name matches project title (see ui/stylesheet.css)
_BEM_BLOCK = "multimodal-fake-news"
_BEM_RESET = f"{_BEM_BLOCK}__reset"
_BEM_BTN = f"{_BEM_BLOCK}__btn"
_BEM_DIVIDER = f"{_BEM_BLOCK}__divider"
_BEM_APP_BAR = f"{_BEM_BLOCK}__app-bar"


_UI_THEME = gr.themes.Origin()


def _launch_styling_kwargs() -> dict[str, object]:
    """App-level theme/CSS for ``Blocks.launch()`` (Gradio 6+)."""
    return {
        "theme": _UI_THEME,
        "css_paths": [_STYLESHEET_PATH],
    }


def _section_divider() -> gr.HTML:
    """Render a horizontal rule between major UI sections."""
    return gr.HTML(f'<hr class="{_BEM_DIVIDER}" />', padding=False)


def _build_header() -> gr.Button:
    """Render the title and top Reset control.

    Returns:
        The Reset button (wired later).
    """
    with gr.Row(elem_classes=[_BEM_APP_BAR]):
        gr.Markdown("# Multimodal Fake News Detection on Social Media")
        reset_btn = gr.Button(
            "Reset",
            icon=str(_RESET_ICON) if _RESET_ICON.is_file() else None,
            variant="primary",
            size="sm",
            elem_classes=[_BEM_RESET],
        )
    return reset_btn


def _build_model_selector() -> tuple[gr.Dropdown, gr.Markdown]:
    """Build the model section: heading, dropdown, and helper text.

    Returns:
        The ``(model dropdown, model info markdown)`` components.
    """
    gr.Markdown("## Select Model")
    model_in = gr.Dropdown(label="Model", choices=MODEL_CHOICES, value=DEFAULT_MODEL)
    model_info = gr.Markdown(value=model_info_markdown(DEFAULT_MODEL))
    return model_in, model_info


def _build_inputs() -> tuple[
    gr.Tabs,
    gr.Tab,
    gr.Tab,
    gr.Textbox,
    gr.Button,
    gr.Image,
    gr.Textbox,
    gr.Button,
]:
    """Build the inputs section as Text / Image tabs.

    Returns:
        ``(tabs, text tab, image tab, text input, clear-text button, image
        input, image URL, load-URL button)``.
    """
    gr.Markdown("## View Input")
    with gr.Tabs(selected="text") as input_tabs:
        with gr.Tab("Text", id="text") as text_tab:
            text_in = gr.Textbox(
                label="Post text",
                placeholder="News-related headline or social post…",
                lines=6,
            )
            clear_text_btn = gr.Button(
                "Clear text",
                size="sm",
                elem_classes=[_BEM_BTN],
            )
        with gr.Tab("Image", id="image") as image_tab:
            image_in = gr.Image(
                label="Image",
                type="pil",
                sources=["upload", "clipboard"],
                height=IMAGE_PREVIEW_HEIGHT,
            )
            with gr.Row():
                image_url_in = gr.Textbox(
                    label="Or image URL",
                    placeholder="https://…",
                    info="Paste a URL, then Load (or Enter). Works with direct image links and X photo pages.",
                    lines=1,
                    max_lines=1,
                    scale=4,
                )
                load_url_btn = gr.Button(
                    "Load",
                    size="sm",
                    scale=1,
                    elem_classes=[_BEM_BTN],
                )
    return (
        input_tabs,
        text_tab,
        image_tab,
        text_in,
        clear_text_btn,
        image_in,
        image_url_in,
        load_url_btn,
    )


def _build_examples() -> tuple[gr.Tabs, dict[str, gr.Button]]:
    """Build example buttons under Phase 1 / Phase 2 tabs.

    Returns:
        ``(example tabs, map of example key → button)``.
    """
    gr.Markdown("## Load Examples")
    example_btns: dict[str, gr.Button] = {}
    with gr.Tabs(selected="phase1") as example_tabs:
        with gr.Tab("Phase 1", id="phase1"):
            with gr.Row():
                for key, label in EXAMPLE_BUTTONS_PHASE1:
                    example_btns[key] = gr.Button(
                        label, size="sm", elem_classes=[_BEM_BTN]
                    )
        with gr.Tab("Phase 2", id="phase2"):
            with gr.Row():
                for key, label in EXAMPLE_BUTTONS_PHASE2:
                    example_btns[key] = gr.Button(
                        label, size="sm", elem_classes=[_BEM_BTN]
                    )
    return example_tabs, example_btns


def _build_analyse() -> tuple[gr.Markdown, gr.Button, gr.Markdown]:
    """Build example source info, Analyse button, and verdict output.

    Returns:
        The ``(example info, analyse button, verdict)`` components.
    """
    example_info = gr.Markdown(value="")
    analyse_btn = gr.Button(
        "Analyse",
        variant="primary",
        size="sm",
        elem_classes=[_BEM_BTN],
    )
    verdict_out = gr.Markdown(value="")
    return example_info, analyse_btn, verdict_out


def _wire_events(
    c: DemoComponents,
    demo: gr.Blocks,
    demo_examples: dict[str, tuple[str, Image.Image | None]],
) -> None:
    """Connect component events to their handler functions.

    Args:
        c: The demo's component references.
        demo: The enclosing Blocks app (for the initial ``load`` event).
        demo_examples: Preloaded example content for the buttons.
    """
    # Refresh the model info and input-tab visibility both on change and on first load.
    for trigger in (c.model_in.change, demo.load):
        trigger(fn=model_info_markdown, inputs=[c.model_in], outputs=[c.model_info])
        trigger(
            fn=input_visibility_for_model,
            inputs=[c.model_in],
            outputs=[c.text_tab, c.image_tab, c.input_tabs],
        )

    example_outputs = [
        c.text_in,
        c.image_in,
        c.image_url_in,
        c.example_info,
        c.verdict_out,
    ]
    for key, btn in c.example_btns.items():
        btn.click(
            fn=lambda k=key: on_example_selected(k, demo_examples),
            outputs=example_outputs,
        )

    c.clear_text_btn.click(fn=clear_text_input, outputs=[c.text_in])
    for load_trigger in (c.load_url_btn.click, c.image_url_in.submit):
        load_trigger(
            fn=load_image_url_for_ui,
            inputs=[c.image_url_in],
            outputs=[c.image_in, c.verdict_out, c.input_tabs],
        )
    c.analyse_btn.click(
        fn=analyse_for_ui,
        inputs=[c.text_in, c.image_in, c.model_in, c.image_url_in],
        outputs=[c.verdict_out, c.image_in],
    )
    c.reset_btn.click(
        fn=reset_demo,
        outputs=[
            c.model_in,
            c.model_info,
            c.text_in,
            c.image_in,
            c.image_url_in,
            c.example_info,
            c.verdict_out,
            c.text_tab,
            c.image_tab,
            c.input_tabs,
            c.example_tabs,
        ],
    )


def build_demo() -> gr.Blocks:
    """Assemble the Gradio Blocks app from the section builders and wire its events.

    Returns:
        The assembled :class:`gradio.Blocks` demo, ready to ``launch()``.
    """
    demo_examples = load_demo_examples(project_root())

    with gr.Blocks(
        title="Multimodal Fake News Detection on Social Media",
    ) as demo:
        reset_btn = _build_header()
        _section_divider()
        model_in, model_info = _build_model_selector()
        _section_divider()
        example_tabs, example_btns = _build_examples()
        _section_divider()
        (
            input_tabs,
            text_tab,
            image_tab,
            text_in,
            clear_text_btn,
            image_in,
            image_url_in,
            load_url_btn,
        ) = _build_inputs()
        _section_divider()
        example_info, analyse_btn, verdict_out = _build_analyse()

        components = DemoComponents(
            model_in=model_in,
            model_info=model_info,
            input_tabs=input_tabs,
            text_tab=text_tab,
            image_tab=image_tab,
            text_in=text_in,
            clear_text_btn=clear_text_btn,
            image_in=image_in,
            image_url_in=image_url_in,
            load_url_btn=load_url_btn,
            example_btns=example_btns,
            example_tabs=example_tabs,
            example_info=example_info,
            analyse_btn=analyse_btn,
            reset_btn=reset_btn,
            verdict_out=verdict_out,
        )
        _wire_events(components, demo, demo_examples)

    return demo


def parse_args() -> argparse.Namespace:
    """Parse the command-line arguments for launching the app.

    Returns:
        The parsed namespace with ``share``, ``host``, and ``port``.
    """
    parser = argparse.ArgumentParser(description="Multimodal fake news Gradio PoC")
    parser.add_argument(
        "--share",
        action="store_true",
        help="Create a temporary public gradio.live URL (typically ~1 week; good for demos)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Server bind address")
    parser.add_argument("--port", type=int, default=7860, help="Server port")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app = build_demo()
    if args.share:
        print("Public share link will appear below (temporary gradio.live URL; ~1 week).")
    app.launch(
        share=args.share,
        server_name=args.host,
        server_port=args.port,
        **_launch_styling_kwargs(),
    )
