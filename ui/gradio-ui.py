#!/usr/bin/env python3
"""
Gradio proof-of-concept — multimodal fake news check.

Run from project root (use the project virtualenv):

    .venv/bin/pip install -r ui/requirements.txt
    .venv/bin/python ui/gradio-ui.py

If you see ``ModuleNotFoundError: No module named 'gradio'``, you are not using
``.venv`` — activate it (``source .venv/bin/activate``) or use the paths above.

Inference is in ``ui/inference.py`` (loads ``runs/`` checkpoints).
Copy ``runs/`` from Colab to the project root before using all models.

Deploy:
  Local:     .venv/bin/python ui/gradio-ui.py
  Public:    .venv/bin/python ui/gradio-ui.py --share
  HF Space:  see ui/README.md
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import gradio as gr

# ``inference`` lives alongside this file; add ui/ to sys.path before importing it.
_UI_DIR = Path(__file__).resolve().parent
if str(_UI_DIR) not in sys.path:
    sys.path.insert(0, str(_UI_DIR))

from inference import TFIDF_PIPELINE_NAME, get_engine  # noqa: E402
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

RUN_DIRS: dict[str, str] = {
    "text_tfidf": "text_tfidf_baseline",
    "text_distilbert": "text_distilbert_baseline",
    "image_resnet18": "image_resnet18_baseline",
    "fusion_late": "fusion_late_logistic",
    "fusion_early": "fusion_early_concat",
    "fusion_attention": "fusion_attention",
}

# Per-model predicate checking that the required checkpoint exists in its run dir.
ARTEFACT_CHECKS: dict[str, Callable[[Path], bool]] = {
    "text_tfidf": lambda run: (run / TFIDF_PIPELINE_NAME).is_file(),
    "text_distilbert": lambda run: (run / "model" / "config.json").is_file(),
    "image_resnet18": lambda run: any(run.glob("resnet18*.pt")),
    "fusion_late": lambda run: (run / "late_fusion_combiner.pkl").is_file(),
    "fusion_early": lambda run: (run / "early_fusion_head.pt").is_file(),
    "fusion_attention": lambda run: (run / "attention_fusion_head.pt").is_file(),
}

TEXT_TSV = "data/fake_news_final_text.tsv"
IMAGE_TSV = "data/fake_news_final_image.tsv"

# Frozen cohort rows (text + image); labels are not shown in the UI.
EXAMPLE_FAKE_ID = "fnn:politifact:fake:politifact13468"
EXAMPLE_REAL_ID = "fnn:gossipcop:real:gossipcop-954027"

SCOPE_NOTICE = (
    "Please use **news-related social post text** (headline or short post), "
    "matching the style of the **FakeNewsNet** and **Fakeddit** training data. "
    "Add an **image** when using image or fusion models. "
    "Or use **Load real example** / **Load fake example** to fill the inputs from the cohort."
)


def load_cohort_example(
    root: Path,
    sample_id: str,
) -> tuple[str, str | None]:
    """Load the post text and local image path for one frozen cohort row.

    Args:
        root: Repository root containing the cohort TSVs and images.
        sample_id: The ``sample_id`` to look up in both cohort exports.

    Returns:
        A ``(text, image_path)`` pair. ``text`` is empty and ``image_path`` is
        ``None`` if the TSVs or the image file are missing.
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

    image_abs: str | None = None
    if image_rel:
        candidate = root / image_rel
        if candidate.is_file():
            image_abs = str(candidate)

    return text_value, image_abs


def load_demo_examples(root: Path) -> dict[str, tuple[str, str | None]]:
    """Preload the real and fake demo posts (content only; labels are not shown).

    Args:
        root: Repository root containing the cohort TSVs and images.

    Returns:
        A dict keyed by ``"real"`` / ``"fake"``, each mapping to a
        ``(text, image_path)`` pair.
    """
    return {
        "real": load_cohort_example(root, EXAMPLE_REAL_ID),
        "fake": load_cohort_example(root, EXAMPLE_FAKE_ID),
    }


def apply_demo_example(
    kind: str,
    examples: dict[str, tuple[str, str | None]],
) -> tuple[str, str | None]:
    """Return the text and image path for the chosen demo example.

    Args:
        kind: ``"real"`` or ``"fake"``.
        examples: The preloaded examples from :func:`load_demo_examples`.

    Returns:
        The ``(text, image_path)`` pair, or ``("", None)`` if ``kind`` is unknown.
    """
    return examples.get(kind, ("", None))


def clear_text_input() -> str:
    """Return an empty string to clear the text input."""
    return ""


DISCLAIMER = (
    "*Model-based estimate for demonstration — not a definitive truth claim.*"
)


def project_root() -> Path:
    """Return the repository root (the parent of ``ui/``)."""
    return Path(__file__).resolve().parent.parent


def runs_dir() -> Path:
    """Return the ``runs/`` directory at the project root."""
    return project_root() / "runs"


def artefacts_present(model_key: str) -> bool:
    """Check whether the trained artefacts for a model exist under ``runs/``.

    Args:
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        ``True`` if the run directory and the model-specific checkpoint file(s)
        are present, otherwise ``False``.
    """
    run_path = runs_dir() / RUN_DIRS[model_key]
    if not run_path.is_dir():
        return False
    check = ARTEFACT_CHECKS.get(model_key, lambda run: (run / "metrics.json").is_file())
    return check(run_path)


def validate_inputs(
    text: str | None,
    image: Any,
    model_key: str,
) -> str | None:
    """Validate the inputs required by the selected model.

    Args:
        text: Raw post text (may be ``None``).
        image: The uploaded image, or ``None`` if none was provided.
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
        return "Please upload an image for this model."
    if model_key in FUSION:
        if not text_clean:
            return "Please enter post text for multimodal fusion."
        if not has_image:
            return "Please upload an image for multimodal fusion."
    return None


def model_info_markdown(model_key: str) -> str:
    """Render the descriptive helper text shown under the model selector.

    Args:
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        A Markdown blurb (modality, paradigm, architecture, inputs), or an empty
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


def input_visibility_for_model(
    model_key: str,
) -> tuple[dict, dict, dict]:
    """Compute the visibility of the text and image inputs for a model.

    Args:
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        Three Gradio update objects for ``(text input, clear-text button,
        image input)`` toggling visibility to match the model's modality.
    """
    show_text = model_key in TEXT_ONLY or model_key in FUSION
    show_image = model_key in IMAGE_ONLY or model_key in FUSION
    return (
        gr.update(visible=show_text),
        gr.update(visible=show_text),
        gr.update(visible=show_image),
    )


def format_verdict(label: str, score_fake: float, model_label: str) -> str:
    """Format the prediction as the Markdown verdict shown in the UI.

    Args:
        label: The predicted label (e.g. ``"Likely fake"``).
        score_fake: ``P(fake)`` in ``[0, 1]``.
        model_label: The display name of the model used.

    Returns:
        A Markdown string with the estimate, score, model, and disclaimer.
    """
    return (
        f"### Estimate: **{label}**\n\n"
        f"- **P(fake):** {score_fake:.3f}\n"
        f"- **Model:** {model_label}\n\n"
        f"{DISCLAIMER}"
    )


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
    return get_engine(project_root()).predict(text, image, model_key)


def analyse(
    text: str | None,
    image: Any,
    model_key: str,
) -> tuple[str, dict[str, Any]]:
    """Validate inputs, run inference, and format the result for the UI.

    Args:
        text: Raw post text (may be ``None``).
        image: The uploaded PIL image, or ``None``.
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        A ``(markdown, detail)`` pair: the Markdown to display, and a detail dict
        with the raw result plus ``model`` and ``latency_ms`` (or an ``error``
        key when validation fails, artefacts are missing, or inference raises).
    """
    model_label = model_display_name(model_key)

    err = validate_inputs(text, image, model_key)
    if err:
        return f"### ⚠️ {err}", {"error": err, "model": model_key}

    if not artefacts_present(model_key):
        run_id = RUN_DIRS[model_key]
        msg = (
            f"### Artefacts missing\n\n"
            f"Expected trained outputs under `runs/{run_id}/` at the project root.\n\n"
            f"Copy `runs/` from Colab after training. "
            f"TF-IDF also needs `{TFIDF_PIPELINE_NAME}` (re-run TF-IDF save cell)."
        )
        return msg, {
            "error": "artefacts_missing",
            "expected_run": run_id,
            "model": model_key,
        }

    t0 = time.perf_counter()
    try:
        result = run_inference(text, image, model_key)
    except Exception as exc:
        return (
            f"### Runtime error\n\n`{type(exc).__name__}: {exc}`",
            {"error": "runtime", "model": model_key, "detail": str(exc)},
        )

    latency_ms = (time.perf_counter() - t0) * 1000
    label = result.get("label", "unknown")
    score_fake = float(result.get("score_fake", 0.0))
    detail = {**result, "model": model_key, "latency_ms": round(latency_ms, 2)}
    return format_verdict(label, score_fake, model_label), detail


def analyse_for_ui(
    text: str | None,
    image: Any,
    model_key: str,
) -> str:
    """Gradio click handler returning only the verdict Markdown.

    Wraps :func:`analyse` and discards the detail dict, since the UI shows the
    formatted verdict rather than the raw JSON.

    Args:
        text: Raw post text (may be ``None``).
        image: The uploaded PIL image, or ``None``.
        model_key: A key in :data:`MODEL_CATALOG`.

    Returns:
        The Markdown verdict string.
    """
    verdict, _detail = analyse(text, image, model_key)
    return verdict


@dataclass
class DemoComponents:
    """References to the interactive components, shared between builders and wiring."""

    model_in: gr.Dropdown
    model_info: gr.Markdown
    text_in: gr.Textbox
    clear_text_btn: gr.Button
    image_in: gr.Image
    load_real_btn: gr.Button
    load_fake_btn: gr.Button
    analyse_btn: gr.Button
    verdict_out: gr.Markdown


def _build_header() -> None:
    """Render the title and scope-notice Markdown (no interactive components)."""
    gr.Markdown(
        "# Multimodal Fake News Detection on Social Media\n"
        "Choose a **model** first — only the inputs that model needs are shown."
    )
    gr.Markdown(SCOPE_NOTICE)


def _build_model_selector() -> tuple[gr.Dropdown, gr.Markdown]:
    """Build the model dropdown and its descriptive helper text.

    Returns:
        The ``(model dropdown, model info markdown)`` components.
    """
    model_in = gr.Dropdown(label="Model", choices=MODEL_CHOICES, value=DEFAULT_MODEL)
    model_info = gr.Markdown(value=model_info_markdown(DEFAULT_MODEL))
    return model_in, model_info


def _build_inputs() -> tuple[gr.Textbox, gr.Button, gr.Image]:
    """Build the text and image input row.

    Returns:
        The ``(text input, clear-text button, image input)`` components.
    """
    with gr.Row():
        with gr.Column(scale=1):
            text_in = gr.Textbox(
                label="Post text",
                placeholder="News-related headline or social post…",
                lines=4,
                visible=True,
            )
            clear_text_btn = gr.Button("Clear text", size="sm", visible=True)
        image_in = gr.Image(
            label="Image",
            type="pil",
            sources=["upload", "clipboard"],
            visible=True,
        )
    return text_in, clear_text_btn, image_in


def _build_actions() -> tuple[gr.Button, gr.Button, gr.Button, gr.Markdown]:
    """Build the example-loader buttons, the analyse button, and the result area.

    Returns:
        The ``(load-real button, load-fake button, analyse button, verdict
        markdown)`` components.
    """
    with gr.Row():
        load_real_btn = gr.Button("Load real example", size="sm")
        load_fake_btn = gr.Button("Load fake example", size="sm")
    analyse_btn = gr.Button("Analyse", variant="primary")
    verdict_out = gr.Markdown(label="Result")
    return load_real_btn, load_fake_btn, analyse_btn, verdict_out


def _wire_events(
    c: DemoComponents,
    demo: gr.Blocks,
    demo_examples: dict[str, tuple[str, str | None]],
) -> None:
    """Connect component events to their handler functions.

    Args:
        c: The demo's component references.
        demo: The enclosing Blocks app (for the initial ``load`` event).
        demo_examples: Preloaded example content for the loader buttons.
    """
    # Refresh the model info and input visibility both on change and on first load.
    for trigger in (c.model_in.change, demo.load):
        trigger(fn=model_info_markdown, inputs=[c.model_in], outputs=[c.model_info])
        trigger(
            fn=input_visibility_for_model,
            inputs=[c.model_in],
            outputs=[c.text_in, c.clear_text_btn, c.image_in],
        )

    c.load_real_btn.click(
        fn=lambda: apply_demo_example("real", demo_examples),
        outputs=[c.text_in, c.image_in],
    )
    c.load_fake_btn.click(
        fn=lambda: apply_demo_example("fake", demo_examples),
        outputs=[c.text_in, c.image_in],
    )
    c.clear_text_btn.click(fn=clear_text_input, outputs=[c.text_in])
    c.analyse_btn.click(
        fn=analyse_for_ui,
        inputs=[c.text_in, c.image_in, c.model_in],
        outputs=[c.verdict_out],
    )


def build_demo() -> gr.Blocks:
    """Assemble the Gradio Blocks app from the section builders and wire its events.

    Returns:
        The assembled :class:`gradio.Blocks` demo, ready to ``launch()``.
    """
    demo_examples = load_demo_examples(project_root())

    with gr.Blocks(
        title="Multimodal Fake News Detection on Social Media",
        theme=gr.themes.Origin(),
    ) as demo:
        _build_header()
        model_in, model_info = _build_model_selector()
        text_in, clear_text_btn, image_in = _build_inputs()
        load_real_btn, load_fake_btn, analyse_btn, verdict_out = _build_actions()

        components = DemoComponents(
            model_in=model_in,
            model_info=model_info,
            text_in=text_in,
            clear_text_btn=clear_text_btn,
            image_in=image_in,
            load_real_btn=load_real_btn,
            load_fake_btn=load_fake_btn,
            analyse_btn=analyse_btn,
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
        help="Create a temporary public gradio.live URL (good for demos; link expires)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Server bind address")
    parser.add_argument("--port", type=int, default=7860, help="Server port")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app = build_demo()
    if args.share:
        print("Public share link will appear below (temporary gradio.live URL).")
    app.launch(
        share=args.share,
        server_name=args.host,
        server_port=args.port,
    )
