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
  LAN:       .venv/bin/python ui/gradio-ui.py --host 0.0.0.0 --port 7860
  HF Space:  see ui/README.md
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Any

_UI_DIR = Path(__file__).resolve().parent
if str(_UI_DIR) not in sys.path:
    sys.path.insert(0, str(_UI_DIR))

import gradio as gr

from inference import TFIDF_PIPELINE_NAME, get_engine

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
    """Compact label for the dropdown (paradigm + architecture family)."""
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
    """Load post text and local image path for one cohort row."""
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
    """Preload real and fake cohort posts (content only)."""
    return {
        "real": load_cohort_example(root, EXAMPLE_REAL_ID),
        "fake": load_cohort_example(root, EXAMPLE_FAKE_ID),
    }


def apply_demo_example(
    kind: str,
    examples: dict[str, tuple[str, str | None]],
) -> tuple[str, str | None]:
    """Return text and image path for the chosen cohort example."""
    return examples.get(kind, ("", None))


def clear_text_input() -> str:
    return ""


DISCLAIMER = (
    "*Model-based estimate for demonstration — not a definitive truth claim.*"
)


def project_root() -> Path:
    """Repository root (parent of ui/)."""
    return Path(__file__).resolve().parent.parent


def runs_dir() -> Path:
    return project_root() / "runs"


def artefacts_present(model_key: str) -> bool:
    run_id = RUN_DIRS[model_key]
    run_path = runs_dir() / run_id
    if not run_path.is_dir():
        return False
    if model_key == "text_distilbert":
        return (run_path / "model" / "config.json").is_file()
    if model_key == "text_tfidf":
        return (run_path / TFIDF_PIPELINE_NAME).is_file()
    if model_key == "image_resnet18":
        return any(run_path.glob("resnet18*.pt"))
    if model_key == "fusion_late":
        return (run_path / "late_fusion_combiner.pkl").is_file()
    if model_key == "fusion_early":
        return (run_path / "early_fusion_head.pt").is_file()
    if model_key == "fusion_attention":
        return (run_path / "attention_fusion_head.pt").is_file()
    return (run_path / "metrics.json").is_file()


def validate_inputs(
    text: str | None,
    image: Any,
    model_key: str,
) -> str | None:
    """Return an error message, or None if inputs are acceptable."""
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
    """Helper text under the model selector."""
    c = MODEL_CATALOG.get(model_key, {})
    if not c:
        return ""
    return (
        f"**{c['modality']}** — {c['paradigm']}  \n"
        f"*{c['architecture']}*  \n"
        f"**Inputs:** {c['inputs']}"
    )


def model_display_name(model_key: str) -> str:
    c = MODEL_CATALOG.get(model_key, {})
    if not c:
        return model_key
    return f"{c['modality']} — {c['name']} ({c['paradigm']})"


def input_visibility_for_model(
    model_key: str,
):
    """Show text and/or image inputs depending on the selected model."""
    show_text = model_key in TEXT_ONLY or model_key in FUSION
    show_image = model_key in IMAGE_ONLY or model_key in FUSION
    return (
        gr.update(visible=show_text),
        gr.update(visible=show_text),
        gr.update(visible=show_image),
    )


def format_verdict(label: str, score_fake: float, model_label: str) -> str:
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
    """Delegate to lazy-loaded checkpoints under runs/."""
    return get_engine(project_root()).predict(text, image, model_key)


def analyse(
    text: str | None,
    image: Any,
    model_key: str,
) -> tuple[str, dict[str, Any]]:
    """Gradio handler: validate, predict, return markdown + detail dict."""
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
    """Gradio handler: verdict only (no raw detail JSON in the UI)."""
    verdict, _detail = analyse(text, image, model_key)
    return verdict


def build_demo() -> gr.Blocks:
    demo_examples = load_demo_examples(project_root())

    with gr.Blocks(
        title="Multimodal Fake News Detection on Social Media",
        theme=gr.themes.Origin(),
    ) as demo:
        gr.Markdown(
            "# Multimodal Fake News Detection on Social Media\n"
            "Choose a **model** first — only the inputs that model needs are shown."
        )
        gr.Markdown(SCOPE_NOTICE)

        model_in = gr.Dropdown(
            label="Model",
            choices=MODEL_CHOICES,
            value=DEFAULT_MODEL,
        )
        model_info = gr.Markdown(value=model_info_markdown(DEFAULT_MODEL))

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

        with gr.Row():
            load_real_btn = gr.Button("Load real example", size="sm")
            load_fake_btn = gr.Button("Load fake example", size="sm")

        analyse_btn = gr.Button("Analyse", variant="primary")

        verdict_out = gr.Markdown(label="Result")

        model_in.change(
            fn=model_info_markdown,
            inputs=[model_in],
            outputs=[model_info],
        )
        model_in.change(
            fn=input_visibility_for_model,
            inputs=[model_in],
            outputs=[text_in, clear_text_btn, image_in],
        )
        demo.load(
            fn=model_info_markdown,
            inputs=[model_in],
            outputs=[model_info],
        )
        demo.load(
            fn=input_visibility_for_model,
            inputs=[model_in],
            outputs=[text_in, clear_text_btn, image_in],
        )

        load_real_btn.click(
            fn=lambda: apply_demo_example("real", demo_examples),
            outputs=[text_in, image_in],
        )
        load_fake_btn.click(
            fn=lambda: apply_demo_example("fake", demo_examples),
            outputs=[text_in, image_in],
        )
        clear_text_btn.click(fn=clear_text_input, outputs=[text_in])

        analyse_btn.click(
            fn=analyse_for_ui,
            inputs=[text_in, image_in, model_in],
            outputs=[verdict_out],
        )

    return demo


def parse_args() -> argparse.Namespace:
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
