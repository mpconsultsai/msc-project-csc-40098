# Demo UI

This directory contains the project's **proof-of-concept demo UI**: a Gradio app
that takes a single social-media post (text and/or image) and returns a fake/real
verdict using the trained models. It runs locally and loads the checkpoints
produced by the training stage.

The models it serves are produced in [`training/README.md`](../training/README.md);
the worked examples are drawn from the pipeline's frozen cohort exports.

## Contents

- [What you need before you start](#1-what-you-need-before-you-start)
- [How to run locally](#2-how-to-run-locally)
- [Using the app](#3-using-the-app)
- [How inference is wired](#4-how-inference-is-wired)
- [Deployment options](#5-deployment-options)

---

## 1. What you need before you start

| Requirement | Detail |
|-------------|--------|
| **Python environment** | The project-root `.venv` with `ui/requirements.txt` installed (see [Step 2.1](#step-21--install-dependencies)). |
| **Trained artefacts** | A `runs/` folder copied from Colab/Drive to the project root. Each model needs the files listed in [Section 4](#4-how-inference-is-wired). |
| **Cohort data (optional)** | `data/fake_news_final_*.tsv` and `data/processed/images/`, used only by the *Load example* buttons. |

> Run all commands from the **project root** so `data/` and `runs/` resolve. If a model's artefacts are missing, the app still launches and shows an "Artefacts missing" message instead of a prediction.

## 2. How to run locally

### Step 2.1 — Install dependencies

This assumes the project-root `.venv` described in the [main README](../README.md).

```bash
cd "/path/to/MSC Project"
.venv/bin/pip install -r ui/requirements.txt
```

### Step 2.2 — Add the trained artefacts

Copy the full `runs/` folder from Colab/Drive to the project root. For the TF-IDF
model, re-run the TF-IDF notebook's save cell once so `tfidf_pipeline.joblib`
exists.

### Step 2.3 — Launch the app

```bash
.venv/bin/python ui/gradio-ui.py
```

Open `http://127.0.0.1:7860`.

## 3. Using the app

- The app uses Gradio's **Origin** theme (fixed; no theme switcher).
- Inputs start **empty**. Use **Load real example** or **Load fake example** to fill the post text and image from the frozen cohort (`data/fake_news_final_*.tsv`).
- Models are **lazy-loaded** once per app session and cached in memory, so the first prediction for a model is slower.

## 4. How inference is wired

A prediction flows through the app as follows:

```
Gradio (gradio-ui.py)
    → analyse()              # validates input, checks artefacts, times the call
    → run_inference()
    → ui/inference.py  InferenceEngine.predict()
    → loads from runs/<run_id>/
```

| UI model key | Artefacts needed | Inference path |
|--------------|------------------|----------------|
| `text_tfidf` | `runs/text_tfidf_baseline/tfidf_pipeline.joblib` | sklearn `predict_proba` |
| `text_distilbert` | `runs/text_distilbert_baseline/model/` | Hugging Face tokenizer + model |
| `image_resnet18` | `runs/image_resnet18_baseline/resnet18_state.pt` | torchvision ResNet-18 |
| `fusion_late` | `late_fusion_combiner.pkl` + the unimodal dirs above | DistilBERT + ResNet scores → logistic combiner |
| `fusion_early` | `early_fusion_head.pt` + the unimodal dirs | Frozen encoders → concat → linear head |
| `fusion_attention` | `attention_fusion_head.pt` + the unimodal dirs | Frozen encoders → attention head |

`ui/inference.py` reuses `training/fusion_*.py` and `fusion_common.py`, so the
prediction logic matches the fusion notebook, adapted for a single text + PIL
image. When the artefacts for the selected model are present, the app returns a
label, a fake-probability score, and the inference latency.

## 5. Deployment options

### 1. Temporary public link (fastest)

Gradio hosts a 72-hour tunnel — no account required:

```bash
.venv/bin/python ui/gradio-ui.py --share
```

Copy the `https://….gradio.live` URL from the terminal to share the running app.

### 2. Hugging Face Spaces (persistent URL)

1. Create a new **Space** on [huggingface.co/spaces](https://huggingface.co/spaces) (SDK: Gradio).
2. Upload `ui/gradio-ui.py` as `app.py` (or symlink), plus `ui/requirements.txt` as `requirements.txt`.
3. Add the model files via **Git LFS** or load them from the Hub at runtime. Without the `runs/` artefacts the Space shows the UI and an "Artefacts missing" message; include the checkpoints for live predictions.
4. For a full Space in this repo, duplicate the app entrypoint and pin the dependencies in the Space root.
