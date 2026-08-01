# Gradio PoC UI

Local demo that scores a single news-like social post (text and/or image) as likely fake or likely real using the project’s trained models.

---

## Prerequisites

| Need | Notes |
|------|--------|
| This repository | `git clone https://github.com/mpconsultsai/msc-project-csc-40098.git` then work from that folder. |
| Python 3 | With a virtual environment at `.venv` (see root [README](../README.md)). |
| Model files | Copy into `ui/models/` (not in git — see [Model checkpoints](#model-checkpoints-uimodels) below). |
| Cohort data (optional) | Needed only for **Phase 1** Examples: `data/fake_news_final_*.tsv` and cohort images under `data/`. **Phase 2** Examples use files under `ui/assets/examples/`. |

If a model’s files are missing, the app still starts and shows an **Artefacts missing** message instead of a prediction.

---

## Run locally (step by step)

### 1. Clone the repository and open a terminal there

```bash
git clone https://github.com/mpconsultsai/msc-project-csc-40098.git
cd msc-project-csc-40098
```

(Skip the clone if you already have the project; still `cd` into its root.)

### 2. Create / activate the virtual environment (if needed)

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
```

### 3. Install UI dependencies

```bash
pip install -r ui/requirements.txt
```

Pinned for reproducibility: **Gradio 6.20.0** (see `ui/requirements.txt` for the full PoC stack).

(Or: `.venv/bin/pip install -r ui/requirements.txt` without activating.)

### 4. Copy the trained model files

Place checkpoints in `ui/models/` as listed in [Model checkpoints](#model-checkpoints-uimodels). Typical source: Colab `My Drive/runs/`.

At minimum for a full demo you need DistilBERT’s `model/model.safetensors` (~255 MB) plus the other listed files. Without DistilBERT weights, TF–IDF and ResNet can still run if their files are present; fusion models will not.

### 5. Start the app

```bash
.venv/bin/python ui/gradio-ui.py
```

### 6. Open the UI in a browser

Go to [http://127.0.0.1:7860](http://127.0.0.1:7860).

Stop the server with `Ctrl+C` in the terminal.

---

## Using the app (short guide)

1. Under **Select Model**, choose a model (text-only, image-only, or fusion). Only the **View Input** tabs that model needs are shown.
2. Optionally open **Load Examples** → **Phase 1** or **Phase 2** and click a button to load a prepared case (a source note appears above **Analyse**). Or enter your own text and/or image under **View Input**.
3. On the **Image** tab you can paste an image URL and click **Load** (direct image links and X photo pages).
4. Click **Analyse** to see the verdict and P(fake) below the button.
5. **Reset** (top right) clears inputs and returns to the default model.

The first prediction for each model in a session is slower (lazy load); later calls are usually faster.

---

## Model checkpoints (`ui/models/`)

This folder is **gitignored**. After training in Colab, copy artefacts from `My Drive/runs/` into `ui/models/`:

```
ui/models/
├── tfidf_pipeline.joblib          ← runs/text_tfidf_baseline/
├── model/                         ← runs/text_distilbert_baseline/model/
│   ├── config.json, tokenizer.*
│   └── model.safetensors           ← ~255 MB
├── resnet18_state.pt              ← runs/image_resnet18_baseline/
├── late_fusion_combiner.pkl       ← runs/fusion_late_logistic/
├── early_fusion_head.pt           ← runs/fusion_early_concat/
└── attention_fusion_head.pt       ← runs/fusion_attention/
```

| Model in the UI | Files under `ui/models/` |
|-----------------|---------------------------|
| TF–IDF + logistic | `tfidf_pipeline.joblib` |
| DistilBERT | `model/` (including `model.safetensors`) |
| ResNet-18 | `resnet18_state.pt` |
| Late fusion | `late_fusion_combiner.pkl` + DistilBERT `model/` + `resnet18_state.pt` |
| Early fusion | `early_fusion_head.pt` + DistilBERT `model/` + `resnet18_state.pt` |
| Attention fusion | `attention_fusion_head.pt` + DistilBERT `model/` + `resnet18_state.pt` |

Fusion models need the unimodal files (`model/`, `resnet18_state.pt`) as well. Without `model.safetensors`, DistilBERT and all fusion models will not run (TF–IDF and ResNet still work once their files are present).

Inference code (`ui/inference.py`) reuses the same fusion helpers as training, so scores match the locked experiments when these artefacts are present.
