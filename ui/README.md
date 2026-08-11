# Gradio PoC UI

Local demo that scores a single news-like social post (text and/or image) as likely fake or likely real using the project’s trained models.

---

## Prerequisites

| Need | Notes |
|------|--------|
| This repository | Clone into `path/to/msc-project-csc-40098` (see step 1 below). |
| Python 3 | With a virtual environment at `.venv` (see root [README](../README.md)). |
| Model files | Unpack **`msc-poc-model-weights.zip`** into the repo (creates `ui/models/`), or copy checkpoints manually — see [Model checkpoints](#model-checkpoints-uimodels). Not stored in git. |
| Cohort data (optional) | Needed only for **Phase 1** Examples: `data/fake_news_final_*.tsv` and cohort images under `data/`. **Phase 2** Examples use files under `ui/assets/examples/`. |

If a model’s files are missing, the app still starts and shows an **Artefacts missing** message instead of a prediction.

---

## Run locally (step by step)

### 1. Clone the repository and open a terminal there

```bash
git clone https://github.com/mpconsultsai/msc-project-csc-40098.git path/to/msc-project-csc-40098
cd path/to/msc-project-csc-40098
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

### 4. Install model checkpoints

Checkpoints are **not in git**. Use either option below.

**Option A — submission zip (recommended for assessors).**  
If you have `msc-poc-model-weights.zip` (submitted with the assessment), unpack it from the **repository root** so `ui/models/` is created in place:

```bash
unzip msc-poc-model-weights.zip
```

The archive contains `ui/models/` with all six locked study models; no Colab training is required.

**Option B — copy from training runs.**  
After training in Colab, copy artefacts from `My Drive/runs/` into `ui/models/` as listed in [Model checkpoints](#model-checkpoints-uimodels).

At minimum for a full demo you need DistilBERT’s `model/model.safetensors` (~255 MB) plus the other listed files. Without DistilBERT weights, TF–IDF and ResNet can still run if their files are present; fusion models will not.

### 5. Start the app

```bash
.venv/bin/python ui/gradio-ui.py
```

### 6. Open the UI in a browser

Go to [http://127.0.0.1:7860](http://127.0.0.1:7860).

Stop the server with `Ctrl+C` in the terminal.

---

## Running the Gradio PoC

1. **Select Model** — text-only, image-only, or fusion (defaults to attention fusion when checkpoints are present). Only the **View Input** tabs that model needs are shown.
2. **Load Examples** → **Phase 1** (GossipCop real, PolitiFact fake) or **Phase 2** (Snopes viral claim, BBC Earth) — or enter your own text and/or image. A source note appears above **Analyse** when an example is loaded.
3. **Image** tab — upload a file or paste a URL and click **Load** (direct image links and X photo pages).
4. **Analyse** — verdict, P(fake), and for fusion models optional late-fusion scores or attention weights.
5. **Reset** (top right) — clear inputs and restore the default model.

The first prediction for each model in a session is slower (lazy-loaded checkpoints); later calls are usually faster.

---

## Model checkpoints (`ui/models/`)

This folder is **gitignored**. For assessment submission, frozen weights are provided as **`msc-poc-model-weights.zip`** — unpack from the repo root (see step 4 above). Alternatively, after training in Colab, copy artefacts from `My Drive/runs/` into `ui/models/`:

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
