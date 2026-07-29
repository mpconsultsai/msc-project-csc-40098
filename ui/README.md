# Gradio PoC UI

Local demo that scores a single news-like social post (text and/or image) as likely fake or likely real using the project’s trained models.

---

## Prerequisites

| Need | Notes |
|------|--------|
| This repository | `git clone https://github.com/mpconsultsai/msc-project-csc-40098.git` then work from that folder. |
| Python 3 | With a virtual environment at `.venv` (see root [README](../README.md)). |
| Model files | Copy into `ui/models/` (not in git — see [models/README.md](models/README.md)). |
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

Place checkpoints in `ui/models/` as listed in [models/README.md](models/README.md). Typical source: Colab `My Drive/runs/`.

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

1. Choose a **Model** (text-only, image-only, or fusion). Only the input tabs that model needs are shown.
2. Optionally open **Examples** → **Phase 1** or **Phase 2** and click a button to load a prepared case (source note appears under **Result**). Or type/paste your own post text and/or upload an image.
3. On the **Image** tab you can also paste an image URL and click **Load** (direct image links and X photo pages).
4. Click **Analyse** and read the verdict, fake-probability, and latency under **Result**.
5. **Reset** clears inputs and returns to the default model.

The first prediction for each model in a session is slower (lazy load); later calls are usually faster.

---

## Which files each model uses

| Model in the UI | Files under `ui/models/` |
|-----------------|---------------------------|
| TF–IDF + logistic | `tfidf_pipeline.joblib` |
| DistilBERT | `model/` (including `model.safetensors`) |
| ResNet-18 | `resnet18_state.pt` |
| Late fusion | `late_fusion_combiner.pkl` + DistilBERT `model/` + `resnet18_state.pt` |
| Early fusion | `early_fusion_head.pt` + DistilBERT `model/` + `resnet18_state.pt` |
| Attention fusion | `attention_fusion_head.pt` + DistilBERT `model/` + `resnet18_state.pt` |

Inference code (`ui/inference.py`) reuses the same fusion helpers as training, so scores match the locked experiments when these artefacts are present.

---

## Demo video (thesis)

For the CSC-40098 walkthrough, record a **2--4 minute screen capture** on Mac (QuickTime or **⌘⇧5**) while using the app at http://127.0.0.1:7860.
Save as `documents/demo/poc-walkthrough.mp4` and submit with the thesis bundle (see [documents/demo/README.md](../documents/demo/README.md) for a suggested script).

---

## Sharing and hosting (not required)

Do **not** rely on temporary Gradio `--share` links (`*.gradio.live`): they expire and need the host machine left running. Persistent cloud hosting (e.g. Hugging Face Spaces) is out of scope for this deliverable.
