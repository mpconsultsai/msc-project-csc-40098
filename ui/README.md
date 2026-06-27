# Gradio PoC UI

## Run locally

```bash
cd "/path/to/MSC Project"
.venv/bin/pip install -r ui/requirements.txt
.venv/bin/python ui/gradio-ui.py
```

Open `http://127.0.0.1:7860`.

The app uses Gradio's **Origin** theme (fixed; no theme switcher).

Inputs start **empty**. Use **Load real example** or **Load fake example** to fill post text and image from the frozen cohort (`data/fake_news_final_*.tsv`). Run from the project root so `data/` and `data/processed/images/` are available.

## Deploy options

### 1. Temporary public link (fastest)

Gradio hosts a 72-hour tunnel — no account required:

```bash
.venv/bin/python ui/gradio-ui.py --share
```

Copy the `https://….gradio.live` URL from the terminal for supervisors or thesis demo videos.

### 2. Same Wi‑Fi / LAN

```bash
.venv/bin/python ui/gradio-ui.py --host 0.0.0.0 --port 7860
```

Use your machine’s LAN IP on another device, e.g. `http://192.168.1.x:7860`.

### 3. Hugging Face Spaces (persistent URL)

1. Create a new **Space** on [huggingface.co/spaces](https://huggingface.co/spaces) (SDK: Gradio).
2. Upload `ui/gradio-ui.py` as `app.py` (or symlink), plus `ui/requirements.txt` as `requirements.txt`.
3. Add model files via **Git LFS** or load from Hub at runtime (large `runs/` checkpoints usually stay local for a PoC; document that the Space is UI-only until inference is wired).
4. For a full Space in this repo, duplicate the app entrypoint and pin deps in the Space root.

Inference is not wired yet; a public Space will show the UI and validation messages until `run_inference()` is implemented.

### 4. Thesis / poster

Screenshot the local or `--share` UI for Chapter 7 (PoC testing). No deployment required.

## Wiring models (how it works)

```
Gradio (gradio-ui.py)
    → analyse()
    → ui/inference.py  InferenceEngine.predict()
    → loads from runs/<run_id>/
```

| UI model key | Artefacts needed | Inference path |
|--------------|------------------|----------------|
| `text_tfidf` | `runs/text_tfidf_baseline/tfidf_pipeline.joblib` | sklearn `predict_proba` |
| `text_distilbert` | `runs/text_distilbert_baseline/model/` | Hugging Face tokenizer + model |
| `image_resnet18` | `runs/image_resnet18_baseline/resnet18_state.pt` | torchvision ResNet-18 |
| `fusion_late` | `late_fusion_combiner.pkl` + unimodal dirs above | DistilBERT + ResNet scores → logistic combiner |
| `fusion_early` | `early_fusion_head.pt` + unimodal dirs | Frozen encoders → concat → linear head |
| `fusion_attention` | `attention_fusion_head.pt` + unimodal dirs | Frozen encoders → attention head |

Models are **lazy-loaded** once per app session (cached in memory). First prediction may be slower.

**Setup:** copy the full `runs/` folder from Colab/Drive to the project root. Re-run the TF-IDF notebook save cell once so `tfidf_pipeline.joblib` exists.

**Code:** `ui/inference.py` reuses `training/fusion_*.py` and `fusion_common.py` — same logic as the fusion notebook, adapted for a single text + PIL image.
