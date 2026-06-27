# Training

This directory contains the model-training stage of the project. It trains and
evaluates the single-modality (text and image) baselines and the three
multimodal fusion methods (early, late, and attention-based) used to answer the
research questions.

**All training runs in Google Colab.** TF-IDF runs on a CPU runtime; the
DistilBERT, image, and fusion notebooks need a GPU runtime (a free T4 is
sufficient). Data and trained artefacts live in Google Drive so they persist
between sessions.

The data-preparation steps that produce the inputs for this stage are described
separately in [`pipeline/README.md`](../pipeline/README.md).

## Contents

- [What you need before you start](#1-what-you-need-before-you-start)
- [How to run training in Google Colab](#2-how-to-run-training-in-google-colab)
- [Notebooks and modules](#3-notebooks-and-modules)
- [Outputs](#4-outputs)
- [Fusion methods (RQ2 / RQ3)](#5-fusion-methods-rq2--rq3)

---

## 1. What you need before you start

| Requirement | Detail |
|-------------|--------|
| **Google account** | Required for Google Colab and Google Drive. |
| **Input data** | `fake_news_final_text.tsv` and `fake_news_final_image.tsv`, produced by step **12** of [`pipeline/README.md`](../pipeline/README.md). |
| **Images** | `images.zip` (~1.1–1.3 GB), produced by the pipeline. Only needed for the image and fusion notebooks. |

> The inputs are **frozen** at this stage. Do not edit anything under `pipeline/`
> from the training notebooks; treat the TSVs as read-only.

## 2. How to run training in Google Colab

The setup has two parts: a **one-time** upload of data and helper code to Google
Drive, and a short **per-session** setup that you run at the top of each
notebook. All notebooks share the same `colab_setup.py` helper, so this setup is
identical across notebooks.

### Step 2.1 — One-time: prepare Google Drive

Upload the project files to your Google Drive using exactly this layout. The
folder names and the `data/` and `training/` structure must match, because the
setup helper looks for these specific paths.

```
My Drive/
├── data/
│   ├── fake_news_final_text.tsv
│   ├── fake_news_final_image.tsv
│   └── images.zip
└── training/
    ├── colab_setup.py
    ├── cohort_text.py
    ├── cohort_image.py
    ├── cohort_multimodal.py
    ├── fusion_common.py
    ├── fusion_late.py
    ├── fusion_early.py
    └── fusion_attention.py
```

Copy every `.py` file from this `training/` directory into `My Drive/training/`.
Whenever you change one of these files, re-upload it so Colab uses the latest
version.

> A `My Drive/runs/` folder is **created automatically** the first time you run a
> notebook — you do not need to create it yourself. Trained checkpoints and
> metrics are saved there so they persist between sessions.

### Step 2.2 — Open a notebook and select the runtime

1. Open the desired notebook (see the [table in Section 3](#3-notebooks-and-modules)) in Google Colab.
2. For the GPU notebooks (DistilBERT, image, fusion), select a GPU runtime:
   **Runtime → Change runtime type → T4 GPU**. The TF-IDF notebook runs on the
   default CPU runtime.

### Step 2.3 — Run the per-session setup cells

Every notebook starts with the same two setup cells. Run them top to bottom at
the start of each session (and again after any Colab restart).

**Cell A — bootstrap (identical in every notebook).** Mounts Google Drive and
copies `My Drive/training/` into the Colab runtime:

```python
import shutil, sys
from pathlib import Path
from google.colab import drive

drive.mount("/content/drive")
PROJECT_ROOT = Path("/content/msc")
TRAINING_SRC = Path("/content/drive/MyDrive/training")
TRAINING = PROJECT_ROOT / "training"
if not TRAINING_SRC.is_dir():
    raise FileNotFoundError("Sync the repo's training/ folder to My Drive/training/")
if TRAINING.exists():
    shutil.rmtree(TRAINING)
shutil.copytree(TRAINING_SRC, TRAINING)
sys.path.insert(0, str(TRAINING))
```

**Cell B — project setup (per notebook).** Copies the required TSVs (and images,
where needed) into the runtime. Call `require_cuda()` first **only** for the GPU
notebooks:

```python
from colab_setup import require_cuda, setup_colab_project

require_cuda()  # GPU notebooks only; omit for the TF-IDF notebook
ctx = setup_colab_project(
    tsv_names=["fake_news_final_text.tsv"],  # see the table below
    need_images=False,                        # see the table below
)
PROJECT_ROOT = ctx.project_root
```

Use these arguments per notebook:

| Notebook | `tsv_names` | `need_images` | `require_cuda()` |
|----------|-------------|---------------|------------------|
| `training_text_tfidf.ipynb` | `["fake_news_final_text.tsv"]` | `False` | No (CPU) |
| `training_text_distilbert.ipynb` | `["fake_news_final_text.tsv"]` | `False` | Yes |
| `training_image_resnet.ipynb` | `["fake_news_final_image.tsv"]` | `True` | Yes |
| `training_fusion.ipynb` | `["fake_news_final_text.tsv", "fake_news_final_image.tsv"]` | `True` | Yes |

### Step 2.4 — Run the notebooks in order

The fusion notebook reuses the DistilBERT and ResNet checkpoints, so run the
single-modality notebooks first:

1. `training_text_tfidf.ipynb` *(text baseline, CPU)*
2. `training_text_distilbert.ipynb` *(text baseline, GPU)*
3. `training_image_resnet.ipynb` *(image baseline, GPU)*
4. `training_fusion.ipynb` *(early / late / attention fusion, GPU)*

The single-modality notebooks call `persist_run_to_drive()` after training, which
saves their results to `My Drive/runs/`. The fusion notebook calls
`sync_runs_from_drive()` during setup to pull those checkpoints back in. If you
run the fusion notebook before completing steps 2 and 3, it will report that the
required checkpoints are missing.

## 3. Notebooks and modules

**Notebooks (open these in Colab):**

| Notebook | Runtime | Purpose |
|----------|---------|---------|
| `training_text_tfidf.ipynb` | CPU | TF-IDF text baseline → `runs/text_tfidf_baseline/` (also writes `tfidf_pipeline.joblib` for the demo UI). |
| `training_text_distilbert.ipynb` | GPU | DistilBERT text baseline → `runs/text_distilbert_baseline/`. |
| `training_image_resnet.ipynb` | GPU | ResNet-18 image baseline → `runs/image_resnet18_baseline/`. |
| `training_fusion.ipynb` | GPU | RQ2 / RQ3: late, early, and attention fusion plus a summary table. |

**Supporting modules (synced to `My Drive/training/`; not run directly):**

| Module | Purpose |
|--------|---------|
| `colab_setup.py` | Shared Colab setup: GPU check, Drive/TSV copying, image unzip, run syncing. |
| `cohort_text.py`, `cohort_image.py`, `cohort_multimodal.py` | Shared data loading and `split_study`; `cohort_multimodal.py` joins modalities for fusion. |
| `fusion_common.py`, `fusion_late.py`, `fusion_early.py`, `fusion_attention.py` | Shared fusion utilities and one module per fusion method. |

**Repository-only files (not uploaded to Drive):**

| File | Purpose |
|------|---------|
| `requirements.txt` | Pinned dependency reference. The notebooks install these in Colab with `!pip install`; the file documents the exact versions. |
| `pyrightconfig.json`, `typings/` | Editor type-checking support only (e.g. a stub for `google.colab`). They have no effect on training. |

> **Optional — local editor environment.** Training does not need anything
> installed locally. If you want type-checking and autocompletion while editing
> the `.py` modules in your IDE, create a virtual environment from
> `requirements.txt` (the notebooks themselves still run in Colab):
>
> ```bash
> python3 -m venv .venv
> source .venv/bin/activate        # Windows: .venv\Scripts\activate
> pip install -r training/requirements.txt
> ```

## 4. Outputs

All results are written under `runs/`, with one subfolder per experiment:

| Run folder | Produced by |
|------------|-------------|
| `text_tfidf_baseline/` | `training_text_tfidf.ipynb` |
| `text_distilbert_baseline/` | `training_text_distilbert.ipynb` |
| `image_resnet18_baseline/` | `training_image_resnet.ipynb` |
| `fusion_late_logistic/` | `training_fusion.ipynb` (late) |
| `fusion_early_concat/` | `training_fusion.ipynb` (early) |
| `fusion_attention/` | `training_fusion.ipynb` (attention) |

Each fusion run writes `metrics.json` (macro-F1, per-class F1/recall, ROC-AUC,
average precision), `predictions_val.tsv`, `confusion_matrix.png`,
`roc_pr_curves.png` (ROC and precision–recall, with recall on the x-axis), and,
where applicable, a model checkpoint.

---

## 5. Fusion methods (RQ2 / RQ3)

All three fusion mechanisms live in `training_fusion.ipynb` and are trained on the
**same multimodal cohort** (45,868 train / 4,486 validation, after dropping rows
with missing images).

**Shared policy**

- **Primary metric:** macro-F1 on the pooled validation set.
- Train the fusion layer on the **train split only**; report on validation, with
  no hyperparameter tuning on validation.
- Reuse the same single-modality checkpoints: DistilBERT
  (`runs/text_distilbert_baseline/model/`) and ResNet-18
  (`runs/image_resnet18_baseline/resnet18_state.pt`).
- Encoders are **frozen** unless stated otherwise.
- Balanced class weights for all trainable heads/combiners; fixed
  hyperparameters; seed **42**.

| Module | Role |
|--------|------|
| `fusion_common.py` | Artefact checks, frozen-encoder loading, embedding extraction, metrics. |
| `fusion_late.py` | Late fusion (prediction scores → logistic regression). |
| `fusion_early.py` | Early fusion (concatenated embeddings → linear head). |
| `fusion_attention.py` | Attention fusion (softmax modality weighting). |

### Late fusion (`fusion_late.py`)

**Fusion point:** after each modality produces a final prediction score.

1. Run frozen DistilBERT → `score_text` = P(fake) from softmax.
2. Run frozen ResNet-18 → `score_image` = P(fake).
3. Fit an sklearn `LogisticRegression` on `[score_text, score_image]` (train split only).
4. Predict on validation; threshold at 0.5 for class labels.

**Why use it:** a simple, interpretable multimodal baseline, common in applied ML
when single-modality models already exist.

### Early fusion (`fusion_early.py`)

**Fusion point:** at the feature level, before the final classifier.

| Modality | Embedding source | Dim |
|----------|------------------|-----|
| Text | DistilBERT `[CLS]` token (`last_hidden_state[:, 0]`) | 768 |
| Image | ResNet-18 global average pool (the layer before `fc`) | 512 |

**Fusion:** `concat(text_emb, image_emb)` → a single linear layer → 2-class logits.

**Training:** only the linear head (8 epochs, AdamW `lr=1e-3`, batch 256, balanced
cross-entropy). Encoders are not updated.

**Why use it:** a standard early-fusion baseline that lets the head learn
cross-modal interactions in one step.

**Output:** `runs/fusion_early_concat/`.

### Attention fusion (`fusion_attention.py`)

**Fusion point:** at the feature level, with learned modality weighting.

1. Use the same frozen embeddings as early fusion (768 + 512).
2. Project each to **256-d** with a linear layer and `tanh` activation.
3. Stack modalities → **softmax attention** over {text, image} (weights sum to 1 per sample).
4. Take the weighted sum of the projected vectors → linear classifier → 2-class logits.

**Interpretability:** `mean_attn_text` / `mean_attn_image` in the metrics report
the average modality emphasis on validation.

**Why use it:** a lightweight alternative to late fusion that can **down-weight** a
weak modality per sample, rather than only globally.

**Output:** `runs/fusion_attention/`.

### Comparison

| Method | Fusion point | Trainable params | Complexity |
|--------|--------------|------------------|------------|
| Late | Scores (2-d) | Logistic (~3 weights) | Lowest |
| Early | Concatenated features (1280-d) | Linear head (~2.5k) | Medium |
| Attention | Gated features (256-d) | Projection + attention + classifier (~200k) | Higher |

**RQ2** compares the three fusion types on the same validation set. **Stage 1**
uses fixed hyperparameters with no validation tuning.

**Stage 2 (optional)** runs a hyperparameter search on the **best Stage-1 method
only** (provisional leader: attention). Pre-registered axes:

| Axis | Values | Rationale |
|------|--------|-----------|
| `softmax_temperature` | 1.0 (Stage 1), **0.5** | Sharper modality choice when text and image conflict. |
| `proj_dim` | **128**, 256, **512** | Bottleneck vs capacity in the projection space. |
| `use_layer_norm` | False (Stage 1), **True** | Balance DistilBERT vs ResNet vector scales before gating. |

Example Stage-2 config:
`AttentionFusionConfig(proj_dim=512, softmax_temperature=0.5, use_layer_norm=True)`.
The defaults reproduce Stage 1 (`256`, `1.0`, `False`). All settings are logged in
`metrics.json` for the demo UI. Report tuned and fixed results in **separate**
results rows; late and early fusion are not tuned unless the Stage-1 ranking
changes.

**RQ3** compares the best fusion method to **text-only** and **image-only** on the
same 4,486 `sample_id`s (filter the DistilBERT validation predictions to the
multimodal validation IDs). Use the best Stage-1 model, or the Stage-2 tuned
variant if completed and clearly labelled.
