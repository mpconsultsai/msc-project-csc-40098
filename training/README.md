# Training

This directory contains the model-training stage of the project. It trains and
evaluates the single-modality (text and image) baselines and the three
multimodal fusion methods (early, late, and attention-based) used to answer the
research questions.

**All training runs in Google Colab.** TF-IDF runs on a CPU runtime; the
DistilBERT, image, and fusion notebooks require **CUDA GPU acceleration**
(reported runs: Google Colab, NVIDIA T4). Data and trained artefacts live in Google Drive so they persist
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

> **Data access and licensing.** The cohort data (the TSVs and the extracted
> `images.zip`) is **not redistributed publicly**. FakeNewsNet is published as
> article IDs/URLs and Fakeddit as Reddit metadata; the underlying article text and
> images remain subject to the original publishers' and Reddit's terms, so this
> project does **not** host them openly. For assessment or reproduction, the data is
> shared **privately** only (e.g. a Google Drive link) and staged on
> Drive as described in [section 2](#2-how-to-run-training-in-google-colab). The
> notebooks themselves and the code are public; only the derived dataset is access
> controlled. The pipeline that regenerates this data from the original sources is
> documented in [`pipeline/README.md`](../pipeline/README.md).

## 2. How to run training in Google Colab

The setup has two parts: a **one-time** upload of data and helper code to Google
Drive, and a short **per-session** setup that you run at the top of each
notebook. All notebooks share the same `colab_setup.py` helper, so this setup is
identical across notebooks.

### Step 2.1 — One-time: upload files to Google Drive

Colab cannot see files on your local machine. Upload them **once** to
[Google Drive](https://drive.google.com) (same Google account you use in Colab).

**1. Create folders** in My Drive:

- `data`
- `training/src`

**2. Upload into `My Drive/data/`** (from your local project's `data/` folder):

- `fake_news_final_text.tsv`
- `fake_news_final_image.tsv`
- `images.zip` — for image and fusion notebooks only (~1.1 GB). This is **not**
  produced by the pipeline automatically; zip the downloaded images folder
  `data/processed/images/` (created by pipeline step **06**), then upload the zip:

```bash
cd "/path/to/MSC Project/data"
zip -r images.zip processed/images/ -x "*.log"
```

> **Image payload QC.** A few publisher CDN files were saved as `.jpg` but contained
> AVIF or JPEG2000 bytes. These passed local pipeline validation but broke Colab
> training. Normalise to real JPEG before zipping (see project decision log). The
> image and fusion notebooks call `verify_jpeg_payloads()` after load to catch
> stale zips early.

**3. Upload into `My Drive/training/src/`** — all `.py` files from this repo's
`training/src/` folder (including `colab_setup.py`).

**4. The folder structure should be as follows:**

```
My Drive/
├── data/
│   ├── fake_news_final_text.tsv
│   ├── fake_news_final_image.tsv
│   └── images.zip
└── training/
    └── src/
        └── *.py
```

You do **not** need to upload the notebooks — open them from the Colab badges in
[Section 3](#3-notebooks-and-modules). You do **not** need to create `runs/` —
notebooks create `My Drive/runs/` automatically when training finishes.

> **Tip:** Wait until large uploads show 100% in Drive before opening Colab.
> If you edit `training/src/` locally, re-upload the changed file(s).

> **If Colab errors:** `training/src/` missing → upload the `.py` files;
> TSV or `images.zip` missing → check they are in `My Drive/data/`, not only on your local machine;
> wrong account → re-run Cell A and pick the correct Google account.

> **GitHub won't render a notebook** (`metadata.widgets` / missing `state`): Colab
> sometimes adds broken widget metadata after training progress bars (tqdm,
> DistilBERT `Trainer`, etc.). **After a full Colab run**, before committing
> notebooks to GitHub:
>
> ```bash
> python training/scripts/clean_notebook_for_github.py training/notebooks/*.ipynb
> ```
>
> Or in Colab: **Edit → Clear all outputs**, then download and commit. Keep
> authoritative metrics in `My Drive/runs/` (`metrics.json`, plots) — the GitHub
> notebooks are a **code + reproducibility record**, not the primary results store.

### Step 2.2 — Open a notebook and select the runtime

1. Open the desired notebook (see the [table in Section 3](#3-notebooks-and-modules)) in Google Colab.
2. For the GPU notebooks (DistilBERT, image, fusion), select a GPU runtime:
   **Runtime → Change runtime type → T4 GPU**. The TF-IDF notebook runs on the
   default CPU runtime.

### Step 2.3 — Run the per-session setup cells

Every notebook starts with the same setup cells. Run them top to bottom at
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
    raise FileNotFoundError("Sync the repo's training/src/ to My Drive/training/src/")
if TRAINING.exists():
    shutil.rmtree(TRAINING)
shutil.copytree(TRAINING_SRC, TRAINING)
sys.path.insert(0, str(TRAINING / "src"))  # importable modules live in training/src/
```

**Cell B — pinned dependencies (per notebook).** Installs the version-pinned
libraries from `colab_setup.PINNED_DEPENDENCIES` for the groups this notebook
needs. The `base` group (`scikit-learn`, `pandas`) is always included; `torch`
and `torchvision` are **not** installed here — Colab's preinstalled, CUDA-matched
build is used as-is:

```python
from colab_setup import install_dependencies

install_dependencies(["text", "image"])  # groups vary per notebook (see table)
```

**Cell C — project setup (per notebook).** Copies the required TSVs (and images,
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

| Notebook | `install_dependencies(...)` | `tsv_names` | `need_images` | `require_cuda()` |
|----------|------------------------------|-------------|---------------|------------------|
| `training_text_tfidf.ipynb` | `()` (base) | `["fake_news_final_text.tsv"]` | `False` | No (CPU) |
| `training_text_distilbert.ipynb` | `["text"]` | `["fake_news_final_text.tsv"]` | `False` | Yes |
| `training_image_resnet.ipynb` | `["image"]` | `["fake_news_final_image.tsv"]` | `True` | Yes |
| `training_fusion.ipynb` | `["text", "image"]` | `["fake_news_final_text.tsv", "fake_news_final_image.tsv"]` | `True` | Yes |

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

### Step 2.4a — Typical runtimes (Google Colab T4)

Wall-clock times vary with queue load and first-time image unzip. Indicative
figures from the locked benchmark runs (full cohort, seed 42):

| Notebook | Runtime | Typical duration |
|----------|---------|------------------|
| `training_text_tfidf.ipynb` | CPU | ~1–2 min |
| `training_text_distilbert.ipynb` | GPU | ~10–15 min |
| `training_image_resnet.ipynb` | GPU | ~15 min train (+ few min first unzip) |
| `training_fusion.ipynb` (Steps 2–4) | GPU | ~25–35 min total (~7 min per fusion step) |

After **Runtime → Restart**, run cells **from the top** in order — later cells
depend on `PROJECT_ROOT`, `train_df`, `val_df`, and fitted models from earlier
cells.

### Step 2.5 — How to run and share for review

The notebooks themselves are straightforward to distribute; the **dataset**
requires care, as it is not redistributed publicly (see the data-access and
licensing note in [Section 1](#1-what-you-need-before-you-start)). The governing
constraint is that, when a notebook is executed, Colab mounts the **reviewer's own**
Google Drive. The helper modules and data must therefore be reachable from *their*
`My Drive/` at the paths the setup helper expects.

**Recommended approach — a single shared Google Drive folder.**

1. Place all required assets in one Google Drive folder, following the
   [Step 2.1 layout](#step-21--one-time-prepare-google-drive): the four notebooks,
   `training/src/`, the cohort TSVs, and `images.zip`.
2. Grant the reviewer access to that folder (or share an individual notebook using
   Colab's **Share** control, top right).
3. The reviewer opens the folder in Drive and selects **"Add shortcut to Drive"**,
   so the contents appear under their `My Drive/` at the expected paths.
4. The reviewer opens a notebook, selects **File → Save a copy in Drive**, chooses
   the appropriate runtime ([Step 2.2](#step-22--open-a-notebook-and-select-the-runtime)),
   and runs it via **Runtime → Run all**.

Each notebook also begins with a **"How to run this notebook"** cell, so the
required steps are presented directly within the notebook.

**Alternative — GitHub "Open in Colab" links.** The badges in
[Section 3](#3-notebooks-and-modules) open the latest committed version of a
notebook directly from GitHub. This is convenient for distributing the **code** to
a wider audience, but the Drive assets above are still required for end-to-end
execution, as the link carries only the notebook and not the dataset.

## 3. Notebooks and modules

On disk, the runnable notebooks live in `training/notebooks/` and the importable
helper modules in `training/src/`. The tables below list them by name.

**Notebooks (open these in Colab):**

| Notebook | Runtime | Open | Purpose |
|----------|---------|------|---------|
| `training_text_tfidf.ipynb` | CPU | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mpconsultsai/msc-project-csc-40098/blob/main/training/notebooks/training_text_tfidf.ipynb) | TF-IDF text baseline → `runs/text_tfidf_baseline/` (also writes `tfidf_pipeline.joblib` for the demo UI). |
| `training_text_distilbert.ipynb` | GPU | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mpconsultsai/msc-project-csc-40098/blob/main/training/notebooks/training_text_distilbert.ipynb) | DistilBERT text baseline → `runs/text_distilbert_baseline/`. |
| `training_image_resnet.ipynb` | GPU | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mpconsultsai/msc-project-csc-40098/blob/main/training/notebooks/training_image_resnet.ipynb) | ResNet-18 image baseline → `runs/image_resnet18_baseline/`. |
| `training_fusion.ipynb` | GPU | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mpconsultsai/msc-project-csc-40098/blob/main/training/notebooks/training_fusion.ipynb) | RQ2 / RQ3: late, early, and attention fusion plus a summary table. |

> The **Open in Colab** badges load the notebook straight from GitHub. They give
> you the code only — before it runs end to end you still need the Drive assets
> from [section 2](#2-how-to-run-training-in-google-colab) (`training/`, the TSVs,
> and `images.zip` for the image/fusion notebooks). In Colab, use
> **File → Save a copy in Drive** so your edits and outputs persist.

**Supporting modules (in `training/src/`, synced to `My Drive/training/src/`; not run directly):**

| Module | Purpose |
|--------|---------|
| `colab_setup.py` | Shared Colab setup: pinned dependency install (`install_dependencies` / `PINNED_DEPENDENCIES`), GPU check, Drive/TSV copying, image unzip, run syncing. |
| `cohort_text.py`, `cohort_image.py`, `cohort_multimodal.py` | Shared data loading and `split_study`; `cohort_multimodal.py` joins modalities for fusion. `cohort_image.verify_jpeg_payloads` fails fast if any cohort `.jpg` is not JPEG on disk (guards against extension/payload mismatches before training). |
| `fusion_common.py`, `fusion_late.py`, `fusion_early.py`, `fusion_attention.py` | Shared fusion utilities and one module per fusion method. |

**Repository-only files (not uploaded to Drive):**

| File | Purpose |
|------|---------|
| `pyrightconfig.json`, `typings/` | Editor type-checking support only (e.g. a stub for `google.colab`). They have no effect on training. |

There is **no `requirements.txt` for training**. Instead, the dependency versions
are pinned **once** in `colab_setup.PINNED_DEPENDENCIES` and installed per notebook
via `install_dependencies([...])` (Cell B above), so every notebook resolves the
same versions from a single source of truth. `torch` / `torchvision` are
deliberately excluded and left to Colab's preinstalled, CUDA-matched build. Each
install prints the resolved versions (`report_versions`) so the exact set used is
captured in the notebook output.

> **Optional — local editor environment.** Training does not need anything
> installed locally. If you want type-checking and autocompletion while editing
> the `.py` modules in your IDE, create a virtual environment and install the
> same libraries listed in `colab_setup.PINNED_DEPENDENCIES` (the notebooks
> themselves still run in Colab):
>
> ```bash
> python3 -m venv .venv
> source .venv/bin/activate        # Windows: .venv\Scripts\activate
> pip install torch torchvision transformers datasets accelerate \
>     scikit-learn pandas pillow matplotlib tqdm
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

Each run writes `metrics.json` (macro-F1, accuracy, ROC-AUC, average precision,
and per-class `recall_real` / `recall_fake` where applicable), `predictions_val.tsv`,
and plot PNGs where the notebook generates them. Unimodal runs also record
`f1_real` / `f1_fake`. Fusion runs save combiner/head checkpoints. The fusion
notebook Step 5 table reads recall from `metrics.json`, or derives it from
`predictions_val.tsv` for older unimodal runs.

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
