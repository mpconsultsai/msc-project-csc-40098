# Training

Modality baselines and fusion — **training only** (no changes to `pipeline/`).

**Inputs:** `data/fake_news_final_text.tsv`, `data/fake_news_final_image.tsv` (from [`pipeline/README.md`](../pipeline/README.md) step **12**).

## Compute policy

| Where | What |
|-------|------|
| **Google Colab** | **All training notebooks** (TF-IDF on CPU; DistilBERT, image, fusion on GPU) |
| **Local Mac (optional)** | `pipeline/` data prep, thesis writing, syncing artefacts into `runs/` |

All Colab notebooks share **`colab_setup.py`** (see below).

## Environment (local, optional)

Only needed if you run a notebook on your Mac instead of Colab:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r training/requirements.txt
```

## Notebooks and Python modules

| File | Purpose |
|------|---------|
| `training_text_tfidf.ipynb` | **Colab (CPU):** TF-IDF → `runs/text_tfidf_baseline/` (+ `tfidf_pipeline.joblib` for PoC) |
| `training_text_distilbert.ipynb` | **Colab:** DistilBERT → `runs/text_distilbert_baseline/` |
| `training_image_resnet.ipynb` | **Colab:** ResNet-18 image baseline → `runs/image_resnet18_baseline/` |
| `training_fusion.ipynb` | **Colab:** RQ2/RQ3 — late, early, attention fusion + summary |
| `cohort_text.py` / `cohort_image.py` / `cohort_multimodal.py` | Shared load + `split_study`; multimodal join for fusion |
| `fusion_common.py` / `fusion_late.py` / `fusion_early.py` / `fusion_attention.py` | Shared + per-method fusion code |

**Colab data (persistent on Google Drive):**

```
My Drive/data/
  fake_news_final_text.tsv
  fake_news_final_image.tsv
  images.zip
My Drive/training/
  cohort_image.py
  cohort_text.py
  colab_setup.py
  cohort_multimodal.py
  fusion_common.py
  fusion_late.py
  fusion_early.py
  fusion_attention.py
My Drive/runs/                    # auto-written by unimodal + fusion runs
  text_distilbert_baseline/model/
  image_resnet18_baseline/resnet18_state.pt
  fusion_late_logistic/
  fusion_early_concat/
  fusion_attention/
```

Unimodal notebooks call **`persist_run_to_drive()`** after saving locally. The fusion notebook calls **`sync_runs_from_drive()`** in setup.

Each session: **bootstrap cell** (mount Drive, copy `training/`) → **`colab_setup.py`** (TSVs, images) → notebook-specific training. Call **`require_cuda()`** only for GPU notebooks (DistilBERT, image, fusion).

### Shared Colab setup (`colab_setup.py`)

All notebooks use the same bootstrap + `setup_colab_project(...)`. GPU notebooks also call `require_cuda()` first.

1. **Bootstrap** (identical) — mount Drive, copy `My Drive/training/` → `/content/msc/training/`
2. **`colab_setup`** (per notebook) — `setup_colab_project(...)`; add `require_cuda()` when using GPU

```python
from colab_setup import require_cuda, setup_colab_project

require_cuda()
ctx = setup_colab_project(
    tsv_names=["fake_news_final_text.tsv"],  # add image TSV / need_images=True as needed
    need_images=False,
)
PROJECT_ROOT = ctx.project_root
```

| Notebook | `tsv_names` | `need_images` | GPU |
|----------|-------------|---------------|-----|
| `training_text_tfidf.ipynb` | text TSV only | `False` | No (CPU) |
| `training_text_distilbert.ipynb` | text TSV only | `False` | Yes |
| `training_image_resnet.ipynb` | image TSV only | `True` | Yes |
| `training_fusion.ipynb` | text + image TSVs | `True` | Yes |

Sync **`colab_setup.py`** to `My Drive/training/` whenever you change it locally.

---

## Fusion methods (RQ2 / RQ3)

Three fusion mechanisms in `training_fusion.ipynb`, all on the **same multimodal cohort** (45,868 train / 4,486 val after dropping missing images). **Primary metric:** macro-F1 on pooled validation. **Encoders frozen** unless noted.

### Shared policy

- Train fusion layer on **train split only**; report on **validation** (no val hyperparameter tuning).
- Same unimodal checkpoints: DistilBERT (`runs/text_distilbert_baseline/model/`) and ResNet-18 (`runs/image_resnet18_baseline/resnet18_state.pt`).
- Balanced class weights for all trainable heads / combiners.
- Fixed hyperparameters, seed **42**.

| File | Role |
|------|------|
| `fusion_common.py` | Artifact checks, frozen encoder load, embedding extraction, metrics |
| `fusion_late.py` | Late fusion (scores → logistic regression) |
| `fusion_early.py` | Early fusion (concat embeddings → linear head) |
| `fusion_attention.py` | Attention fusion (softmax modality weights) |

### Late fusion (`fusion_late.py`)

**When fusion happens:** After each modality produces a **final prediction score**.

**Pipeline:**

1. Run frozen DistilBERT → `score_text` = P(fake) from softmax.
2. Run frozen ResNet-18 → `score_image` = P(fake).
3. Fit **sklearn LogisticRegression** on `[score_text, score_image]` (train only).
4. Predict on val; threshold 0.5 for class labels.

**Typical use:** Simple, interpretable baseline for multimodal fusion; common in applied ML when unimodal models already exist.

**Thesis result (full train):** macro-F1 **0.785**, val *n* = 4,486.

**Output:** `runs/fusion_late_logistic/`

### Early fusion (`fusion_early.py`)

**When fusion happens:** At **feature level**, before the final classifier.

**Embeddings (frozen encoders):**

| Modality | Source | Dim |
|----------|--------|-----|
| Text | DistilBERT `[CLS]` token (`last_hidden_state[:, 0]`) | 768 |
| Image | ResNet-18 global average pool (layer before `fc`) | 512 |

**Fusion:** `concat(text_emb, image_emb)` → **single linear layer** → 2-class logits.

**Training:** Only the linear head (8 epochs, AdamW lr=1e-3, batch 256, balanced CE loss). Encoders not updated.

**Typical use:** Standard early-fusion baseline in multimodal papers; lets the head learn cross-modal interactions in one shot.

**Output:** `runs/fusion_early_concat/`

### Attention fusion (`fusion_attention.py`)

**When fusion happens:** At feature level, with **learned modality weighting**.

**Steps:**

1. Same frozen embeddings as early fusion (768 + 512).
2. Linear projection each to **256-d**, `tanh` activation.
3. Stack modalities → **softmax attention** over {text, image} (weights sum to 1 per sample).
4. Weighted sum of projected vectors → linear classifier → 2-class logits.

**Interpretability:** `mean_attn_text` / `mean_attn_image` in metrics show average modality emphasis on validation.

**Typical use:** Common lightweight alternative to late fusion when you want the model to **down-weight** a weak modality per sample (not only globally like late fusion coefficients).

**Output:** `runs/fusion_attention/`

### Comparison (RQ2 / RQ3)

| Method | Fusion point | Trainable params | Complexity |
|--------|--------------|------------------|------------|
| Late | Scores (2-d) | Logistic (~3 weights) | Lowest |
| Early | Concat features (1280-d) | Linear head (~2.5k) | Medium |
| Attention | Gated features (256-d) | Proj + attn + cls (~200k) | Higher |

**RQ2:** Compare the three fusion types on the same val set (Stage 1: fixed hyperparameters, no val tuning).

**Stage 2 (optional):** Hyperparameter search on the **best Stage-1 method only** (provisional leader: attention). Pre-registered axes:

| Axis | Values | Rationale |
|------|--------|-----------|
| `softmax_temperature` | 1.0 (Stage 1), **0.5** | Sharper modality choice when text/image conflict |
| `proj_dim` | **128**, 256, **512** | Bottleneck vs capacity in projection space |
| `use_layer_norm` | False (Stage 1), **True** | Balance DistilBERT vs ResNet vector scales before gating |

Example Stage 2 config: `AttentionFusionConfig(proj_dim=512, softmax_temperature=0.5, use_layer_norm=True)`. Defaults preserve Stage 1 (`256`, `1.0`, `False`). Settings are logged in `metrics.json` for PoC inference.

Report tuned vs fixed results in **separate** Results rows. Late/early are not tuned unless Stage 1 ranking changes.

**RQ3:** Compare best fusion to **text-only** and **image-only** on the **same 4,486 `sample_id`s** (filter DistilBERT val predictions to multimodal val IDs). Use best Stage-1 model, or Stage-2 tuned variant if completed and clearly labelled.

### Fusion run IDs

| Run folder | Method |
|------------|--------|
| `fusion_late_logistic` | Late |
| `fusion_early_concat` | Early |
| `fusion_attention` | Attention |

Each writes `metrics.json` (macro-F1, per-class F1/recall, ROC-AUC, average precision), `predictions_val.tsv`, `confusion_matrix.png`, `roc_pr_curves.png` (ROC + precision–recall with recall on the x-axis), and (where applicable) a model checkpoint under `runs/`.
