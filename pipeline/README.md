# Pipeline (`pipeline/`)

Numbered Python entrypoints live in **`pipeline/`** (project root). Run from the repo root, e.g. `python pipeline/04_consolidate_fakenews_tsv.py …`.

Names are ordered **01–11** by typical pipeline position. **Cohort** steps (05–11) assume `data/fakenews.tsv` already exists; see **`pipeline/DATASETS_OVERVIEW.md`** §7.2.

**Interactive reporting / EDA** (optional, any time after inputs exist) is in **`notebooks/fakenews_preprocessing_eda.ipynb`** — see **`reporting/README.md`**.

## What is actually required?

**Initial unified table:** run **`04_consolidate_fakenews_tsv.py all`** (or `fakeddit` / `fakenewsnet` only) — see **`pipeline/DATASETS_OVERVIEW.md`** §7.2. **`01`** can invoke `04_consolidate_fakenews_tsv.py all` after a crawl. After **`data/fakenews.tsv`** exists:

| Goal | Run |
|------|-----|
| **Stratified multimodal cohort → `fake_news_final.tsv`** (thesis path) | **05 → 06** (plus **`cohort_reddit_placeholder_sha256.txt`** at project root) → **08 → 09 → 10 → 11**. Optional: **07** if the fetch log has duplicate `sample_id`s. |
| **Raw corpora only** | **01** (FakeNewsNet crawl) and/or **02** (Fakeddit download), depending which sources you use. |
| **FNN crawl hygiene** | **03** (dedupe failure log) — optional. |
| **Fetch / final / EDA exploration** | **`notebooks/fakenews_preprocessing_eda.ipynb`** — optional; see **`reporting/README.md`**. |

**Minimal cohort chain (data-producing):** `05` → `06` → `08` → `09` → `10` → `11`.  
**Supporting file:** `cohort_reddit_placeholder_sha256.txt` (project root; referenced by `06`, not executed).

| # | File | Stage |
|---|------|--------|
| 01 | `01_acquire_fakenewsnet_crawl.py` | Acquire — crawl FNN articles |
| 02 | `02_acquire_fakeddit_metadata.py` | Acquire — Fakeddit TSVs from Drive |
| 03 | `03_qa_fnn_dedupe_crawl_failures.py` | QA — dedupe FNN crawl failure log |
| 04 | `04_consolidate_fakenews_tsv.py` | **Consolidate** — build `data/fakenews.tsv` from Fakeddit + FNN |
| 05 | `05_cohort_build_plan.py` | Cohort — stratified plan TSV |
| 06 | `06_cohort_fetch_images.py` | Cohort — fetch images for plan rows |
| 07 | `07_cohort_dedupe_fetch_log.py` | Cohort — dedupe cohort fetch log |
| 08 | `08_cohort_image_validation.py` | Cohort — heuristic image QC + validity score |
| 09 | `09_cohort_merge_image_validation_into_fakenews.py` | Cohort — merge image validation into `fakenews.tsv` |
| 10 | `10_cohort_merge_fetch_log_into_fakenews.py` | Cohort — merge fetch paths into `fakenews.tsv` |
| 11 | `11_cohort_export_final_tsv.py` | Cohort — export `fake_news_final.tsv` |

**Consolidation:** `04_consolidate_fakenews_tsv.py` — builds `data/fakenews.tsv`; `01_acquire_fakenewsnet_crawl.py` calls `all` after a crawl if you do not pass `--no-consolidate-image-refs`.

Full paths and outputs: **`pipeline/DATA_PIPELINE_FILES_REFERENCE.md`**.
