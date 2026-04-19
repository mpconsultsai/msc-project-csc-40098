# Data pipeline — included files only

Every path below exists in this repository (or is a **fixed output location** written by those scripts). Filenames under **`pipeline/`** use a **stage prefix** (`01_`–`11_`) for acquisition → QA → consolidate → cohort. **`pipeline/04_consolidate_fakenews_tsv.py`** builds **`data/fakenews.tsv`** from Fakeddit + FNN sources (see **`pipeline/DATASETS_OVERVIEW.md`** §7.2). Merge/export scripts assume **`data/fakenews.tsv`** is already present or created by consolidation.

**Interactive reporting / EDA** is in **`notebooks/fakenews_preprocessing_eda.ipynb`** (see **`reporting/README.md`**).

---

## `pipeline/` (11 scripts)

| Stage | Path | Role in pipeline |
|-------|------|------------------|
| 01 acquire | `pipeline/01_acquire_fakenewsnet_crawl.py` | FakeNewsNet article crawl → `data/processed/fakenewsnet/`. |
| 01 acquire | `pipeline/02_acquire_fakeddit_metadata.py` | Download Fakeddit v2 metadata from Drive. |
| 02 FNN QA | `pipeline/03_qa_fnn_dedupe_crawl_failures.py` | Dedupe FNN `crawl_failures.jsonl`. |
| 03 consolidate | `pipeline/04_consolidate_fakenews_tsv.py` | Build `data/fakenews.tsv` from Fakeddit multimodal TSVs + FNN `news content.json` (+ optional `crawl_failures.jsonl` filter). |
| 04 cohort | `pipeline/05_cohort_build_plan.py` | Build stratified cohort plan TSV under `data/processed/cohorts/`. |
| 04 cohort | `pipeline/06_cohort_fetch_images.py` | Cohort image fetch → `cohort_image_fetch.log` + image files. |
| 04 cohort | `pipeline/07_cohort_dedupe_fetch_log.py` | Dedupe `cohort_image_fetch.log`. |
| 04 cohort | `pipeline/08_cohort_image_validation.py` | Image QC + validity score → `data/processed/cohorts/image_validation/`. |
| 04 cohort | `pipeline/09_cohort_merge_image_validation_into_fakenews.py` | Merge image-validation QC columns into `data/fakenews.tsv`. |
| 04 cohort | `pipeline/10_cohort_merge_fetch_log_into_fakenews.py` | Merge fetch columns into `data/fakenews.tsv`. |
| 04 cohort | `pipeline/11_cohort_export_final_tsv.py` | Write `data/fake_news_final.tsv` from `fakenews.tsv`. |

---

## Project root (supporting data)

| Path | Role |
|------|------|
| `cohort_reddit_placeholder_sha256.txt` | SHA-256 blocklist for `06_cohort_fetch_images.py` (not a pipeline script). |

---

## `reporting/`

| Path | Role |
|------|------|
| `reporting/README.md` | Pointer to **`notebooks/fakenews_preprocessing_eda.ipynb`** for interactive reporting. |

---

## Project root (dependencies)

| Path | Role |
|------|------|
| `requirements.txt` | Notebook / general Python deps (e.g. EDA in **`notebooks/fakenews_preprocessing_eda.ipynb`**). |
| `requirements-fakenewsnet-crawl.txt` | Deps for `01_acquire_fakenewsnet_crawl.py`. |

---

## `notebooks/`

| Path | Role |
|------|------|
| `notebooks/fakenews_preprocessing_eda.ipynb` | Interactive QA: `fakenews.tsv`, FNN crawl stats, Fakeddit quality, cohort fetch log (§7), gated export (§8), image checks. |

---

## Canonical data paths (written by the pipeline; usually gitignored)

| Path | Produced by |
|------|-------------|
| `data/fakenews.tsv` | `04_consolidate_fakenews_tsv.py` (initial build) or restore; **updated** by merge scripts. |
| `data/fake_news_final.tsv` | `11_cohort_export_final_tsv.py`. |
| `data/processed/cohorts/multimodal_plan_n50000_seed42.tsv` | Typical output of `05_cohort_build_plan.py` (name may vary with args). |
| `data/processed/cohorts/image_validation/cohort_image_validation.tsv` | `08_cohort_image_validation.py`. |
| `data/processed/cohorts/image_validation/cohort_image_validation_summary.log` | `08_cohort_image_validation.py`. |
| `data/processed/images/` | Image files + `cohort_image_fetch.log` from `06_cohort_fetch_images.py`. |
| `data/processed/fakenewsnet/` | `01_acquire_fakenewsnet_crawl.py` (article JSON, `crawl_failures.jsonl`, etc.). |

---

## Optional `outputs/` (not produced by removed CLI scripts)

You may keep optional HTML/PNG/Markdown exports under **`outputs/`** (e.g. cohort fetch summary, final cohort summary, fakenews viz) from **notebook exports** or older runs. Nothing in **`pipeline/`** writes these by default.

---

*Companion narrative: **`pipeline/DATASETS_OVERVIEW.md`** §7 (reproducible command order).*
