# Data pipeline — included files only

Every path below exists in this repository (or is a **fixed output location** written by those scripts). Filenames use a **stage prefix** (`01_`–`16_`) for acquisition → QA → consolidate → optional bulk images → cohort → EDA. **`scripts/05_consolidate_fakenews_tsv.py`** builds **`data/fakenews.tsv`** from Fakeddit + FNN sources (see **`data/DATASETS_OVERVIEW.md`** §7.2). Merge/export scripts assume **`data/fakenews.tsv`** is already present or created by consolidation.

---

## `scripts/` (17 files)

| Stage | Path | Role in pipeline |
|-------|------|------------------|
| 03 consolidate | `scripts/05_consolidate_fakenews_tsv.py` | Build `data/fakenews.tsv` from Fakeddit multimodal TSVs + FNN `news content.json` (+ optional `crawl_failures.jsonl` filter). |
| 01 acquire | `scripts/01_acquire_fakenewsnet_crawl.py` | FakeNewsNet article crawl → `data/processed/fakenewsnet/`. |
| 01 acquire | `scripts/02_acquire_fakeddit_metadata.py` | Download Fakeddit v2 metadata from Drive. |
| 02 FNN QA | `scripts/03_qa_fnn_dedupe_crawl_failures.py` | Dedupe FNN `crawl_failures.jsonl`. |
| 02 FNN QA | `scripts/04_qa_fnn_visualize_crawl_failures.py` | PNGs under `outputs/fnn_failure_viz/`. |
| 04 optional | `scripts/06_images_full_table_fetch.py` | Optional full-table image fetch (working TSV under `data/processed/images/`). |
| 05 cohort | `scripts/07_cohort_build_plan.py` | Build stratified cohort plan TSV under `data/processed/cohorts/`. |
| 05 cohort | `scripts/08_cohort_fetch_images.py` | Cohort image fetch → `cohort_image_fetch.log` + image files. |
| 05 cohort | `scripts/08_cohort_reddit_placeholder_sha256.txt` | SHA-256 blocklist for `08_cohort_fetch_images.py`. |
| 05 cohort | `scripts/09_cohort_dedupe_fetch_log.py` | Dedupe `cohort_image_fetch.log`. |
| 05 cohort | `scripts/10_cohort_summarize_fetch.py` | `outputs/cohort_fetch_report/*.md` + `*.json`. |
| 05 cohort | `scripts/11_cohort_validate_images_option1.py` | Option-1 QC → `outputs/cohort_image_validation/`. |
| 05 cohort | `scripts/12_cohort_merge_option1_into_fakenews.py` | Merge option-1 QC columns into `data/fakenews.tsv`. |
| 05 cohort | `scripts/13_cohort_merge_fetch_log_into_fakenews.py` | Merge fetch columns into `data/fakenews.tsv`. |
| 05 cohort | `scripts/14_cohort_export_final_tsv.py` | Write `data/fake_news_final.tsv` from `fakenews.tsv`. |
| 05 cohort | `scripts/15_cohort_summarize_final.py` | `outputs/fake_news_final_report/*.md` + `*.json`. |
| 06 EDA | `scripts/16_eda_visualize_fakenews_tsv.py` | Charts / `outputs/fakenews_viz/` from `fakenews.tsv`. |

---

## Project root (dependencies)

| Path | Role |
|------|------|
| `requirements.txt` | Notebook / general Python deps (e.g. EDA, `16_eda_visualize_fakenews_tsv.py`). |
| `requirements-fakenewsnet-crawl.txt` | Deps for `01_acquire_fakenewsnet_crawl.py`. |

---

## `notebooks/`

| Path | Role |
|------|------|
| `notebooks/fakenews_preprocessing_eda.ipynb` | EDA for `fakenews.tsv`, FNN crawl stats, Fakeddit quality. |

---

## Canonical data paths (written by the pipeline; usually gitignored)

| Path | Produced by |
|------|-------------|
| `data/fakenews.tsv` | `05_consolidate_fakenews_tsv.py` (initial build) or restore; **updated** by merge scripts. |
| `data/fake_news_final.tsv` | `14_cohort_export_final_tsv.py`. |
| `data/processed/cohorts/multimodal_plan_n50000_seed42.tsv` | Typical output of `07_cohort_build_plan.py` (name may vary with args). |
| `data/processed/images/` | Image files + `cohort_image_fetch.log` from `08_cohort_fetch_images.py` (and optionally `06_images_full_table_fetch.py`). |
| `data/processed/fakenewsnet/` | `01_acquire_fakenewsnet_crawl.py` (article JSON, `crawl_failures.jsonl`, etc.). |

---

## Canonical `outputs/` paths (written by the pipeline)

| Path | Produced by |
|------|-------------|
| `outputs/cohort_fetch_report/cohort_image_fetch_summary.md` | `10_cohort_summarize_fetch.py` |
| `outputs/cohort_fetch_report/cohort_image_fetch_summary.json` | `10_cohort_summarize_fetch.py` |
| `outputs/cohort_image_validation/option1_validation.tsv` | `11_cohort_validate_images_option1.py` |
| `outputs/cohort_image_validation/option1_validation_summary.log` | `11_cohort_validate_images_option1.py` |
| `outputs/fake_news_final_report/fake_news_final_summary.md` | `15_cohort_summarize_final.py` |
| `outputs/fake_news_final_report/fake_news_final_summary.json` | `15_cohort_summarize_final.py` |
| `outputs/fnn_failure_viz/` | `04_qa_fnn_visualize_crawl_failures.py` |
| `outputs/fakenews_viz/` | `16_eda_visualize_fakenews_tsv.py` |

---

*Companion narrative: **`data/DATASETS_OVERVIEW.md`** §7 (reproducible command order).*
