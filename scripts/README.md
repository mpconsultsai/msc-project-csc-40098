# Pipeline scripts (stage-prefixed)

Names are ordered **01–16** by typical pipeline position. **Cohort** steps (07–15) assume `data/fakenews.tsv` already exists; see **`data/DATASETS_OVERVIEW.md`** §7.2.

## What is actually required?

**Initial unified table:** run **`05_consolidate_fakenews_tsv.py all`** (or `fakeddit` / `fakenewsnet` only) — see **`data/DATASETS_OVERVIEW.md`** §7.2. **`01`** can invoke `05_consolidate_fakenews_tsv.py all` after a crawl. After **`data/fakenews.tsv`** exists:

| Goal | Run |
|------|-----|
| **Stratified multimodal cohort → `fake_news_final.tsv`** (thesis path) | **07 → 08** (plus `08_cohort_reddit_placeholder_sha256.txt`) → **11 → 12 → 13 → 14**. Optional: **09** if the fetch log has duplicate `sample_id`s. **10** and **15** are reporting only (nice for documentation, not needed to produce data). |
| **Raw corpora only** | **01** (FakeNewsNet crawl) and/or **02** (Fakeddit download), depending which sources you use. |
| **FNN crawl hygiene / plots** | **03** (dedupe failure log), **04** (charts) — optional. |
| **Different image strategy** | **06** = full-table image fetch for many rows — **not** the cohort pipeline; pick **either** the cohort chain **or** 06, not both as the “main” path. |
| **Exploratory charts** | **16** — optional. |

**Minimal cohort chain (data-producing):** `07` → `08` → `11` → `12` → `13` → `14`.  
**Supporting file:** `08_cohort_reddit_placeholder_sha256.txt` (referenced by `08`, not executed).

| # | File | Stage |
|---|------|--------|
| 01 | `01_acquire_fakenewsnet_crawl.py` | Acquire — crawl FNN articles |
| 02 | `02_acquire_fakeddit_metadata.py` | Acquire — Fakeddit TSVs from Drive |
| 03 | `03_qa_fnn_dedupe_crawl_failures.py` | QA — dedupe FNN crawl failure log |
| 04 | `04_qa_fnn_visualize_crawl_failures.py` | QA — charts for FNN failures |
| 05 | `05_consolidate_fakenews_tsv.py` | **Consolidate** — build `data/fakenews.tsv` from Fakeddit + FNN |
| 06 | `06_images_full_table_fetch.py` | Optional — full-table image download (not the primary cohort path) |
| 07 | `07_cohort_build_plan.py` | Cohort — stratified plan TSV |
| 08 | `08_cohort_fetch_images.py` | Cohort — fetch images for plan rows |
| 08 | `08_cohort_reddit_placeholder_sha256.txt` | Cohort — blocklist used by fetch |
| 09 | `09_cohort_dedupe_fetch_log.py` | Cohort — dedupe cohort fetch log |
| 10 | `10_cohort_summarize_fetch.py` | Cohort — fetch report |
| 11 | `11_cohort_validate_images_option1.py` | Cohort — option-1 image QC |
| 12 | `12_cohort_merge_option1_into_fakenews.py` | Cohort — merge QC into `fakenews.tsv` |
| 13 | `13_cohort_merge_fetch_log_into_fakenews.py` | Cohort — merge fetch paths into `fakenews.tsv` |
| 14 | `14_cohort_export_final_tsv.py` | Cohort — export `fake_news_final.tsv` |
| 15 | `15_cohort_summarize_final.py` | Cohort — final summary report |
| 16 | `16_eda_visualize_fakenews_tsv.py` | EDA — charts from `fakenews.tsv` |

**Consolidation:** `05_consolidate_fakenews_tsv.py` — builds `data/fakenews.tsv`; `01_acquire_fakenewsnet_crawl.py` calls `all` after a crawl if you do not pass `--no-consolidate-image-refs`.

Full paths and outputs: **`data/DATA_PIPELINE_FILES_REFERENCE.md`**.
