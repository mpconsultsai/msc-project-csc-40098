# Processed data (local only)

This directory holds **generated** artefacts: crawled FakeNewsNet JSON, downloaded Fakeddit metadata, image files, cohort plans, and merge logs. It is **gitignored** except for this file.

After cloning the repo, populate it by following **`data/README.md`** (acquisition) and **`data/DATASETS_OVERVIEW.md`** §7 (cohort order). Typical layout:

- `fakenewsnet/` — article crawl output from `scripts/01_acquire_fakenewsnet_crawl.py`
- `fakeddit/` — v2 TSVs from `scripts/02_acquire_fakeddit_metadata.py`
- `cohorts/` — stratified plan TSVs from `scripts/07_cohort_build_plan.py`
- `images/` — fetched images and `cohort_image_fetch.log`

Unified tables **`data/fakenews.tsv`** and **`data/fake_news_final.tsv`** live at the project root when present.
