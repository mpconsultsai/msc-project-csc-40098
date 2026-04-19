# Reporting (`reporting/`)

Optional **report** and **visualisation** entrypoints. They read **already-generated** artefacts (`cohort_image_fetch.log`, `fake_news_final.tsv`, `fakenews.tsv`, etc.) and write under `outputs/`. They do **not** mutate the main data pipeline; **safe to re-run anytime** after the inputs exist.

Run from the **project root**:

| Report script | Inputs (defaults) | Output (defaults) |
|---------------|-------------------|-------------------|
| `report_cohort_fetch_summary.py` | `data/processed/images/cohort_image_fetch.log`, optional cohort plan TSV | `outputs/cohort_fetch_report/` (`.md` + `.json`) |
| `report_fake_news_final.py` | `data/fake_news_final.tsv` | `outputs/fake_news_final_report/` (`.md` + `.json`) |
| `report_fakenews_eda.py` | `data/fakenews.tsv` | `outputs/fakenews_viz/` (PNGs + `index.html`) |

Examples:

```bash
python reporting/report_cohort_fetch_summary.py
python reporting/report_fake_news_final.py
python reporting/report_fakenews_eda.py
```

**Data-producing cohort steps** live under **`pipeline/`** — see **`pipeline/README.md`** and **`pipeline/DATASETS_OVERVIEW.md`** §7.
