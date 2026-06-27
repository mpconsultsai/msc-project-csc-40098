# MSc project — multimodal fake-news data preparation

This repository is a **data preprocessing pipeline** for two public corpora — **FakeNewsNet (FNN)** and **Fakeddit** — so downstream work can use aligned **text and image** fields for machine learning (multimodal fake-news detection). It does not train models here; it focuses on acquisition, normalization into a unified table, image fetching, and quality gating. Numbered steps live under **`pipeline/`**; generated tables and downloads go under **`data/`** (see **[`pipeline/README.md`](pipeline/README.md)**).

**What you get**

- **FakeNewsNet:** crawl article bodies and image URL candidates from the official CSV indices (no Twitter graph in this path).
- **Fakeddit:** download v2 text/metadata TSVs (official train/val/test splits in filenames).
- **Unified schema:** a working TSV (`data/fakenews.tsv`, built locally) plus optional cohort scripts that fetch images, run option-1 image QC, and export a gated training file (`data/fake_news_final.tsv`).

**Documentation (start here)**

| Doc | Purpose |
|-----|---------|
| [pipeline/README.md](pipeline/README.md) | Pipeline flow, scripts 01–12, schema, clone commands, outputs |
| [reporting/README.md](reporting/README.md) | Interactive reporting / EDA (`notebooks/fakenews_preprocessing_eda.ipynb`) |
| [training/README.md](training/README.md) | Model training on final cohort TSVs |

**Python environment**

```bash
cd "/path/to/MSC Project"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows, activate with `.venv\Scripts\activate`. In the IDE, point the Python interpreter at `.venv/bin/python` (macOS or Linux) or `.venv\Scripts\python.exe` (Windows) if it is not detected automatically.

**Upstream repos (nested clones next to scripts, gitignored)**

| Path | Source |
|------|--------|
| `pipeline/fakenewsnet/` | [KaiDMML/FakeNewsNet](https://github.com/KaiDMML/FakeNewsNet) — minimal CSVs under `dataset/` |
| `pipeline/fakeddit/` | [entitize/Fakeddit](https://github.com/entitize/Fakeddit) — scripts; large TSVs from Google Drive per upstream README |

Update clones: `git -C pipeline/fakenewsnet pull` and `git -C pipeline/fakeddit pull`.

**`data/`** starts empty in a fresh checkout; after running the pipeline it holds root-level TSVs (e.g. `fakenews.tsv`) and **`data/processed/`** outputs only.

**Thesis or proposal drafts** may live under a local `documents/` folder; that folder is gitignored so large or personal files stay out of git. Reproducible data-prep documentation is under **`pipeline/`** as linked above.
