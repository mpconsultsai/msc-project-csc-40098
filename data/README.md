# Data directory

This folder holds **documentation for the preprocessing pipeline** (FakeNewsNet + Fakeddit → text + image fields for ML). Large clones and generated files stay **gitignored**; see **`DATASETS_OVERVIEW.md`** for full layout and schema.

**Summary of datasets and artefacts:** **`DATASETS_OVERVIEW.md`**. **Script/output inventory:** **`DATA_PIPELINE_FILES_REFERENCE.md`**. **Stage-ordered script list:** **`../scripts/README.md`**.

Official GitHub repositories are cloned here as **nested Git repos** (ignored by the MSc project root so their history is not committed).

## FakeNewsNet

- **Remote:** [github.com/KaiDMML/FakeNewsNet](https://github.com/KaiDMML/FakeNewsNet)
- **Local:** `data/fakenewsnet/`
- **Minimal CSVs:** `data/fakenewsnet/dataset/` (`politifact_*.csv`, `gossipcop_*.csv`)
- **Collection code:** `data/fakenewsnet/code/` (Twitter keys, `config.json`, crawlers)

Authors do not redistribute tweets/social graphs. You can still collect **article JSON** (body text + image URLs) without Twitter:

```powershell
pip install -r requirements-fakenewsnet-crawl.txt
python scripts/01_acquire_fakenewsnet_crawl.py --out data/processed/fakenewsnet --resume
```

Relative `data/...` paths are resolved from the **project root** (where `scripts/` lives), not necessarily your shell’s current directory, so **`--resume`** finds existing `news content.json` files reliably.

**Speed:** default **`--post-download-sleep 0.2`** caps FakeNewsNet’s 2 s pause after each download (much faster). Optional **`--workers 6`** runs parallel fetches (may increase HTTP 429/403); use **`--post-download-sleep 2`** only if you need upstream-like politeness.

Use **`--resume`** so you can stop and run again later (skips existing `news content.json`). Add **`--retry-empty`** if some files exist but have no body text. Failed rows are appended to **`data/processed/fakenewsnet/crawl_failures.jsonl`** (`reason`: `no_article`, `empty_body`, or `exception`). By default, **`(news_source, label, news_id)`** keys already present in that log are **skipped** on the next run (faster; avoids hammering dead URLs). Use **`--retry-known-failures`** to fetch them again (e.g. after clearing the log or changing network). Optional **`--log-skipped`** writes **`crawl_skipped_resume.jsonl`**. Upstream logs a traceback on every bad URL; the script **hides** those by default — use **`--verbose-crawl`** only when debugging.

(On older Python you can try `pip install -r data/fakenewsnet/requirements.txt` instead.)

Output: `data/processed/fakenewsnet/<politifact|gossipcop>/<fake|real>/<id>/news content.json`. **Twitter** features need upstream `main.py`, API keys, and key server per `data/fakenewsnet/README.md`.

Update clone:

```powershell
git -C "data/fakenewsnet" pull
```

## Fakeddit

- **Remote:** [github.com/entitize/Fakeddit](https://github.com/entitize/Fakeddit)
- **Local:** `data/fakeddit/` — README and helper scripts.
- **TSVs (default):** `python scripts/02_acquire_fakeddit_metadata.py` → **`data/processed/fakeddit/v2_text_metadata/`** (no images/comments unless you pass **`--images`** / **`--comments`**).

Update clone:

```powershell
git -C "data/fakeddit" pull
```

## Notebooks

- **`notebooks/fakenews_preprocessing_eda.ipynb`** — exploratory analysis of **`data/fakenews.tsv`**, FakeNewsNet index vs crawl vs **`crawl_failures.jsonl`**, and Fakeddit multimodal TSV preprocessing stats. Run Jupyter with the **project root** as the working directory, or rely on the notebook `ROOT` logic when the kernel cwd is **`notebooks/`**.
