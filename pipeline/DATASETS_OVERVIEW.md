# Datasets overview — generated and planned artefacts

This note summarises **what you have on disk**, **what the crawl/download steps produce**, and **how a unified training table represents both corpora** (FakeNewsNet + Fakeddit) for multimodal ML: **text and image** fields, with explicit flags for URL vs downloaded vs training-eligible media. Operational commands for clones and downloads are in **`DATA_LAYOUT.md`** (same folder); the script/output inventory is **`DATA_PIPELINE_FILES_REFERENCE.md`**.

---

## 1. Scope and sources

| Corpus | Upstream | Role in the project |
|--------|----------|---------------------|
| **FakeNewsNet** | [KaiDMML/FakeNewsNet](https://github.com/KaiDMML/FakeNewsNet) | News articles (PolitiFact + GossipCop indices); article bodies and image URLs via **local crawl**, not a redistributed archive. |
| **Fakeddit** | [entitize/Fakeddit](https://github.com/entitize/Fakeddit); TSVs from Google Drive | Reddit multimodal benchmark (title + `image_url`); **official splits** in filenames. |

**Out of scope here:** full Twitter social graph collection for FakeNewsNet (requires API keys and upstream `main.py`); Fakeddit **image archive** and **comment** dumps unless you opt in via `pipeline/02_acquire_fakeddit_metadata.py --images` / `--comments`.

---

## 2. FakeNewsNet — what exists on disk

### 2.1 Shallow Git clone (nested repo, gitignored at project root)

- **Path:** `pipeline/fakenewsnet/`
- **Contents:** upstream `README`, `dataset/*.csv`, `code/` (crawlers, `config.json`).

### 2.2 Minimal index CSVs (in the clone)

Four files under **`pipeline/fakenewsnet/dataset/`**:

- `politifact_fake.csv`, `politifact_real.csv`
- `gossipcop_fake.csv`, `gossipcop_real.csv`

Typical columns: **`id`**, **`news_url`**, **`title`**, **`tweet_ids`**. These are **not** full article text; labels are implied by **filename** (`fake` vs `real`) and **source** (PolitiFact vs GossipCop).

**Scale (indicative):** order of **23k** rows in total across the four files (PolitiFact ~1.1k; GossipCop ~22k).

### 2.3 Generated crawl output (gitignored)

- **Path:** `data/processed/fakenewsnet/`
- **Layout:** `<politifact|gossipcop>/<fake|real>/<id>/news content.json`
- **Produced by:** `pipeline/01_acquire_fakenewsnet_crawl.py` (newspaper3k + optional Wayback fallback; **no Twitter**).

**Sidecar files:**

- **`crawl_failures.jsonl`** — append-only JSON lines for failed or empty-body fetches (`reason`: `no_article`, `empty_body`, `exception`).
- **`_manifest.json`** — last run metadata and counts (when a run completes).
- Optional: **`crawl_skipped_resume.jsonl`** if `--log-skipped` is used.

**Images inside `news content.json` (when the article crawl succeeds):** upstream/newspaper-style JSON often includes **`top_img`** (string URL) and **`images`** (list of URLs). These are **candidates only** until you run a separate **image fetch** step: links may be trackers, duplicates, or dead. The consolidated table should carry the **chosen primary URL** (see §4) plus **availability flags**, not assume the file is training-ready.

**Limitations (document in thesis):** link rot, **403/404/503**, bot blocking, empty or parking-page “text”; many URLs will **not** yield usable bodies. **Twitter** engagement and user graphs are **not** collected by this script.

### 2.4 Record counts — minimal index CSVs (in clone)

Row counts are **data lines excluding the header** (one row ≈ one news item in the benchmark index).

| File | Rows |
|------|------:|
| `politifact_fake.csv` | 432 |
| `politifact_real.csv` | 624 |
| `gossipcop_fake.csv` | 5,323 |
| `gossipcop_real.csv` | 16,817 |
| **Total** | **23,196** |

**Refresh:** count lines in `pipeline/fakenewsnet/dataset/*.csv` (minus one header per file).

### 2.5 Crawl output — progress and `crawl_failures.jsonl` (snapshot)

The following figures are a **point-in-time snapshot** from this machine (**2026-03-28**). They **change** as `01_acquire_fakenewsnet_crawl.py` runs. Regenerate before reports or thesis submission.

**Successful article JSON files** (files named `news content.json` under `data/processed/fakenewsnet/`): **124** at snapshot time.

**Failure log** path: **`data/processed/fakenewsnet/crawl_failures.jsonl`**. Each line is one JSON object (append-only). Fields used for analysis:

| Field | Meaning |
|--------|---------|
| `ts` | UTC timestamp of the logged event |
| `reason` | `no_article` — crawler returned nothing (live site + Wayback); `empty_body` — parse succeeded but `text` empty (no file written); `exception` — unexpected error |
| `news_source` / `label` | `politifact` or `gossipcop`; `fake` or `real` |
| `news_id` | Story id (matches index CSV `id`) |
| `news_url` | URL attempted |

**Important:** the log records **events**, not necessarily unique stories. The same `news_id` can appear **more than once** if you re-run without `--resume` or retry failures, so prefer **unique `news_id`** counts for “how many distinct items failed at least once”.

**Snapshot summary** (from `crawl_failures.jsonl` at the same date):

| Metric | Value |
|--------|------:|
| Append events (lines in file) | 86 |
| Events with `reason == no_article` | 52 |
| Events with `reason == empty_body` | 34 |
| Events with `reason == exception` | 0 |
| Distinct `news_id` with ≥1 logged failure | 41 |
| Distinct `news_id` with `no_article` (at least once) | 27 |
| Distinct `news_id` with `empty_body` (at least once) | 14 |
| Source/label mix in this snapshot | all events `politifact/fake` (crawl had not finished other splits yet) |

**Analyse / clean the failure log (scripts in repo):** `pipeline/03_qa_fnn_dedupe_crawl_failures.py` (dedupe by `news_id`, keep latest `ts`). For tabular summaries or charts, use **`notebooks/fakenews_preprocessing_eda.ipynb`** or ad-hoc Python/pandas on `crawl_failures.jsonl`.

**Regenerate successful JSON count (PowerShell):**

```powershell
(Get-ChildItem "data\processed\fakenewsnet" -Recurse -Filter "news content.json").Count
```

When a full crawl finishes, **`data/processed/fakenewsnet/_manifest.json`** (written by the crawler) also holds **`counts.attempted`**, **`written_ok`**, **`failed`**, **`skipped_resume`** for that run.

---

## 3. Fakeddit — what exists on disk

### 3.1 Shallow Git clone (nested repo, gitignored)

- **Path:** `pipeline/fakeddit/` — scripts and README; **not** the large TSVs.

### 3.2 Processed download: v2 text/metadata (gitignored)

- **Path:** `data/processed/fakeddit/v2_text_metadata/` (from `pipeline/02_acquire_fakeddit_metadata.py` by default; corpus root **`data/processed/fakeddit/`**).

**Typical contents:**

- **`multimodal_only_samples/`** — `multimodal_train.tsv`, `multimodal_validate.tsv`, `multimodal_test_public.tsv` (image + text–oriented work; aligns with paper’s multimodal focus).
- **`all_samples (also includes non multimodal)/`** — `all_train.tsv`, `all_validate.tsv`, `all_test_public.tsv` (broader; includes rows without images).

**Important columns (multimodal track):** e.g. **`id`**, **`clean_title`**, **`title`**, **`image_url`**, **`hasImage`**, **`2_way_label`**, **`3_way_label`**, **`6_way_label`**, **`subreddit`**, etc. (exact set = file header). Treat **`image_url`** / **`hasImage`** as **metadata-level** pointers: they do not guarantee the URL still serves a decodable image until you download and validate.

**Official split:** encoded by **which file** a row comes from (`train` / `validate` / `test`). Keeping **`split_official`** in a consolidated table preserves **comparability** with published Fakeddit results.

**Not downloaded by default:** bulk image archive; comment TSVs (optional flags on the download script).

### 3.3 Record counts — v2 TSVs (snapshot **2026-03-28**)

Counts are **data rows excluding the header** per file.

**`multimodal_only_samples/`** (typical for text + `image_url` work):

| File | Rows |
|------|------:|
| `multimodal_train.tsv` | 564,183 |
| `multimodal_validate.tsv` | 59,364 |
| `multimodal_test_public.tsv` | 59,337 |
| **Total** | **682,884** |

**`all_samples (also includes non multimodal)/`**:

| File | Rows |
|------|------:|
| `all_train.tsv` | 878,528 |
| `all_validate.tsv` | 92,481 |
| `all_test_public.tsv` | 92,481 |
| **Total** | **1,063,490** |

**Refresh:** re-run line counts on your copies under `data/processed/fakeddit/v2_text_metadata/` after any re-download. If you used `data/fakeddit_dataset/`, a flat `data/processed/v2_text_metadata/`, or `data/fakenewsnet_collected/`, see **`pipeline/DATA_LAYOUT.md`** for clone locations and typical paths under `data/processed/`.

---

## 4. Consolidated training table (`data/fakenews.tsv`)

The working unified file is **`data/fakenews.tsv`** (typically **gitignored** at the project root because of size). It should follow **one row per sample** and a **`dataset`** column. Schema target:

| Column | Meaning |
|--------|---------|
| `dataset` | `fakeddit` \| `fakenewsnet` |
| `sample_id` | Stable ID, e.g. `fd:{id}` / `fnn:{source}:{label}:{id}` |
| `split_official` | Fakeddit: `train` \| `validation` \| `test` from source file; FakeNewsNet: **empty** (no author test split in minimal release) |
| `split_study` | Optional: **your** train/val/test for **joint** experiments only |
| `label_binary` | Project-wide 0/1 (define mapping, e.g. 1 = fake); Fakeddit from `2_way_label`; FNN from path `fake`/`real` |
| `label_fine` | Fakeddit: e.g. `6_way_label`; FNN: empty |
| `text` | Fakeddit: `clean_title`; FNN: crawled `text` with fallback to index `title` |
| `title_raw` | Original title where useful |
| `image_ref` | **Primary image URL** when available: Fakeddit from `image_url`; FNN from crawled JSON — prefer **`top_img`**, else first URL in **`images[]`** that passes a simple heuristic (e.g. skip obvious trackers/pixels if you implement one). Empty if no candidate. |
| `has_image_ref` | **Boolean:** `true` iff `image_ref` is non-empty after consolidation (metadata-level only; not proof the URL works). |
| `image_local_path` | Optional: relative path under the project (e.g. `data/.../images/...`) after your **image download** step succeeds; empty until then. |
| `image_download_ok` | Optional **boolean:** `true` once a file exists at `image_local_path` and passes basic checks (readable, min size, expected format). Populate in the image-ingestion script, not by hand. |
| `image_preprocessed_path` | Optional: relative path to a **materialised** training-oriented asset (e.g. fixed-size RGB crop) if you choose not to rely on on-the-fly transforms only; usually empty. |
| `image_training_ready` | Optional **boolean:** `true` when a preprocessed file exists at `image_preprocessed_path` (or define equivalently as “validated for backbone input” in your image script). Omit both if all resize/normalise happens in the dataloader. |
| `article_url` | FNN: `news_url` / JSON `url`; Fakeddit: optional |
| `domain` | FNN: `politifact` \| `gossipcop`; Fakeddit: e.g. `subreddit` or `domain` |
| `provenance` | Path or pointer to source TSV / JSON (audit) |

**Deprecated / redundant name:** if you keep a column literally called `image_url`, treat it as an alias of **`image_ref`** or drop it to avoid two different meanings. Prefer **`has_image_ref`** over raw upstream **`hasImage`** in the unified file so the definition is one place.

### 4.1 Image references, flags, and training preparation

- **Include `image_ref` whenever a URL can be chosen** from the source row (Fakeddit column or FNN `news content.json`). If there is no candidate, leave empty and set **`has_image_ref = false`**.
- **Separate “reference exists” from “usable for training”:** many URLs will 404, return HTML, or be 1×1 tracking pixels. Use **`image_download_ok`** (and optionally log failures to a JSONL like the article crawl) after you fetch bytes to disk.
- **Conversion for model input is a training-pipeline concern, not implied by flags:** at load time, typical steps include decode → **RGB** → resize/crop to backbone input (e.g. 224×224) → **normalisation** (ImageNet mean/std or backbone defaults). **`image_download_ok`** means you have a **valid raster file** (or raw bytes) suitable for those transforms; it does **not** mean tensors are prebuilt. You only need an extra column (e.g. `image_preprocessed_path`) or a **`image_training_ready`** boolean if you **materialise** fixed-size crops or tensor caches on disk for reproducibility or speed—otherwise keep preprocessing **deterministic in code** and avoid duplicating “ready for ResNet” state in the TSV.
- **Multimodal cohort definition:** for strict text+image experiments, filter rows with **`has_image_ref`** and evidence of a successful fetch (e.g. **`cohort_multimodal_image_ok`** / local path columns after the cohort pipeline — see §7). The **final gated training export** for this project is **`data/fake_news_final.tsv`** (validity score ≥ 75). Report **counts before and after** each gate per `dataset` in methods/limitations.

**Evaluation clarity:**

- **Fakeddit-only benchmark-style:** filter `dataset == fakeddit`, use **`split_official`** and the **public test** file as test.
- **Joint / cross-dataset:** assign **`split_study`** with a documented rule (e.g. stratify by `dataset`). This is **not** the same protocol as the official Fakeddit test unless you **also** report a row that holds the official test out unchanged.

---

## 5. What is “generated” vs “curated upstream”

| Artefact | Generated locally | From upstream only |
|----------|-------------------|---------------------|
| FNN index CSVs | No | Yes (in clone) |
| FNN `news content.json` tree | Yes (crawl) | — |
| FNN `crawl_failures.jsonl` | Yes | — |
| Fakeddit TSVs on Drive | No | Downloaded |
| `data/fakenews.tsv` (unified) | Yes — produced by **your** consolidation step (see §7) | Fakeddit TSVs + FNN crawl outputs |
| Enrichment columns (`image_option1_*`, `cohort_*`) | Yes — merge scripts in §7 | After cohort fetch + image validation |
| `data/fake_news_final.tsv` | Yes — `11_cohort_export_final_tsv.py` | Gated subset for training |

---

## 6. Citations (point to upstream READMEs / papers)

- **FakeNewsNet:** cite papers listed in `pipeline/fakenewsnet/README.md` (e.g. Shu et al., repository paper).
- **Fakeddit:** Nakamura et al.; links in `pipeline/fakeddit/README.md` and the dataset site/paper.

---

## 7. Related project files, scripts, and reproducibility

### 7.1 Authoritative index

- **`pipeline/DATA_PIPELINE_FILES_REFERENCE.md`** — table of **every** file under `pipeline/`, pipeline stages, data/output paths, and maintenance notes. **Start here** for cross-referencing.

### 7.2 Base table: `data/fakenews.tsv`

The unified TSV must match the **§4** schema (at least the columns you use for modelling). **Build or refresh** it with **`pipeline/04_consolidate_fakenews_tsv.py`** (`all` | `fakeddit` | `fakenewsnet`), which reads Fakeddit multimodal TSVs under `v2_text_metadata/` and FakeNewsNet `news content.json` trees (and index CSVs for URLs), and omits FNN rows listed in **`crawl_failures.jsonl`**. Alternatively, restore rows from a backed-up `fakenews.tsv`. **`01_acquire_fakenewsnet_crawl.py`** can run `04_consolidate_fakenews_tsv.py all` after a crawl when that script is present.

All **cohort** steps below **assume** `data/fakenews.tsv` already exists at the project root.

### 7.3 Scripts in `pipeline/` (by role)

Stage prefixes match **`pipeline/README.md`**. Names below are **basename only**; run as `python pipeline/<name>`.

**Corpus acquisition**

| Script | Role |
|--------|------|
| `01_acquire_fakenewsnet_crawl.py` | Crawl FNN article JSON under `data/processed/fakenewsnet/`. |
| `02_acquire_fakeddit_metadata.py` | Download Fakeddit v2 text/metadata from Drive (optional `--images`, `--comments`). |
| `requirements-fakenewsnet-crawl.txt` | Python deps for the FNN crawl on recent Python versions. |

**FNN crawl diagnostics**

| Script | Role |
|--------|------|
| `03_qa_fnn_dedupe_crawl_failures.py` | Dedupe `crawl_failures.jsonl` (latest `ts` per id). |

**Consolidation** (build unified `data/fakenews.tsv` once Fakeddit + FNN inputs exist)

| Script | Role |
|--------|------|
| `04_consolidate_fakenews_tsv.py` | Merge Fakeddit multimodal TSVs + FNN `news content.json` into `data/fakenews.tsv` (see §7.2). |

**Cohort multimodal pipeline** (primary path for the stratified ~50k image cohort)

| Script | Role |
|--------|------|
| `05_cohort_build_plan.py` | Build stratified plan TSV (e.g. `data/processed/cohorts/multimodal_plan_n50000_seed42.tsv`). |
| `06_cohort_fetch_images.py` | Download images for plan rows; append `data/processed/images/cohort_image_fetch.log`. Uses `cohort_reddit_placeholder_sha256.txt` (project root). |
| `07_cohort_dedupe_fetch_log.py` | Optional: dedupe cohort log after parallel mistakes. |
| `08_cohort_image_validation.py` | Heuristic image QC → `data/processed/cohorts/image_validation/` (`cohort_image_validation.tsv`). |
| `09_cohort_merge_image_validation_into_fakenews.py` | Adds `image_option1_*` columns to `data/fakenews.tsv`. |
| `10_cohort_merge_fetch_log_into_fakenews.py` | Adds `cohort_image_*` / `cohort_multimodal_image_ok` columns. |
| `11_cohort_export_final_tsv.py` | Writes `data/fake_news_final.tsv` (default: score ≥ 75). |

**Reporting / EDA (optional; see `reporting/README.md`)**

| Artefact | Role |
|----------|------|
| `notebooks/fakenews_preprocessing_eda.ipynb` | Single interactive notebook: full `fakenews.tsv` profiling, FNN vs crawl, Fakeddit source stats, image checks, cohort fetch log (§7), gated export `fake_news_final.tsv` (§8). |

### 7.4 Reproducible order (cohort multimodal, after `data/fakenews.tsv` exists)

Run from the project root (adjust paths if you change defaults):

1. **`python pipeline/05_cohort_build_plan.py`** — create/refresh the cohort plan TSV (set `--input`, `--n`, `--seed` as needed).
2. **`python pipeline/06_cohort_fetch_images.py`** — fetch until target successes (e.g. `--stop-after-ok 50000`). Uses plan + blocklist.
3. *(Optional)* **`python pipeline/07_cohort_dedupe_fetch_log.py`** — if the log contains duplicate `sample_id` lines.
4. **`python pipeline/08_cohort_image_validation.py`** — full validation sweep (`--resume` as needed); then **`python pipeline/08_cohort_image_validation.py --sort-only`** if you only need re-sorting.
5. **`python pipeline/09_cohort_merge_image_validation_into_fakenews.py`** — merge scores into `fakenews.tsv` (pass **`--no-backup`** to skip writing `*.image_validation_merge.bak`).
6. **`python pipeline/10_cohort_merge_fetch_log_into_fakenews.py`** — merge fetch paths/status into `fakenews.tsv` (pass **`--no-backup`** to skip `*.cohort_fetch_merge.bak`).
7. **`python pipeline/11_cohort_export_final_tsv.py`** — write `fake_news_final.tsv`.

*(Optional exploration after the relevant inputs exist:)* open **`notebooks/fakenews_preprocessing_eda.ipynb`** — see **`reporting/README.md`**.

**Data layout and clone commands:** **`DATA_LAYOUT.md`**.
