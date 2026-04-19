# Fake news final cohort — breakdown summary

*Generated (UTC): `2026-04-19T11:21:23Z`*

## Source

- **Input:** `/Users/markphillips/Downloads/MSC Project/data/fake_news_final.tsv`
- **Rows:** **9** (validity-score gate: score ≥ 75, successful cohort fetch + QC)
- **Score implementation:** `pipeline/08_cohort_image_validation.py`

## How `image_option1_validity_score` is computed (heuristic image QC)

Scores are produced by **`pipeline/08_cohort_image_validation.py`** (post-download sweep). The value is an integer **1–100** heuristic: **higher ≈ more suitable as a static raster for a vision encoder**, not semantic correctness, aesthetic quality, or label veracity.

### Measured from each image file
- **Decode** with Pillow; record **format**, **width**, **height**.
- **Animation:** `n_frames > 1` → treated as animated (multi-frame GIF/WebP).
- **Aspect ratio:** `max(w,h)/min(w,h)`; flag **extreme_aspect** if ≥ 6.
- **Tiny image:** flag **tiny_image** if shorter side **&lt; 48** px.
- **Greyscale stats** (image resized so longest side ≤ `--max-side-stats`, default **512**):
  - **Histogram entropy** (256-bin, Shannon bits) on grey levels.
  - **Pixel variance**; flags **very_low_entropy** (&lt; 2.0) and **low_variance** (&lt; 80).

### Score assembly (then clamp 1–100)
Sum of five parts, then **caps**, then clamp:
1. **Resolution** (up to **24** pts): points from **shorter side** thresholds (e.g. ≥256, ≥224, …).
2. **Aspect** (up to **18** pts): narrower aspect ratios score higher.
3. **Static vs animated** (**18** if static, **0** if animated).
4. **Format** (up to **12** pts): JPEG/WebP highest; PNG/GIF lower; other/unknown lower still.
5. **Texture** (up to **30** pts, capped in code): entropy band + variance penalties.
**Caps (after the sum):** if **extreme_aspect** → score ≤ **55**; **tiny_image** → ≤ **35**; **animated** → ≤ **45**. Decode failure → **1**.

### What this validation does *not* do
- No OCR or text-on-image detection; no CLIP/semantic model; **`phash`** (if installed) is **not** part of the score.
- This cohort file keeps rows with **score ≥ 75** (`image_option1_training_eligible=true`).

## Documentation and literature context

### Internal project references
These artefacts record or implement the pipeline; **none** are a substitute for citing primary datasets in the thesis.

- **`pipeline/DATA_PIPELINE_FILES_REFERENCE.md`** — Tracked inventory of scripts and canonical output paths (always in git).
- **`pipeline/DATASETS_OVERVIEW.md`** — Unified TSV conventions: separate has_image_ref from image_download_ok / training-ready; multimodal cohort filtering expectations.
- **`documents/msc_decisions_log.md`** — Optional local notes (gitignored by default): cohort fetch scope, validity-score threshold, training eligibility decisions.
- **`documents/msc_proposal.tex`** — Optional local thesis/proposal (gitignored by default): SMART goals; risks on broken/missing images.
- **`pipeline/08_cohort_image_validation.py`** — Authoritative implementation of the heuristic validity score and buckets.
- **`data/processed/cohorts/image_validation/cohort_image_validation.tsv`** — Per-image audit trail (score, flags, entropy, variance, optional phash).

### Primary corpora (cite via your thesis bibliography)
- **FakeNewsNet (GossipCop, PolitiFact)** — Shu et al. (as in msc_proposal.tex / project bibliography).
- **Fakeddit** — Nakamura et al. (as in msc_proposal.tex / project bibliography).

### How this relates to the wider literature
Multimodal misinformation papers often report filtering missing text/image pairs, broken URLs, or obviously unusable media when building social benchmarks; they do not standardise this project's composite entropy/aspect/resolution score. Treat this heuristic as an explicit, reproducible engineering rule for this dissertation pipeline.

### Illustrative related work (not the source of the validity-score formula)
Recent work sometimes emphasises noisy or incomplete social-media multimodal inputs (e.g. Srivastava et al., 2025, CLIP-based detection in noisy environments — ICONAT); that line of work motivates rigorous data handling but is not the source of the validity-score formula.

*There is **no single paper** that defines this project's composite `validity_score`; describe it as your documented data-quality gate and point readers to the decision log + validation script.*

## Totals

| Metric | Value |
|--------|------:|
| Rows | 9 |
| `validity_score` min | 89 |
| `validity_score` max | 100 |
| `validity_score` mean | 97.22 |
| `validity_score` median | 100 |
| `validity_score` p10 / p90 | 89.0 / 100.0 |

## `validity_score` breakdown (this file)

*Distribution of `image_option1_validity_score` across the **gated** rows (all values here are ≥ 75).* 

| Score | Count | % of rows |
|------:|------:|----------:|
| 89 | 1 | **11.11%** |
| 92 | 1 | **11.11%** |
| 96 | 1 | **11.11%** |
| 98 | 1 | **11.11%** |
| 100 | 5 | **55.56%** |

### Score bands (this file)

| Band (inclusive) | Count | % of rows |
|-------------------|------:|----------:|
| 85–89 | 1 | **11.11%** |
| 90–94 | 1 | **11.11%** |
| 95–99 | 2 | **22.22%** |
| 100 | 5 | **55.56%** |

### By dataset (score summary)

| Dataset | n | min | max | mean | median |
|---------|--:|----:|----:|-----:|-------:|
| fakeddit | 9 | 89 | 100 | 97.22 | 100 |

## By dataset

| Dataset | Count | % of rows |
|---------|------:|----------:|
| fakeddit | 9 | **100.00%** |

## By `label_binary` (all rows)

*Convention: **0 = real**, **1 = fake**.*

| `label_binary` | Count | % of rows |
|----------------|------:|----------:|
| `0` | 4 | **44.44%** |
| `1` | 5 | **55.56%** |

## By dataset × `label_binary`

| Dataset | `0` (real) | % within dataset | `1` (fake) | % within dataset | Total |
|---------|------------:|-----------------:|-----------:|-----------------:|------:|
| fakeddit | 4 | **44.44%** | 5 | **55.56%** | 9 |

## Fakeddit: `split_official`

| split_official | Count | % of Fakeddit rows |
|----------------|------:|-------------------:|
| `train` | 6 | **66.67%** |
| `validation` | 3 | **33.33%** |

*Fakeddit row total: 9.*

## FakeNewsNet: news source (from `sample_id`)

Pattern `fnn:<source>:…` on `dataset=fakenewsnet` rows only.

| Source | Count | % of FNN rows |
|--------|------:|--------------:|

## Cohort fetch sanity (`cohort_multimodal_image_ok`)

| Value | Count |
|-------|------:|
| `(empty)` | 9 |
