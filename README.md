# Multimodal Fake News Detection on Social Media

This repository covers the end-to-end workflow for **multimodal (text + image)
fake-news detection** on two public corpora, **FakeNewsNet (FNN)** and
**Fakeddit**. It is organised into three stages: preparing the data, training the
models, and a proof-of-concept demo UI.

## Documentation Map

| Stage | Folder | What it covers |
|-------|--------|----------------|
| **Data preparation** | [`pipeline/`](pipeline/README.md) | Acquire FNN + Fakeddit, build the unified table, fetch/validate images, export the gated cohort TSVs. Runs locally. |
| **Model training** | [`training/`](training/README.md) | Text, image, and fusion baselines on the cohort TSVs. Runs in Google Colab. |
| **Demo UI** | [`ui/`](ui/README.md) | Gradio proof-of-concept for single text + image predictions. Runs locally. |

## Python environment

The pipeline and demo UI run locally from a virtual environment built at the
project root; model training runs in Colab (see [`training/README.md`](training/README.md)).

```bash
cd "/path/to/MSC Project"
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

In the IDE, point the Python interpreter at `.venv/bin/python` (macOS/Linux) or
`.venv\Scripts\python.exe` (Windows) if it is not detected automatically.

| Requirements file | Used for |
|-------------------|----------|
| `requirements.txt` | Shared / pipeline dependencies. |
| `requirements-fakenewsnet-crawl.txt` | The FakeNewsNet crawl (pipeline step `01`). |
| `ui/requirements.txt` | The Gradio demo UI. |
| `training/requirements.txt` | Pinned reference for the Colab notebooks (optional local editor env). |

## Upstream repositories

The two source corpora are cloned under `pipeline/` as nested Git repos
(gitignored):

| Path | Source |
|------|--------|
| `pipeline/fakenewsnet/` | [KaiDMML/FakeNewsNet](https://github.com/KaiDMML/FakeNewsNet) — minimal CSVs under `dataset/` |
| `pipeline/fakeddit/` | [entitize/Fakeddit](https://github.com/entitize/Fakeddit) — scripts; large TSVs from Google Drive per the upstream README |

Update the clones with `git -C pipeline/fakenewsnet pull` and
`git -C pipeline/fakeddit pull`.

## Repository notes

- **`data/`** starts empty in a fresh checkout; after running the pipeline it holds the root-level TSVs (e.g. `fakenews.tsv`) and `data/processed/` outputs. These are gitignored due to their size and have to be generated at run time.
