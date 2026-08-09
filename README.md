# Multimodal Fake News Detection on Social Media

This repository covers the end-to-end workflow for **multimodal (text + image)
fake-news detection** on two public corpora, **FakeNewsNet (FNN)** and
**Fakeddit**. It is organised into three stages: preparing the data, training the
models, and a proof-of-concept demo Gradio UI.

## Documentation Map

| Stage | Folder | What it covers |
|-------|--------|----------------|
| **Data preparation** | [`pipeline/`](pipeline/README.md) | Acquire FNN + Fakeddit, build the unified table, fetch/validate images, export the gated cohort TSVs. Runs locally. |
| **Model training** | [`training/`](training/README.md) | Text, image, and fusion baselines on the cohort TSVs. Runs in Google Colab. |
| **Gradio PoC** | [`ui/`](ui/README.md) | Single text + image predictions. Runs locally. Model weights: unpack `msc-poc-model-weights.zip` at repo root (see `ui/README.md`). |

## Python environment

The **pipeline** and **demo UI** run locally; **model training runs in Colab**
(see [`training/README.md`](training/README.md)). Each stage has its own
self-contained `requirements.txt`, so create a virtual environment and install
the file for whichever stage you are working on:

```bash
cd "/path/to/MSC Project"
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r pipeline/requirements.txt   # data preparation
pip install -r ui/requirements.txt         # demo UI
```

In the IDE, point the Python interpreter at `.venv/bin/python` (macOS/Linux) or
`.venv\Scripts\python.exe` (Windows) if it is not detected automatically.

| Requirements file | Used for |
|-------------------|----------|
| `pipeline/requirements.txt` | Data-preparation scripts and the EDA notebook (incl. the FakeNewsNet crawl, step `01`). |
| `ui/requirements.txt` | The Gradio PoC. |

Training has no requirements file — the Colab notebooks install version-pinned
dependencies from a single source of truth (`colab_setup.PINNED_DEPENDENCIES`) via
`install_dependencies([...])`, leaving `torch`/`torchvision` to Colab's preinstalled
build (see [`training/README.md`](training/README.md)).

## Upstream repositories

The two source corpora should be cloned under `pipeline/sources/` as nested Git repositories. These are external to this repository but are required to produce the final cohort dataset.

| Path | Source |
|------|--------|
| `pipeline/sources/fakenewsnet/` | [KaiDMML/FakeNewsNet](https://github.com/KaiDMML/FakeNewsNet) — minimal CSVs under `dataset/` |
| `pipeline/sources/fakeddit/` | [entitize/Fakeddit](https://github.com/entitize/Fakeddit) — scripts; large TSVs from Google Drive per the upstream README |

Update the clones with `git -C pipeline/sources/fakenewsnet pull` and
`git -C pipeline/sources/fakeddit pull`.

## Repository notes

- **`data/`** starts empty in a fresh checkout; after running the pipeline it holds the root-level TSVs (e.g. `fakenews.tsv`) and `data/processed/` outputs. These are gitignored due to their size and have to be generated at run time.
