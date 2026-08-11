# Multimodal Fake News Detection on Social Media

This repository covers the end-to-end workflow for **multimodal (text + image)
fake-news detection** on two public corpora, **FakeNewsNet (FNN)** and
**Fakeddit**. It is organised into three stages: preparing the data, training the
models, and a proof-of-concept demo Gradio UI.

## Getting started

### 1. Clone the repository

```bash
git clone https://github.com/mpconsultsai/msc-project-csc-40098.git
cd msc-project-csc-40098
```

(Skip the clone if you already have the project; open a terminal at the repository root.)

### 2. Run the Gradio PoC (assessors / demo)

This is the usual entry point for marking: load the submitted checkpoints and score text + image posts locally.

```bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r ui/requirements.txt

unzip msc-poc-model-weights.zip   # copy the submission zip to repo root first

.venv/bin/python ui/gradio-ui.py
```

Open the URL printed in the terminal (typically `http://127.0.0.1:7860`). Full walkthrough, Phase 1/2 examples, and troubleshooting: [`ui/README.md`](ui/README.md).

### 3. Reproduce data or training (optional)

| Goal | Guide |
|------|--------|
| Rebuild the cohort from FNN + Fakeddit | [`pipeline/README.md`](pipeline/README.md) — install `pipeline/requirements.txt`, run numbered scripts locally |
| Re-run baselines and fusion notebooks | [`training/README.md`](training/README.md) — cohort TSVs and `images.zip` on Google Drive; notebooks run in Colab |

The **pipeline** and **demo UI** run locally; **model training runs in Colab**. Each local stage has its own `requirements.txt` (see [Python environment](#python-environment) below).

## Documentation Map

| Stage | Folder | What it covers |
|-------|--------|----------------|
| **Data preparation** | [`pipeline/`](pipeline/README.md) | Acquire FNN + Fakeddit, build the unified table, fetch/validate images, export the gated cohort TSVs. Runs locally. |
| **Model training** | [`training/`](training/README.md) | Text, image, and fusion baselines on the cohort TSVs. Runs in Google Colab. |
| **Gradio PoC** | [`ui/`](ui/README.md) | Single text + image predictions. Runs locally. Model weights: unpack `msc-poc-model-weights.zip` at repo root (see `ui/README.md`). |

## Python environment

Create one virtual environment at the repository root and install the requirements file for whichever stage you are working on:

```bash
cd path/to/msc-project-csc-40098
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
