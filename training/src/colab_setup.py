"""Shared Google Colab setup: GPU check, Drive TSVs, training helpers, image unzip.

Used by training_* notebooks. Copy this file to ``My Drive/training/`` with ``cohort_*.py``.

Dependency versions are pinned centrally in :data:`PINNED_DEPENDENCIES` and installed
via :func:`install_dependencies`, so every notebook resolves the same versions. ``torch``
/ ``torchvision`` are left to Colab's preinstalled, CUDA-matched build.

Typical notebook pattern (three cells)::

    # Cell A — identical in every Colab notebook (mount + copy training/)
    import shutil, sys
    from pathlib import Path
    from google.colab import drive
    drive.mount("/content/drive")
    PROJECT_ROOT = Path("/content/msc")
    TRAINING_SRC = Path("/content/drive/MyDrive/training")
    TRAINING = PROJECT_ROOT / "training"
    if not TRAINING_SRC.is_dir():
        raise FileNotFoundError("Sync repo training/ to My Drive/training/")
    if TRAINING.exists():
        shutil.rmtree(TRAINING)
    shutil.copytree(TRAINING_SRC, TRAINING)
    sys.path.insert(0, str(TRAINING / "src"))  # importable modules live in training/src/

    # Cell B — pinned dependencies (groups vary per notebook; base is implicit)
    from colab_setup import install_dependencies
    install_dependencies(["text", "image"])  # e.g. fusion; tfidf uses install_dependencies()

    # Cell C — per notebook (example: image + fusion)
    from colab_setup import require_cuda, setup_colab_project
    require_cuda()
    ctx = setup_colab_project(
        tsv_names=["fake_news_final_text.tsv", "fake_news_final_image.tsv"],
        need_images=True,
    )
    PROJECT_ROOT = ctx.project_root
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PROJECT_ROOT = Path("/content/msc")
DEFAULT_DRIVE_MY = Path("/content/drive/MyDrive")
DEFAULT_DRIVE_DATA = DEFAULT_DRIVE_MY / "data"
LOCAL_ZIP_COPY = Path("/content/images_local.zip")
MIN_JPG_COUNT = 1000

#: Single source of truth for pinned training dependencies, shared by every
#: ``training_*`` notebook. ``torch`` / ``torchvision`` are intentionally **absent**:
#: Colab ships a build matched to its GPU/CUDA drivers, so they are used as-is
#: (see :func:`require_cuda`); reinstalling them risks a CPU-only or mismatched wheel.
#: Bump a version here and it updates consistently across all notebooks.
PINNED_DEPENDENCIES: dict[str, str] = {
    "scikit-learn": "scikit-learn==1.5.2",
    "pandas": "pandas==2.2.2",
    "Pillow": "Pillow==11.1.0",
    "pillow-heif": "pillow-heif==0.21.0",
    "tqdm": "tqdm==4.66.5",
    "transformers": "transformers==4.44.2",
    "datasets": "datasets==2.21.0",
    "accelerate": "accelerate==0.34.2",
}

#: Per-notebook dependency sets, composed from :data:`PINNED_DEPENDENCIES` keys.
#: ``base`` is always installed; notebooks add ``text`` and/or ``image`` as needed.
DEPENDENCY_GROUPS: dict[str, tuple[str, ...]] = {
    "base": ("scikit-learn", "pandas"),
    "text": ("transformers", "datasets", "accelerate"),
    "image": ("Pillow", "pillow-heif", "tqdm"),
}


@dataclass
class ColabSetup:
    """Resolved paths and counts returned by :func:`setup_colab_project`.

    Attributes:
        project_root: Root the project was set up under (``/content/msc``).
        data_dir: The ``data/`` directory holding the copied TSVs.
        training_dir: The ``training/`` directory with the synced helper modules.
        images_dir: The directory holding the unzipped/linked images.
        image_count: Number of ``.jpg`` images available under ``images_dir``.
        tsv_paths: Mapping of TSV name to its copied path.
    """

    project_root: Path
    data_dir: Path
    training_dir: Path
    images_dir: Path
    image_count: int
    tsv_paths: dict[str, Path]


def require_cuda(*, strict: bool = True) -> bool:
    """Print GPU info; raise if CUDA unavailable and strict=True."""
    import torch

    ok = torch.cuda.is_available()
    if ok:
        print("CUDA: True | GPU:", torch.cuda.get_device_name(0))
        print("VRAM GB:", round(torch.cuda.get_device_properties(0).total_memory / 1e9, 1))
    else:
        msg = (
            "GPU not available. Runtime → Change runtime type → T4 GPU → "
            "Restart session, then re-run from the top."
        )
        if strict:
            raise RuntimeError(msg)
        print("WARNING:", msg)
    return ok


def register_image_codecs() -> None:
    """Register AVIF/HEIF with Pillow (one cohort file is AVIF with a ``.jpg`` name)."""
    try:
        import pillow_heif

        pillow_heif.register_heif_opener()
    except ImportError:
        pass


def report_versions(packages: Iterable[str] | None = None) -> dict[str, str]:
    """Print and return the resolved versions of the key training dependencies.

    Run after :func:`install_dependencies` so the exact versions used are captured
    in the notebook output as a reproducibility record. ``torch`` and ``torchvision``
    (provided by Colab) are always included.

    Args:
        packages: Distribution names to report. Defaults to all pinned dependencies.

    Returns:
        Mapping of distribution name to its resolved version (or ``"(not installed)"``).
    """
    from importlib.metadata import PackageNotFoundError, version

    names = list(packages) if packages is not None else list(PINNED_DEPENDENCIES)
    for extra in ("torch", "torchvision"):
        if extra not in names:
            names.append(extra)

    resolved: dict[str, str] = {}
    print("Resolved dependency versions:")
    for name in names:
        try:
            resolved[name] = version(name)
        except PackageNotFoundError:
            resolved[name] = "(not installed)"
        print(f"  {name:>14}: {resolved[name]}")
    return resolved


def install_dependencies(
    groups: Iterable[str] = (),
    *,
    quiet: bool = True,
    report: bool = True,
) -> dict[str, str]:
    """Install the pinned training dependencies for the requested groups.

    The ``base`` group (``scikit-learn``, ``pandas``) is always installed; pass extra
    groups (``"text"``, ``"image"``) per notebook. ``torch`` / ``torchvision`` are not
    installed here: Colab's preinstalled, CUDA-matched build is used as-is. All version
    pins live in :data:`PINNED_DEPENDENCIES`, so every notebook stays in sync.

    Args:
        groups: Extra dependency groups beyond ``base`` (keys of :data:`DEPENDENCY_GROUPS`).
        quiet: Pass ``-q`` to ``pip install``.
        report: Print and return the resolved versions afterwards (reproducibility record).

    Returns:
        Mapping of distribution name to resolved version when ``report`` is true, else ``{}``.

    Raises:
        KeyError: If a requested group is not defined in :data:`DEPENDENCY_GROUPS`.
        RuntimeError: If ``pip install`` exits non-zero.
    """
    keys: list[str] = []
    for group in ("base", *groups):
        if group not in DEPENDENCY_GROUPS:
            raise KeyError(
                f"Unknown dependency group {group!r}; known: {sorted(DEPENDENCY_GROUPS)}"
            )
        for key in DEPENDENCY_GROUPS[group]:
            if key not in keys:
                keys.append(key)

    specs = [PINNED_DEPENDENCIES[key] for key in keys]
    cmd = [sys.executable, "-m", "pip", "install"]
    if quiet:
        cmd.append("-q")
    cmd.extend(specs)

    print("Installing pinned dependencies:", " ".join(specs))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError("pip install failed:\n" + (proc.stdout + proc.stderr)[-2000:])

    if "image" in tuple(groups):
        register_image_codecs()

    return report_versions(keys) if report else {}


def _tsv_candidates(name: str, drive_data: Path, drive_my: Path) -> list[Path]:
    return [
        drive_data / name,
        drive_my / name,
        drive_my / "MSC Project" / "data" / name,
    ]


def copy_tsv_from_drive(
    name: str,
    project_root: Path,
    *,
    drive_data: Path = DEFAULT_DRIVE_DATA,
    drive_my: Path = DEFAULT_DRIVE_MY,
    allow_upload: bool = True,
) -> Path:
    """Copy one cohort TSV from Drive to project_root/data/."""
    dst = project_root / "data" / name
    dst.parent.mkdir(parents=True, exist_ok=True)
    src = next((p for p in _tsv_candidates(name, drive_data, drive_my) if p.is_file()), None)
    if src is not None:
        if not dst.is_file() or dst.stat().st_mtime < src.stat().st_mtime:
            shutil.copy2(src, dst)
        print("TSV:", dst)
        return dst
    if allow_upload:
        try:
            from google.colab import files
        except ImportError:
            raise FileNotFoundError(f"Add {name} to My Drive/data/") from None
        print(f"Not on Drive — upload {name}:")
        uploaded = files.upload()
        key = next(k for k in uploaded if k.endswith(".tsv"))
        dst.write_bytes(uploaded[key])
        print("TSV:", dst)
        return dst
    raise FileNotFoundError(f"Add {name} to {drive_data}")


def sync_training_helpers(
    project_root: Path = DEFAULT_PROJECT_ROOT,
    *,
    drive_training: Path | None = None,
    allow_upload: bool = True,
) -> Path:
    """Copy My Drive/training/ → project_root/training/ (modules under src/: cohort_*.py, colab_setup.py)."""
    src = drive_training or (DEFAULT_DRIVE_MY / "training")
    dst = project_root / "training"
    if src.is_dir():
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
        print("Training helpers:", dst)
        return dst
    if allow_upload:
        try:
            from google.colab import files
        except ImportError:
            raise FileNotFoundError(f"Copy training/ to {src}") from None
        print("Upload cohort_*.py and colab_setup.py from your local training/ folder:")
        uploaded = files.upload()
        dst.mkdir(parents=True, exist_ok=True)
        for name, data in uploaded.items():
            (dst / name).write_bytes(data)
        return dst
    raise FileNotFoundError(f"Copy repo training/ to {src}")


def _check_zip_file(path: Path) -> None:
    size_mb = path.stat().st_size / 1e6
    head = path.read_bytes()[:4]
    print(f"Zip candidate: {path} | {size_mb:.1f} MB | first bytes: {head!r}")
    if size_mb < 800:
        raise ValueError(
            f"{path.name} is only {size_mb:.1f} MB — expect ~1.1–1.3 GB. "
            "Re-upload the full zip from your local machine."
        )
    if head[:2] != b"PK":
        raise ValueError(f"{path.name} is not a ZIP (got header {head!r}).")


def _copy_zip_from_drive(drive_zip: Path) -> Path:
    want = drive_zip.stat().st_size
    local_zip = LOCAL_ZIP_COPY
    if local_zip.is_file() and local_zip.stat().st_size != want:
        print(f"Removing stale local zip ({local_zip.stat().st_size} vs Drive {want} bytes)")
        local_zip.unlink()
    if not local_zip.is_file() or local_zip.stat().st_size != want:
        print("Copying zip to Colab disk (~2–5 min)...", drive_zip)
        shutil.copy2(drive_zip, local_zip)
    print("Testing zip integrity (unzip -t)...")
    test = subprocess.run(["unzip", "-t", str(local_zip)], capture_output=True, text=True)
    if test.returncode != 0:
        tail = (test.stdout + test.stderr)[-2000:]
        raise RuntimeError(f"Zip failed integrity check:\n{tail}")
    print("Zip OK:", local_zip, f"({want / 1e9:.2f} GB)")
    return local_zip


def unzip_images_to_project(
    zip_path: Path,
    images_dir: Path,
    *,
    copy_zip_to_local: bool = True,
) -> int:
    """Unzip images.zip so *.jpg land in images_dir. Returns jpg count."""
    images_dir.mkdir(parents=True, exist_ok=True)
    marker = images_dir / ".colab_unzip_done"
    count = len(list(images_dir.glob("*.jpg")))
    if marker.is_file() and count > MIN_JPG_COUNT:
        print("Images already unzipped:", images_dir, f"({count} jpgs)")
        return count

    _check_zip_file(zip_path)
    if copy_zip_to_local:
        zip_path = _copy_zip_from_drive(zip_path)

    tmp = Path("/content/_images_unzip_tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)

    print("Unzipping with system unzip (~3–8 min)...", zip_path)
    proc = subprocess.run(
        ["unzip", "-q", "-o", str(zip_path), "-d", str(tmp)],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError("unzip failed:\n" + (proc.stdout + proc.stderr)[-2000:])

    jpgs = list(tmp.rglob("*.jpg"))
    if not jpgs:
        raise FileNotFoundError(f"No .jpg files found inside {zip_path}")

    first = jpgs[0]
    if first.parent.name == "images" and (tmp / "images").is_dir():
        src = tmp / "images"
    elif "processed" in first.parts and (tmp / "processed" / "images").is_dir():
        src = tmp / "processed" / "images"
    elif first.parent == tmp:
        src = tmp
    else:
        src = first.parent

    for p in src.glob("*.jpg"):
        dest = images_dir / p.name
        if not dest.is_file():
            shutil.copy2(p, dest)

    shutil.rmtree(tmp)
    marker.write_text("ok\n")
    count = len(list(images_dir.glob("*.jpg")))
    print("Unzipped", count, "images ->", images_dir)
    return count


def ensure_images(
    project_root: Path = DEFAULT_PROJECT_ROOT,
    *,
    drive_data: Path = DEFAULT_DRIVE_DATA,
    drive_my: Path = DEFAULT_DRIVE_MY,
) -> int:
    """Use Drive images folder, symlink, or unzip images.zip into project_root."""
    images_dir = project_root / "data/processed/images"
    drive_images = drive_data / "processed" / "images"
    drive_jpg = len(list(drive_images.glob("*.jpg"))) if drive_images.is_dir() else 0

    if drive_jpg > MIN_JPG_COUNT:
        print(f"Using unzipped folder on Drive ({drive_jpg} jpgs):", drive_images)
        images_dir.parent.mkdir(parents=True, exist_ok=True)
        if not images_dir.exists():
            images_dir.symlink_to(drive_images, target_is_directory=True)
            print("Linked", images_dir, "->", drive_images)
        (images_dir / ".colab_unzip_done").write_text("from_drive_folder\n")
        return len(list(images_dir.glob("*.jpg")))

    zip_candidates = [drive_data / "images.zip", drive_my / "images.zip"]
    images_zip = next((p for p in zip_candidates if p.is_file()), None)
    if images_zip is None:
        count = len(list(images_dir.glob("*.jpg"))) if images_dir.is_dir() else 0
        if count > MIN_JPG_COUNT:
            return count
        raise FileNotFoundError(
            f"Add images.zip to {drive_data} or unzipped folder {drive_images}"
        )
    return unzip_images_to_project(images_zip, images_dir)


def setup_colab_project(
    *,
    tsv_names: list[str],
    need_images: bool = False,
    project_root: Path = DEFAULT_PROJECT_ROOT,
    drive_data: Path = DEFAULT_DRIVE_DATA,
    drive_my: Path = DEFAULT_DRIVE_MY,
    allow_upload: bool = True,
) -> ColabSetup:
    """Copy TSVs (and optionally images) into project_root. Assumes Drive already mounted."""
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "data").mkdir(parents=True, exist_ok=True)

    tsv_paths: dict[str, Path] = {}
    for name in tsv_names:
        tsv_paths[name] = copy_tsv_from_drive(
            name,
            project_root,
            drive_data=drive_data,
            drive_my=drive_my,
            allow_upload=allow_upload,
        )

    training_dir = sync_training_helpers(
        project_root, allow_upload=allow_upload
    )
    src_dir = training_dir / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    image_count = 0
    images_dir = project_root / "data/processed/images"
    if need_images:
        register_image_codecs()
        image_count = ensure_images(project_root, drive_data=drive_data, drive_my=drive_my)
    elif images_dir.is_dir():
        image_count = len(list(images_dir.glob("*.jpg")))

    if need_images and image_count <= MIN_JPG_COUNT:
        raise FileNotFoundError(
            f"Expected >{MIN_JPG_COUNT} images under {images_dir}, got {image_count}"
        )

    sample = images_dir / "fd_2vkbtj.jpg"
    if need_images and not sample.is_file():
        sample = next(images_dir.glob("*.jpg"), None)
        assert sample and sample.is_file(), f"No sample jpg under {images_dir}"

    ctx = ColabSetup(
        project_root=project_root,
        data_dir=project_root / "data",
        training_dir=training_dir,
        images_dir=images_dir,
        image_count=image_count,
        tsv_paths=tsv_paths,
    )
    print("Setup OK:", ctx)
    return ctx


DEFAULT_DRIVE_RUNS = DEFAULT_DRIVE_MY / "runs"


def _drive_runs_sources(drive_my: Path = DEFAULT_DRIVE_MY) -> list[Path]:
    return [
        drive_my / "runs",
        drive_my / "MSC Project" / "runs",
    ]


def sync_runs_from_drive(
    project_root: Path = DEFAULT_PROJECT_ROOT,
    *,
    drive_my: Path = DEFAULT_DRIVE_MY,
) -> Path:
    """Copy ``My Drive/runs/`` into ``project_root/runs/`` (used by fusion notebook)."""
    runs_dst = project_root / "runs"
    runs_dst.mkdir(parents=True, exist_ok=True)
    synced = False
    for src in _drive_runs_sources(drive_my):
        if not src.is_dir():
            continue
        for child in src.iterdir():
            dst = runs_dst / child.name
            if child.is_dir():
                shutil.copytree(child, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(child, dst)
        print("Synced runs:", src, "→", runs_dst)
        synced = True
    if not synced:
        print(
            "No My Drive/runs/ yet — complete DistilBERT + ResNet notebooks once; "
            "they auto-save checkpoints to Drive."
        )
    return runs_dst


def persist_run_to_drive(
    local_run_dir: Path,
    *,
    drive_my: Path = DEFAULT_DRIVE_MY,
) -> Path:
    """Copy ``project_root/runs/{run_id}/`` to ``My Drive/runs/{run_id}/`` (persistent)."""
    local_run_dir = Path(local_run_dir)
    if not local_run_dir.is_dir():
        raise FileNotFoundError(f"Run folder not found: {local_run_dir}")
    dst = drive_my / "runs" / local_run_dir.name
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(local_run_dir, dst, dirs_exist_ok=True)
    print("Persisted to Drive:", dst)
    return dst
