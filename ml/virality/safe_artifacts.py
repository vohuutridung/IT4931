from __future__ import annotations

import os
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _artifact_roots() -> tuple[Path, ...]:
    raw_roots = os.getenv("ML_ARTIFACT_ROOTS", "ml,/app/ml,/tmp")
    roots: list[Path] = []
    for raw in raw_roots.split(","):
        value = raw.strip()
        if not value:
            continue
        path = Path(value)
        roots.append((path if path.is_absolute() else _PROJECT_ROOT / path).resolve())
    return tuple(roots)


def safe_pickle_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser().resolve()
    if any(candidate == root or root in candidate.parents for root in _artifact_roots()):
        return candidate
    raise ValueError(f"Refusing to load or write pickle artifact outside ML_ARTIFACT_ROOTS: {candidate}")
