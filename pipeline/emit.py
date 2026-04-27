"""Atomic JSON emitters for site/src/_data/*.json."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
DATA_DIR = REPO / "site" / "src" / "_data"


def write_json(name: str, payload: Any) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = DATA_DIR / f"{name}.json"
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=str(DATA_DIR), prefix=f".{name}.", suffix=".json.tmp"
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, target)
    finally:
        if Path(tmp_path).exists():
            try:
                Path(tmp_path).unlink()
            except Exception:
                pass
    return target
