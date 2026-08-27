"""Registry fixtures that remain stable across the production activation lifecycle."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def draft_registry(repo_root: Path) -> dict[str, Any]:
    """Return the frozen registry definition in its pre-activation state."""

    registry = json.loads(
        (repo_root / "docs/research/registry/OPP-E07-V1.json").read_text(encoding="utf-8")
    )
    if not isinstance(registry, dict):
        raise TypeError("checked-in registry must be an object")
    registry["status"] = "draft"
    registry["activation"] = None
    return registry


def write_draft_registry(repo_root: Path, directory: Path) -> Path:
    """Write an isolated draft fixture without depending on deployed registry state."""

    path = directory / "OPP-E07-V1.draft.json"
    path.write_text(
        json.dumps(draft_registry(repo_root), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path
