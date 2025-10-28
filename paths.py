# paths.py
# -*- coding: utf-8 -*-
"""Utilities to resolve runtime-dependent storage directories."""

from __future__ import annotations

import os
from pathlib import Path


def runtime_base(default: Path) -> Path:
    """Return the directory that should be used for writable assets.

    Client builds run from temporary locations, so we allow overriding the
    default module directory via the ``APP_DATA_ROOT`` environment variable.
    When the override is present we ensure the directory exists and fall back
    to ``default`` if anything fails.
    """

    override = os.environ.get("APP_DATA_ROOT")
    if override:
        try:
            path = Path(override).expanduser()
            path.mkdir(parents=True, exist_ok=True)
            return path
        except Exception:
            return default
    return default
