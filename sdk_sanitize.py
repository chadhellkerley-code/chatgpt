"""Helpers para sanear parámetros opcionales antes de llamar al SDK."""
from __future__ import annotations

import inspect
from typing import Any


def ensure_list(value: Any) -> list:
    """Devuelve una lista copia del valor dado."""

    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(value)
    if isinstance(value, str) and value.strip() == "":
        return []
    return [value]


def ensure_dict(value: Any) -> dict:
    """Normaliza valores opcionales a diccionarios."""

    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    return {}


def clean_kwargs(func: Any, **kwargs: Any) -> dict:
    """Filtra kwargs no soportados o con valor None para una función dada."""

    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):  # pragma: no cover - objetos sin firma
        signature = None

    allow_any = False
    allowed: set[str] | None = None
    if signature is not None:
        allowed = set()
        for param in signature.parameters.values():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                allow_any = True
            elif param.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                allowed.add(param.name)

    cleaned: dict[str, Any] = {}
    for key, value in kwargs.items():
        if value is None:
            continue
        if allowed is not None and key not in allowed:
            if not allow_any:
                continue
        cleaned[key] = value
    return cleaned
