"""Adaptadores para obtener reels mediante distintos métodos disponibles."""

from __future__ import annotations

import logging
from typing import Iterable, List, Sequence


logger = logging.getLogger(__name__)


def _invoke_with_amount(method, hashtag: str, amount: int):
    try:
        return method(hashtag, amount=amount)
    except TypeError:
        try:
            return method(hashtag, amount)
        except TypeError:
            try:
                return method(hashtag, count=amount)
            except TypeError:
                return method(hashtag)


def fetch_hashtag_reels(client, hashtag: str, limit: int) -> Sequence:
    """Devuelve reels filtrados para un hashtag usando la API disponible."""

    amount = max(limit * 3, 40)
    candidates: Iterable = ()

    for name in (
        "hashtag_medias_reels_v1",
        "hashtag_medias_reels",
        "hashtag_medias_recent",
        "hashtag_medias_recent_v1",
        "hashtag_medias",
        "hashtag_medias_v1",
    ):
        method = getattr(client, name, None)
        if not callable(method):
            continue
        try:
            result = _invoke_with_amount(method, hashtag, amount)
        except Exception as exc:  # pragma: no cover - depende de la API real
            logger.debug("Método %s falló para #%s: %s", name, hashtag, exc)
            continue
        if result:
            candidates = result
            break

    reels: List = []
    for media in candidates or []:
        product_type = getattr(media, "product_type", None)
        media_type = getattr(media, "media_type", None)

        product_label = product_type.lower() if isinstance(product_type, str) else ""
        if product_label in ("clips", "reel"):
            reels.append(media)
        elif media_type == 2:
            reels.append(media)
        elif isinstance(media_type, str) and media_type.lower() in ("clip", "video"):
            reels.append(media)

        if len(reels) >= limit:
            break

    return reels

