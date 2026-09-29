from __future__ import annotations

import re
from collections.abc import Iterable

from .errors import AstraValidationError, clip

MAX_IDS_PER_REQUEST = 500
MAX_HISTORICAL_FEEDS = 100
MAX_IDS_PER_URL = 200

_HEX64 = re.compile(r"[0-9a-f]{64}")


def normalize_feed_id(feed_id: str) -> str:
    if not isinstance(feed_id, str):
        raise AstraValidationError("feed id must be a string")
    lower = feed_id.lower()
    hexpart = lower[2:] if lower.startswith("0x") else lower
    if not _HEX64.fullmatch(hexpart):
        raise AstraValidationError(f"invalid feed id: {clip(feed_id)!r}")
    return hexpart


def unique_feed_ids(ids: Iterable[str]) -> list[str]:
    if isinstance(ids, str):
        raise AstraValidationError("pass a list of feed ids, not a single string")
    return list(dict.fromkeys(normalize_feed_id(i) for i in ids))


def normalize_feed_ids(ids: Iterable[str], limit: int) -> list[str]:
    unique = unique_feed_ids(ids)
    if not unique:
        raise AstraValidationError("at least one feed id is required")
    if len(unique) > limit:
        raise AstraValidationError(f"at most {limit} distinct feed ids per request, got {len(unique)}")
    return unique
