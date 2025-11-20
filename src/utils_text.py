"""Text utilities for cleaning and fuzzy matching.

These helpers avoid any scraping or external network calls; they operate on
local text fields only.
"""
from __future__ import annotations

import re
from typing import Iterable, Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz, process


def normalize_text(value: Optional[str]) -> str:
    """Normalize text for downstream keyword and similarity checks."""

    if not isinstance(value, str):
        return ""
    cleaned = value.replace("\n", " ").strip().lower()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def contains_any(text: str, keywords: Iterable[str]) -> bool:
    """Return True if any keyword appears as a whole word in the text."""

    pattern = r"\\b(" + "|".join(re.escape(k.lower()) for k in keywords) + r")\\b"
    return bool(re.search(pattern, text))


def fuzzy_match_value(target: str, candidates: Iterable[str], threshold: int = 75) -> Tuple[Optional[str], int]:
    """Fuzzy match a target string against candidate strings.

    Returns the best matching candidate and its score if above the threshold;
    otherwise returns (None, 0).
    """

    if not target or not candidates:
        return None, 0
    result = process.extractOne(target, candidates, scorer=fuzz.QRatio)
    if result and result[1] >= threshold:
        return result[0], int(result[1])
    return None, 0


def fuzzy_match_product(
    search_text: str, catalog: pd.DataFrame, threshold: int = 70
) -> Tuple[Optional[str], int]:
    """Match free text to a product_id using catalog search blobs."""

    if not search_text or catalog.empty:
        return None, 0
    choices = catalog[["product_id", "search_blob"]].dropna()
    result = process.extractOne(search_text, choices["search_blob"], scorer=fuzz.WRatio)
    if result and result[1] >= threshold:
        best_blob = result[0]
        product_id = choices.loc[choices["search_blob"] == best_blob, "product_id"].iloc[0]
        return str(product_id), int(result[1])
    return None, 0
