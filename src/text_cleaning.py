"""Utilities for cleaning and parsing social media text."""
from __future__ import annotations

import re
from typing import List, Set

import pandas as pd

# Common colors and sizes to pull from captions/hashtags. Extend as needed.
COLOR_KEYWORDS: Set[str] = {
    "white",
    "black",
    "blue",
    "pink",
    "green",
    "gray",
    "grey",
    "beige",
    "brown",
    "yellow",
    "purple",
    "red",
    "navy",
}
SIZE_KEYWORDS: Set[str] = {
    "newborn",
    "0-3m",
    "3-6m",
    "6-9m",
    "9-12m",
    "12m",
    "12-18m",
    "18m",
    "2t",
    "3t",
    "one size",
    "small",
    "medium",
    "large",
}
MATERIAL_KEYWORDS: Set[str] = {
    "cotton",
    "organic",
    "bamboo",
    "wool",
    "linen",
    "silicone",
    "glass",
    "stainless",
    "wood",
}


def normalize_text(text: str | float | None) -> str:
    """Normalize text for downstream keyword matching.

    Args:
        text: Raw caption or hashtag string.

    Returns:
        Lowercased text with line breaks removed. Empty string if the
        input is missing.
    """

    if not isinstance(text, str):
        return ""
    cleaned = text.replace("\n", " ").strip().lower()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def extract_hashtags(text: str) -> List[str]:
    """Extract hashtags from a caption or hashtag string."""

    return re.findall(r"#(\w+)", text)


def extract_attributes(text: str | float | None) -> Set[str]:
    """Extract useful product attributes such as color, size, material.

    The function uses simple keyword matching; feel free to expand the
    dictionaries above with domain-specific terms.
    """

    normalized = normalize_text(text)
    tokens = set(re.split(r"[^\w\-]+", normalized))

    attributes: Set[str] = set()
    for keyword in COLOR_KEYWORDS:
        if keyword in tokens:
            attributes.add(keyword)
    for keyword in SIZE_KEYWORDS:
        if keyword in normalized:
            attributes.add(keyword)
    for keyword in MATERIAL_KEYWORDS:
        if keyword in tokens:
            attributes.add(keyword)
    return attributes


def explode_hashtags(df: pd.DataFrame, hashtag_column: str = "hashtags") -> pd.DataFrame:
    """Split hashtags stored as comma-separated strings into lists."""

    df = df.copy()
    df[hashtag_column] = (
        df[hashtag_column]
        .fillna("")
        .apply(lambda x: [tag.strip() for tag in str(x).split(",") if tag.strip()])
    )
    return df


def clean_social_posts(df: pd.DataFrame) -> pd.DataFrame:
    """Apply common cleaning steps to social post data."""

    df = df.copy()
    df["caption_clean"] = df["caption"].apply(normalize_text)
    df["hashtags_list"] = df["hashtags"].fillna("").apply(extract_hashtags)
    df["attributes"] = df.apply(
        lambda row: extract_attributes(" ".join([row.get("caption", ""), " ".join(row.get("hashtags_list", []))])),
        axis=1,
    )
    return df
