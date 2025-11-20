"""Functions to connect social products to e-commerce catalog entries."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Sequence

import pandas as pd
from rapidfuzz import fuzz, process


@dataclass
class MatchResult:
    """Represents a catalog match for a social product."""

    social_key: str
    catalog_id: str | int | None
    catalog_name: str | None
    score: float
    source: str


def _stringify(row: pd.Series, fields: Sequence[str]) -> str:
    parts = [str(row.get(field, "")) for field in fields if row.get(field, "")]
    return " ".join(parts)


def match_products_to_catalog(
    social_products: pd.DataFrame,
    catalog_df: pd.DataFrame,
    image_matches: Optional[pd.DataFrame] = None,
    similarity_threshold: int = 75,
) -> pd.DataFrame:
    """Match aggregated social products to catalog entries using text similarity.

    Args:
        social_products: Aggregated product dataframe with columns like
            ``product_key``, ``category``, ``brand``, ``model``.
        catalog_df: E-commerce catalog with ``product_name``, ``brand``,
            ``model``, ``category``. Optionally contains ``product_id``.
        image_matches: Optional dataframe mapping ``post_id`` to
            ``product_id`` from an external image-matching system.
        similarity_threshold: Minimum fuzzy match score (0-100) to accept
            a text-based match.

    Returns:
        Dataframe of social products with catalog match columns attached.
    """

    catalog_df = catalog_df.copy()
    if "product_id" not in catalog_df.columns:
        catalog_df["product_id"] = catalog_df.index.astype(str)

    catalog_df["match_name"] = catalog_df.apply(
        lambda row: _stringify(row, ["brand", "model", "product_name", "category"]),
        axis=1,
    )

    image_lookup: Dict[str, str] = {}
    if image_matches is not None:
        image_lookup = {
            str(row["post_id"]): str(row["product_id"]) for _, row in image_matches.iterrows()
        }

    match_results: list[MatchResult] = []
    for _, social_row in social_products.iterrows():
        social_name = _stringify(
            social_row, ["brand", "model", "category", "example_attributes", "product_key"]
        )
        social_key = str(social_row.get("product_key"))

        image_matched_catalog_id: Optional[str] = None
        for post_id in social_row.get("post_ids", []):
            if str(post_id) in image_lookup:
                image_matched_catalog_id = image_lookup[str(post_id)]
                break

        if image_matched_catalog_id:
            catalog_row = catalog_df.loc[catalog_df["product_id"] == image_matched_catalog_id].head(1)
            if not catalog_row.empty:
                match_results.append(
                    MatchResult(
                        social_key=social_key,
                        catalog_id=image_matched_catalog_id,
                        catalog_name=str(catalog_row.iloc[0]["product_name"]),
                        score=100.0,
                        source="image_match",
                    )
                )
                continue

        best_match = process.extractOne(
            social_name,
            catalog_df["match_name"],
            scorer=fuzz.token_set_ratio,
        )
        if best_match and best_match[1] >= similarity_threshold:
            matched_name, score, catalog_index = best_match
            match_results.append(
                MatchResult(
                    social_key=social_key,
                    catalog_id=str(catalog_df.iloc[catalog_index]["product_id"]),
                    catalog_name=str(catalog_df.iloc[catalog_index]["product_name"]),
                    score=float(score),
                    source="text_match",
                )
            )
        else:
            match_results.append(
                MatchResult(
                    social_key=social_key,
                    catalog_id=None,
                    catalog_name=None,
                    score=0.0,
                    source="no_match",
                )
            )

    matches_df = pd.DataFrame(match_results)
    return social_products.merge(matches_df, left_on="product_key", right_on="social_key", how="left")


def summarize_catalog_prices(catalog_df: pd.DataFrame) -> pd.DataFrame:
    """Compute price statistics for each catalog item."""

    if "product_id" not in catalog_df.columns:
        catalog_df = catalog_df.copy()
        catalog_df["product_id"] = catalog_df.index.astype(str)

    price_stats = (
        catalog_df.groupby("product_id")
        .agg(
            price_min=("price", "min"),
            price_median=("price", "median"),
            price_max=("price", "max"),
            avg_rating=("rating", "mean"),
            example_url=("url", "first"),
        )
        .reset_index()
    )
    return price_stats


def attach_price_info(matched_df: pd.DataFrame, catalog_price_stats: pd.DataFrame) -> pd.DataFrame:
    """Add price statistics to matched social products."""

    return matched_df.merge(catalog_price_stats, left_on="catalog_id", right_on="product_id", how="left")
