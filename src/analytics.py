"""Analytics utilities for ranking products and detecting trends."""
from __future__ import annotations

from typing import Iterable

import pandas as pd

from .text_cleaning import clean_social_posts, normalize_text

# Map of product categories to keyword triggers. Expand with domain knowledge.
CATEGORY_KEYWORDS = {
    "stroller": ["stroller", "pram", "buggy"],
    "baby bottle": ["bottle", "feeding bottle", "silicone bottle"],
    "onesie": ["onesie", "bodysuit", "romper"],
    "crib": ["crib", "cot", "bassinet"],
    "diaper": ["diaper", "nappy", "pampers"],
    "carrier": ["carrier", "wrap", "sling"],
    "car seat": ["car seat", "infant seat"],
    "pacifier": ["pacifier", "dummy", "soother"],
    "toy": ["toy", "rattle", "teether"],
}


def make_product_key(row: pd.Series) -> str:
    """Build a stable product key for grouping."""

    return " | ".join([
        row.get("product_category", "unknown"),
        row.get("brand", "unknown"),
        row.get("model", ""),
    ]).strip()


def infer_category(text: str) -> str:
    """Infer a category from caption/hashtags using keyword search."""

    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return category
    return "unknown"


def detect_brand_and_model(text: str, known_brands: Iterable[str]) -> tuple[str | None, str | None]:
    """Detect brand and model names using known brands and capitalized patterns."""

    brand_found = None
    model_found = None
    for brand in known_brands:
        if normalize_text(brand) in text:
            brand_found = brand
            break

    capitalized_tokens = [token for token in text.split() if token.istitle() or token.isupper()]
    if capitalized_tokens:
        model_found = capitalized_tokens[0]

    return brand_found, model_found


def prepare_social_products(social_df: pd.DataFrame, catalog_df: pd.DataFrame) -> pd.DataFrame:
    """Clean raw posts and extract product signals."""

    social_df = clean_social_posts(social_df)
    known_brands = set(catalog_df["brand"].dropna().str.lower()) if "brand" in catalog_df.columns else set()

    def _extract(row: pd.Series):
        text = " ".join([row.get("caption_clean", ""), " ".join(row.get("hashtags_list", []))])
        category = infer_category(text)
        brand, model = detect_brand_and_model(text, known_brands)
        return pd.Series({
            "product_category": category,
            "brand": brand,
            "model": model,
        })

    social_df = social_df.join(social_df.apply(_extract, axis=1))
    social_df["brand"] = social_df["brand"].fillna("unknown")
    social_df["model"] = social_df["model"].fillna("")
    social_df["product_key"] = social_df.apply(make_product_key, axis=1)
    return social_df


def aggregate_product_popularity(social_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate social metrics at the product level."""

    social_df = social_df.copy()
    if "product_key" not in social_df.columns:
        social_df["product_key"] = social_df.apply(make_product_key, axis=1)

    grouped = (
        social_df.groupby("product_key")
        .agg(
            category=("product_category", "first"),
            brand=("brand", "first"),
            model=("model", "first"),
            example_attributes=("attributes", lambda x: ", ".join(sorted({a for attrs in x for a in attrs}))),
            post_count=("post_id", "nunique"),
            total_likes=("likes", "sum"),
            total_comments=("comments", "sum"),
            median_engagement=("likes", "median"),
            post_ids=("post_id", lambda ids: list(ids)),
        )
        .reset_index()
    )
    grouped["engagement_score"] = grouped["total_likes"] + grouped["total_comments"]
    return grouped


def compute_time_trends(social_df: pd.DataFrame, recent_months: int = 3, prior_months: int = 3) -> pd.DataFrame:
    """Calculate growth ratios for each product key."""

    df = social_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    latest_date = df["timestamp"].max()
    if pd.isna(latest_date):
        df["trend_growth"] = 0.0
        return df[["product_key", "trend_growth"]].drop_duplicates()

    recent_cutoff = latest_date - pd.DateOffset(months=recent_months)
    prior_cutoff = recent_cutoff - pd.DateOffset(months=prior_months)

    recent = df[df["timestamp"] >= recent_cutoff]
    prior = df[(df["timestamp"] >= prior_cutoff) & (df["timestamp"] < recent_cutoff)]

    recent_counts = recent.groupby("product_key").size().rename("recent_posts")
    prior_counts = prior.groupby("product_key").size().rename("prior_posts")

    trends = (
        pd.concat([recent_counts, prior_counts], axis=1)
        .fillna(0)
        .reset_index()
    )
    trends["trend_growth"] = (trends["recent_posts"] + 1) / (trends["prior_posts"] + 1)
    return trends[["product_key", "trend_growth"]]


def rank_top_items(products_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Return the top N products by engagement score."""

    return products_df.sort_values(by=["engagement_score", "post_count"], ascending=False).head(top_n)


def summarize_categories(social_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Return the top categories with engagement metrics."""

    summary = (
        social_df.groupby("product_category")
        .agg(
            post_count=("post_id", "nunique"),
            total_likes=("likes", "sum"),
            total_comments=("comments", "sum"),
            avg_engagement=("likes", "mean"),
        )
        .reset_index()
        .sort_values(by=["post_count", "total_likes"], ascending=False)
        .head(top_n)
    )
    return summary
