"""End-to-end data preparation for newborn product market research.

This script ONLY works on local CSV/JSON exports obtained via official APIs or
approved tools. It does **not** scrape websites or automate logins.

Run:
    python data_pipeline.py \
        --social-posts social_posts.csv \
        --products products_catalog.csv \
        --output aggregated_products.csv \
        --image-matches image_matches.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

from src.utils_text import contains_any, fuzzy_match_product, fuzzy_match_value, normalize_text

BABY_KEYWORDS = [
    "baby",
    "newborn",
    "infant",
    "stroller",
    "pram",
    "crib",
    "cot",
    "diaper",
    "nappy",
    "bottle",
    "pacifier",
    "onesie",
    "swaddle",
    "carrier",
    "sling",
    "car seat",
    "high chair",
]

CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    "stroller": ["stroller", "pram", "pushchair"],
    "crib": ["crib", "cot", "bassinet"],
    "baby bottle": ["bottle", "feeding bottle"],
    "pacifier": ["pacifier", "soother", "dummy"],
    "diaper": ["diaper", "nappy"],
    "onesie": ["onesie", "bodysuit"],
    "carrier": ["carrier", "wrap", "sling"],
    "car seat": ["car seat", "infant seat"],
    "high chair": ["high chair"],
}


REQUIRED_SOCIAL_COLUMNS = {
    "post_id",
    "image_id",
    "image_url",
    "caption",
    "hashtags",
    "likes",
    "comments",
    "posted_at",
    "platform",
}

REQUIRED_PRODUCT_COLUMNS = {
    "product_id",
    "product_name",
    "brand",
    "model",
    "category",
    "price",
    "currency",
    "url",
    "rating",
    "marketplace",
}

IMAGE_MATCH_COLUMNS = {"image_id", "product_id", "score"}


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)


def validate_columns(df: pd.DataFrame, required: Iterable[str], name: str) -> None:
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"{name} is missing required columns: {sorted(missing)}")


def clean_social(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["caption", "hashtags"]:
        df[col] = df[col].apply(normalize_text)
    df["likes"] = pd.to_numeric(df.get("likes"), errors="coerce").fillna(0).astype(int)
    df["comments"] = pd.to_numeric(df.get("comments"), errors="coerce").fillna(0).astype(int)
    df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce")
    df["text_blob"] = (df["caption"].fillna("") + " " + df["hashtags"].fillna("")).str.strip()
    return df


def filter_baby_posts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["is_baby_related"] = df["text_blob"].apply(lambda t: contains_any(t, BABY_KEYWORDS))
    return df[df["is_baby_related"]]


def infer_category(text: str) -> Optional[str]:
    for category, keywords in CATEGORY_KEYWORDS.items():
        if contains_any(text, keywords):
            return category
    return None


def infer_brand_model(row: pd.Series, catalog: pd.DataFrame, brand_threshold: int = 80, model_threshold: int = 75) -> pd.Series:
    text = row.get("text_blob", "")
    brand, brand_score = fuzzy_match_value(text, catalog["brand"].dropna().unique(), threshold=brand_threshold)
    model, model_score = fuzzy_match_value(text, catalog["model"].dropna().unique(), threshold=model_threshold)
    row["inferred_brand"] = brand
    row["inferred_brand_score"] = brand_score
    row["inferred_model"] = model
    row["inferred_model_score"] = model_score
    row["product_category"] = row.get("product_category") or infer_category(text)
    return row


def apply_image_matches(posts: pd.DataFrame, image_matches: Optional[pd.DataFrame]) -> pd.DataFrame:
    if image_matches is None or image_matches.empty:
        posts["product_id"] = np.nan
        return posts
    matched = posts.merge(image_matches, on="image_id", how="left", suffixes=("", "_img"))
    matched.rename(columns={"product_id": "product_id"}, inplace=True)
    return matched


def apply_text_matches(posts: pd.DataFrame, catalog: pd.DataFrame, threshold: int = 70) -> pd.DataFrame:
    posts = posts.copy()
    catalog = catalog.copy()
    catalog["search_blob"] = (
        catalog[["product_name", "brand", "model", "category"]]
        .fillna("")
        .agg(" ".join, axis=1)
        .apply(normalize_text)
    )

    unmatched_mask = posts["product_id"].isna()
    for idx in posts[unmatched_mask].index:
        search_text = posts.at[idx, "text_blob"]
        product_id, score = fuzzy_match_product(search_text, catalog, threshold=threshold)
        posts.at[idx, "product_id"] = product_id
        posts.at[idx, "text_match_score"] = score
    return posts


def aggregate_popularity(posts: pd.DataFrame) -> pd.DataFrame:
    posts = posts.copy()
    posts = posts.dropna(subset=["product_id"])
    now = pd.Timestamp.utcnow()
    recent_cutoff = now - pd.Timedelta(days=90)

    grouped = posts.groupby("product_id").agg(
        num_posts=("post_id", "nunique"),
        total_likes=("likes", "sum"),
        total_comments=("comments", "sum"),
        avg_likes=("likes", "mean"),
        avg_comments=("comments", "mean"),
        recent_post_count=("posted_at", lambda x: (x >= recent_cutoff).sum()),
    )
    grouped = grouped.reset_index()
    return grouped


def aggregate_prices(catalog: pd.DataFrame) -> pd.DataFrame:
    catalog = catalog.copy()
    catalog["price"] = pd.to_numeric(catalog["price"], errors="coerce")
    price_stats = catalog.groupby("product_id").agg(
        min_price=("price", "min"),
        max_price=("price", "max"),
        median_price=("price", "median"),
        avg_price=("price", "mean"),
        currency=("currency", "first"),
    )
    price_stats = price_stats.reset_index()

    url_samples = (
        catalog.sort_values("url")
        .groupby("product_id")
        .apply(lambda g: g["url"].dropna().unique()[:3])
        .reset_index(name="price_urls")
    )
    price_stats = price_stats.merge(url_samples, on="product_id", how="left")
    for i in range(3):
        price_stats[f"price_url_{i+1}"] = price_stats["price_urls"].apply(lambda urls: urls[i] if isinstance(urls, np.ndarray) and len(urls) > i else None)
    price_stats.drop(columns=["price_urls"], inplace=True)
    return price_stats


def build_product_dimension(catalog: pd.DataFrame) -> pd.DataFrame:
    deduped = catalog.sort_values("product_id").drop_duplicates("product_id")
    return deduped[["product_id", "product_name", "brand", "model", "category", "currency"]]


def pipeline(
    social_path: Path,
    products_path: Path,
    output_path: Path,
    image_match_path: Optional[Path] = None,
    keyword_list: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    social_df = read_csv(social_path)
    validate_columns(social_df, REQUIRED_SOCIAL_COLUMNS, "social_posts")
    products_df = read_csv(products_path)
    validate_columns(products_df, REQUIRED_PRODUCT_COLUMNS, "products_catalog")

    image_matches = None
    if image_match_path:
        image_matches = read_csv(image_match_path)
        validate_columns(image_matches, IMAGE_MATCH_COLUMNS, "image_matches")

    social_clean = clean_social(social_df)
    if keyword_list:
        global BABY_KEYWORDS
        BABY_KEYWORDS = list(keyword_list)
    baby_posts = filter_baby_posts(social_clean)

    baby_posts["product_category"] = baby_posts["text_blob"].apply(infer_category)
    baby_posts = baby_posts.apply(lambda row: infer_brand_model(row, products_df), axis=1)

    matched = apply_image_matches(baby_posts, image_matches)
    matched = apply_text_matches(matched, products_df)

    popularity = aggregate_popularity(matched)
    prices = aggregate_prices(products_df)
    product_dim = build_product_dimension(products_df)

    aggregated = popularity.merge(product_dim, on="product_id", how="left")
    aggregated = aggregated.merge(prices, on="product_id", how="left")

    aggregated.to_csv(output_path, index=False)
    return aggregated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare aggregated newborn product data")
    parser.add_argument("--social-posts", required=True, help="Path to social_posts.csv")
    parser.add_argument("--products", required=True, help="Path to products_catalog.csv")
    parser.add_argument("--output", default="aggregated_products.csv", help="Output CSV path")
    parser.add_argument("--image-matches", help="Optional image_matches.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    aggregated = pipeline(
        social_path=Path(args.social_posts),
        products_path=Path(args.products),
        output_path=Path(args.output),
        image_match_path=Path(args.image_matches) if args.image_matches else None,
    )
    print(f"Saved aggregated dataset to {args.output}. {len(aggregated)} products included.")


if __name__ == "__main__":
    main()
