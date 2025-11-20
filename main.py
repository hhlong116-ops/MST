"""Pipeline entrypoint for newborn product market research.

This script assumes you already have CSV/JSON exports from official social
media APIs and e-commerce platforms. It avoids scraping HTML pages or
bypassing platform protections.

Usage example:
    python main.py \
        --social-data data/social_posts.csv \
        --catalog-data data/catalog.csv \
        --image-matches data/image_matches.csv \
        --output-dir output \
        --top-n 10

Replace the file paths with your datasets. If you use an API, plug the
responses into CSV/JSON files and point the arguments accordingly.
"""
from __future__ import annotations

import argparse
from typing import Optional

from src.analytics import (
    aggregate_product_popularity,
    compute_time_trends,
    prepare_social_products,
    rank_top_items,
    summarize_categories,
)
from src.io_utils import (
    ensure_output_dir,
    load_catalog_dataset,
    load_image_matches,
    load_social_dataset,
    write_csv,
)
from src.matching import attach_price_info, match_products_to_catalog, summarize_catalog_prices


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Newborn product market research pipeline")
    parser.add_argument("--social-data", required=True, help="Path to social media dataset (CSV/JSON)")
    parser.add_argument("--catalog-data", required=True, help="Path to e-commerce product catalog dataset")
    parser.add_argument(
        "--image-matches", help="Optional CSV/JSON mapping social post_id to catalog product_id from an image-matching service"
    )
    parser.add_argument("--output-dir", default="output", help="Directory to store pipeline outputs")
    parser.add_argument("--top-n", type=int, default=10, help="Number of top products/categories to include in summaries")
    return parser.parse_args()


def main(args: Optional[argparse.Namespace] = None) -> None:
    args = args or parse_args()

    output_dir = ensure_output_dir(args.output_dir)

    social_df = load_social_dataset(args.social_data)
    catalog_df = load_catalog_dataset(args.catalog_data)
    image_matches = load_image_matches(args.image_matches)

    social_enriched = prepare_social_products(social_df, catalog_df)
    product_popularity = aggregate_product_popularity(social_enriched)

    trends = compute_time_trends(social_enriched)
    product_popularity = product_popularity.merge(trends, on="product_key", how="left")

    matched = match_products_to_catalog(product_popularity, catalog_df, image_matches=image_matches)
    price_stats = summarize_catalog_prices(catalog_df)
    matched_with_prices = attach_price_info(matched, price_stats)

    detailed_output_path = output_dir / "product_popularity.csv"
    write_csv(matched_with_prices, detailed_output_path)

    top_products = rank_top_items(matched_with_prices, top_n=args.top_n)
    top_categories = summarize_categories(social_enriched, top_n=args.top_n)

    summary_output_path = output_dir / "top_products_summary.csv"
    category_output_path = output_dir / "top_categories_summary.csv"
    write_csv(top_products, summary_output_path)
    write_csv(top_categories, category_output_path)

    print("=== Top categories by volume and engagement ===")
    print(top_categories[["product_category", "post_count", "total_likes", "total_comments", "avg_engagement"]])

    print("\n=== Top products by engagement and posts ===")
    print(top_products[["product_key", "brand", "model", "category", "price_min", "price_median", "price_max", "engagement_score"]])

    print(f"\nDetailed product-level CSV saved to: {detailed_output_path}")
    print(f"Summary CSV saved to: {summary_output_path}")
    print(f"Category summary CSV saved to: {category_output_path}")


if __name__ == "__main__":
    main()
