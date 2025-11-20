"""Input/output helpers for the newborn product research workflow."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd


def read_data(path: str | Path) -> pd.DataFrame:
    """Read CSV or JSON into a dataframe."""

    path = Path(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".json", ".jsonl"}:
        with path.open() as f:
            try:
                data = json.load(f)
                return pd.DataFrame(data)
            except json.JSONDecodeError:
                return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported file format: {path.suffix}")


def ensure_output_dir(path: str | Path) -> Path:
    """Create the output directory if it does not exist."""

    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_csv(df: pd.DataFrame, path: str | Path) -> None:
    """Persist dataframe to CSV with UTF-8 encoding."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def load_social_dataset(path: str | Path) -> pd.DataFrame:
    """Load the social media dataset."""

    df = read_data(path)
    expected_columns = {"post_id", "image_url", "caption", "hashtags", "likes", "comments", "timestamp"}
    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in social dataset: {missing}")
    return df


def load_catalog_dataset(path: str | Path) -> pd.DataFrame:
    """Load e-commerce catalog dataset."""

    df = read_data(path)
    expected_columns = {"product_name", "brand", "model", "category", "price", "currency", "url", "seller"}
    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in catalog dataset: {missing}")
    if "rating" not in df.columns:
        df["rating"] = None
    return df


def load_image_matches(path: Optional[str | Path]) -> Optional[pd.DataFrame]:
    """Load optional image matches dataset mapping post_id to product_id."""

    if path is None:
        return None
    df = read_data(path)
    expected_columns = {"post_id", "product_id"}
    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in image match dataset: {missing}")
    return df
