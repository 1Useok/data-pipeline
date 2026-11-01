"""
Continuous Future Data Generator.

Generates continuous futures data from individual contract data by selecting
the nearest month contract (alphabetically first ISIN) for each trading day.

Algorithm:
1. Read monthly individual contract data from MFT_raw/.../future/
2. For each date and product:
   - Filter contracts by ISIN prefix
   - Select alphabetically first contract (nearest month)
   - Keep all minute bars for that contract
3. Rename sec_code from ISIN to product name
4. Merge with existing data, deduplicate by (sec_code, trade_time)
5. Save to MFT_raw/.../continuous_future/
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import polars as pl

from mft_common.storage import atomic_write_parquet
from mft_koscom.convert.schema_contract import normalize_raw_price_frame

from .products import PRODUCTS, FutureProduct, get_all_products

logger = logging.getLogger(__name__)


def select_nearest_month_contract(
    df: pl.DataFrame,
    product: FutureProduct,
) -> pl.DataFrame:
    """
    Select nearest month contract data for a product.

    For each unique date, selects the alphabetically first contract
    (which corresponds to the nearest expiration month in KRX ISIN encoding).

    Args:
        df: DataFrame with individual contract data (must have sec_code, trade_time)
        product: Product definition with ISIN prefix

    Returns:
        DataFrame with only nearest month contract data, sec_code renamed to product name
    """
    # Filter by ISIN prefix
    filtered = df.filter(pl.col("sec_code").str.starts_with(product.isin_prefix))

    if filtered.is_empty():
        return pl.DataFrame(schema=df.schema)

    # Extract date from trade_time
    filtered = filtered.with_columns(
        pl.col("trade_time").dt.date().alias("_date")
    )

    # For each date, find the alphabetically first contract (nearest month)
    nearest_contracts = (
        filtered.group_by("_date")
        .agg(pl.col("sec_code").sort().first().alias("_nearest_contract"))
    )

    # Join back to get only nearest month data
    result = filtered.join(nearest_contracts, on="_date", how="inner")
    result = result.filter(pl.col("sec_code") == pl.col("_nearest_contract"))

    # Drop helper columns
    result = result.drop(["_date", "_nearest_contract"])

    # Rename sec_code to product name
    result = result.with_columns(pl.lit(product.sec_code).alias("sec_code"))

    return result


def process_month(
    source_path: Path,
    products: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Process a single month's data for all products.

    Args:
        source_path: Path to source parquet file (individual contracts)
        products: List of product keys to process (None = all)

    Returns:
        Combined DataFrame with continuous future data for all products
    """
    if not source_path.exists():
        logger.warning(f"Source file not found: {source_path}")
        return pl.DataFrame()

    # Read source data
    df = pl.read_parquet(source_path)

    if df.is_empty():
        logger.warning(f"Source file is empty: {source_path}")
        return pl.DataFrame()

    logger.info(f"Read {len(df):,} rows from {source_path.name}")

    # Determine which products to process
    product_keys = products or list(PRODUCTS.keys())
    product_list = [PRODUCTS[k] for k in product_keys if k in PRODUCTS]

    # Process each product
    results = []
    for product in product_list:
        result = select_nearest_month_contract(df, product)
        if not result.is_empty():
            logger.info(f"  {product.sec_code}: {len(result):,} rows")
            results.append(result)

    if not results:
        return pl.DataFrame()

    # Combine all products
    combined = pl.concat(results, how="diagonal_relaxed")

    return combined


def merge_and_deduplicate(
    new_data: pl.DataFrame,
    existing_path: Path,
) -> pl.DataFrame:
    """
    Merge new data with existing file and deduplicate.

    Args:
        new_data: New continuous future data
        existing_path: Path to existing parquet file

    Returns:
        Merged and deduplicated DataFrame
    """
    new_data = normalize_raw_price_frame(new_data, "continuous_future")
    if existing_path.exists():
        existing = normalize_raw_price_frame(pl.read_parquet(existing_path), "continuous_future")
        logger.info(f"Merging with existing {len(existing):,} rows")

        # Combine and deduplicate
        combined = pl.concat([existing, new_data], how="vertical_relaxed")
        combined = combined.unique(subset=["sec_code", "trade_time"], keep="last")
    else:
        combined = new_data

    # Sort by trade_time
    combined = combined.sort("trade_time")

    return combined


def generate_continuous_future(
    source_dir: Path,
    target_dir: Path,
    year_months: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Generate continuous future data from individual contract data.

    Args:
        source_dir: Directory containing individual contract data (future/)
        target_dir: Directory to save continuous future data (continuous_future/)
        year_months: List of year-months to process (e.g., ["2024-01", "2024-02"])
                    If None, processes all available months
        products: List of product keys to process (e.g., ["kospi", "usd"])
                 If None, processes all products
        dry_run: If True, don't write files

    Returns:
        Dictionary mapping year-month to rows processed
    """
    source_dir = Path(source_dir)
    target_dir = Path(target_dir)

    # Find source files
    if year_months:
        source_files = [source_dir / f"{ym}.parquet" for ym in year_months]
        source_files = [f for f in source_files if f.exists()]
    else:
        source_files = sorted(source_dir.glob("*.parquet"))

    if not source_files:
        logger.warning(f"No source files found in {source_dir}")
        return {}

    logger.info(f"Processing {len(source_files)} months")
    logger.info(f"Products: {products or 'all'}")

    # Create target directory
    if not dry_run:
        target_dir.mkdir(parents=True, exist_ok=True)

    results = {}

    for source_path in source_files:
        year_month = source_path.stem  # e.g., "2024-01"
        target_path = target_dir / f"{year_month}.parquet"

        logger.info(f"\n[{year_month}] Processing...")

        # Process month
        new_data = process_month(source_path, products)

        if new_data.is_empty():
            logger.info(f"[{year_month}] No data generated")
            results[year_month] = 0
            continue

        # Merge with existing
        merged = merge_and_deduplicate(new_data, target_path)

        if dry_run:
            logger.info(f"[{year_month}] DRY RUN: Would save {len(merged):,} rows")
        else:
            atomic_write_parquet(merged, target_path, compression="lz4")
            logger.info(f"[{year_month}] Saved {len(merged):,} rows to {target_path}")

        results[year_month] = len(merged)

    return results


def get_available_months(source_dir: Path) -> List[str]:
    """Get list of available year-months in source directory."""
    source_dir = Path(source_dir)
    files = sorted(source_dir.glob("*.parquet"))
    return [f.stem for f in files]


def validate_data(
    target_dir: Path,
    year_month: str,
) -> Tuple[bool, str]:
    """
    Validate continuous future data for a month.

    Returns:
        Tuple of (is_valid, message)
    """
    target_path = Path(target_dir) / f"{year_month}.parquet"

    if not target_path.exists():
        return False, f"File not found: {target_path}"

    df = pl.read_parquet(target_path)

    if df.is_empty():
        return False, "File is empty"

    # Check expected products
    sec_codes = df["sec_code"].unique().to_list()
    expected = {p.sec_code for p in get_all_products()}
    missing = expected - set(sec_codes)

    if missing:
        return False, f"Missing products: {missing}"

    # Check for duplicates
    dup_count = len(df) - len(df.unique(subset=["sec_code", "trade_time"]))
    if dup_count > 0:
        return False, f"Found {dup_count} duplicate rows"

    return True, f"Valid: {len(df):,} rows, products: {sec_codes}"
