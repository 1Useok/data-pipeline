"""
Convert raw OHLCV data to backtesting format with adjusted prices.
Simplified local version — no Ray, no permission management.
"""

import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

import polars as pl

from mft_common.storage import atomic_write_parquet
from mft_koscom.convert.schema_contract import normalize_backtesting_price_frame

# Column renaming rules
COLUMN_RENAME_MAP: Dict[str, str] = {
    "sec_code": "isin",
    "trade_time": "datetime",
    "open_price": "open",
    "high_price": "high",
    "low_price": "low",
    "close_price": "close",
    "trade_volume": "volume",
    "cum_value": "cum_value",
    "first_ask_price_best": "first_ask",
    "first_bid_price_best": "first_bid",
    "last_ask_price_best": "last_ask",
    "last_bid_price_best": "last_bid",
    "누적수정비율": "cum_adj_ratio",
}

ASSET_TYPES_WITH_ADJ_RATIO: Set[str] = {"stock"}


def ensure_datetime_ns_precision(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure all datetime columns have nanosecond precision."""
    conversions = []
    for col_name, dtype in df.schema.items():
        if isinstance(dtype, pl.Datetime) and dtype.time_unit == "us":
            conversions.append(
                pl.col(col_name).dt.cast_time_unit("ns").alias(col_name)
            )
    if conversions:
        df = df.with_columns(conversions)
    return df


def get_available_months(price_dir: str, timeframe: str, asset_type: str) -> List[str]:
    """Get list of available months from price data directory."""
    asset_dir = os.path.join(price_dir, timeframe, asset_type)
    if not os.path.exists(asset_dir):
        return []
    months = []
    for file in os.listdir(asset_dir):
        if file.endswith(".parquet"):
            year_month = file.replace(".parquet", "")
            if len(year_month) == 7 and year_month[4] == "-":
                months.append(year_month)
    return sorted(months)


def load_stock_info(info_path: str) -> pl.DataFrame:
    """Load stock info for ticker mapping.

    Source parquet schema uses `isin` and `ticker`. Downstream joins key on
    `sec_code` (historical name) so we alias here rather than updating every call site.
    """
    if not os.path.exists(info_path):
        raise FileNotFoundError(f"Stock info file not found: {info_path}")
    return pl.read_parquet(info_path).select(
        pl.col("isin").alias("sec_code"),
        pl.col("ticker"),
    )


def load_krx_change_data(krx_path: str) -> pl.DataFrame:
    """Load KRX change data with adjustment ratios.

    Source parquet schema: `datetime, isin, cum_adj_ratio, ...`.
    Downstream expects `date, sec_code, 누적수정비율` — alias at load time so
    the join/column references in process_stock_month stay unchanged.
    """
    if not os.path.exists(krx_path):
        raise FileNotFoundError(f"KRX change file not found: {krx_path}")
    df = pl.read_parquet(krx_path)
    required_source_cols = ["datetime", "isin", "cum_adj_ratio"]
    missing = [c for c in required_source_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing columns in KRX change data: {missing}. "
            f"Found: {df.columns}"
        )
    return df.select(
        pl.col("datetime").dt.date().alias("date"),
        pl.col("isin").alias("sec_code"),
        pl.col("cum_adj_ratio").alias("누적수정비율"),
    )


def process_derivative_month(
    year_month: str,
    timeframe: str,
    asset_type: str,
    raw_dir: str,
    bt_dir: str,
) -> Dict:
    """Process one month's derivative data: simple column rename."""
    start_time = time.time()
    result = {"year_month": year_month, "timeframe": timeframe, "asset_type": asset_type, "status": "success", "rows": 0}

    try:
        price_path = os.path.join(raw_dir, timeframe, asset_type, f"{year_month}.parquet")
        if not os.path.exists(price_path):
            result["status"] = "skipped"
            return result

        price_df = pl.read_parquet(price_path)
        rename_cols = {old: new for old, new in COLUMN_RENAME_MAP.items() if old in price_df.columns and old != "누적수정비율"}
        price_df = price_df.rename(rename_cols)

        key_cols = ["isin", "datetime"]
        price_cols = ["open", "high", "low", "close", "volume", "cum_value"]
        bid_ask_cols = ["first_ask", "first_bid", "last_ask", "last_bid"]
        all_cols = key_cols + price_cols + bid_ask_cols
        final_cols = [c for c in all_cols if c in price_df.columns]
        remaining = [c for c in price_df.columns if c not in final_cols]
        final_cols.extend(remaining)

        price_df = price_df.select(final_cols).sort(["isin", "datetime"])
        price_df = ensure_datetime_ns_precision(price_df)
        price_df = normalize_backtesting_price_frame(price_df, asset_type)

        output_path = os.path.join(bt_dir, timeframe, asset_type, f"{year_month}.parquet")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        atomic_write_parquet(price_df, output_path, compression="lz4")

        result["rows"] = len(price_df)
        result["duration"] = time.time() - start_time
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result


def process_stock_month(
    year_month: str,
    timeframe: str,
    raw_dir: str,
    bt_dir: str,
    stock_info: pl.DataFrame,
    krx_change_dir: str,
) -> Dict:
    """Process one month's stock data: join with KRX adjustment ratios."""
    start_time = time.time()
    result = {"year_month": year_month, "timeframe": timeframe, "asset_type": "stock", "status": "success", "rows": 0, "warnings": []}

    try:
        price_path = os.path.join(raw_dir, timeframe, "stock", f"{year_month}.parquet")
        if not os.path.exists(price_path):
            result["status"] = "skipped"
            return result

        price_df = pl.read_parquet(price_path)

        krx_path = os.path.join(krx_change_dir, f"{year_month}.parquet")
        if not os.path.exists(krx_path):
            result["status"] = "skipped"
            result["error"] = f"KRX change file not found: {krx_path}"
            return result

        krx_df = load_krx_change_data(krx_path)

        price_df = price_df.with_columns(pl.col("trade_time").dt.date().alias("date"))
        joined_df = price_df.join(krx_df, on=["sec_code", "date"], how="left")

        missing_count = joined_df["누적수정비율"].is_null().sum()
        if missing_count > 0:
            result["warnings"].append(
                f"{missing_count} rows have no KRX adjustment ratio; using 1.0"
            )
            joined_df = joined_df.with_columns(pl.col("누적수정비율").fill_null(1.0))

        joined_df = joined_df.join(stock_info, on="sec_code", how="left", suffix="_info")
        if "ticker_info" in joined_df.columns:
            if "ticker" in joined_df.columns:
                joined_df = joined_df.with_columns(
                    pl.coalesce([pl.col("ticker"), pl.col("ticker_info")]).alias("ticker")
                ).drop("ticker_info")
            else:
                joined_df = joined_df.rename({"ticker_info": "ticker"})

        # Calculate adjusted prices BEFORE renaming
        orig_price_to_adj = {
            "open_price": "adj_open", "high_price": "adj_high",
            "low_price": "adj_low", "close_price": "adj_close",
            "first_ask_price_best": "adj_first_ask", "first_bid_price_best": "adj_first_bid",
            "last_ask_price_best": "adj_last_ask", "last_bid_price_best": "adj_last_bid",
        }
        adj_exprs = [
            (pl.col(old) * pl.col("누적수정비율")).alias(new)
            for old, new in orig_price_to_adj.items()
            if old in joined_df.columns
        ]
        if adj_exprs:
            joined_df = joined_df.with_columns(adj_exprs)

        rename_cols = {old: new for old, new in COLUMN_RENAME_MAP.items() if old in joined_df.columns}
        joined_df = joined_df.rename(rename_cols).drop("date")

        key_cols = ["isin", "ticker", "datetime"]
        price_cols = ["open", "high", "low", "close", "volume", "cum_value"]
        adj_cols = ["adj_open", "adj_high", "adj_low", "adj_close", "cum_adj_ratio"]
        bid_ask_cols = ["first_ask", "first_bid", "last_ask", "last_bid", "adj_first_ask", "adj_first_bid", "adj_last_ask", "adj_last_bid"]
        all_cols = key_cols + price_cols + adj_cols + bid_ask_cols
        final_cols = [c for c in all_cols if c in joined_df.columns]
        remaining = [c for c in joined_df.columns if c not in final_cols]
        final_cols.extend(remaining)

        joined_df = joined_df.select(final_cols).sort(["isin", "datetime"])
        joined_df = ensure_datetime_ns_precision(joined_df)
        joined_df = normalize_backtesting_price_frame(joined_df, "stock")

        output_path = os.path.join(bt_dir, timeframe, "stock", f"{year_month}.parquet")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        atomic_write_parquet(joined_df, output_path, compression="lz4")

        result["rows"] = len(joined_df)
        result["duration"] = time.time() - start_time
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result


def convert_to_backtesting(
    raw_dir: str,
    bt_dir: str,
    info_path: str,
    krx_change_dir: str,
    months: Optional[List[str]] = None,
    timeframes: Optional[List[str]] = None,
    asset_types: Optional[List[str]] = None,
) -> List[Dict]:
    """Convert raw data to backtesting format. Sequential processing."""
    timeframes = timeframes or ["1m"]
    asset_types = asset_types or ["stock", "future", "option"]

    stock_info = None
    if "stock" in asset_types:
        try:
            stock_info = load_stock_info(info_path)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"stock_info is required for stock conversion: {info_path}"
            ) from None

    results = []
    for tf in timeframes:
        for asset in asset_types:
            if asset == "stock":
                available = get_available_months(raw_dir, tf, "stock")
                process_months = months or available
                for ym in process_months:
                    if ym not in available:
                        results.append({
                            "year_month": ym,
                            "timeframe": tf,
                            "asset_type": asset,
                            "status": "skipped",
                            "rows": 0,
                            "error": "raw month file not found",
                        })
                        continue
                    r = process_stock_month(ym, tf, raw_dir, bt_dir, stock_info, krx_change_dir)
                    results.append(r)
                    print(f"  {tf}/{asset}/{ym}: {r['status']} ({r.get('rows', 0)} rows)")
            else:
                process_months = months or get_available_months(raw_dir, tf, asset)
                for ym in process_months:
                    r = process_derivative_month(ym, tf, asset, raw_dir, bt_dir)
                    results.append(r)
                    print(f"  {tf}/{asset}/{ym}: {r['status']} ({r.get('rows', 0)} rows)")

    return results
