"""Build January-compatible higher-timeframe files from canonical 1-minute bars."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import polars as pl

from mft_common.storage import atomic_write_parquet, parquet_update_lock
from mft_koscom.convert.schema_contract import (
    normalize_backtesting_price_frame,
    normalize_raw_price_frame,
)


TIMEFRAMES = {"5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h", "1d": "1d"}


def _raw_aggregations(asset_type: str) -> list[pl.Expr]:
    result = [
        pl.col("open_price").first(),
        pl.col("high_price").max(),
        pl.col("low_price").min(),
        pl.col("close_price").last(),
        pl.col("trade_volume").sum(),
        pl.col("cum_value").last(),
    ]
    if asset_type == "stock":
        result.extend(
            [
                pl.col("first_ask_price_best").first(),
                pl.col("first_bid_price_best").first(),
                pl.col("last_ask_price_best").last(),
                pl.col("last_bid_price_best").last(),
            ]
        )
    return result


def resample_raw_frame(df: pl.DataFrame, asset_type: str, timeframe: str) -> pl.DataFrame:
    source = normalize_raw_price_frame(df, asset_type).sort(["sec_code", "trade_time"])
    result = source.group_by_dynamic(
        "trade_time",
        every=TIMEFRAMES[timeframe],
        group_by="sec_code",
        closed="left",
        label="left",
    ).agg(_raw_aggregations(asset_type))
    return normalize_raw_price_frame(result, asset_type).sort(["sec_code", "trade_time"])


def _backtesting_aggregations(asset_type: str) -> list[pl.Expr]:
    result = [
        pl.col("open").first(), pl.col("high").max(), pl.col("low").min(),
        pl.col("close").last(), pl.col("volume").sum(), pl.col("cum_value").last(),
    ]
    if asset_type == "stock":
        result.extend(
            [
                pl.col("adj_open").first(), pl.col("adj_high").max(),
                pl.col("adj_low").min(), pl.col("adj_close").last(),
                pl.col("cum_adj_ratio").last(),
                pl.col("first_ask").first(), pl.col("first_bid").first(),
                pl.col("last_ask").last(), pl.col("last_bid").last(),
                pl.col("adj_first_ask").first(), pl.col("adj_first_bid").first(),
                pl.col("adj_last_ask").last(), pl.col("adj_last_bid").last(),
            ]
        )
    return result


def resample_backtesting_frame(df: pl.DataFrame, asset_type: str, timeframe: str) -> pl.DataFrame:
    source = normalize_backtesting_price_frame(df, asset_type).sort(["isin", "datetime"])
    group_columns = ["isin", "ticker"] if asset_type == "stock" else ["isin"]
    result = source.group_by_dynamic(
        "datetime",
        every=TIMEFRAMES[timeframe],
        group_by=group_columns,
        closed="left",
        label="left",
    ).agg(_backtesting_aggregations(asset_type))
    return normalize_backtesting_price_frame(result, asset_type).sort(["isin", "datetime"])


def rebuild_higher_timeframes(
    data_root: str | Path | None = None,
    *,
    months: Iterable[str] | None = None,
) -> dict:
    root_value = data_root or os.environ.get("DATA_DB_PATH")
    if not root_value:
        raise ValueError("DATA_DB_PATH is required")
    root = Path(root_value)
    raw_price = root / "MFT_raw" / "KR" / "price"
    bt_price = root / "MFT_backtesting" / "KR" / "price"
    selected_months = list(months or sorted(path.stem for path in (raw_price / "1m" / "stock").glob("*.parquet")))
    files: list[dict] = []

    for month in selected_months:
        for asset in ("stock", "future", "option"):
            raw_source = raw_price / "1m" / asset / f"{month}.parquet"
            bt_source = bt_price / "1m" / asset / f"{month}.parquet"
            if raw_source.exists():
                raw_df = pl.read_parquet(raw_source)
                for timeframe in ("1h", "1d"):
                    output = raw_price / timeframe / asset / f"{month}.parquet"
                    output.parent.mkdir(parents=True, exist_ok=True)
                    frame = resample_raw_frame(raw_df, asset, timeframe)
                    with parquet_update_lock(output):
                        atomic_write_parquet(frame, output, compression="lz4")
                    files.append({"kind": "raw", "timeframe": timeframe, "asset": asset, "month": month, "rows": len(frame), "path": str(output)})
            if bt_source.exists():
                bt_df = pl.read_parquet(bt_source)
                for timeframe in TIMEFRAMES:
                    output = bt_price / timeframe / asset / f"{month}.parquet"
                    output.parent.mkdir(parents=True, exist_ok=True)
                    frame = resample_backtesting_frame(bt_df, asset, timeframe)
                    with parquet_update_lock(output):
                        atomic_write_parquet(frame, output, compression="lz4")
                    files.append({"kind": "backtesting", "timeframe": timeframe, "asset": asset, "month": month, "rows": len(frame), "path": str(output)})
    return {"status": "success", "file_count": len(files), "files": files}
