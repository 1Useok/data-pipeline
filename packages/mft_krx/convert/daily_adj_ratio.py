"""Normalize KRX daily change files and recalculate adjustment ratios."""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl

from mft_common.storage import atomic_write_parquet, parquet_update_lock

OUTPUT_COLUMNS = [
    "datetime",
    "isin",
    "ticker",
    "korean_name",
    "base_price",
    "end_close",
    "change",
    "change_rate",
    "volume",
    "trading_value",
    "market",
    "adj_ratio",
    "cum_adj_ratio",
]

DAILY_RENAME = {
    "date": "datetime",
    "sec_code": "isin",
    "종목코드": "ticker",
    "종목명": "korean_name",
    "시작일 기준가": "base_price",
    "종료일 종가": "end_close",
    "대비": "change",
    "등락률": "change_rate",
    "거래량": "volume",
    "거래대금": "trading_value",
}


def _as_kst_datetime(df: pl.DataFrame, column: str = "datetime") -> pl.DataFrame:
    dtype = df.schema[column]
    if dtype == pl.Date:
        return df.with_columns(
            pl.col(column).cast(pl.Datetime("ns")).dt.replace_time_zone("Asia/Seoul")
        )
    if isinstance(dtype, pl.Datetime):
        expr = pl.col(column).dt.cast_time_unit("ns")
        if dtype.time_zone:
            expr = expr.dt.convert_time_zone("Asia/Seoul")
        else:
            expr = expr.dt.replace_time_zone("Asia/Seoul")
        return df.with_columns(expr.alias(column))
    return df.with_columns(
        pl.col(column)
        .cast(pl.String)
        .str.to_datetime(strict=False)
        .dt.cast_time_unit("ns")
        .dt.replace_time_zone("Asia/Seoul")
        .alias(column)
    )


def normalize_change_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize either a crawler daily frame or an existing monthly frame."""
    rename = {
        source: target
        for source, target in DAILY_RENAME.items()
        if source in df.columns and target not in df.columns
    }
    if rename:
        df = df.rename(rename)

    required = {"datetime", "isin", "ticker", "base_price", "end_close"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"KRX change frame missing columns {missing}; found={df.columns}")

    df = _as_kst_datetime(df)
    defaults: dict[str, pl.Expr] = {
        "korean_name": pl.lit(None, dtype=pl.String),
        "change": pl.lit(None, dtype=pl.Float64),
        "change_rate": pl.lit(None, dtype=pl.Float64),
        "volume": pl.lit(None, dtype=pl.Float64),
        "trading_value": pl.lit(None, dtype=pl.Float64),
        "market": pl.lit(None, dtype=pl.String),
        "adj_ratio": pl.lit(1.0),
        "cum_adj_ratio": pl.lit(1.0),
    }
    missing_exprs = [expr.alias(name) for name, expr in defaults.items() if name not in df.columns]
    if missing_exprs:
        df = df.with_columns(missing_exprs)

    return df.select(
        pl.col("datetime"),
        pl.col("isin").cast(pl.String),
        pl.col("ticker").cast(pl.String),
        pl.col("korean_name").cast(pl.String),
        pl.col("base_price").cast(pl.Float64, strict=False),
        pl.col("end_close").cast(pl.Float64, strict=False),
        pl.col("change").cast(pl.Float64, strict=False),
        pl.col("change_rate").cast(pl.Float64, strict=False),
        pl.col("volume").cast(pl.Float64, strict=False),
        pl.col("trading_value").cast(pl.Float64, strict=False),
        pl.col("market").cast(pl.String),
        pl.col("adj_ratio").cast(pl.Float64, strict=False),
        pl.col("cum_adj_ratio").cast(pl.Float64, strict=False),
    )


def calculate_adjustment_ratios(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate per-session and backward cumulative adjustment ratios.

    ``adj_ratio[t]`` is today's base price divided by the previous close.
    Historical prices before that session must be multiplied by the ratio, so
    ``cum_adj_ratio[t]`` is the product of all *future* session ratios.
    """
    if df.is_empty():
        return df
    df = df.sort(["isin", "datetime"])
    df = df.with_columns(
        pl.col("end_close").shift(1).over("isin").alias("_previous_close")
    ).with_columns(
        pl.when(
            pl.col("_previous_close").is_not_null()
            & (pl.col("_previous_close") > 0)
            & pl.col("base_price").is_not_null()
            & (pl.col("base_price") > 0)
        )
        .then(pl.col("base_price") / pl.col("_previous_close"))
        .otherwise(1.0)
        .alias("adj_ratio")
    )
    df = df.with_columns(
        (
            pl.col("adj_ratio").product().over("isin")
            / pl.col("adj_ratio").cum_prod().over("isin")
        ).alias("cum_adj_ratio")
    ).drop("_previous_close")
    return df.select(OUTPUT_COLUMNS)


def _load_sources(krx_root: Path, archive_root: Path) -> pl.DataFrame:
    output_dir = krx_root / "전종목등락률"
    sources: list[tuple[Path, int]] = []
    sources.extend((path, 0) for path in sorted(output_dir.glob("????-??.parquet")))
    for item in ("전종목등락률", "ETF전종목등락률"):
        # Legacy local runs wrote raw daily files into the backtesting tree.
        # Keep reading them during migration; new writers use MFT_archive.
        sources.extend(
            (path, 1)
            for path in sorted((krx_root / item).glob("????-??-??.parquet"))
        )
        sources.extend(
            (path, 1)
            for path in sorted((archive_root / item).glob("????-??-??.parquet"))
        )
    if not sources:
        raise FileNotFoundError(f"No KRX change parquet sources under {krx_root}")

    frames: list[pl.DataFrame] = []
    for path, priority in sources:
        frame = normalize_change_frame(pl.read_parquet(path)).with_columns(
            pl.lit(priority, dtype=pl.Int8).alias("_source_priority")
        )
        frames.append(frame)

    combined = pl.concat(frames, how="diagonal_relaxed")
    return (
        combined.sort(["isin", "datetime", "_source_priority"])
        .unique(subset=["datetime", "isin"], keep="last", maintain_order=True)
        .drop("_source_priority")
    )


def _ensure_stock_info(
    df: pl.DataFrame,
    info_path: Path,
    krx_root: Path,
    archive_root: Path,
) -> None:
    existing: pl.DataFrame | None = None
    if info_path.exists():
        schema = pl.read_parquet_schema(info_path)
        if not {"isin", "ticker"}.issubset(schema):
            raise ValueError(f"Existing stock_info has incompatible schema: {info_path}")
        existing = pl.read_parquet(info_path)

    frames = [
        df.sort(["isin", "datetime"])
        .group_by("isin", maintain_order=True)
        .agg(pl.col("ticker").last())
    ]
    for item in ("전종목시세", "ETF전종목시세"):
        daily_paths = list(sorted((krx_root / item).glob("????-??-??.parquet")))
        daily_paths.extend(sorted((archive_root / item).glob("????-??-??.parquet")))
        for daily_path in daily_paths:
            daily = pl.read_parquet(daily_path)
            isin_col = "isin" if "isin" in daily.columns else "sec_code"
            ticker_col = "ticker" if "ticker" in daily.columns else "종목코드"
            if isin_col in daily.columns and ticker_col in daily.columns:
                frames.append(
                    daily.select(
                        pl.col(isin_col).cast(pl.String).alias("isin"),
                        pl.col(ticker_col).cast(pl.String).alias("ticker"),
                    )
                )

    latest = (
        pl.concat(frames, how="diagonal_relaxed")
        .filter(pl.col("isin").is_not_null() & pl.col("ticker").is_not_null())
        .unique(subset=["isin"], keep="last", maintain_order=True)
        .with_columns(
            pl.when(pl.col("ticker").str.starts_with("A"))
            .then(pl.col("ticker"))
            .otherwise(pl.concat_str([pl.lit("A"), pl.col("ticker")]))
            .alias("ticker")
        )
        .sort("isin")
    )
    if existing is not None:
        # Preserve richer historical metadata while adding newly listed ISINs.
        latest = (
            pl.concat([existing, latest], how="diagonal_relaxed")
            .unique(subset=["isin"], keep="first", maintain_order=True)
            .sort("isin")
        )
    atomic_write_parquet(latest, info_path)


def full_recalculate(data_root: str | Path | None = None) -> dict:
    """Merge all available KRX daily files and atomically rewrite month files."""
    root_value = data_root or os.environ.get("DATA_DB_PATH")
    if not root_value:
        raise ValueError("DATA_DB_PATH is required")
    root = Path(root_value)

    krx_root = root / "MFT_backtesting" / "KR" / "krx_crawled"
    archive_root = root / "MFT_archive" / "KR" / "krx_crawled"
    output_dir = krx_root / "전종목등락률"
    output_dir.mkdir(parents=True, exist_ok=True)

    normalized = calculate_adjustment_ratios(_load_sources(krx_root, archive_root))
    months = normalized["datetime"].dt.strftime("%Y-%m").unique().sort().to_list()
    files: list[str] = []
    for year_month in months:
        month_df = normalized.filter(
            pl.col("datetime").dt.strftime("%Y-%m") == year_month
        )
        path = output_dir / f"{year_month}.parquet"
        with parquet_update_lock(path):
            atomic_write_parquet(month_df, path)
        files.append(str(path))

    info_path = root / "MFT_backtesting" / "KR" / "info" / "stock_info.parquet"
    _ensure_stock_info(normalized, info_path, krx_root, archive_root)
    return {"status": "success", "rows": len(normalized), "files": files}
