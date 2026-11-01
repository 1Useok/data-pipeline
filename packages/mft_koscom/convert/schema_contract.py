"""January-2026 price schema contracts used by every local writer."""

from __future__ import annotations

import polars as pl


S = pl.String
F = pl.Float64
RAW_DT = pl.Datetime("us", "Asia/Seoul")
BT_DT = pl.Datetime("ns", "Asia/Seoul")

RAW_SCHEMAS: dict[str, tuple[tuple[str, pl.DataType], ...]] = {
    "stock": (
        ("sec_code", S), ("trade_time", RAW_DT), ("open_price", F), ("high_price", F),
        ("low_price", F), ("close_price", F), ("trade_volume", F), ("cum_value", F),
        ("first_ask_price_best", F), ("first_bid_price_best", F),
        ("last_ask_price_best", F), ("last_bid_price_best", F),
    ),
    "future": (
        ("sec_code", S), ("trade_time", RAW_DT), ("open_price", F), ("high_price", F),
        ("low_price", F), ("close_price", F), ("trade_volume", F), ("cum_value", F),
    ),
    "option": (
        ("sec_code", S), ("trade_time", RAW_DT), ("open_price", F), ("high_price", F),
        ("low_price", F), ("close_price", F), ("trade_volume", F), ("cum_value", F),
    ),
    "continuous_future": (
        ("sec_code", S), ("trade_time", RAW_DT), ("open_price", F), ("high_price", F),
        ("low_price", F), ("close_price", F), ("trade_volume", F), ("cum_value", F),
        ("rollover_spread", F),
    ),
}

BT_SCHEMAS: dict[str, tuple[tuple[str, pl.DataType], ...]] = {
    "stock": (
        ("isin", S), ("ticker", S), ("datetime", BT_DT), ("open", F), ("high", F),
        ("low", F), ("close", F), ("volume", F), ("cum_value", F), ("adj_open", F),
        ("adj_high", F), ("adj_low", F), ("adj_close", F), ("cum_adj_ratio", F),
        ("first_ask", F), ("first_bid", F), ("last_ask", F), ("last_bid", F),
        ("adj_first_ask", F), ("adj_first_bid", F), ("adj_last_ask", F), ("adj_last_bid", F),
    ),
    "future": (
        ("isin", S), ("datetime", BT_DT), ("open", F), ("high", F), ("low", F),
        ("close", F), ("volume", F), ("cum_value", F),
    ),
    "option": (
        ("isin", S), ("datetime", BT_DT), ("open", F), ("high", F), ("low", F),
        ("close", F), ("volume", F), ("cum_value", F),
    ),
}


def _datetime_expression(df: pl.DataFrame, name: str, target: pl.Datetime) -> pl.Expr:
    source = df.schema[name]
    expr = pl.col(name)
    if source == pl.Date:
        expr = expr.cast(pl.Datetime(target.time_unit))
    elif isinstance(source, pl.Datetime):
        expr = expr.dt.cast_time_unit(target.time_unit)
    else:
        expr = expr.cast(pl.String).str.to_datetime(strict=False).dt.cast_time_unit(target.time_unit)

    source_zone = source.time_zone if isinstance(source, pl.Datetime) else None
    if source_zone:
        expr = expr.dt.convert_time_zone(target.time_zone)
    else:
        expr = expr.dt.replace_time_zone(target.time_zone)
    return expr.alias(name)


def _normalize(df: pl.DataFrame, schema: tuple[tuple[str, pl.DataType], ...]) -> pl.DataFrame:
    additions = [pl.lit(None, dtype=dtype).alias(name) for name, dtype in schema if name not in df.columns]
    if additions:
        df = df.with_columns(additions)
    expressions: list[pl.Expr] = []
    for name, dtype in schema:
        if isinstance(dtype, pl.Datetime):
            expressions.append(_datetime_expression(df, name, dtype))
        else:
            expressions.append(pl.col(name).cast(dtype, strict=False).alias(name))
    return df.select(expressions)


def normalize_raw_price_frame(df: pl.DataFrame, asset_type: str) -> pl.DataFrame:
    return _normalize(df, RAW_SCHEMAS[asset_type])


def normalize_backtesting_price_frame(df: pl.DataFrame, asset_type: str) -> pl.DataFrame:
    return _normalize(df, BT_SCHEMAS[asset_type])
