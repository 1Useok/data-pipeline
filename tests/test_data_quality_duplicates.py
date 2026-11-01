"""Data-quality duplicate accounting regression tests."""

from datetime import datetime

import polars as pl
from pandas import Timestamp

import data_quality_check


def test_only_one_minute_prices_are_checked():
    assert data_quality_check.PRICE_TIMEFRAMES == ("1m",)


def test_duplicate_count_reports_excess_rows(tmp_path):
    path = tmp_path / "price.parquet"
    pl.DataFrame(
        {
            "trade_time": [datetime(2026, 7, 24, 9, 0)] * 2,
            "sec_code": ["KR4B01TEST01"] * 2,
            "open_price": [1.0, 1.0],
            "high_price": [1.0, 1.0],
            "low_price": [1.0, 1.0],
            "close_price": [1.0, 1.0],
            "trade_volume": [1, 1],
        }
    ).write_parquet(path)
    checks = []

    data_quality_check._check_price_day(
        checks,
        name="option",
        path=path,
        target=Timestamp("2026-07-24"),
        datetime_col="trade_time",
        id_col="sec_code",
        price_columns=("open_price", "high_price", "low_price", "close_price"),
        volume_col="trade_volume",
    )

    assert checks[0]["status"] == "FAIL"
    assert "dup=1" in checks[0]["actual"]


def test_null_price_and_negative_volume_fail_integrity(tmp_path):
    path = tmp_path / "invalid.parquet"
    pl.DataFrame(
        {
            "trade_time": [datetime(2026, 7, 24, 9, 0)],
            "sec_code": ["KR7005930003"],
            "open_price": [None],
            "high_price": [1.0],
            "low_price": [1.0],
            "close_price": [1.0],
            "trade_volume": [-1],
        },
        schema_overrides={"open_price": pl.Float64},
    ).write_parquet(path)
    checks = []

    data_quality_check._check_price_day(
        checks,
        name="stock",
        path=path,
        target=Timestamp("2026-07-24"),
        datetime_col="trade_time",
        id_col="sec_code",
        price_columns=("open_price", "high_price", "low_price", "close_price"),
        volume_col="trade_volume",
    )

    assert checks[0]["status"] == "FAIL"
    assert "invalid_values=1" in checks[0]["actual"]
    assert "negative_volume=1" in checks[0]["actual"]


def test_raw_bt_parity_detects_value_mismatch(tmp_path):
    raw = tmp_path / "raw.parquet"
    bt = tmp_path / "bt.parquet"
    when = datetime(2026, 7, 24, 9, 0)
    pl.DataFrame(
        {
            "trade_time": [when],
            "sec_code": ["KR7005930003"],
            "open_price": [1.0],
            "high_price": [2.0],
            "low_price": [1.0],
            "close_price": [2.0],
            "trade_volume": [10],
        }
    ).write_parquet(raw)
    pl.DataFrame(
        {
            "datetime": [when],
            "isin": ["KR7005930003"],
            "open": [1.0],
            "high": [2.0],
            "low": [1.0],
            "close": [1.5],
            "volume": [10],
        }
    ).write_parquet(bt)
    checks = []

    data_quality_check._check_raw_bt_parity(
        checks,
        asset="stock",
        raw_path=raw,
        bt_path=bt,
        target=Timestamp("2026-07-24"),
    )

    assert checks[0]["status"] == "FAIL"
    assert "value_mismatch=1" in checks[0]["actual"]
