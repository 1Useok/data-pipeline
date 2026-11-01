from datetime import datetime

import polars as pl

from mft_koscom.convert.schema_contract import (
    BT_SCHEMAS,
    RAW_SCHEMAS,
    normalize_raw_price_frame,
)
from mft_koscom.resample import resample_raw_frame
from mft_krx.convert.crawled import SCHEMAS, normalize_crawled_frame


def test_recent_kis_stock_is_coerced_to_january_raw_schema():
    recent = pl.DataFrame(
        {
            "sec_code": ["KR7005930003"],
            "ticker": ["A005930"],
            "trade_time": [datetime(2026, 8, 13, 9, 0)],
            "open_price": [70000.0],
            "high_price": [70100.0],
            "low_price": [69900.0],
            "close_price": [70050.0],
            "trade_volume": [100],
        }
    )

    normalized = normalize_raw_price_frame(recent, "stock")

    assert list(normalized.schema.items()) == list(RAW_SCHEMAS["stock"])
    assert "ticker" not in normalized.columns
    assert normalized["trade_volume"].dtype == pl.Float64
    assert normalized["trade_time"].dtype == pl.Datetime("us", "Asia/Seoul")
    assert normalized["cum_value"].null_count() == 1
    assert normalized["first_ask_price_best"].null_count() == 1


def test_krx_raw_columns_are_mapped_and_ordered_like_january():
    raw = pl.DataFrame(
        {
            "date": [datetime(2026, 8, 13)],
            "sec_code": ["KR7005930003"],
            "종목코드": ["005930"],
            "종목명": ["삼성전자"],
            "시장구분": ["KOSPI"],
            "소속부": [""],
            "종가": [70000],
            "대비": [100],
            "등락률": [0.14],
            "시가": [69900],
            "고가": [70100],
            "저가": [69800],
            "거래량": [1000],
            "거래대금": [70_000_000],
            "시가총액": [400_000_000_000_000],
            "상장주식수": [5_969_782_550],
        }
    )

    normalized = normalize_crawled_frame("전종목시세", raw)

    assert list(normalized.schema.items()) == list(SCHEMAS["전종목시세"].columns)
    assert normalized.columns[:6] == [
        "datetime", "isin", "ticker", "korean_name", "market_type", "division"
    ]
    assert normalized["volume"].dtype == pl.Float64


def test_hourly_resample_matches_january_aggregation_rules():
    raw = pl.DataFrame(
        {
            "sec_code": ["KR7005930003", "KR7005930003"],
            "trade_time": [datetime(2026, 8, 13, 9, 0), datetime(2026, 8, 13, 9, 1)],
            "open_price": [100.0, 105.0],
            "high_price": [110.0, 108.0],
            "low_price": [99.0, 101.0],
            "close_price": [105.0, 103.0],
            "trade_volume": [10.0, 20.0],
            "cum_value": [1_000.0, 3_000.0],
            "first_ask_price_best": [106.0, 104.0],
            "first_bid_price_best": [104.0, 102.0],
            "last_ask_price_best": [106.0, 104.0],
            "last_bid_price_best": [104.0, 102.0],
        }
    )

    hourly = resample_raw_frame(raw, "stock", "1h")

    assert list(hourly.schema.items()) == list(RAW_SCHEMAS["stock"])
    assert hourly.to_dicts()[0] == {
        "sec_code": "KR7005930003",
        "trade_time": datetime(2026, 8, 13, 9, 0, tzinfo=hourly["trade_time"][0].tzinfo),
        "open_price": 100.0,
        "high_price": 110.0,
        "low_price": 99.0,
        "close_price": 103.0,
        "trade_volume": 30.0,
        "cum_value": 3_000.0,
        "first_ask_price_best": 106.0,
        "first_bid_price_best": 104.0,
        "last_ask_price_best": 104.0,
        "last_bid_price_best": 102.0,
    }


def test_all_price_contracts_keep_january_timezone():
    assert all(
        dict(schema)["trade_time"] == pl.Datetime("us", "Asia/Seoul")
        for schema in RAW_SCHEMAS.values()
    )
    assert all(
        dict(schema)["datetime"] == pl.Datetime("ns", "Asia/Seoul")
        for schema in BT_SCHEMAS.values()
    )
