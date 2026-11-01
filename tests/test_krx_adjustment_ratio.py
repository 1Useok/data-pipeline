"""Tests for the restored KRX adjustment-ratio converter."""

from datetime import datetime

import polars as pl

from mft_krx.convert.daily_adj_ratio import full_recalculate


def _daily_frame(day: datetime, base: float, close: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [day],
            "sec_code": ["KR7005930003"],
            "종목코드": ["005930"],
            "종목명": ["삼성전자"],
            "시작일 기준가": [base],
            "종료일 종가": [close],
            "대비": [close - base],
            "등락률": [(close / base - 1) * 100],
            "거래량": [1000],
            "거래대금": [close * 1000],
            "market": ["kospi"],
        }
    )


def test_full_recalculate_builds_month_and_backward_cumulative_ratio(tmp_path):
    source = tmp_path / "MFT_backtesting/KR/krx_crawled/전종목등락률"
    source.mkdir(parents=True)
    _daily_frame(datetime(2026, 4, 21), 100, 100).write_parquet(
        source / "2026-04-21.parquet"
    )
    # A 2:1 price adjustment: today's base is half the previous close.
    _daily_frame(datetime(2026, 4, 22), 50, 55).write_parquet(
        source / "2026-04-22.parquet"
    )

    result = full_recalculate(tmp_path)
    output = pl.read_parquet(source / "2026-04.parquet").sort("datetime")

    assert result["status"] == "success"
    assert output["adj_ratio"].to_list() == [1.0, 0.5]
    assert output["cum_adj_ratio"].to_list() == [0.5, 1.0]
    info = pl.read_parquet(
        tmp_path / "MFT_backtesting/KR/info/stock_info.parquet"
    )
    assert info.to_dicts() == [{"isin": "KR7005930003", "ticker": "A005930"}]

    # A rerun is idempotent even though the generated month is now also a source.
    full_recalculate(tmp_path)
    assert len(pl.read_parquet(source / "2026-04.parquet")) == 2
