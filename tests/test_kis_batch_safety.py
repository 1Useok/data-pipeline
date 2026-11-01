"""KIS batch completeness gates."""

from datetime import datetime

import pytest
import pandas as pd
import polars as pl

import kis_minute_batch
from mft_kis import universe


def _results(failed: int = 0) -> dict:
    return {
        asset: {
            "total": 10,
            "success": 8,
            "failed": failed,
            "no_data": max(10 - 8 - failed, 0),
            "expected_total": 8,
            "unexpected_no_data": 0,
            "unexpected_no_data_codes": [],
            "failed_codes": ["BAD"] if failed else [],
            "rows": 100,
        }
        for asset in ("stock", "future", "option")
    }


def test_complete_collection_is_accepted(monkeypatch):
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_FAILED_COUNT", 0)
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_FAILED_RATIO", 0.0)
    kis_minute_batch.validate_collection_results(_results())


def test_any_persistent_failure_fails_by_default(monkeypatch):
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_FAILED_COUNT", 0)
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_FAILED_RATIO", 0.0)
    with pytest.raises(RuntimeError, match="Incomplete KIS"):
        kis_minute_batch.validate_collection_results(_results(failed=1))


def test_expected_instrument_without_rows_fails_collection(monkeypatch):
    results = _results()
    results["stock"]["unexpected_no_data"] = 1
    results["stock"]["unexpected_no_data_codes"] = ["005930"]
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_FAILED_COUNT", 0)
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_FAILED_RATIO", 0.0)
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_UNEXPECTED_NO_DATA_COUNT", 0)
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_UNEXPECTED_NO_DATA_RATIO", 0.0)

    with pytest.raises(RuntimeError, match="expected-data instruments"):
        kis_minute_batch.validate_collection_results(results)


def test_one_unexpected_no_data_is_tolerated_by_default(monkeypatch):
    results = _results()
    results["option"]["unexpected_no_data"] = 1
    results["option"]["unexpected_no_data_codes"] = ["B09F9W980"]
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_UNEXPECTED_NO_DATA_COUNT", 1)
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_UNEXPECTED_NO_DATA_RATIO", 0.0)

    kis_minute_batch.validate_collection_results(results)


def test_multiple_unexpected_no_data_still_fail(monkeypatch):
    results = _results()
    results["option"]["unexpected_no_data"] = 2
    results["option"]["unexpected_no_data_codes"] = ["ONE", "TWO"]
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_UNEXPECTED_NO_DATA_COUNT", 1)
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_UNEXPECTED_NO_DATA_RATIO", 0.0)

    with pytest.raises(RuntimeError, match="exceeds allowed=1"):
        kis_minute_batch.validate_collection_results(results)


def test_failed_attempt_does_not_write_metric_or_warn(monkeypatch):
    emitted = []
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_FAILED_COUNT", 0)
    monkeypatch.setattr(kis_minute_batch, "KIS_MAX_FAILED_RATIO", 0.0)
    monkeypatch.setattr(
        kis_minute_batch,
        "_emit_silent_skip_metric",
        lambda *args: emitted.append(args),
    )

    with pytest.raises(RuntimeError, match="Incomplete KIS"):
        kis_minute_batch.finalize_collection_results("20260803", _results(failed=1))

    assert emitted == []


def test_accepted_optional_no_data_is_recorded_without_warning(monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(kis_minute_batch, "get_data_db_path", lambda: tmp_path)
    monkeypatch.setattr(kis_minute_batch, "KIS_SILENT_SKIP_WARN_RATIO", 0.05)
    monkeypatch.setattr(
        kis_minute_batch,
        "send",
        lambda level, message: sent.append((level, message)) or True,
    )

    kis_minute_batch.finalize_collection_results("20260803", _results())
    kis_minute_batch.finalize_collection_results("20260803", _results())

    assert sent == []
    metric = pl.read_parquet(
        tmp_path / "logs" / "kis_minute_silent_skip" / "2026-08.parquet"
    )
    day = metric.filter(pl.col("date") == "2026-08-03")
    assert len(day) == 3
    assert day["accepted"].all()
    assert not day["alert_sent"].any()
    assert day["no_data"].sum() == 6
    assert day["optional_no_data"].sum() == 6
    assert day["silent_skip"].sum() == 0


def test_unexpected_no_data_metric_warns_only_once(monkeypatch, tmp_path):
    sent = []
    results = _results()
    results["stock"]["unexpected_no_data"] = 2
    results["stock"]["unexpected_no_data_codes"] = ["005930", "069500"]
    monkeypatch.setattr(kis_minute_batch, "get_data_db_path", lambda: tmp_path)
    monkeypatch.setattr(kis_minute_batch, "KIS_SILENT_SKIP_WARN_RATIO", 0.05)
    monkeypatch.setattr(
        kis_minute_batch,
        "send",
        lambda level, message: sent.append((level, message)) or True,
    )

    kis_minute_batch._emit_silent_skip_metric("20260803", results)
    kis_minute_batch._emit_silent_skip_metric("20260803", results)

    assert len(sent) == 1
    assert "unexpected no-data rate" in sent[0][1]


def test_legacy_metric_suppresses_repeated_warning(monkeypatch, tmp_path):
    metric_dir = tmp_path / "logs" / "kis_minute_silent_skip"
    metric_dir.mkdir(parents=True)
    legacy = pl.DataFrame(
        {
            "date": ["2026-08-03"] * 3,
            "asset": ["stock", "future", "option"],
            "total": [10, 10, 10],
            "success": [8, 8, 8],
            "failed": [0, 0, 0],
            "silent_skip": [2, 2, 2],
            "silent_skip_pct": [20.0, 20.0, 20.0],
        }
    )
    legacy.write_parquet(metric_dir / "2026-08.parquet")
    sent = []
    monkeypatch.setattr(kis_minute_batch, "get_data_db_path", lambda: tmp_path)
    monkeypatch.setattr(kis_minute_batch, "KIS_SILENT_SKIP_WARN_RATIO", 0.05)
    monkeypatch.setattr(
        kis_minute_batch,
        "send",
        lambda level, message: sent.append((level, message)) or True,
    )

    kis_minute_batch.finalize_collection_results("20260803", _results())

    assert sent == []
    migrated = pl.read_parquet(metric_dir / "2026-08.parquet")
    assert migrated["alert_sent"].all()


def test_stock_raw_rows_keep_ticker_for_master_only_instruments():
    source = pd.DataFrame(
        {
            "stck_cntg_hour": ["090000"],
            "stck_oprc": [100],
            "stck_hgpr": [101],
            "stck_lwpr": [99],
            "stck_prpr": [100],
            "cntg_vol": [10],
        }
    )
    result = kis_minute_batch._to_mft_raw_stock(
        source,
        ticker="900110",
        isin="HK0000057197",
        date_str="20260422",
    )
    assert result["ticker"].to_list() == ["A900110"]


def test_stock_raw_rows_repair_source_ohlc_envelope():
    source = pd.DataFrame(
        {
            "stck_cntg_hour": ["090000"],
            "stck_oprc": [2030],
            "stck_hgpr": [2045],
            "stck_lwpr": [2035],
            "stck_prpr": [2045],
            "cntg_vol": [456],
        }
    )

    result = kis_minute_batch._to_mft_raw_stock(
        source,
        ticker="198080",
        isin="KR7198080004",
        date_str="20260731",
    )

    assert result["low_price"].to_list() == [2030.0]
    assert result["high_price"].to_list() == [2045.0]


def test_derivative_night_session_extended_hour_rolls_to_next_day():
    source = pd.DataFrame(
        {
            "stck_bsop_date": ["20260803", "20260803", "20260803"],
            "stck_cntg_hour": ["235900", "240000", "300000"],
            "futs_oprc": [1, 2, 3],
            "futs_hgpr": [1, 2, 3],
            "futs_lwpr": [1, 2, 3],
            "futs_prpr": [1, 2, 3],
            "cntg_vol": [1, 1, 1],
        }
    )

    result = kis_minute_batch._to_mft_raw_derivative(
        source,
        ticker="A01609",
        isin="KR4A01690000",
        date_str="20260804",
    )

    assert result["trade_time"].to_list() == [
        datetime(2026, 8, 3, 23, 59),
        datetime(2026, 8, 4, 0, 0),
        datetime(2026, 8, 4, 6, 0),
    ]


def test_first_month_write_deduplicates_new_rows(tmp_path):
    path = tmp_path / "2026-07.parquet"
    rows = pl.DataFrame(
        {
            "sec_code": ["KR4B01TEST01", "KR4B01TEST01"],
            "trade_time": [pd.Timestamp("2026-07-24 09:00:00")] * 2,
            "close_price": [1.0, 2.0],
        }
    )

    kis_minute_batch._append_to_monthly(rows, path)

    saved = pl.read_parquet(path)
    assert len(saved) == 1
    assert saved["close_price"].to_list() == [2.0]


def test_complete_derivative_day_is_reused_without_api_calls(tmp_path):
    path = tmp_path / "future" / "2026-08.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "sec_code": ["KR4A01690000"],
            "trade_time": [datetime(2026, 8, 4, 9, 0)],
            "open_price": [100.0],
            "high_price": [101.0],
            "low_price": [99.0],
            "close_price": [100.0],
            "trade_volume": [10],
        }
    ).write_parquet(path)
    instruments = [
        {
            "isin": "KR4A01690000",
            "ticker": "A01609",
            "expects_data": True,
        }
    ]

    result = kis_minute_batch._reuse_complete_asset_day(
        path, "20260804", instruments
    )

    assert result is not None
    assert result["failed"] == 0
    assert result["unexpected_no_data"] == 0
    assert result["rows"] == 1


def test_incomplete_derivative_day_is_not_reused(tmp_path):
    path = tmp_path / "option" / "2026-08.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "sec_code": ["KR4B01680000"],
            "trade_time": [datetime(2026, 8, 4, 9, 0)],
            "open_price": [100.0],
            "high_price": [101.0],
            "low_price": [99.0],
            "close_price": [100.0],
            "trade_volume": [10],
        }
    ).write_parquet(path)
    instruments = [
        {"isin": "KR4B01680000", "ticker": "B0160800", "expects_data": True},
        {"isin": "KR4B01680001", "ticker": "B0160801", "expects_data": True},
    ]

    assert kis_minute_batch._reuse_complete_asset_day(
        path, "20260804", instruments
    ) is None


def test_derivative_universe_deduplicates_repeated_isin(monkeypatch):
    repeated = pl.DataFrame(
        {
            "isin": ["KR4B0168A016", "KR4B0168A016"],
            "ticker": ["A0168A01", "A0168A01"],
            "korean_name": ["option", "option"],
            "volume": [10, 10],
        }
    )
    monkeypatch.setattr(universe, "_load_krx_parquet", lambda *args: repeated)
    monkeypatch.setattr(universe, "_get_kis_ticker", lambda isin, ticker: ticker)

    result = universe.load_derivative_universe("2026-07-24")

    assert len(result["index_options"]) == 1


def test_zero_volume_future_is_not_marked_expected(monkeypatch):
    future = pl.DataFrame(
        {
            "isin": ["KR4A75680009"],
            "ticker": ["A7568000"],
            "korean_name": ["미국달러 F 202608"],
            "volume": [0],
        }
    )
    monkeypatch.setattr(universe, "_load_krx_parquet", lambda *args: future)
    monkeypatch.setattr(universe, "_get_kis_ticker", lambda isin, ticker: "A75608")

    result = universe.load_derivative_universe("2026-07-24")

    assert result["currency_futures"][0]["expects_data"] is False


def test_derivative_market_classification_covers_index_and_commodity_futures():
    assert universe._classify_derivative("KR4A04680000") == ("index_futures", "F")
    assert universe._classify_derivative("KR4AAD690006") == ("index_futures", "F")
    assert universe._classify_derivative("KR4A88680009") == ("index_futures", "CF")
    assert universe._classify_derivative("KR4A75680004") == ("currency_futures", "CF")
    assert universe._classify_derivative("KR4A65690005") == ("bond_futures", "CF")
    assert universe._classify_derivative("KR4ABG690002") == ("stock_futures", "JF")


def test_day_and_night_derivative_sessions_are_both_kept(monkeypatch):
    sessions = pl.DataFrame(
        {
            "isin": ["KR4B01687502", "KR4B01687502"],
            "ticker": ["B0168750", "B0168750"],
            "korean_name": [
                "코스피200 C 202608 750.0 (주간)",
                "코스피200 C 202608 750.0 (야간)",
            ],
            "volume": [1, 2],
        }
    )
    monkeypatch.setattr(universe, "_load_krx_parquet", lambda *args: sessions)
    monkeypatch.setattr(universe, "_get_kis_ticker", lambda isin, ticker: "B01608750")

    result = universe.load_derivative_universe("2026-07-30")

    assert {item["kis_market"] for item in result["index_options"]} == {"O", "EU"}


def test_etf_universe_uses_krx_market_and_marks_expected_data(monkeypatch):
    etf = pl.DataFrame(
        {
            "isin": ["KR7069500007"],
            "ticker": ["069500"],
            "korean_name": ["KODEX 200"],
            "volume": [100],
        }
    )
    monkeypatch.setattr(
        universe,
        "_load_krx_parquet",
        lambda item, *args: etf if "ETF" in item else pl.DataFrame(),
    )
    monkeypatch.setattr(universe, "_load_elw_master", lambda: {})
    monkeypatch.setattr(universe, "_load_kospi_master", lambda: {})

    result = universe.load_stock_universe("2026-07-24")

    assert result == [
        {
            "ticker": "069500",
            "isin": "KR7069500007",
            "kis_market": "J",
            "name": "KODEX 200",
            "asset_type": "etf",
            "expects_data": True,
        }
    ]


def test_stock_universe_uses_krx_market_and_marks_traded_stock_expected(monkeypatch):
    stock = pl.DataFrame(
        {
            "isin": ["KR7005930003"],
            "ticker": ["005930"],
            "korean_name": ["삼성전자"],
            "volume": [1234],
        }
    )
    monkeypatch.setattr(
        universe,
        "_load_krx_parquet",
        lambda item, *args: stock if "전종목시세" in item and "ETF" not in item else pl.DataFrame(),
    )
    monkeypatch.setattr(universe, "_load_elw_master", lambda: {})
    monkeypatch.setattr(universe, "_load_kospi_master", lambda: {})

    result = universe.load_stock_universe("2026-07-24")

    assert result == [
        {
            "ticker": "005930",
            "isin": "KR7005930003",
            "kis_market": "J",
            "name": "삼성전자",
            "asset_type": "stock",
            "expects_data": True,
        }
    ]


def test_derivative_master_cache_is_used_without_download(monkeypatch, tmp_path):
    cache_dir = tmp_path / "logs" / "kis_master"
    cache_dir.mkdir(parents=True)
    count = universe._MIN_DERIVATIVE_MASTER_CODES
    expected = {
        f"KR4{index:09d}": f"A{index:08d}"
        for index in range(count)
    }
    pl.DataFrame(
        {"isin": list(expected), "ticker": list(expected.values())}
    ).write_parquet(
        cache_dir / f"derivative_codes_{universe.date.today().isoformat()}.parquet"
    )
    monkeypatch.setattr(universe, "_get_data_root", lambda: tmp_path)
    monkeypatch.setattr(universe, "_isin_to_kis_code", {})
    monkeypatch.setattr(universe, "_master_download_attempted", False)
    monkeypatch.setattr(
        universe.requests,
        "get",
        lambda *args, **kwargs: pytest.fail("network download should not run"),
    )

    assert universe._download_kis_master_codes() == expected


def test_stale_derivative_cache_is_fallback_when_daily_download_fails(monkeypatch, tmp_path):
    cache_dir = tmp_path / "logs" / "kis_master"
    cache_dir.mkdir(parents=True)
    count = universe._MIN_DERIVATIVE_MASTER_CODES
    expected = {
        f"KR4{index:09d}": f"A{index:08d}"
        for index in range(count)
    }
    pl.DataFrame(
        {"isin": list(expected), "ticker": list(expected.values())}
    ).write_parquet(cache_dir / "derivative_codes_2000-01-01.parquet")

    class FailedResponse:
        status_code = 503

    monkeypatch.setattr(universe, "_get_data_root", lambda: tmp_path)
    monkeypatch.setattr(universe, "_isin_to_kis_code", {})
    monkeypatch.setattr(universe, "_master_download_attempted", False)
    monkeypatch.setattr(
        universe.requests,
        "get",
        lambda *args, **kwargs: FailedResponse(),
    )

    assert universe._download_kis_master_codes() == expected


def test_incomplete_derivative_master_fails_once_without_raw_fallback(monkeypatch, tmp_path):
    calls = []

    class EmptyResponse:
        status_code = 503

    monkeypatch.setattr(universe, "_get_data_root", lambda: tmp_path)
    monkeypatch.setattr(universe, "_isin_to_kis_code", {})
    monkeypatch.setattr(universe, "_master_download_attempted", False)
    monkeypatch.setattr(
        universe.requests,
        "get",
        lambda *args, **kwargs: calls.append(args[0]) or EmptyResponse(),
    )

    with pytest.raises(RuntimeError, match="Incomplete KIS derivative master"):
        universe._get_kis_ticker("KR4B77F89352", "B77F8935")
    first_attempt_calls = len(calls)
    with pytest.raises(RuntimeError, match="mapping is unavailable"):
        universe._get_kis_ticker("KR4B77F89352", "B77F8935")

    assert first_attempt_calls == len(universe._KIS_PIPE_MASTER_URLS) + len(
        universe._KIS_FIXED_MASTER_URLS
    )
    assert len(calls) == first_attempt_calls


@pytest.mark.parametrize(
    ("isin", "raw_ticker", "expected"),
    [
        ("KR4B09F89352", "B09F8935", "B09F8W935"),
        ("KR4CAFBQ7751", "CAFBQ775", "CAFBQW775"),
        ("KR4B01687007", "B0168700", "B01608700"),
        ("KR4B01686272", "B0168627", "B01608627"),
        ("KR4AC9680001", "AC968000", "AC9608"),
        ("KR4A016C0004", "A016C000", "A01612"),
    ],
)
def test_historical_derivative_code_reconstruction(isin, raw_ticker, expected):
    assert universe._derive_historical_kis_ticker(isin, raw_ticker) == expected


def test_historical_derivative_cache_lookup(monkeypatch, tmp_path):
    cache_dir = tmp_path / "logs" / "kis_master"
    cache_dir.mkdir(parents=True)
    pl.DataFrame(
        {"isin": ["KR4B01687007"], "ticker": ["B01608700"]}
    ).write_parquet(cache_dir / "derivative_codes_2026-08-07.parquet")
    monkeypatch.setattr(universe, "_get_data_root", lambda: tmp_path)
    monkeypatch.setattr(universe, "_historical_derivative_cache_root", None)
    monkeypatch.setattr(universe, "_historical_derivative_codes", {})

    assert (
        universe._lookup_historical_derivative_cache("KR4B01687007")
        == "B01608700"
    )


def test_historical_derivative_caches_are_loaded_once(monkeypatch, tmp_path):
    cache_dir = tmp_path / "logs" / "kis_master"
    cache_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "isin": ["KR4B01687007", "KR4B01686272"],
            "ticker": ["B01608700", "B01608627"],
        }
    ).write_parquet(cache_dir / "derivative_codes_2026-08-07.parquet")
    monkeypatch.setattr(universe, "_get_data_root", lambda: tmp_path)
    monkeypatch.setattr(universe, "_historical_derivative_cache_root", None)
    monkeypatch.setattr(universe, "_historical_derivative_codes", {})
    calls = 0
    original = universe.pl.read_parquet

    def counted_read(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(universe.pl, "read_parquet", counted_read)

    assert universe._lookup_historical_derivative_cache("KR4B01687007") == "B01608700"
    assert universe._lookup_historical_derivative_cache("KR4B01686272") == "B01608627"
    assert calls == 1


def test_krx_universe_loader_prefers_daily_archive(tmp_path):
    archive = tmp_path / "MFT_archive" / "KR" / "krx_crawled" / "전종목시세"
    converted = tmp_path / "MFT_backtesting" / "KR" / "krx_crawled" / "전종목시세"
    archive.mkdir(parents=True)
    converted.mkdir(parents=True)
    pl.DataFrame({"source": ["archive"]}).write_parquet(
        archive / "2026-05-07.parquet"
    )
    pl.DataFrame({"source": ["converted"]}).write_parquet(
        converted / "2026-05-07.parquet"
    )

    result = universe._load_krx_parquet("전종목시세", "2026-05-07", tmp_path)

    assert result["source"].to_list() == ["archive"]
