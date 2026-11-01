"""Regression tests for KIS derivative boundary-date fallback."""

import asyncio

import pytest

from mft_kis.feature.futures_minute import FuturesMinuteFetcher
from mft_kis.feature.options_minute import OptionsMinuteFetcher


class BoundaryApi:
    def __init__(self):
        self.query_dates = []

    async def request_data(self, tr_id, params, method):
        query_date = params["FID_INPUT_DATE_1"]
        self.query_dates.append(query_date)
        if query_date == "20260730":
            return {"rt_cd": "0", "output2": []}
        return {
            "rt_cd": "0",
            "output2": [
                {
                    "stck_bsop_date": "20260730",
                    "stck_cntg_hour": "090000",
                    "futs_prpr": "1",
                    "futs_oprc": "1",
                    "futs_hgpr": "1",
                    "futs_lwpr": "1",
                    "cntg_vol": "1",
                    "acml_tr_pbmn": "1",
                }
            ],
        }


class NightSessionApi:
    def __init__(self):
        self.query_dates = []
        self.query_hours = []

    async def request_data(self, tr_id, params, method):
        self.query_dates.append(params["FID_INPUT_DATE_1"])
        self.query_hours.append(params["FID_INPUT_HOUR_1"])
        return {
            "rt_cd": "0",
            "output2": [
                {
                    "stck_bsop_date": "20260804",
                    "stck_cntg_hour": "180000",
                    "futs_prpr": "99",
                },
                {
                    "stck_bsop_date": "20260803",
                    "stck_cntg_hour": "185100",
                    "futs_prpr": "1",
                },
                {
                    "stck_bsop_date": "20260731",
                    "stck_cntg_hour": "243400",
                    "futs_prpr": "2",
                },
            ],
        }


@pytest.mark.parametrize("fetcher_class", [FuturesMinuteFetcher, OptionsMinuteFetcher])
def test_empty_target_boundary_retries_with_next_calendar_date(fetcher_class):
    api = BoundaryApi()
    fetcher = fetcher_class(
        api,
        ticker="TEST",
        date="20260730",
        kis_market="F" if fetcher_class is FuturesMinuteFetcher else "O",
    )

    result = asyncio.run(fetcher.fetch_data())

    assert api.query_dates == ["20260730", "20260731"]
    assert len(result) == 1
    assert result.iloc[0]["stck_bsop_date"] == "20260730"


@pytest.mark.parametrize("fetcher_class", [FuturesMinuteFetcher, OptionsMinuteFetcher])
def test_night_market_queries_preceding_session_boundaries(fetcher_class):
    api = NightSessionApi()
    fetcher = fetcher_class(
        api,
        ticker="TEST",
        date="20260804",
        kis_market="CM" if fetcher_class is FuturesMinuteFetcher else "EU",
    )

    result = asyncio.run(fetcher.fetch_data())

    assert api.query_dates == ["20260803", "20260804"]
    assert api.query_hours == ["235959", "153000"]
    assert result["stck_bsop_date"].to_list() == ["20260803"]


class NightBoundaryApi:
    def __init__(self):
        self.queries = []

    async def request_data(self, tr_id, params, method):
        query_date = params["FID_INPUT_DATE_1"]
        self.queries.append((query_date, params["FID_INPUT_HOUR_1"]))
        if query_date == "20260202":
            return {"rt_cd": "0", "output2": []}
        return {
            "rt_cd": "0",
            "output2": [
                {
                    "stck_bsop_date": "20260202",
                    "stck_cntg_hour": "300000",
                    "futs_prpr": "1",
                }
            ],
        }


@pytest.mark.parametrize("fetcher_class", [FuturesMinuteFetcher, OptionsMinuteFetcher])
def test_night_market_keeps_preceding_session_from_target_boundary(fetcher_class):
    api = NightBoundaryApi()
    fetcher = fetcher_class(
        api,
        ticker="B01602575",
        date="20260203",
        kis_market="CM" if fetcher_class is FuturesMinuteFetcher else "EU",
    )

    result = asyncio.run(fetcher.fetch_data())

    assert api.queries == [
        ("20260202", "235959"),
        ("20260203", "153000"),
    ]
    assert result["stck_bsop_date"].to_list() == ["20260202"]


class SplitNightSessionApi:
    def __init__(self):
        self.queries = []

    async def request_data(self, tr_id, params, method):
        query = (params["FID_INPUT_DATE_1"], params["FID_INPUT_HOUR_1"])
        self.queries.append(query)
        time_value = "235900" if query[1] == "235959" else "300000"
        return {
            "rt_cd": "0",
            "output2": [{
                "stck_bsop_date": "20260202",
                "stck_cntg_hour": time_value,
                "futs_prpr": "1",
            }],
        }


@pytest.mark.parametrize("fetcher_class", [FuturesMinuteFetcher, OptionsMinuteFetcher])
def test_night_market_combines_evening_and_after_midnight_segments(fetcher_class):
    api = SplitNightSessionApi()
    fetcher = fetcher_class(
        api,
        ticker="A01603",
        date="20260203",
        kis_market="CM" if fetcher_class is FuturesMinuteFetcher else "EU",
    )

    result = asyncio.run(fetcher.fetch_data())

    assert api.queries == [
        ("20260202", "235959"),
        ("20260203", "153000"),
    ]
    assert result["stck_cntg_hour"].to_list() == ["235900", "300000"]


class NightPaginationApi:
    def __init__(self):
        self.queries = []

    async def request_data(self, tr_id, params, method):
        query = (params["FID_INPUT_DATE_1"], params["FID_INPUT_HOUR_1"])
        self.queries.append(query)
        if len(self.queries) == 1:
            return {"rt_cd": "0", "output2": []}
        if len(self.queries) == 2:
            rows = [
                {
                    "stck_bsop_date": "20260202",
                    "stck_cntg_hour": f"{300000 - index:06d}",
                    "futs_prpr": str(index),
                }
                for index in range(101)
            ]
            rows.append({
                "stck_bsop_date": "20260202",
                "stck_cntg_hour": "280900",
                "futs_prpr": "102",
            })
            return {"rt_cd": "0", "output2": rows}
        return {
            "rt_cd": "0",
            "output2": [
                {
                    "stck_bsop_date": "20260202",
                    "stck_cntg_hour": "280800",
                    "futs_prpr": "103",
                },
                {
                    "stck_bsop_date": "20260202",
                    "stck_cntg_hour": "280700",
                    "futs_prpr": "104",
                },
            ],
        }


@pytest.mark.parametrize("fetcher_class", [FuturesMinuteFetcher, OptionsMinuteFetcher])
def test_night_pagination_switches_from_boundary_to_session_date(fetcher_class):
    api = NightPaginationApi()
    fetcher = fetcher_class(
        api,
        ticker="A01603",
        date="20260203",
        kis_market="CM" if fetcher_class is FuturesMinuteFetcher else "EU",
    )

    result = asyncio.run(fetcher.fetch_data())

    assert api.queries == [
        ("20260202", "235959"),
        ("20260203", "153000"),
        ("20260202", "280900"),
    ]
    assert len(result) == 104
    assert set(result["stck_bsop_date"]) == {"20260202"}
