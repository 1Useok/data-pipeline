"""Regression tests for unified-to-KRX stock minute fallback."""

import asyncio

from mft_kis.feature.stock_minute import StockMinuteFetcher


class FakeApi:
    def __init__(self):
        self.markets = []

    async def request_data(self, tr_id, params, method):
        market = params["FID_COND_MRKT_DIV_CODE"]
        self.markets.append(market)
        if market == "UN":
            return {"rt_cd": "0", "output2": []}
        return {
            "rt_cd": "0",
            "output2": [
                {
                    "stck_bsop_date": "20260724",
                    "stck_cntg_hour": "090000",
                    "stck_prpr": "100",
                    "stck_oprc": "100",
                    "stck_hgpr": "100",
                    "stck_lwpr": "100",
                    "cntg_vol": "1",
                    "acml_tr_pbmn": "100",
                }
            ],
        }


def test_unified_empty_response_falls_back_to_krx():
    api = FakeApi()
    fetcher = StockMinuteFetcher(
        api,
        ticker="069500",
        date="20260724",
        kis_market="UN",
    )

    result = asyncio.run(fetcher.fetch_data())

    assert api.markets == ["UN", "J"]
    assert len(result) == 1
