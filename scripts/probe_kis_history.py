#!/usr/bin/env python3
"""Read-only KIS historical-retention probe; never writes market data."""

from __future__ import annotations

import asyncio

from kis_minute_batch import _get_kis_config
from mft_kis.api.rest_api import RestApi
from mft_kis.feature.options_minute import OptionsMinuteFetcher
from mft_kis.feature.stock_minute import StockMinuteFetcher


def _summary(frame):
    if frame is None:
        return {"status": "request_failed", "rows": None}
    if frame.empty:
        return {"status": "empty", "rows": 0}
    return {
        "status": "success",
        "rows": len(frame),
        "first_time": str(frame.iloc[0].get("stck_cntg_hour")),
        "last_time": str(frame.iloc[-1].get("stck_cntg_hour")),
    }


async def main() -> None:
    api = RestApi(_get_kis_config(), context="data")
    await api.is_connected()
    try:
        stock = await StockMinuteFetcher(
            api, ticker="005930", date="20260507", kis_market="J"
        ).fetch_data()
        option = await OptionsMinuteFetcher(
            api, ticker="B01608700", date="20260812", kis_market="O"
        ).fetch_data()
        print({"stock_20260507": _summary(stock), "option_20260812": _summary(option)})
    finally:
        await api.close()


if __name__ == "__main__":
    asyncio.run(main())
