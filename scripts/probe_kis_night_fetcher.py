#!/usr/bin/env python3
"""Read-only end-to-end probe of the production KIS night fetcher."""

from __future__ import annotations

import argparse
import asyncio
import json

from kis_minute_batch import _get_kis_config
from mft_kis.api.rest_api import RestApi
from mft_kis.feature.futures_minute import FuturesMinuteFetcher
from mft_kis.feature.options_minute import OptionsMinuteFetcher


async def run(args) -> int:
    api = RestApi(_get_kis_config(), context="data")
    await api.is_connected()
    fetcher_class = FuturesMinuteFetcher if args.asset == "future" else OptionsMinuteFetcher
    try:
        frame = await fetcher_class(
            api,
            ticker=args.ticker,
            date=args.date,
            kis_market=args.market,
        ).fetch_data()
    finally:
        await api.close()

    if frame is None:
        result = {"status": "request_failed"}
    elif frame.empty:
        result = {"status": "empty", "rows": 0}
    else:
        result = {
            "status": "ok",
            "rows": len(frame),
            "row_dates": sorted(frame["stck_bsop_date"].astype(str).unique().tolist()),
            "first_time": str(frame["stck_cntg_hour"].iloc[0]),
            "last_time": str(frame["stck_cntg_hour"].iloc[-1]),
            "min_time": str(frame["stck_cntg_hour"].astype(str).min()),
            "max_time": str(frame["stck_cntg_hour"].astype(str).max()),
            "unique_times": int(frame["stck_cntg_hour"].astype(str).nunique()),
        }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if frame is None else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset", choices=("future", "option"), required=True)
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--market", required=True)
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
