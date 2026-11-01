#!/usr/bin/env python3
"""Read-only probe for KIS domestic derivative night-session minute ranges."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timedelta

from kis_minute_batch import _get_kis_config
from mft_kis.api.rest_api import RestApi


async def run(args) -> int:
    api = RestApi(_get_kis_config(), context="data")
    await api.is_connected()
    dates = [args.date]
    if args.next_date:
        dates.append(
            (datetime.strptime(args.date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        )
    results = []
    try:
        for query_date in dates:
            for hour in args.hours:
                response = await api.request_data(
                    "FHKIF03020200",
                    {
                        "FID_COND_MRKT_DIV_CODE": args.market,
                        "FID_INPUT_ISCD": args.ticker,
                        "FID_INPUT_DATE_1": query_date,
                        "FID_INPUT_HOUR_1": hour,
                        "FID_HOUR_CLS_CODE": "60",
                        "FID_PW_DATA_INCU_YN": "Y",
                        "FID_FAKE_TICK_INCU_YN": "N",
                    },
                    "GET",
                )
                rows = (response or {}).get("output2", [])
                results.append({
                    "query_date": query_date,
                    "hour": hour,
                    "rt_cd": (response or {}).get("rt_cd"),
                    "msg_cd": (response or {}).get("msg_cd"),
                    "msg1": (response or {}).get("msg1"),
                    "rows": len(rows),
                    "row_dates": sorted({str(row.get("stck_bsop_date", "")) for row in rows}),
                    "first_time": rows[0].get("stck_cntg_hour") if rows else None,
                    "last_time": rows[-1].get("stck_cntg_hour") if rows else None,
                })
    finally:
        await api.close()
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--date", required=True, help="YYYYMMDD target business date")
    parser.add_argument("--market", default="EU")
    parser.add_argument("--hours", nargs="+", default=["153000", "235959", "300000", "060000"])
    parser.add_argument("--next-date", action="store_true")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
