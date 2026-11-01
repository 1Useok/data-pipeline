#!/usr/bin/env python3
"""Audit resumable KIS minute stages without modifying active data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl

from backfill_kis_minutes import FETCHERS, stage_valid, trading_sessions


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/data_db"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    work = args.root / "_backfill_work" / "kis_minute"
    records = []
    for date_text in trading_sessions(args.start, args.end):
        for asset in FETCHERS:
            path = work / date_text / f"{asset}.parquet.stage"
            if not path.exists():
                continue
            frame = pl.read_parquet(path)
            records.append({
                "date": date_text,
                "asset": asset,
                "path": str(path),
                "valid": stage_valid(path, asset, date_text),
                "rows": len(frame),
                "codes": int(frame["sec_code"].n_unique()) if "sec_code" in frame.columns else 0,
                "calendar_dates": sorted(
                    value.isoformat()
                    for value in frame["trade_time"].dt.date().unique().to_list()
                ) if "trade_time" in frame.columns else [],
            })

    result = {
        "window": {"start": args.start, "end": args.end},
        "stages": len(records),
        "valid": sum(item["valid"] for item in records),
        "invalid": sum(not item["valid"] for item in records),
        "rows": sum(item["rows"] for item in records),
        "codes_sum": sum(item["codes"] for item in records),
        "invalid_records": [item for item in records if not item["valid"]],
        "latest_records": records[-12:],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["invalid"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
