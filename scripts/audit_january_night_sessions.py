#!/usr/bin/env python3
"""Inspect January raw-minute timestamp conventions for KIS night derivatives."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl

from mft_kis.universe import load_derivative_universe


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/data_db"))
    parser.add_argument("--date", default="2026-01-30")
    parser.add_argument("--isin")
    args = parser.parse_args()

    universe = load_derivative_universe(args.date, args.root)
    night = [
        {**item, "asset": asset}
        for asset, kinds in {
            "future": ("index_futures", "stock_futures", "currency_futures", "bond_futures"),
            "option": ("index_options", "stock_options"),
        }.items()
        for kind in kinds
        for item in universe[kind]
        if item.get("kis_market") in {"CM", "EU"}
    ]

    output = []
    for asset in ("future", "option"):
        items = [item for item in night if item["asset"] == asset]
        codes = {str(item["isin"]) for item in items}
        path = args.root / "MFT_raw" / "KR" / "price" / "1m" / asset / "2026-01.parquet"
        rows = (
            pl.scan_parquet(path)
            .filter(pl.col("sec_code").cast(pl.String).is_in(codes))
            .select("sec_code", "trade_time")
            .sort(["sec_code", "trade_time"])
            .collect()
        )
        by_code = {
            str(code): {
                "rows": len(group),
                "first": group["trade_time"].min().isoformat(),
                "last": group["trade_time"].max().isoformat(),
                "dates": sorted({value.date().isoformat() for value in group["trade_time"]}),
            }
            for (code,), group in rows.group_by("sec_code", maintain_order=True)
        }
        for item in items:
            output.append({
                "asset": asset,
                "isin": str(item["isin"]),
                "ticker": str(item["ticker"]),
                "kis_market": item["kis_market"],
                "name": item.get("name"),
                "raw": by_code.get(str(item["isin"])),
            })

    if args.isin:
        selected = [item for item in output if item["isin"] == args.isin]
        detail = {}
        for asset in ("future", "option"):
            path = args.root / "MFT_raw" / "KR" / "price" / "1m" / asset / "2026-01.parquet"
            frame = (
                pl.scan_parquet(path)
                .filter(pl.col("sec_code").cast(pl.String) == args.isin)
                .select("trade_time")
                .collect()
            )
            if not frame.is_empty():
                detail[asset] = {
                    "file_min": frame["trade_time"].min().isoformat(),
                    "file_max": frame["trade_time"].max().isoformat(),
                    "february_calendar_rows_in_january_file": int(
                        frame.filter(pl.col("trade_time").dt.month() == 2).height
                    ),
                    "january_29_to_31": (
                        frame.filter(
                            pl.col("trade_time").dt.date().is_between(
                                pl.date(2026, 1, 29), pl.date(2026, 1, 31)
                            )
                        )
                        .with_columns(
                            pl.col("trade_time").dt.date().alias("date"),
                            pl.col("trade_time").dt.hour().alias("hour"),
                        )
                        .group_by("date", "hour")
                        .len()
                        .sort("date", "hour")
                        .to_dicts()
                    ),
                }
        print(json.dumps({"items": selected, "detail": detail}, ensure_ascii=False, indent=2, default=str))
        return 0

    print(json.dumps({
        "date": args.date,
        "night_instruments": len(night),
        "with_january_rows": sum(item["raw"] is not None for item in output),
        "items": output,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
