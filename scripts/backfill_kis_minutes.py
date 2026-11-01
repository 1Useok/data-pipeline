#!/usr/bin/env python3
"""Resumable historical KIS minute backfill with staged, monthly commits."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from kis_minute_batch import (
    _collect_asset_group,
    _get_kis_config,
    _to_mft_raw_derivative,
    _to_mft_raw_stock,
)
from mft_common.storage import atomic_write_parquet, parquet_update_lock
from mft_common.krx_calendar import krx_session_iso_dates, previous_krx_session_date
from mft_kis.api.rest_api import RestApi
from mft_kis.feature.futures_minute import FuturesMinuteFetcher
from mft_kis.feature.options_minute import OptionsMinuteFetcher
from mft_kis.feature.stock_minute import StockMinuteFetcher
from mft_kis.universe import load_derivative_universe, load_stock_universe
from mft_koscom.continuous_future.generator import generate_continuous_future
from mft_koscom.convert.raw_to_backtesting import convert_to_backtesting
from mft_koscom.convert.schema_contract import RAW_SCHEMAS, normalize_raw_price_frame
from mft_koscom.resample import rebuild_higher_timeframes


FETCHERS = {
    "stock": (StockMinuteFetcher, _to_mft_raw_stock),
    "future": (FuturesMinuteFetcher, _to_mft_raw_derivative),
    "option": (OptionsMinuteFetcher, _to_mft_raw_derivative),
}

KIS_BACKFILL_CONCURRENCY = max(
    1,
    min(8, int(os.environ.get("KIS_BACKFILL_CONCURRENCY", "4"))),
)
KIS_BACKFILL_STAGE_BATCH_SIZE = max(
    50,
    int(os.environ.get("KIS_BACKFILL_STAGE_BATCH_SIZE", "400")),
)


async def collect_asset_group(
    api_client,
    instruments: list[dict],
    fetcher_class,
    date_str: str,
    converter_func,
    asset_type: str,
) -> dict:
    """Collect one asset group without increasing the shared API rate limit.

    ``RestApi`` owns a module-level limiter, so a small number of workers only
    overlaps network latency; it cannot exceed the configured three requests
    per second.  Keeping this wrapper in the historical backfill leaves the
    normal daily DAG's conservative sequential behaviour unchanged.
    """
    worker_count = min(KIS_BACKFILL_CONCURRENCY, len(instruments))
    if worker_count <= 1:
        return await _collect_asset_group(
            api_client,
            instruments,
            fetcher_class,
            date_str,
            converter_func,
            asset_type,
        )

    chunks = [instruments[index::worker_count] for index in range(worker_count)]
    results = await asyncio.gather(*(
        _collect_asset_group(
            api_client,
            chunk,
            fetcher_class,
            date_str,
            converter_func,
            f"{asset_type}[{index + 1}/{worker_count}]",
        )
        for index, chunk in enumerate(chunks)
    ))

    frames = [result.pop("data") for result in results]
    combined = pl.concat(
        [frame for frame in frames if not frame.is_empty()],
        how="diagonal_relaxed",
    ) if any(not frame.is_empty() for frame in frames) else pl.DataFrame()
    return {
        "data": combined,
        "success": sum(int(result["success"]) for result in results),
        "failed": sum(int(result["failed"]) for result in results),
        "no_data": sum(int(result["no_data"]) for result in results),
        "expected_total": sum(int(result["expected_total"]) for result in results),
        "unexpected_no_data": sum(
            int(result["unexpected_no_data"]) for result in results
        ),
        "unexpected_no_data_codes": [
            code
            for result in results
            for code in result["unexpected_no_data_codes"]
        ],
        "failed_codes": [
            code for result in results for code in result["failed_codes"]
        ],
        "aborted": sum(int(result["aborted"]) for result in results),
        "total": sum(int(result["total"]) for result in results),
    }


def trading_sessions(start: str, end: str) -> list[str]:
    return krx_session_iso_dates(start, end)


def stage_valid(path: Path, asset: str, date_text: str) -> bool:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return False
        schema = pl.read_parquet_schema(path)
        if list(schema.items()) != list(RAW_SCHEMAS[asset]):
            return False
        dates = set(
            pl.scan_parquet(path)
            .select(pl.col("trade_time").dt.date().unique())
            .collect()["trade_time"]
            .to_list()
        )
        target_date = datetime.fromisoformat(date_text).date()
        if asset == "stock":
            return dates == {target_date}
        session_start = previous_krx_session_date(date_text)
        allowed_dates = {
            session_start,
            session_start + timedelta(days=1),
            target_date,
        }
        return bool(dates) and dates.issubset(allowed_dates)
    except Exception:
        return False


def stage_complete(
    path: Path,
    asset: str,
    date_text: str,
    expected_codes: set[str],
) -> bool:
    return expected_codes.issubset(stage_codes(path, asset, date_text))


def stage_codes(path: Path, asset: str, date_text: str) -> set[str]:
    if not stage_valid(path, asset, date_text):
        return set()
    return set(
        pl.scan_parquet(path)
        .select(pl.col("sec_code").cast(pl.String).unique())
        .collect()["sec_code"]
        .to_list()
    )


def active_codes_by_date(path: Path) -> dict[str, set[str]]:
    if not path.exists():
        return {}
    pairs = (
        pl.scan_parquet(path)
        .select(
            pl.col("trade_time").dt.strftime("%Y-%m-%d").alias("date"),
            pl.col("sec_code").cast(pl.String),
        )
        .unique()
        .collect()
    )
    result: dict[str, set[str]] = defaultdict(set)
    for date_text, sec_code in pairs.iter_rows():
        result[str(date_text)].add(str(sec_code))
    return dict(result)


def write_report(root: Path, report: dict) -> None:
    filename = (
        "kis_minute_historical_merge_20260814.json"
        if report.get("mode") == "merge_existing"
        else "kis_minute_historical_backfill_20260814.json"
    )
    path = root / "logs" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def date_universes(root: Path, date_text: str) -> dict[str, list[dict]]:
    stock = [item for item in load_stock_universe(date_text, root) if item.get("expects_data")]
    derivative = load_derivative_universe(date_text, root)
    futures = [
        item
        for kind in ("index_futures", "stock_futures", "currency_futures", "bond_futures")
        for item in derivative[kind]
        if item.get("expects_data")
    ]
    options = [
        item
        for kind in ("index_options", "stock_options")
        for item in derivative[kind]
        if item.get("expects_data")
    ]
    return {"stock": stock, "future": futures, "option": options}


def merge_stages(root: Path, work: Path, sessions: list[str]) -> list[dict]:
    raw = root / "MFT_raw" / "KR" / "price" / "1m"
    grouped: dict[tuple[str, str], list[pl.DataFrame]] = defaultdict(list)
    for date_text in sessions:
        for asset in FETCHERS:
            path = work / date_text / f"{asset}.parquet.stage"
            if stage_valid(path, asset, date_text):
                frame = normalize_raw_price_frame(pl.read_parquet(path), asset)
                months = frame.select(
                    pl.col("trade_time").dt.strftime("%Y-%m").unique()
                )["trade_time"].to_list()
                for month in months:
                    grouped[(str(month), asset)].append(
                        frame.filter(pl.col("trade_time").dt.strftime("%Y-%m") == month)
                    )

    results = []
    for (month, asset), stage_frames in sorted(grouped.items()):
        target = raw / asset / f"{month}.parquet"
        frames = []
        if target.exists():
            frames.append(normalize_raw_price_frame(pl.read_parquet(target), asset))
        frames.extend(stage_frames)
        combined = (
            pl.concat(frames, how="vertical_relaxed")
            .unique(subset=["sec_code", "trade_time"], keep="last")
            .sort(["sec_code", "trade_time"])
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        with parquet_update_lock(target):
            atomic_write_parquet(combined, target, compression="lz4")
        actual = pl.read_parquet_schema(target)
        if list(actual.items()) != list(RAW_SCHEMAS[asset]):
            raise RuntimeError(f"January schema validation failed: {target}: {actual}")
        results.append({"path": str(target), "rows": len(combined), "stages": len(stage_frames)})
    return results


def rebuild_outputs(root: Path, months: list[str]) -> dict:
    raw = root / "MFT_raw" / "KR" / "price"
    bt = root / "MFT_backtesting" / "KR" / "price"
    continuous = generate_continuous_future(
        source_dir=raw / "1m" / "future",
        target_dir=raw / "1m" / "continuous_future",
        year_months=months,
    )
    converted = convert_to_backtesting(
        raw_dir=str(raw),
        bt_dir=str(bt),
        info_path=str(root / "MFT_backtesting" / "KR" / "info" / "stock_info.parquet"),
        krx_change_dir=str(root / "MFT_backtesting" / "KR" / "krx_crawled" / "전종목등락률"),
        months=months,
        timeframes=["1m"],
        asset_types=["stock", "future", "option"],
    )
    failures = [item for item in converted if item.get("status") != "success"]
    if failures:
        raise RuntimeError(f"Backtesting conversion failed: {failures}")
    resampled = rebuild_higher_timeframes(root, months=months)
    return {"continuous": continuous, "converted": converted, "resampled": resampled}


async def run(args) -> int:
    root: Path = args.root
    work = root / "_backfill_work" / "kis_minute"
    raw = root / "MFT_raw" / "KR" / "price" / "1m"
    sessions = trading_sessions(args.start, args.end)
    coverage: dict[tuple[str, str], dict[str, set[str]]] = {}
    for month in sorted({value[:7] for value in sessions}):
        for asset in FETCHERS:
            coverage[(month, asset)] = active_codes_by_date(raw / asset / f"{month}.parquet")

    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "merge_existing" if args.merge_existing else "fetch",
        "window": {"start": args.start, "end": args.end},
        "staged": [], "skipped_active": [], "skipped_stage": [], "failed": [],
        "source_no_data": [],
    }
    if args.merge_existing:
        report["monthly_merge"] = merge_stages(root, work, sessions)
        months = sorted(
            {value[:7] for value in sessions}
            | {Path(item["path"]).stem for item in report["monthly_merge"]}
        )
        report["derived_outputs"] = rebuild_outputs(root, months)
        report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        write_report(root, report)
        print(json.dumps({
            "monthly_merge": len(report["monthly_merge"]),
            "months": months,
            "mode": "merge_existing",
        }, ensure_ascii=False), flush=True)
        return 0

    api = RestApi(_get_kis_config(), context="data")
    await api.is_connected()
    try:
        for date_index, date_text in enumerate(sessions, 1):
            try:
                universes = date_universes(root, date_text)
            except Exception as exc:
                report["failed"].append({"date": date_text, "asset": "universe", "error": str(exc)})
                write_report(root, report)
                continue

            missing_assets = []
            expected_by_asset: dict[str, set[str]] = {}
            missing_codes_by_asset: dict[str, set[str]] = {}
            for asset in FETCHERS:
                expected_codes = {str(item["isin"]) for item in universes[asset]}
                expected_by_asset[asset] = expected_codes
                observed_codes = coverage[(date_text[:7], asset)].get(date_text, set())
                missing_codes = expected_codes - observed_codes
                missing_codes_by_asset[asset] = missing_codes
                if missing_codes:
                    missing_assets.append(asset)
                else:
                    report["skipped_active"].append({
                        "date": date_text,
                        "asset": asset,
                        "expected_codes": len(expected_codes),
                        "observed_codes": len(observed_codes),
                    })
            if not missing_assets:
                continue
            print(f"[{date_index}/{len(sessions)}] {date_text}: {missing_assets}", flush=True)

            for asset in missing_assets:
                stage = work / date_text / f"{asset}.parquet.stage"
                required_codes = missing_codes_by_asset[asset]
                if stage_complete(
                    stage, asset, date_text, required_codes
                ):
                    report["skipped_stage"].append({"date": date_text, "asset": asset, "path": str(stage)})
                    continue
                remaining_codes = required_codes - stage_codes(stage, asset, date_text)
                instruments = [
                    item for item in universes[asset]
                    if str(item["isin"]) in remaining_codes
                ]
                fetcher, converter = FETCHERS[asset]
                print(
                    f"  {asset}: {len(instruments)} missing traded instruments "
                    f"of {len(expected_by_asset[asset])}",
                    flush=True,
                )
                batch_count = (
                    len(instruments) + KIS_BACKFILL_STAGE_BATCH_SIZE - 1
                ) // KIS_BACKFILL_STAGE_BATCH_SIZE
                for batch_index, batch_start in enumerate(
                    range(0, len(instruments), KIS_BACKFILL_STAGE_BATCH_SIZE), 1
                ):
                    batch = instruments[
                        batch_start:batch_start + KIS_BACKFILL_STAGE_BATCH_SIZE
                    ]
                    result = await collect_asset_group(
                        api,
                        batch,
                        fetcher,
                        date_text.replace("-", ""),
                        converter,
                        asset,
                    )
                    api_failure = int(result["failed"])
                    source_no_data = int(result["unexpected_no_data"])
                    data = result.pop("data")
                    if not data.is_empty():
                        frame = normalize_raw_price_frame(data, asset).unique(
                            subset=["sec_code", "trade_time"], keep="last"
                        ).sort(["sec_code", "trade_time"])
                        new_rows = len(frame)
                        if stage_valid(stage, asset, date_text):
                            frame = (
                                pl.concat(
                                    [
                                        normalize_raw_price_frame(
                                            pl.read_parquet(stage), asset
                                        ),
                                        frame,
                                    ],
                                    how="vertical_relaxed",
                                )
                                .unique(
                                    subset=["sec_code", "trade_time"], keep="last"
                                )
                                .sort(["sec_code", "trade_time"])
                            )
                        stage.parent.mkdir(parents=True, exist_ok=True)
                        atomic_write_parquet(frame, stage, compression="lz4")
                        report["staged"].append({
                            "date": date_text,
                            "asset": asset,
                            "batch": batch_index,
                            "batch_count": batch_count,
                            "rows": len(frame),
                            "new_rows": new_rows,
                            "partial": bool(api_failure or source_no_data),
                            "path": str(stage),
                            "stats": result,
                        })
                        print(
                            f"  {asset}: staged batch {batch_index}/{batch_count}: "
                            f"{new_rows} new rows ({len(frame)} cumulative)",
                            flush=True,
                        )

                    if api_failure:
                        report["failed"].append({
                            "date": date_text,
                            "asset": asset,
                            "batch": batch_index,
                            "error": "incomplete KIS response",
                            "stats": result,
                        })
                        print(f"  {asset}: FAILED {result}", flush=True)
                    if source_no_data:
                        report["source_no_data"].append({
                            "date": date_text,
                            "asset": asset,
                            "batch": batch_index,
                            "codes": result["unexpected_no_data_codes"],
                            "count": source_no_data,
                            "stats": result,
                        })
                        print(f"  {asset}: SOURCE_NO_DATA {result}", flush=True)
                    write_report(root, report)
    finally:
        await api.close()

    if args.stage_only:
        report["stage_only"] = True
        report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        write_report(root, report)
        print(json.dumps({
            "staged": len(report["staged"]),
            "skipped_active": len(report["skipped_active"]),
            "skipped_stage": len(report["skipped_stage"]),
            "failed": len(report["failed"]),
            "source_no_data": len(report["source_no_data"]),
            "stage_only": True,
        }, ensure_ascii=False), flush=True)
        return 1 if report["failed"] else 0

    report["monthly_merge"] = merge_stages(root, work, sessions)
    months = sorted(
        {value[:7] for value in sessions}
        | {Path(item["path"]).stem for item in report["monthly_merge"]}
    )
    report["derived_outputs"] = rebuild_outputs(root, months)
    report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    write_report(root, report)
    print(json.dumps({
        "staged": len(report["staged"]),
        "skipped_active": len(report["skipped_active"]),
        "skipped_stage": len(report["skipped_stage"]),
        "failed": len(report["failed"]),
        "source_no_data": len(report["source_no_data"]),
    }, ensure_ascii=False), flush=True)
    return 1 if report["failed"] else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/data_db"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument(
        "--stage-only",
        action="store_true",
        help="fetch and validate resumable daily stages without modifying active monthly files",
    )
    parser.add_argument(
        "--merge-existing",
        action="store_true",
        help="merge already validated stages and rebuild outputs without calling KIS",
    )
    args = parser.parse_args()
    if args.stage_only and args.merge_existing:
        parser.error("--stage-only and --merge-existing are mutually exclusive")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
