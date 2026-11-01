#!/usr/bin/env python3
"""Resumable KRX archive backfill followed by January-schema conversion."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from pandas import Timestamp

from common import setup_proxy
from mft_common.krx_calendar import krx_session_dates
from mft_common.storage import atomic_write_parquet
from mft_krx.convert import convert_archive_to_backtesting, full_recalculate
from mft_krx.crawlers.core import (
    etf전종목등락률_13102,
    etf전종목시세_13101,
    업종별분포_12024,
    업종분류현황_12025,
    외국인보유량_개별_12023,
    전종목등락률_12002,
    전종목시세_12001,
    지수구성종목_11006,
    지수목록_조회,
    코스피지수시세추이_11003,
    투자자별거래실적개별종목_12010,
    파생전종목시세_12501,
)


CORE_TARGETS = (
    ("코스피지수시세추이", 코스피지수시세추이_11003, True),
    ("전종목등락률", 전종목등락률_12002, False),
    ("업종별분포", 업종별분포_12024, False),
    ("업종분류현황", 업종분류현황_12025, False),
    ("ETF전종목시세", etf전종목시세_13101, False),
    ("ETF전종목등락률", etf전종목등락률_13102, False),
    ("투자자별거래실적개별종목", 투자자별거래실적개별종목_12010, False),
    ("외국인보유량", 외국인보유량_개별_12023, False),
    ("전종목시세", 전종목시세_12001, False),
    ("파생전종목시세", 파생전종목시세_12501, False),
)


def sessions(start: str, end: str) -> list[Timestamp]:
    return [Timestamp(value) for value in krx_session_dates(start, end)]


def valid_parquet(path: Path) -> bool:
    try:
        return path.exists() and pl.read_parquet_schema(path) and path.stat().st_size > 0
    except Exception:
        return False


def crawl_with_retry(func, *args, attempts: int, **kwargs) -> pl.DataFrame:
    error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            frame = func(*args, **kwargs)
            if frame is not None and len(frame):
                return pl.from_pandas(frame)
            raise RuntimeError("empty KRX response")
        except Exception as exc:
            error = exc
            if attempt == attempts:
                break
            delay = min(60, 2 ** attempt)
            print(f"    attempt {attempt}/{attempts} failed: {exc}; retry in {delay}s", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"KRX crawl exhausted retries: {error}")


def resolved_indices() -> list[dict]:
    from collections import Counter

    source = 지수목록_조회()
    counts = Counter(item.get("codeName", "") for item in source)
    prefixes = {"KOSPI": "코스피", "KOSDAQ": "코스닥", "KRX": "KRX"}
    result = []
    for item in source:
        name = item.get("codeName", item.get("full_code"))
        if counts[name] > 1 and item.get("marketName"):
            name = f"{prefixes.get(item['marketName'], item['marketName'])} {name}"
        result.append({
            "name": str(name).replace("/", "_").replace("\\", "_"),
            "market": item.get("full_code", "1"),
            "ind_id": item.get("short_code", ""),
        })
    return result


def observed_date_bounds(directory: Path) -> tuple[str | None, str | None]:
    values = []
    for path in directory.glob("*.parquet"):
        try:
            values.append(datetime.strptime(path.stem, "%Y-%m-%d").date().isoformat())
        except ValueError:
            continue
    return (min(values), max(values)) if values else (None, None)


def write_manifest(root: Path, report: dict, manifest_name: str) -> None:
    path = root / "logs" / manifest_name
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/data_db"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--core", action="store_true")
    parser.add_argument("--indices", action="store_true")
    parser.add_argument("--sleep", type=float, default=3.0)
    parser.add_argument("--attempts", type=int, default=5)
    args = parser.parse_args()
    if not args.core and not args.indices:
        parser.error("at least one of --core or --indices is required")

    setup_proxy()
    archive = args.root / "MFT_archive" / "KR" / "krx_crawled"
    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "window": {"start": args.start, "end": args.end},
        "saved": [], "skipped_existing": [], "no_data": [], "failed": [],
    }
    dates = sessions(args.start, args.end)

    indices = resolved_indices() if args.indices else []
    for item in indices:
        first_observed, last_observed = observed_date_bounds(
            archive / "지수구성종목" / item["name"]
        )
        item["first_observed"] = first_observed
        item["last_observed"] = last_observed
    report["index_contract"] = indices
    scope = "core_indices" if args.core and args.indices else "core" if args.core else "indices"
    manifest_name = f"krx_historical_backfill_{scope}_20260814.json"
    for date_index, target_date in enumerate(dates, 1):
        date_text = target_date.date().isoformat()
        print(f"[{date_index}/{len(dates)}] {date_text}", flush=True)

        if args.core:
            for name, func, range_query in CORE_TARGETS:
                path = archive / name / f"{date_text}.parquet"
                if valid_parquet(path):
                    report["skipped_existing"].append(str(path))
                    continue
                try:
                    frame = crawl_with_retry(
                        func,
                        target_date,
                        *( [target_date] if range_query else [] ),
                        attempts=args.attempts,
                    )
                    path.parent.mkdir(parents=True, exist_ok=True)
                    atomic_write_parquet(frame, path)
                    report["saved"].append({"path": str(path), "rows": len(frame)})
                    print(f"  {name}: saved {len(frame)}", flush=True)
                except Exception as exc:
                    report["failed"].append({"date": date_text, "item": name, "error": str(exc)})
                    print(f"  {name}: FAILED {exc}", flush=True)
                time.sleep(args.sleep)

        for item_index, item in enumerate(indices, 1):
            path = archive / "지수구성종목" / item["name"] / f"{date_text}.parquet"
            if valid_parquet(path):
                report["skipped_existing"].append(str(path))
                continue
            # An index first observed after the requested date did not yet
            # exist. Treat that as a structural absence without repeatedly
            # hitting the KRX endpoint for every pre-inception session.
            if item["first_observed"] and date_text < item["first_observed"]:
                report["no_data"].append({
                    "date": date_text,
                    "item": item["name"],
                    "reason": "before_first_observed",
                })
                continue
            try:
                frame = crawl_with_retry(
                    지수구성종목_11006,
                    date=target_date,
                    index_name=item["name"],
                    market=item["market"],
                    ind_id=item["ind_id"],
                    attempts=args.attempts,
                )
                path.parent.mkdir(parents=True, exist_ok=True)
                atomic_write_parquet(frame, path)
                report["saved"].append({"path": str(path), "rows": len(frame)})
                print(f"  index {item_index}/{len(indices)} {item['name']}: {len(frame)}", flush=True)
            except RuntimeError as exc:
                # A current index may not have existed on an older date. Keep
                # that structural absence separate from request failures.
                if "empty KRX response" in str(exc):
                    report["no_data"].append({"date": date_text, "item": item["name"]})
                else:
                    report["failed"].append({"date": date_text, "item": item["name"], "error": str(exc)})
                    print(f"  index {item_index}/{len(indices)} {item['name']}: FAILED {exc}", flush=True)
            time.sleep(args.sleep)

        write_manifest(args.root, report, manifest_name)

    months = sorted({value.strftime("%Y-%m") for value in dates})
    items = [name for name, _, _ in CORE_TARGETS if name != "전종목등락률"] if args.core else []
    if args.indices:
        items.append("지수구성종목")
    report["conversion"] = convert_archive_to_backtesting(args.root, months=months, items=items)
    if args.core:
        report["adjustment"] = full_recalculate(args.root)
    report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    write_manifest(args.root, report, manifest_name)
    print(json.dumps({
        "saved": len(report["saved"]),
        "skipped_existing": len(report["skipped_existing"]),
        "structural_no_data": len(report["no_data"]),
        "failed": len(report["failed"]),
    }, ensure_ascii=False), flush=True)
    return 1 if report["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
