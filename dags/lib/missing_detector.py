"""Detect missing daily outputs for the local KR pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import os
from pathlib import Path

import polars as pl
from pandas import Timestamp

from common import (
    get_data_db_path,
    get_krx_crawled_path,
    get_mft_bt_path,
    get_mft_raw_path,
    is_trading_day,
    market_date_expr,
)

KRX_REQUIRED_ITEMS = [
    "전종목시세",
    "전종목등락률",
    "업종별분포",
    "업종분류현황",
    "ETF전종목시세",
    "ETF전종목등락률",
    "외국인보유량",
    "파생전종목시세",
    "코스피지수시세추이",
    "투자자별거래실적개별종목",
]

INDEX_CONSTITUENTS_ITEM = "지수구성종목"
PRICE_ASSETS = ["stock", "future", "option"]
PRICE_TIMEFRAMES = ["1m"]
BT_PRICE_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "1d"]


@dataclass(frozen=True)
class MissingOutput:
    dag_id: str
    date: str
    reason: str
    path: str


def _as_date(value: date | str | Timestamp | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return Timestamp(value).date()


def _trading_days_in_window(end: date | str | Timestamp | None = None, days: int = 7) -> list[date]:
    """Return the last N trading days ending at ``end``."""
    current = _as_date(end)
    result: list[date] = []
    while len(result) < days:
        if is_trading_day(Timestamp(current)):
            result.append(current)
        current = current - timedelta(days=1)
    return list(reversed(result))


def _month_file_has_date(path: Path, dt: date, column: str = "datetime") -> bool:
    if not path.exists():
        return False
    try:
        df = pl.read_parquet(path, columns=[column])
    except Exception:
        return False
    if column not in df.columns or df.is_empty():
        return False
    return not df.filter(market_date_expr(column) == dt).is_empty()


def _check_krx_daily(dt: date) -> list[MissingOutput]:
    missing: list[MissingOutput] = []
    base = get_krx_crawled_path()
    date_str = dt.isoformat()
    for item in KRX_REQUIRED_ITEMS:
        path = base / item / f"{date_str}.parquet"
        if not path.exists():
            missing.append(MissingOutput("krx_daily_batch", date_str, "missing KRX daily parquet", str(path)))
    adjustment_path = get_mft_bt_path() / "krx_crawled" / "전종목등락률" / f"{dt.strftime('%Y-%m')}.parquet"
    if not _month_file_has_date(adjustment_path, dt, "datetime"):
        missing.append(
            MissingOutput(
                "krx_convert_adj_ratio",
                date_str,
                "missing normalized KRX adjustment rows",
                str(adjustment_path),
            )
        )
    stock_info = get_mft_bt_path() / "info" / "stock_info.parquet"
    if not stock_info.exists():
        missing.append(
            MissingOutput(
                "krx_convert_adj_ratio",
                date_str,
                "missing stock_info",
                str(stock_info),
            )
        )
    return missing


def _check_index_constituents(dt: date) -> list[MissingOutput]:
    base = get_krx_crawled_path() / INDEX_CONSTITUENTS_ITEM
    date_str = dt.isoformat()
    if base.exists() and any(base.glob(f"*/{date_str}.parquet")):
        return []
    return [MissingOutput("krx_index_constituents", date_str, "missing index constituents parquet", str(base))]


def _check_bt_prices(dt: date) -> list[MissingOutput]:
    missing: list[MissingOutput] = []
    ym = dt.strftime("%Y-%m")
    base = get_mft_bt_path() / "price"
    for tf in BT_PRICE_TIMEFRAMES:
        for asset in PRICE_ASSETS:
            path = base / tf / asset / f"{ym}.parquet"
            if not _month_file_has_date(path, dt, "datetime"):
                missing.append(MissingOutput("convert_raw_to_bt", dt.isoformat(), f"missing bt rows for {tf}/{asset}", str(path)))
    return missing


def _check_raw_prices(dt: date) -> list[MissingOutput]:
    missing: list[MissingOutput] = []
    ym = dt.strftime("%Y-%m")
    base = get_mft_raw_path() / "price" / "1m"
    for asset in PRICE_ASSETS:
        path = base / asset / f"{ym}.parquet"
        if not _month_file_has_date(path, dt, "trade_time"):
            missing.append(MissingOutput("kis_minute_batch", dt.isoformat(), f"missing raw rows for {asset}", str(path)))
    return missing


def _check_kis_collection_metric(dt: date) -> list[MissingOutput]:
    """Require an accepted KIS metric, not merely raw parquet presence."""
    date_str = dt.isoformat()
    path = (
        get_data_db_path()
        / "logs"
        / "kis_minute_silent_skip"
        / f"{dt.strftime('%Y-%m')}.parquet"
    )
    reason = "missing or invalid accepted KIS collection metric"
    try:
        metrics = pl.read_parquet(path).filter(
            pl.col("date").cast(pl.Utf8) == date_str
        )
        required = {
            "asset",
            "total",
            "failed",
            "expected_total",
            "unexpected_no_data",
            "accepted",
        }
        if metrics.is_empty() or not required.issubset(metrics.columns):
            raise ValueError(reason)

        assets = set(metrics["asset"].drop_nulls().to_list())
        accepted = bool(metrics["accepted"].fill_null(False).all())
        max_count = int(os.environ.get("KIS_MAX_FAILED_COUNT", "0"))
        max_ratio = float(os.environ.get("KIS_MAX_FAILED_RATIO", "0"))
        max_unexpected_count = int(
            os.environ.get("KIS_MAX_UNEXPECTED_NO_DATA_COUNT", "1")
        )
        max_unexpected_ratio = float(
            os.environ.get("KIS_MAX_UNEXPECTED_NO_DATA_RATIO", "0")
        )
        metric_rows = list(metrics.iter_rows(named=True))
        failures_within_limit = all(
            0 <= int(row.get("failed") or 0)
            <= max(max_count, int(int(row["total"]) * max_ratio))
            for row in metric_rows
        )
        unexpected_within_limit = all(
            0 <= int(row.get("unexpected_no_data") or 0)
            <= max(
                max_unexpected_count,
                int(int(row["expected_total"]) * max_unexpected_ratio),
            )
            for row in metric_rows
        )
        valid = (
            len(metrics) == len(PRICE_ASSETS)
            and assets == set(PRICE_ASSETS)
            and accepted
            and failures_within_limit
            and unexpected_within_limit
        )
        if valid:
            return []
    except Exception:
        pass
    return [MissingOutput("kis_minute_batch", date_str, reason, str(path))]


def _check_integrity_log(dt: date) -> list[MissingOutput]:
    path = get_data_db_path() / "logs" / f"{dt.isoformat()}_integrity.parquet"
    if not path.exists():
        return [MissingOutput("data_quality_check", dt.isoformat(), "missing integrity log", str(path))]
    try:
        statuses = pl.read_parquet(path, columns=["status"])
        if statuses.is_empty() or statuses.filter(pl.col("status") == "FAIL").height:
            return [
                MissingOutput(
                    "data_quality_check",
                    dt.isoformat(),
                    "integrity log contains failures",
                    str(path),
                )
            ]
    except Exception:
        return [
            MissingOutput(
                "data_quality_check",
                dt.isoformat(),
                "unreadable integrity log",
                str(path),
            )
        ]
    return []


def scan_recent(end: date | str | Timestamp | None = None, days: int = 7) -> dict:
    """Scan recent trading days and group missing outputs by DAG."""
    dates = _trading_days_in_window(end=end, days=days)
    missing: list[MissingOutput] = []
    for dt in dates:
        missing.extend(_check_krx_daily(dt))
        missing.extend(_check_index_constituents(dt))
        missing.extend(_check_raw_prices(dt))
        missing.extend(_check_kis_collection_metric(dt))
        missing.extend(_check_bt_prices(dt))
        missing.extend(_check_integrity_log(dt))

    by_dag: dict[str, list[dict]] = {}
    for item in missing:
        by_dag.setdefault(item.dag_id, []).append(item.__dict__)

    return {
        "dates": [d.isoformat() for d in dates],
        "missing": [item.__dict__ for item in missing],
        "by_dag": by_dag,
    }
