"""Final integrity gate for the nightly market data pipeline.

This DAG has no cron schedule.  It is triggered only after KRX collection, KIS
collection, Raw 1-minute conversion, and continuous-future generation have
completed successfully for the same explicit trading date.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from pandas import Timestamp

sys.path.insert(0, str(Path(__file__).parent))

from common import (
    DEFAULT_ARGS,
    get_data_db_path,
    get_krx_crawled_path,
    get_mft_bt_path,
    get_mft_raw_path,
    is_trading_day,
    market_date_expr,
    resolve_target_date,
)
from mft_common.storage import atomic_write_parquet

KRX_ITEMS = [
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
PRICE_ASSETS = ("stock", "future", "option")
PRICE_TIMEFRAMES = ("1m",)


def _add(
    checks: list[dict[str, str]],
    name: str,
    status: str,
    expected: str,
    actual: object,
    message: object = "",
) -> None:
    checks.append(
        {
            "check_name": name,
            "status": status,
            "expected": expected,
            "actual": str(actual),
            "message": str(message),
        }
    )


def _read_day(path: Path, datetime_col: str, target: Timestamp) -> pl.DataFrame:
    return (
        pl.scan_parquet(path)
        .filter(market_date_expr(datetime_col) == target.date())
        .collect()
    )


def _baseline_message(path: Path, datetime_col: str, target: Timestamp, rows: int) -> tuple[str, str]:
    """Compare row count with up to 20 earlier sessions in the same file."""
    dates = (
        pl.read_parquet(path, columns=[datetime_col])
        .with_columns(market_date_expr(datetime_col).alias("_date"))
        .filter(pl.col("_date") < target.date())
        .group_by("_date")
        .len()
        .sort("_date")
        .tail(20)
    )
    if len(dates) < 5:
        return "OK", f"baseline unavailable ({len(dates)} prior sessions)"
    mean_rows = float(dates["len"].mean() or 0)
    ratio = rows / mean_rows if mean_rows else 1.0
    status = "WARN" if ratio < 0.5 else "OK"
    return status, f"20-session mean={mean_rows:.1f}, ratio={ratio:.1%}"


def _check_price_day(
    checks: list[dict[str, str]],
    *,
    name: str,
    path: Path,
    target: Timestamp,
    datetime_col: str,
    id_col: str,
    price_columns: tuple[str, str, str, str],
    volume_col: str,
) -> int:
    if not path.exists():
        _add(checks, name, "FAIL", "readable file with target-date rows", "missing", path)
        return 0
    try:
        schema = pl.read_parquet_schema(path)
        required = {datetime_col, id_col, volume_col, *price_columns}
        missing_columns = sorted(required - set(schema))
        if missing_columns:
            _add(checks, name, "FAIL", f"columns {sorted(required)}", missing_columns, path)
            return 0
        day = _read_day(path, datetime_col, target)
    except Exception as exc:
        _add(checks, name, "FAIL", "readable parquet", type(exc).__name__, exc)
        return 0

    rows = len(day)
    if rows == 0:
        _add(checks, name, "FAIL", ">0 target-date rows", 0, path)
        return 0

    keys = [id_col, datetime_col]
    duplicates = rows - len(day.unique(subset=keys))
    null_keys = int(day.select(pl.any_horizontal(pl.col(keys).is_null()).sum()).item())
    open_col, high_col, low_col, close_col = price_columns
    value_columns = [open_col, high_col, low_col, close_col, volume_col]
    invalid_values = int(
        day.select(
            pl.any_horizontal(
                [
                    pl.col(column).is_null() | ~pl.col(column).is_finite()
                    for column in value_columns
                ]
            ).sum()
        ).item()
    )
    negative_volume = int(day.select((pl.col(volume_col) < 0).sum()).item())
    invalid_ohlc = int(
        day.select(
            (
                (pl.col(high_col) < pl.max_horizontal(open_col, low_col, close_col))
                | (pl.col(low_col) > pl.min_horizontal(open_col, high_col, close_col))
                | (pl.col(open_col) < 0)
                | (pl.col(high_col) < 0)
                | (pl.col(low_col) < 0)
                | (pl.col(close_col) < 0)
            ).sum()
        ).item()
    )
    baseline_status, baseline = _baseline_message(path, datetime_col, target, rows)
    status = (
        "FAIL"
        if duplicates or null_keys or invalid_values or invalid_ohlc or negative_volume
        else baseline_status
    )
    _add(
        checks,
        name,
        status,
        "rows>0, unique/non-null keys, finite OHLCV, valid OHLC, volume>=0",
        f"rows={rows}, dup={duplicates}, null_keys={null_keys}, "
        f"invalid_values={invalid_values}, invalid_ohlc={invalid_ohlc}, "
        f"negative_volume={negative_volume}",
        baseline,
    )
    return rows


def _check_raw_bt_parity(
    checks: list[dict[str, str]],
    *,
    asset: str,
    raw_path: Path,
    bt_path: Path,
    target: Timestamp,
) -> None:
    """Require exact 1m key and unadjusted OHLCV parity after conversion."""
    name = f"raw_vs_bt_1m/{asset}"
    try:
        raw = _read_day(raw_path, "trade_time", target).select(
            pl.col("sec_code").alias("isin"),
            pl.col("trade_time").alias("datetime"),
            pl.col("open_price").alias("raw_open"),
            pl.col("high_price").alias("raw_high"),
            pl.col("low_price").alias("raw_low"),
            pl.col("close_price").alias("raw_close"),
            pl.col("trade_volume").alias("raw_volume"),
            pl.lit(True).alias("raw_present"),
        )
        bt = _read_day(bt_path, "datetime", target).select(
            "isin",
            "datetime",
            pl.col("open").alias("bt_open"),
            pl.col("high").alias("bt_high"),
            pl.col("low").alias("bt_low"),
            pl.col("close").alias("bt_close"),
            pl.col("volume").alias("bt_volume"),
            pl.lit(True).alias("bt_present"),
        )
        joined = raw.join(bt, on=["isin", "datetime"], how="full", coalesce=True)
        raw_only = int(joined["bt_present"].is_null().sum())
        bt_only = int(joined["raw_present"].is_null().sum())
        both = joined.filter(
            pl.col("raw_present").fill_null(False)
            & pl.col("bt_present").fill_null(False)
        )
        mismatched = int(
            both.select(
                pl.any_horizontal(
                    [
                        pl.col(f"raw_{column}") != pl.col(f"bt_{column}")
                        for column in ("open", "high", "low", "close", "volume")
                    ]
                ).sum()
            ).item()
        )
        ok = len(raw) > 0 and raw_only == 0 and bt_only == 0 and mismatched == 0
        _add(
            checks,
            name,
            "OK" if ok else "FAIL",
            "identical non-zero keys and unadjusted OHLCV",
            f"raw={len(raw)}, bt={len(bt)}, raw_only={raw_only}, "
            f"bt_only={bt_only}, value_mismatch={mismatched}",
        )
    except Exception as exc:
        _add(checks, name, "FAIL", "comparable Raw/BT 1m files", type(exc).__name__, exc)


def build_integrity_checks(target: Timestamp) -> list[dict[str, str]]:
    """Build all integrity results without Airflow-specific behavior."""
    checks: list[dict[str, str]] = []
    date_str = target.strftime("%Y-%m-%d")
    year_month = target.strftime("%Y-%m")
    krx_path = get_krx_crawled_path()

    for item in KRX_ITEMS:
        path = krx_path / item / f"{date_str}.parquet"
        if not path.exists():
            _add(checks, f"krx_crawled/{item}", "FAIL", "readable non-empty parquet", "missing", path)
            continue
        try:
            rows = pl.scan_parquet(path).select(pl.len()).collect().item()
            _add(
                checks,
                f"krx_crawled/{item}",
                "OK" if rows > 0 else "FAIL",
                ">0 rows",
                rows,
                path,
            )
        except Exception as exc:
            _add(checks, f"krx_crawled/{item}", "FAIL", "readable parquet", type(exc).__name__, exc)

    index_root = krx_path / "지수구성종목"
    index_files = list(index_root.glob(f"*/{date_str}.parquet")) if index_root.exists() else []
    _add(
        checks,
        "krx_crawled/지수구성종목",
        "OK" if index_files else "FAIL",
        ">=1 index constituent file",
        len(index_files),
        index_root,
    )

    adj_path = get_mft_bt_path() / "krx_crawled" / "전종목등락률" / f"{year_month}.parquet"
    if adj_path.exists():
        try:
            adj_day = _read_day(adj_path, "datetime", target)
            bad_ratio = (
                int(
                    adj_day.select(
                        (pl.col("cum_adj_ratio").is_null() | (pl.col("cum_adj_ratio") <= 0)).sum()
                    ).item()
                )
                if not adj_day.is_empty()
                else 0
            )
            _add(
                checks,
                "krx_adjustment_ratio",
                "OK" if len(adj_day) > 0 and bad_ratio == 0 else "FAIL",
                ">0 rows and all cum_adj_ratio>0",
                f"rows={len(adj_day)}, invalid={bad_ratio}",
                adj_path,
            )
        except Exception as exc:
            _add(checks, "krx_adjustment_ratio", "FAIL", "valid monthly ratio parquet", type(exc).__name__, exc)
    else:
        _add(checks, "krx_adjustment_ratio", "FAIL", "monthly ratio parquet", "missing", adj_path)

    info_path = get_mft_bt_path() / "info" / "stock_info.parquet"
    try:
        info_schema = pl.read_parquet_schema(info_path)
        valid_info = {"isin", "ticker"}.issubset(info_schema)
        _add(checks, "stock_info", "OK" if valid_info else "FAIL", "isin+ticker columns", list(info_schema), info_path)
    except Exception as exc:
        _add(checks, "stock_info", "FAIL", "readable parquet with isin+ticker", type(exc).__name__, exc)

    raw_rows: dict[str, int] = {}
    for asset in PRICE_ASSETS:
        raw_rows[asset] = _check_price_day(
            checks,
            name=f"raw_price/1m/{asset}",
            path=get_mft_raw_path() / "price" / "1m" / asset / f"{year_month}.parquet",
            target=target,
            datetime_col="trade_time",
            id_col="sec_code",
            price_columns=("open_price", "high_price", "low_price", "close_price"),
            volume_col="trade_volume",
        )

    bt_rows: dict[tuple[str, str], int] = {}
    for timeframe in PRICE_TIMEFRAMES:
        for asset in PRICE_ASSETS:
            path = get_mft_bt_path() / "price" / timeframe / asset / f"{year_month}.parquet"
            rows = _check_price_day(
                checks,
                name=f"bt_price/{timeframe}/{asset}",
                path=path,
                target=target,
                datetime_col="datetime",
                id_col="isin",
                price_columns=("open", "high", "low", "close"),
                volume_col="volume",
            )
            bt_rows[(timeframe, asset)] = rows

    for asset in PRICE_ASSETS:
        _check_raw_bt_parity(
            checks,
            asset=asset,
            raw_path=get_mft_raw_path() / "price" / "1m" / asset / f"{year_month}.parquet",
            bt_path=get_mft_bt_path() / "price" / "1m" / asset / f"{year_month}.parquet",
            target=target,
        )

    stock_path = get_mft_bt_path() / "price" / "1m" / "stock" / f"{year_month}.parquet"
    try:
        stock_day = _read_day(stock_path, "datetime", target)
        invalid = int(
            stock_day.select(
                (pl.col("cum_adj_ratio").is_null() | (pl.col("cum_adj_ratio") <= 0)).sum()
            ).item()
        )
        missing_ticker = (
            int(stock_day["ticker"].is_null().sum())
            if "ticker" in stock_day.columns
            else len(stock_day)
        )
        _add(
            checks,
            "bt_stock_adjustments",
            "OK" if invalid == 0 and missing_ticker == 0 else "FAIL",
            "all ratios>0 and ticker present",
            f"invalid_ratio={invalid}, missing_ticker={missing_ticker}",
            stock_path,
        )
    except Exception as exc:
        _add(checks, "bt_stock_adjustments", "FAIL", "valid cum_adj_ratio", type(exc).__name__, exc)

    metric_path = get_data_db_path() / "logs" / "kis_minute_silent_skip" / f"{year_month}.parquet"
    try:
        metrics = pl.read_parquet(metric_path).filter(pl.col("date") == date_str)
        failed = int(metrics["failed"].sum()) if not metrics.is_empty() else -1
        unexpected = (
            int(metrics["unexpected_no_data"].sum())
            if "unexpected_no_data" in metrics.columns and not metrics.is_empty()
            else -1
        )
        accepted = (
            bool(metrics["accepted"].fill_null(False).all())
            if "accepted" in metrics.columns and not metrics.is_empty()
            else False
        )
        max_count = int(os.environ.get("KIS_MAX_FAILED_COUNT", "0"))
        max_ratio = float(os.environ.get("KIS_MAX_FAILED_RATIO", "0"))
        max_unexpected_count = int(
            os.environ.get("KIS_MAX_UNEXPECTED_NO_DATA_COUNT", "1")
        )
        max_unexpected_ratio = float(
            os.environ.get("KIS_MAX_UNEXPECTED_NO_DATA_RATIO", "0")
        )
        metric_rows = list(metrics.iter_rows(named=True))
        allowed = sum(
            max(max_count, int(int(row["total"]) * max_ratio))
            for row in metric_rows
        )
        allowed_unexpected = sum(
            max(
                max_unexpected_count,
                int(int(row.get("expected_total") or 0) * max_unexpected_ratio),
            )
            for row in metric_rows
        )
        failures_within_limit = all(
            0 <= int(row.get("failed") or 0)
            <= max(max_count, int(int(row["total"]) * max_ratio))
            for row in metric_rows
        )
        unexpected_within_limit = all(
            0 <= int(row.get("unexpected_no_data") or 0)
            <= max(
                max_unexpected_count,
                int(int(row.get("expected_total") or 0) * max_unexpected_ratio),
            )
            for row in metric_rows
        )
        assets = (
            set(metrics["asset"].drop_nulls().to_list())
            if "asset" in metrics.columns
            else set()
        )
        structurally_valid = (
            len(metrics) == 3
            and assets == set(PRICE_ASSETS)
            and accepted
            and failures_within_limit
            and unexpected_within_limit
        )
        status = (
            "WARN"
            if structurally_valid and unexpected > 0
            else "OK" if structurally_valid else "FAIL"
        )
        _add(
            checks,
            "kis_collection_failures",
            status,
            "3 accepted asset metrics, "
            f"unexpected_no_data<={allowed_unexpected}, failures<={allowed}",
            f"assets={len(metrics)}, accepted={accepted}, "
            f"unexpected_no_data={unexpected}, allowed_unexpected={allowed_unexpected}, "
            f"failed={failed}",
            metric_path,
        )
    except Exception as exc:
        _add(checks, "kis_collection_failures", "FAIL", "valid collection metric", type(exc).__name__, exc)

    continuous_path = get_mft_raw_path() / "price" / "1m" / "continuous_future" / f"{year_month}.parquet"
    try:
        continuous = _read_day(continuous_path, "trade_time", target)
        _add(
            checks,
            "continuous_future",
            "OK" if len(continuous) > 0 else "FAIL",
            ">0 target-date rows",
            len(continuous),
            continuous_path,
        )
    except Exception as exc:
        _add(checks, "continuous_future", "FAIL", "readable target-date data", type(exc).__name__, exc)

    return checks


@task.short_circuit
def check_trading_day_task(logical_date=None, data_interval_end=None, dag_run=None) -> bool:
    target = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    return is_trading_day(target)


@task(execution_timeout=timedelta(minutes=30))
def run_integrity_checks(logical_date=None, data_interval_end=None, dag_run=None) -> str:
    target = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    checks = build_integrity_checks(target)
    result_path = get_data_db_path() / "logs" / f"{target.date().isoformat()}_integrity.parquet"
    atomic_write_parquet(pl.DataFrame(checks), result_path)

    counts = {
        status: sum(1 for check in checks if check["status"] == status)
        for status in ("OK", "WARN", "FAIL")
    }
    summary = (
        f"Integrity: {counts['OK']} OK, {counts['WARN']} WARN, "
        f"{counts['FAIL']} FAIL -> {result_path}"
    )
    print(summary)
    if counts["FAIL"]:
        failures = [check["check_name"] for check in checks if check["status"] == "FAIL"]
        raise AirflowException(summary + f"; failed checks={failures}")
    return summary


@dag(
    dag_id="data_quality_check",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "daily", "final-gate"],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
)
def data_quality_check():
    check = check_trading_day_task()
    integrity = run_integrity_checks()
    check >> integrity


data_quality_check()
