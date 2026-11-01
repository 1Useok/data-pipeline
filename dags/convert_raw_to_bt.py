"""Convert raw 1-minute bars to backtesting format.

Triggered after kis_minute_batch completes.
1. MFT_raw → MFT_backtesting (column rename + adjustment ratios for stocks)
2. Generate 1-minute continuous futures
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from airflow.decorators import dag, task
from pandas import Timestamp

sys.path.insert(0, str(Path(__file__).parent))

from common import (
    DEFAULT_ARGS,
    get_mft_bt_path,
    get_mft_raw_path,
    is_trading_day,
    market_date_expr,
    resolve_target_date,
)
from lib.pipeline import trigger_next_dag


def _month_file_has_date(path: Path, target_date: Timestamp, column: str) -> bool:
    if not path.exists():
        return False
    try:
        values = pl.read_parquet(path, columns=[column])
        return not values.filter(market_date_expr(column) == target_date.date()).is_empty()
    except Exception:
        return False


@task.short_circuit
def check_trading_day_task(logical_date=None, data_interval_end=None, dag_run=None) -> bool:
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    if not is_trading_day(date):
        return False
    return True


@task
def validate_raw_inputs(logical_date=None, data_interval_end=None, dag_run=None) -> dict:
    """Refuse to convert a missing or partial asset-level Raw day."""
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    year_month = date.strftime("%Y-%m")
    missing = []
    for asset in ("stock", "future", "option"):
        path = get_mft_raw_path() / "price" / "1m" / asset / f"{year_month}.parquet"
        if not _month_file_has_date(path, date, "trade_time"):
            missing.append(str(path))
    if missing:
        raise RuntimeError(f"Raw inputs incomplete for {date.date()}: {missing}")
    return {"date": date.date().isoformat(), "assets": 3}


@task(execution_timeout=timedelta(minutes=30))
def convert_raw_to_backtesting(logical_date=None, data_interval_end=None, dag_run=None) -> dict:
    """Convert MFT_raw → MFT_backtesting for today's month."""
    from mft_koscom.convert.raw_to_backtesting import convert_to_backtesting

    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    year_month = date.strftime("%Y-%m")

    raw_dir = str(get_mft_raw_path() / "price")
    bt_dir = str(get_mft_bt_path() / "price")
    info_path = str(get_mft_bt_path() / "info" / "stock_info.parquet")
    krx_change_dir = str(get_mft_bt_path() / "krx_crawled" / "전종목등락률")

    print(f"Converting {year_month}: raw={raw_dir} → bt={bt_dir}")

    results = convert_to_backtesting(
        raw_dir=raw_dir,
        bt_dir=bt_dir,
        info_path=info_path,
        krx_change_dir=krx_change_dir,
        months=[year_month],
        timeframes=["1m"],
        asset_types=["stock", "future", "option"],
    )

    success = sum(1 for r in results if r["status"] == "success")
    bad = [r for r in results if r["status"] != "success"]
    if bad or success != 3:
        raise RuntimeError(f"Raw-to-backtesting conversion incomplete: {bad}")
    return {"success": success, "errors": 0, "year_month": year_month}


@task(execution_timeout=timedelta(minutes=10))
def generate_continuous_futures(logical_date=None, data_interval_end=None, dag_run=None) -> dict:
    """Generate continuous futures from individual contract data."""
    from mft_koscom.continuous_future.generator import generate_continuous_future

    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    year_month = date.strftime("%Y-%m")
    raw_price = get_mft_raw_path() / "price" / "1m"

    source_dir = raw_price / "future"
    target_dir = raw_price / "continuous_future"

    results = generate_continuous_future(
        source_dir=source_dir,
        target_dir=target_dir,
        year_months=[year_month],
    )

    if not results or int(results.get(year_month, 0) or 0) <= 0:
        raise RuntimeError(f"No continuous futures generated for {year_month}: {results}")
    return {"continuous_future": results}


@task(execution_timeout=timedelta(hours=1))
def rebuild_timeframes(logical_date=None, data_interval_end=None, dag_run=None) -> dict:
    """Rebuild every January-defined higher timeframe from canonical 1-minute bars."""
    from mft_koscom.resample import rebuild_higher_timeframes

    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    result = rebuild_higher_timeframes(months=[date.strftime("%Y-%m")])
    if result.get("status") != "success" or result.get("file_count") != 21:
        raise RuntimeError(f"Higher-timeframe rebuild incomplete: {result}")
    return result


@task
def validate_and_trigger_quality(
    conversion_results: dict,
    continuous_results: dict,
    resample_results: dict,
    logical_date=None,
    data_interval_end=None,
    dag_run=None,
    run_id=None,
) -> dict:
    """Validate converted date coverage before launching quality checks."""
    if not conversion_results or not continuous_results or not resample_results:
        raise RuntimeError("Conversion branches returned no validation results")
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    year_month = date.strftime("%Y-%m")
    missing = []
    for asset in ("stock", "future", "option"):
        path = get_mft_bt_path() / "price" / "1m" / asset / f"{year_month}.parquet"
        if not _month_file_has_date(path, date, "datetime"):
            missing.append(str(path))
    if missing:
        raise RuntimeError(f"Backtesting outputs incomplete for {date.date()}: {missing}")
    return trigger_next_dag(
        "data_quality_check",
        date.date().isoformat(),
        source_dag_id="convert_raw_to_bt",
        source_run_id=run_id or "manual",
    )


@dag(
    dag_id="convert_raw_to_bt",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["convert", "daily"],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
)
def convert_raw_to_bt():
    check = check_trading_day_task()
    raw_ready = validate_raw_inputs()
    convert = convert_raw_to_backtesting()
    cont_fut = generate_continuous_futures()
    resampled = rebuild_timeframes()
    quality = validate_and_trigger_quality(convert, cont_fut, resampled)

    check >> raw_ready >> [convert, cont_fut]
    convert >> resampled
    [resampled, cont_fut] >> quality

convert_raw_to_bt()
