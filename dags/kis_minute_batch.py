"""KIS API minute bar collection DAG.

Triggered after krx_daily_batch completes.
Collects 1-minute OHLCV bars for all active instruments:
  1. Stock/ETF/ELW (TR: FHKST03010230, market: J)
  2. Futures (TR: FHKIF03020200, market: F/JF)
  3. Options (TR: FHKIF03020200, market: O/JO)

Saves to MFT_raw/KR/price/1m/{asset}/YYYY-MM.parquet
"""

import asyncio
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pendulum
import polars as pl
from airflow.exceptions import AirflowSkipException
from airflow.decorators import dag, task

sys.path.insert(0, str(Path(__file__).parent))

from common import (
    DEFAULT_ARGS,
    get_data_db_path,
    get_mft_raw_path,
    is_trading_day,
    resolve_target_date,
    setup_proxy,
)
from lib.pipeline import trigger_next_dag
from mft_common.storage import atomic_write_parquet, parquet_update_lock
from mft_koscom.convert.schema_contract import normalize_raw_price_frame
from notify import send

# Rate limit settings
# Global 3 req/sec cap is enforced in RestApi via aiolimiter (see
# packages/mft_kis/api/rest_api.py). Per-request sleep here is just a
# small courtesy gap; real throttling happens at the client level.
SLEEP_BETWEEN_REQUESTS = 0.05
SLEEP_BETWEEN_GROUPS = 10

# Circuit breaker: if CONSECUTIVE_FAILURE_COOLDOWN_THRESHOLD tickers fail
# in a row, sleep CONSECUTIVE_FAILURE_COOLDOWN_SEC before continuing
# (gives the KIS server time to lift any penalty cooldown). If failures
# keep coming after MAX_CONSECUTIVE_FAILURES, abort the group entirely.
CONSECUTIVE_FAILURE_COOLDOWN_THRESHOLD = 5
CONSECUTIVE_FAILURE_COOLDOWN_SEC = 60
MAX_CONSECUTIVE_FAILURES = 30


def _load_env():
    """Load .env file if exists."""
    env_path = Path(__file__).parent.parent / "docker" / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    value = value.strip('"').strip("'")
                    if key.strip() not in os.environ:
                        os.environ[key.strip()] = value

_load_env()

KIS_TASK_TIMEOUT_HOURS = float(os.environ.get("KIS_TASK_TIMEOUT_HOURS", "5"))
KIS_TASK_RETRIES = int(os.environ.get("KIS_TASK_RETRIES", "3"))
KIS_MAX_FAILED_COUNT = int(os.environ.get("KIS_MAX_FAILED_COUNT", "0"))
KIS_MAX_FAILED_RATIO = float(os.environ.get("KIS_MAX_FAILED_RATIO", "0"))
KIS_MAX_UNEXPECTED_NO_DATA_COUNT = int(
    os.environ.get("KIS_MAX_UNEXPECTED_NO_DATA_COUNT", "1")
)
KIS_MAX_UNEXPECTED_NO_DATA_RATIO = float(
    os.environ.get("KIS_MAX_UNEXPECTED_NO_DATA_RATIO", "0")
)
KIS_SILENT_SKIP_WARN_RATIO = float(os.environ.get("KIS_SILENT_SKIP_WARN_RATIO", "0.05"))
if KIS_TASK_TIMEOUT_HOURS < 1:
    raise ValueError("KIS_TASK_TIMEOUT_HOURS must be at least 1")
if KIS_TASK_RETRIES < 0:
    raise ValueError("KIS_TASK_RETRIES must be non-negative")
if KIS_MAX_FAILED_COUNT < 0 or not 0 <= KIS_MAX_FAILED_RATIO <= 1:
    raise ValueError("KIS failure thresholds must be non-negative and ratio <= 1")
if (
    KIS_MAX_UNEXPECTED_NO_DATA_COUNT < 0
    or not 0 <= KIS_MAX_UNEXPECTED_NO_DATA_RATIO <= 1
):
    raise ValueError(
        "KIS unexpected-no-data thresholds must be non-negative and ratio <= 1"
    )
if not 0 <= KIS_SILENT_SKIP_WARN_RATIO <= 1:
    raise ValueError("KIS_SILENT_SKIP_WARN_RATIO must be between 0 and 1")


def _get_kis_config() -> dict:
    """Build KIS API config from environment."""
    return {
        "prod_api": {
            "app_key": os.environ.get("KIS_APP_KEY", os.environ.get("PROD_KIS_APP_KEY", "")),
            "app_secret": os.environ.get("KIS_APP_SECRET", os.environ.get("PROD_KIS_APP_SECRET", "")),
            "base_url": os.environ.get("KIS_BASE_URL", os.environ.get("PROD_BASE_URL", "https://openapi.koreainvestment.com:9443")),
        },
        "kis_api_endpoints": {
            # Stock minute bars: FHKST03010230 (주식일별분봉조회) supports
            # historical dates. The sibling FHKST03010200 (today only) lives
            # at the same URL and is kept for backward compatibility.
            "FHKST03010230": "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            "FHKST03010200": "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            # Futures/options minute bars. The plan-doc URL `/inquire-minute`
            # returns 404; the working endpoint mirrors the stock pattern
            # but lives under /domestic-futureoption/.
            "FHKIF03020200": "/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice",
        },
    }


def _optional_float(row, *names: str) -> float | None:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def _to_mft_raw_stock(df, ticker: str, isin: str, date_str: str) -> pl.DataFrame:
    """Convert KIS stock minute response to MFT_raw format."""
    if df is None or df.empty:
        return pl.DataFrame()

    import pandas as pd

    rows = []
    output_ticker = ticker if str(ticker).startswith("A") else f"A{ticker}"
    for _, row in df.iterrows():
        time_str = str(row.get("stck_cntg_hour", "")).zfill(6)
        dt_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
        open_price = float(row.get("stck_oprc", 0))
        source_high = float(row.get("stck_hgpr", 0))
        source_low = float(row.get("stck_lwpr", 0))
        close_price = float(row.get("stck_prpr", 0))
        rows.append({
            "sec_code": isin,
            "ticker": output_ticker,
            "trade_time": pd.Timestamp(dt_str),
            "open_price": open_price,
            "high_price": max(open_price, source_high, source_low, close_price),
            "low_price": min(open_price, source_high, source_low, close_price),
            "close_price": close_price,
            "trade_volume": int(row.get("cntg_vol", 0)),
            "cum_value": _optional_float(row, "acml_tr_pbmn"),
        })

    if not rows:
        return pl.DataFrame()

    return pl.from_pandas(pd.DataFrame(rows))


def _parse_kis_derivative_time(base_date_str: str, value) -> "pd.Timestamp":
    """Convert a KIS derivative session clock to a calendar timestamp.

    KIS represents the after-midnight part of the derivative night session
    with extended hours (24:00 through 30:00) on the trading date. Timestamp
    parsers reject those values, so add the clock as an elapsed duration to
    midnight instead of interpolating it into an ISO timestamp.
    """
    import pandas as pd

    time_str = str(value or "").strip().zfill(6)
    if len(time_str) != 6 or not time_str.isdigit():
        raise ValueError(f"Invalid KIS derivative time: {value!r}")

    hour = int(time_str[:2])
    minute = int(time_str[2:4])
    second = int(time_str[4:6])
    if hour > 47 or minute > 59 or second > 59:
        raise ValueError(f"Invalid KIS derivative time: {value!r}")

    midnight = pd.Timestamp(datetime.strptime(base_date_str, "%Y%m%d"))
    return midnight + pd.Timedelta(hours=hour, minutes=minute, seconds=second)


def _to_mft_raw_derivative(df, ticker: str, isin: str, date_str: str) -> pl.DataFrame:
    """Convert KIS futures/options minute response to MFT_raw format."""
    if df is None or df.empty:
        return pl.DataFrame()

    import pandas as pd

    rows = []
    for _, row in df.iterrows():
        session_date = str(row.get("stck_bsop_date", date_str) or date_str).replace("-", "")
        open_price = float(row.get("futs_oprc", 0))
        source_high = float(row.get("futs_hgpr", 0))
        source_low = float(row.get("futs_lwpr", 0))
        close_price = float(row.get("futs_prpr", 0))
        rows.append({
            "sec_code": isin,
            "trade_time": _parse_kis_derivative_time(
                session_date, row.get("stck_cntg_hour", "")
            ),
            "open_price": open_price,
            "high_price": max(open_price, source_high, source_low, close_price),
            "low_price": min(open_price, source_high, source_low, close_price),
            "close_price": close_price,
            "trade_volume": int(row.get("cntg_vol", 0)),
            "cum_value": _optional_float(row, "acml_tr_pbmn"),
        })

    if not rows:
        return pl.DataFrame()

    return pl.from_pandas(pd.DataFrame(rows))


async def _collect_asset_group(
    api_client,
    instruments: List[dict],
    fetcher_class,
    date_str: str,
    converter_func,
    asset_type: str,
) -> dict:
    """Collect minute bars for a group of instruments."""
    all_data = []
    success = 0
    failed = 0
    no_data = 0
    unexpected_no_data = 0
    unexpected_no_data_codes = []
    failed_codes = []
    consecutive_failures = 0
    aborted = 0
    expected_total = sum(bool(inst.get("expects_data", False)) for inst in instruments)

    for i, inst in enumerate(instruments):
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            print(f"  {asset_type}: {MAX_CONSECUTIVE_FAILURES} consecutive failures -- stopping group")
            remaining = instruments[i:]
            aborted = len(remaining)
            failed += aborted
            failed_codes.extend(item["ticker"] for item in remaining)
            break

        # Circuit-breaker cooldown: if we hit a streak of failures, give the
        # KIS server time to lift any penalty cooldown before the next call.
        if consecutive_failures > 0 and consecutive_failures % CONSECUTIVE_FAILURE_COOLDOWN_THRESHOLD == 0:
            print(f"  {asset_type}: {consecutive_failures} consecutive failures -- "
                  f"cooldown {CONSECUTIVE_FAILURE_COOLDOWN_SEC}s")
            await asyncio.sleep(CONSECUTIVE_FAILURE_COOLDOWN_SEC)

        try:
            fetcher = fetcher_class(
                api_client,
                ticker=inst["ticker"],
                date=date_str,
                kis_market=inst.get("kis_market", "J"),
            )
            pd_df = await fetcher.fetch_data()

            # `fetch_data()` returns None when all retries are exhausted
            # (including rate-limit retries). Treat that as a real failure
            # so the circuit breaker above can engage — otherwise rate-limit
            # failures look identical to "no trades today" and get ignored.
            if pd_df is None:
                failed += 1
                failed_codes.append(inst["ticker"])
                consecutive_failures += 1
                print(f"  {asset_type} {inst['ticker']}: fetch failed (all retries exhausted)")
            else:
                pl_df = converter_func(pd_df, inst["ticker"], inst["isin"], date_str)
                if not pl_df.is_empty():
                    all_data.append(pl_df)
                    success += 1
                else:
                    no_data += 1
                    if inst.get("expects_data", False):
                        unexpected_no_data += 1
                        unexpected_no_data_codes.append(inst["ticker"])
                consecutive_failures = 0

        except Exception as e:
            failed += 1
            failed_codes.append(inst["ticker"])
            consecutive_failures += 1
            print(f"  {asset_type} {inst['ticker']} failed: {e}")

        processed = i + 1
        if processed % 250 == 0 or processed == len(instruments):
            print(
                f"  {asset_type} progress {processed}/{len(instruments)}: "
                f"rows_from={success}, no_data={no_data}, failed={failed}, "
                f"unexpected_no_data={unexpected_no_data}",
                flush=True,
            )

        if i < len(instruments) - 1:
            await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)

    combined = pl.concat(all_data, how="diagonal_relaxed") if all_data else pl.DataFrame()

    return {
        "data": combined,
        "success": success,
        "failed": failed,
        "no_data": no_data,
        "expected_total": expected_total,
        "unexpected_no_data": unexpected_no_data,
        "unexpected_no_data_codes": unexpected_no_data_codes,
        "failed_codes": failed_codes,
        "aborted": aborted,
        "total": len(instruments),
    }


def _append_to_monthly(new_data: pl.DataFrame, month_file: Path, asset_type: str = "stock"):
    """Append data to monthly parquet file (dedup by sec_code + trade_time)."""
    with parquet_update_lock(month_file):
        frames = [normalize_raw_price_frame(new_data, asset_type)]
        if month_file.exists():
            frames.insert(0, normalize_raw_price_frame(pl.read_parquet(month_file), asset_type))
        combined = pl.concat(frames, how="vertical_relaxed")
        combined = combined.unique(subset=["sec_code", "trade_time"], keep="last")
        atomic_write_parquet(combined.sort(["sec_code", "trade_time"]), month_file, compression="lz4")


def _reuse_complete_asset_day(
    month_file: Path,
    date_str: str,
    instruments: List[dict],
) -> dict | None:
    """Reuse any atomically saved asset day only after strict local checks."""
    if not month_file.exists():
        return None
    try:
        target_date = datetime.strptime(date_str, "%Y%m%d").date()
        day = pl.read_parquet(month_file).filter(
            pl.col("trade_time").dt.date() == target_date
        )
        required = {
            "sec_code", "trade_time", "open_price", "high_price",
            "low_price", "close_price", "trade_volume",
        }
        if day.is_empty() or not required.issubset(day.columns):
            return None
        if len(day.unique(subset=["sec_code", "trade_time"])) != len(day):
            return None
        if int(day.select(pl.any_horizontal(pl.col(list(required)).is_null()).sum()).item()):
            return None
        invalid = int(
            day.select(
                (
                    (pl.col("high_price") < pl.max_horizontal("open_price", "low_price", "close_price"))
                    | (pl.col("low_price") > pl.min_horizontal("open_price", "high_price", "close_price"))
                    | (pl.col("open_price") < 0)
                    | (pl.col("high_price") < 0)
                    | (pl.col("low_price") < 0)
                    | (pl.col("close_price") < 0)
                    | (pl.col("trade_volume") < 0)
                ).sum()
            ).item()
        )
        if invalid:
            return None

        got = set(day["sec_code"].unique().to_list())
        expected = {item["isin"] for item in instruments if item.get("expects_data")}
        if not expected.issubset(got):
            return None
        current = {item["isin"] for item in instruments}
        success = len(got & current)
        return {
            "success": success,
            "failed": 0,
            "no_data": max(len(instruments) - success, 0),
            "expected_total": len(expected),
            "unexpected_no_data": 0,
            "unexpected_no_data_codes": [],
            "failed_codes": [],
            "aborted": 0,
            "total": len(instruments),
            "rows": len(day),
        }
    except Exception as exc:
        print(f"  Existing {month_file.parent.name} day cannot be reused: {exc}")
        return None


def _emit_silent_skip_metric(date_str: str, results: dict) -> None:
    """Persist no-data metrics; alert only for instruments expected to trade."""
    rows = []
    expected_all = 0
    unexpected_all = 0
    failed_all = 0
    metric_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    for asset, stats in results.items():
        total = int(stats.get("total", 0) or 0)
        success = int(stats.get("success", 0) or 0)
        failed = int(stats.get("failed", 0) or 0)
        no_data = int(stats.get("no_data", max(total - success - failed, 0)) or 0)
        expected_total = int(stats.get("expected_total", total) or 0)
        unexpected = int(stats.get("unexpected_no_data", 0) or 0)
        optional_no_data = max(no_data - unexpected, 0)
        no_data_pct = (no_data / total * 100.0) if total else 0.0
        unexpected_pct = (
            unexpected / expected_total * 100.0 if expected_total else 0.0
        )
        rows.append(
            {
                "date": metric_date,
                "asset": asset,
                "total": total,
                "success": success,
                "failed": failed,
                "no_data": no_data,
                "no_data_pct": no_data_pct,
                "expected_total": expected_total,
                "optional_no_data": optional_no_data,
                "unexpected_no_data": unexpected,
                # Keep legacy column names, but give them the corrected
                # meaning: only an expected-to-trade empty response is a
                # silent data-loss candidate.
                "silent_skip": unexpected,
                "silent_skip_pct": unexpected_pct,
                "accepted": True,
                "alert_sent": False,
            }
        )
        expected_all += expected_total
        unexpected_all += unexpected
        failed_all += failed

    if not rows:
        return

    metric_dir = get_data_db_path() / "logs" / "kis_minute_silent_skip"
    metric_dir.mkdir(parents=True, exist_ok=True)
    month_file = metric_dir / f"{date_str[:4]}-{date_str[4:6]}.parquet"
    new_df = pl.DataFrame(rows)
    overall_pct = (unexpected_all / expected_all * 100.0) if expected_all else 0.0
    threshold_pct = KIS_SILENT_SKIP_WARN_RATIO * 100.0
    with parquet_update_lock(month_file):
        existing = None
        already_alerted = False
        if month_file.exists():
            existing = pl.read_parquet(month_file)
            day_rows = existing.filter(pl.col("date").cast(pl.Utf8) == metric_date)
            if not day_rows.is_empty() and "alert_sent" in day_rows.columns:
                already_alerted = bool(day_rows["alert_sent"].fill_null(False).any())
            elif not day_rows.is_empty():
                # Backward compatibility: old accepted rows predate alert_sent.
                old_total = int(day_rows["total"].sum() or 0)
                old_silent = int(day_rows["silent_skip"].sum() or 0)
                old_failed = int(day_rows["failed"].sum() or 0)
                old_pct = (old_silent / old_total * 100.0) if old_total else 0.0
                already_alerted = old_failed == 0 and old_pct > threshold_pct

        should_alert = overall_pct > threshold_pct and not already_alerted
        delivered = False
        if should_alert:
            breakdown = ", ".join(
                f"{row['asset']}={row['silent_skip_pct']:.1f}%" for row in rows
            )
            delivered = bool(
                send(
                    "WARN",
                    f"kis_minute_batch unexpected no-data rate {overall_pct:.1f}% "
                    f"(>{threshold_pct:.1f}%) for {date_str}; {breakdown}; "
                    f"api_failed={failed_all}",
                )
            )
        alert_sent = already_alerted or delivered
        new_df = new_df.with_columns(pl.lit(alert_sent).alias("alert_sent"))
        if existing is not None:
            new_df = pl.concat([existing, new_df], how="diagonal_relaxed")
            new_df = new_df.unique(subset=["date", "asset"], keep="last")
        atomic_write_parquet(new_df.sort(["date", "asset"]), month_file)


async def collect_all_minutes(date_str: str) -> dict:
    """Collect all minute bars for a date."""
    from mft_kis.api.rest_api import RestApi
    from mft_kis.feature.stock_minute import StockMinuteFetcher
    from mft_kis.feature.futures_minute import FuturesMinuteFetcher
    from mft_kis.feature.options_minute import OptionsMinuteFetcher
    from mft_kis.universe import load_stock_universe, load_derivative_universe

    config = _get_kis_config()
    api_client = RestApi(config, context='data')
    await api_client.is_connected()

    date_hyphen = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    data_root = get_data_db_path()
    raw_path = get_mft_raw_path() / "price" / "1m"
    results = {}

    try:
        stock_universe = load_stock_universe(date_hyphen, data_root)
        deriv_universe = load_derivative_universe(date_hyphen, data_root)

        print(f"Universes: stocks={len(stock_universe)}, "
              f"idx_fut={len(deriv_universe['index_futures'])}, "
              f"stk_fut={len(deriv_universe['stock_futures'])}, "
              f"idx_opt={len(deriv_universe['index_options'])}, "
              f"stk_opt={len(deriv_universe['stock_options'])}")

        # 1. Stocks
        print(f"\n[1/3] Collecting stock minute bars ({len(stock_universe)} instruments)...")
        month_file = raw_path / "stock" / f"{date_hyphen[:7]}.parquet"
        stock_result = _reuse_complete_asset_day(month_file, date_str, stock_universe)
        if stock_result is not None:
            print(
                f"  Reusing verified stock day: rows={stock_result['rows']}, "
                f"expected={stock_result['expected_total']}",
                flush=True,
            )
        else:
            stock_result = await _collect_asset_group(
                api_client, stock_universe, StockMinuteFetcher, date_str,
                _to_mft_raw_stock, "stock",
            )
        if "data" in stock_result and not stock_result["data"].is_empty():
            month_file.parent.mkdir(parents=True, exist_ok=True)
            _append_to_monthly(stock_result["data"], month_file, "stock")
        if "data" in stock_result:
            stock_result["rows"] = len(stock_result["data"])
        results["stock"] = {k: v for k, v in stock_result.items() if k != "data"}

        await asyncio.sleep(SLEEP_BETWEEN_GROUPS)

        # 2. Futures
        all_futures = (
            deriv_universe["index_futures"] +
            deriv_universe["stock_futures"] +
            deriv_universe["currency_futures"] +
            deriv_universe["bond_futures"]
        )
        print(f"\n[2/3] Collecting futures minute bars ({len(all_futures)} instruments)...")
        month_file = raw_path / "future" / f"{date_hyphen[:7]}.parquet"
        fut_result = _reuse_complete_asset_day(month_file, date_str, all_futures)
        if fut_result is not None:
            print(
                f"  Reusing verified future day: rows={fut_result['rows']}, "
                f"expected={fut_result['expected_total']}",
                flush=True,
            )
        else:
            fut_result = await _collect_asset_group(
                api_client, all_futures, FuturesMinuteFetcher, date_str,
                _to_mft_raw_derivative, "future",
            )
        if "data" in fut_result and not fut_result["data"].is_empty():
            month_file.parent.mkdir(parents=True, exist_ok=True)
            _append_to_monthly(fut_result["data"], month_file, "future")
        if "data" in fut_result:
            fut_result["rows"] = len(fut_result["data"])
        results["future"] = {k: v for k, v in fut_result.items() if k != "data"}

        await asyncio.sleep(SLEEP_BETWEEN_GROUPS)

        # 3. Options
        all_options = deriv_universe["index_options"] + deriv_universe["stock_options"]
        print(f"\n[3/3] Collecting options minute bars ({len(all_options)} instruments)...")
        month_file = raw_path / "option" / f"{date_hyphen[:7]}.parquet"
        opt_result = _reuse_complete_asset_day(month_file, date_str, all_options)
        if opt_result is not None:
            print(
                f"  Reusing verified option day: rows={opt_result['rows']}, "
                f"expected={opt_result['expected_total']}",
                flush=True,
            )
        else:
            opt_result = await _collect_asset_group(
                api_client, all_options, OptionsMinuteFetcher, date_str,
                _to_mft_raw_derivative, "option",
            )
        if "data" in opt_result and not opt_result["data"].is_empty():
            month_file.parent.mkdir(parents=True, exist_ok=True)
            _append_to_monthly(opt_result["data"], month_file, "option")
        if "data" in opt_result:
            opt_result["rows"] = len(opt_result["data"])
        results["option"] = {k: v for k, v in opt_result.items() if k != "data"}

    finally:
        await api_client.close()

    return results


# --- DAG Definition ---

@task.short_circuit
def check_trading_day(logical_date=None, data_interval_end=None, dag_run=None) -> bool:
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    if not is_trading_day(date):
        print(f"Skip: {date.date()} is not a trading day")
        return False
    return True


@task(
    execution_timeout=timedelta(hours=KIS_TASK_TIMEOUT_HOURS),
    retries=KIS_TASK_RETRIES,
    retry_delay=timedelta(minutes=10),
)
def collect_minute_bars(
    logical_date=None,
    data_interval_end=None,
    dag_run=None,
) -> dict:
    """Collect all KIS minute bars for the date."""
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )

    date_str = date.strftime("%Y%m%d")
    print(f"Collecting minute bars for {date_str}")

    results = asyncio.run(collect_all_minutes(date_str))

    for asset, stats in results.items():
        print(f"  {asset}: {stats['success']}/{stats['total']} success, {stats['failed']} failed")

    return finalize_collection_results(date_str, results)


def validate_collection_results(results: dict) -> None:
    """Fail a batch that would leave a partial trading-day dataset."""
    errors = []
    for asset in ("stock", "future", "option"):
        stats = results.get(asset)
        if not stats:
            errors.append(f"{asset}: missing collection result")
            continue
        total = int(stats.get("total", 0) or 0)
        failed = int(stats.get("failed", 0) or 0)
        rows = int(stats.get("rows", 0) or 0)
        unexpected_no_data = int(stats.get("unexpected_no_data", 0) or 0)
        allowed_failures = max(KIS_MAX_FAILED_COUNT, int(total * KIS_MAX_FAILED_RATIO))
        expected_total = int(stats.get("expected_total", 0) or 0)
        allowed_unexpected = max(
            KIS_MAX_UNEXPECTED_NO_DATA_COUNT,
            int(expected_total * KIS_MAX_UNEXPECTED_NO_DATA_RATIO),
        )
        if total <= 0:
            errors.append(f"{asset}: empty universe")
        if rows <= 0:
            errors.append(f"{asset}: no minute rows collected")
        if failed > allowed_failures:
            codes = stats.get("failed_codes", [])[:10]
            errors.append(
                f"{asset}: failed={failed} exceeds allowed={allowed_failures}; "
                f"sample={codes}"
            )
        if unexpected_no_data > allowed_unexpected:
            codes = stats.get("unexpected_no_data_codes", [])[:10]
            errors.append(
                f"{asset}: expected-data instruments returned no rows="
                f"{unexpected_no_data} exceeds allowed={allowed_unexpected}; "
                f"sample={codes}"
            )
    if errors:
        raise RuntimeError("Incomplete KIS minute collection: " + " | ".join(errors))


def finalize_collection_results(date_str: str, results: dict) -> dict:
    """Accept a complete batch before recording or alerting on no-data rows."""
    validate_collection_results(results)
    _emit_silent_skip_metric(date_str, results)
    return results


@task
def trigger_conversion(
    results: dict,
    logical_date=None,
    data_interval_end=None,
    dag_run=None,
    run_id=None,
) -> dict:
    validate_collection_results(results)
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    return trigger_next_dag(
        "convert_raw_to_bt",
        date.date().isoformat(),
        source_dag_id="kis_minute_batch",
        source_run_id=run_id or "manual",
    )


@dag(
    dag_id="kis_minute_batch",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kis", "daily", "minute"],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
)
def kis_minute_batch():
    check = check_trading_day()
    collect = collect_minute_bars()
    trigger = trigger_conversion(collect)
    check >> collect >> trigger

kis_minute_batch()
