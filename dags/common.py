"""Data pipeline DAG common module.

Merged from server's krx_common.py + kis_common.py.
polars-based data processing, local Windows paths via DATA_DB_PATH env var.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping

import exchange_calendars as xcals
import polars as pl
from pandas import Timestamp

XKRX = xcals.get_calendar("XKRX")
XKRX_START_DATE = date(2005, 12, 1)
SATURDAY_TRADING_END = date(1998, 12, 5)

DAGS_DIR = Path(__file__).parent
PROJECT_ROOT = DAGS_DIR.parent
PACKAGES_DIR = PROJECT_ROOT / "packages"
if str(PACKAGES_DIR) not in sys.path:
    sys.path.insert(0, str(PACKAGES_DIR))

from mft_common.storage import atomic_write_parquet, parquet_update_lock  # noqa: E402

TEMP_DIR = Path(tempfile.gettempdir()) / "market_data_pipeline"

from lib.callbacks import on_failure_callback  # noqa: E402

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}


def get_data_db_path() -> Path:
    """Get DATA_DB_PATH from environment."""
    path = os.environ.get("DATA_DB_PATH")
    if not path:
        raise ValueError("DATA_DB_PATH environment variable not set")
    return Path(path)


def get_krx_crawled_path() -> Path:
    """Path to raw daily KRX crawl archives."""
    return get_data_db_path() / "MFT_archive" / "KR" / "krx_crawled"


def get_krx_backtesting_path() -> Path:
    """Path to January-schema monthly KRX backtesting data."""
    return get_data_db_path() / "MFT_backtesting" / "KR" / "krx_crawled"


def get_mft_raw_path() -> Path:
    """Path to MFT_raw data."""
    return get_data_db_path() / "MFT_raw" / "KR"


def get_mft_bt_path() -> Path:
    """Path to MFT_backtesting data."""
    return get_data_db_path() / "MFT_backtesting" / "KR"


def setup_proxy():
    """No proxy needed in local environment."""
    pass


def to_naive_timestamp(value: Any) -> Timestamp | None:
    """Convert a timestamp to timezone-naive Asia/Seoul time.

    Unlike the former implementation, this preserves the time of day.  Date
    based DAGs should call :func:`resolve_target_date`, which normalizes the
    result after selecting the correct Airflow data interval boundary.
    """
    if value is None:
        return None
    if hasattr(value, "in_timezone"):
        value = value.in_timezone("Asia/Seoul")
    ts = Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("Asia/Seoul").tz_localize(None)
    return ts


def _dag_run_conf(dag_run: Any) -> Mapping[str, Any]:
    conf = getattr(dag_run, "conf", None)
    return conf if isinstance(conf, Mapping) else {}


def resolve_run_timestamp(
    *,
    logical_date: Any = None,
    data_interval_end: Any = None,
    dag_run: Any = None,
    now: Any = None,
) -> Timestamp:
    """Resolve the timestamp represented by an Airflow task invocation.

    A catch-up/manual ``dag_run.conf.date`` has highest priority.  Scheduled
    runs use ``data_interval_end`` rather than ``logical_date`` because Airflow
    defines the latter as the start of the data interval.
    """
    override = _dag_run_conf(dag_run).get("date")
    source = override or data_interval_end or logical_date
    if source is None:
        source = now if now is not None else Timestamp.now(tz="Asia/Seoul")
    resolved = to_naive_timestamp(source)
    if resolved is None:  # pragma: no cover - defensive, source is non-null
        raise ValueError("Unable to resolve Airflow run timestamp")
    return resolved


def resolve_target_date(
    *,
    logical_date: Any = None,
    data_interval_end: Any = None,
    dag_run: Any = None,
    now: Any = None,
) -> Timestamp:
    """Resolve and normalize the trading date for an Airflow task run."""
    return resolve_run_timestamp(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
        now=now,
    ).normalize()


def market_date_expr(column: str) -> pl.Expr:
    '''Return the local calendar date of a timezone-aware datetime column.

    ``cast(pl.Date)`` converts timezone-aware values through UTC first, which
    turns Asia/Seoul midnight into the previous date. ``dt.date()`` preserves
    the wall-clock date represented by the column.
    '''
    return pl.col(column).dt.date()


# --- Temp file functions ---

def get_temp_path(task_name: str, date: Timestamp, run_id: str) -> Path:
    safe_run_id = run_id.replace(":", "_").replace("+", "_")
    return TEMP_DIR / safe_run_id / f"{task_name}_{date.strftime('%Y%m%d')}.parquet"


def _drop_index_column(df: pl.DataFrame) -> pl.DataFrame:
    if "__index_level_0__" in df.columns:
        df = df.drop("__index_level_0__")
    return df


def save_to_temp(df: pl.DataFrame, task_name: str, date: Timestamp, run_id: str) -> str:
    temp_path = get_temp_path(task_name, date, run_id)
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    df = _drop_index_column(df)
    atomic_write_parquet(df, temp_path)
    return str(temp_path)


def load_from_temp(temp_path: str) -> pl.DataFrame:
    return pl.read_parquet(temp_path)


def cleanup_temp(temp_path: str) -> None:
    path = Path(temp_path)
    if path.exists():
        path.unlink()
    if path.parent.exists() and not any(path.parent.iterdir()):
        path.parent.rmdir()


# --- Final save functions ---

def get_parquet_path(item_name: str, date: Timestamp) -> Path:
    """Daily parquet path: krx_crawled/{item}/YYYY-MM-DD.parquet"""
    date_str = date.strftime("%Y-%m-%d")
    return get_krx_crawled_path() / item_name / f"{date_str}.parquet"


def save_to_final(df: pl.DataFrame, item_name: str, date: Timestamp) -> str:
    """Save polars DataFrame to final location (daily file)."""
    if df is None or len(df) == 0:
        return f"skip: no data for {date.strftime('%Y-%m-%d')}"
    file_path = get_parquet_path(item_name, date)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df = _drop_index_column(df)
    atomic_write_parquet(df, file_path)
    return f"saved: {file_path}"


def save_to_final_append(df: pl.DataFrame, file_path: Path, unique_cols: list[str]) -> str:
    """Save polars DataFrame by appending to existing monthly parquet."""
    if df is None or len(df) == 0:
        return "skip: no data"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with parquet_update_lock(file_path):
        if file_path.exists():
            existing = pl.read_parquet(file_path)
            df = pl.concat([existing, df], how="diagonal_relaxed")
            df = df.unique(subset=unique_cols, keep="last")
        df = df.sort(unique_cols)
        atomic_write_parquet(df, file_path)
    return f"saved: {file_path} ({len(df)} rows)"


# --- Crawl helpers ---

def safe_crawl(crawler_func, *args, **kwargs) -> pl.DataFrame:
    """Call crawler function (pandas) and convert to polars."""
    try:
        pd_data = crawler_func(*args, **kwargs)
        if pd_data is None or len(pd_data) == 0:
            return pl.DataFrame()
        return pl.from_pandas(pd_data)
    except (KeyError, ValueError, IndexError) as e:
        print(f"Empty data (expected on weekends/holidays): {e}")
        return pl.DataFrame()
    except Exception as e:
        print(f"Crawler error: {e}")
        raise


# --- Trading day checks ---

_HOLIDAYS_1980_2005: set[date] | None = None


def _load_holidays_1980_2005() -> set[date]:
    global _HOLIDAYS_1980_2005
    if _HOLIDAYS_1980_2005 is not None:
        return _HOLIDAYS_1980_2005
    holidays_file = DAGS_DIR / "holidays_1980_2005.json"
    if not holidays_file.exists():
        _HOLIDAYS_1980_2005 = set()
        return _HOLIDAYS_1980_2005
    with open(holidays_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    _HOLIDAYS_1980_2005 = {date.fromisoformat(d) for d in data.get("holidays", [])}
    return _HOLIDAYS_1980_2005


def is_trading_day(ts: Timestamp) -> bool:
    """Check if date is a KRX trading day."""
    if isinstance(ts, Timestamp):
        d = ts.date()
    elif isinstance(ts, date):
        d = ts
    else:
        d = Timestamp(ts).date()

    if d <= SATURDAY_TRADING_END:
        if d.weekday() == 6:
            return False
    else:
        if d.weekday() >= 5:
            return False

    if d >= XKRX_START_DATE:
        return XKRX.is_session(Timestamp(d))

    holidays = _load_holidays_1980_2005()
    if not holidays:
        return True
    return d not in holidays


def is_market_hours(ts: Timestamp) -> bool:
    """Check if within market hours (09:00~15:45 KST)."""
    if not is_trading_day(ts):
        return False
    hour, minute = ts.hour, ts.minute
    if hour < 9:
        return False
    if hour == 15 and minute > 45:
        return False
    if hour > 15:
        return False
    return True
