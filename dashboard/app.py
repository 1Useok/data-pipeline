"""Streamlit dashboard for the local market data pipeline."""

from __future__ import annotations

import os
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
import streamlit as st

DATA_DB = Path(os.environ.get("DATA_DB_PATH", "/data_db"))
DAGS_DIR = Path(os.environ.get("DAGS_DIR", "/opt/airflow/dags"))
if not DAGS_DIR.exists():
    DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
if DAGS_DIR.exists() and str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

ASSETS = ["stock", "future", "option"]


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _latest_daily_file_date(files: list[Path]) -> str:
    dates = [_parse_iso_date(path.stem) for path in files]
    dates = [dt for dt in dates if dt is not None]
    return max(dates).isoformat() if dates else "N/A"


def _latest_date_in_parquet(path: Path, columns: tuple[str, ...]) -> date | None:
    for column in columns:
        try:
            df = pl.scan_parquet(path).select(pl.col(column).dt.date().max().alias("latest")).collect()
        except Exception:
            continue
        if df.is_empty():
            continue
        value = df.item(0, "latest")
        if value is not None:
            return value
    return None


def _latest_monthly_parquet_date(files: list[Path], columns: tuple[str, ...]) -> str:
    latest: date | None = None
    for path in sorted(files, reverse=True)[:3]:
        value = _latest_date_in_parquet(path, columns)
        if value is not None and (latest is None or value > latest):
            latest = value
    return latest.isoformat() if latest is not None else "N/A"


def get_recent_summaries(days: int = 7) -> pl.DataFrame:
    """Load recent collection summary logs."""
    logs_dir = DATA_DB / "logs"
    if not logs_dir.exists():
        return pl.DataFrame()

    frames = []
    today = date.today()
    for i in range(days):
        d = today - timedelta(days=i)
        path = logs_dir / f"{d.isoformat()}_integrity.parquet"
        if path.exists():
            frames.append(pl.read_parquet(path).with_columns(pl.lit(d.isoformat()).alias("date")))

    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


@st.cache_data(ttl=300)
def get_data_freshness() -> list[dict]:
    """Check latest data month/date for each data source."""
    items = []

    krx_base = DATA_DB / "MFT_backtesting" / "KR" / "krx_crawled"
    if krx_base.exists():
        for item_dir in sorted(krx_base.iterdir()):
            if item_dir.is_dir():
                files = sorted(item_dir.rglob("*.parquet"))
                latest = _latest_daily_file_date(files)
                items.append({"source": f"krx/{item_dir.name}", "latest": latest})

    raw_base = DATA_DB / "MFT_raw" / "KR" / "price" / "1m"
    if raw_base.exists():
        for asset_dir in sorted(raw_base.iterdir()):
            if asset_dir.is_dir():
                files = sorted(asset_dir.glob("*.parquet"))
                latest = _latest_monthly_parquet_date(files, ("trade_time", "datetime"))
                items.append({"source": f"raw/1m/{asset_dir.name}", "latest": latest})

    bt_base = DATA_DB / "MFT_backtesting" / "KR" / "price"
    if bt_base.exists():
        for tf_dir in sorted(bt_base.iterdir()):
            if tf_dir.is_dir():
                for asset_dir in sorted(tf_dir.iterdir()):
                    if asset_dir.is_dir():
                        files = sorted(asset_dir.glob("*.parquet"))
                        latest = _latest_monthly_parquet_date(files, ("datetime", "trade_time"))
                        items.append({"source": f"bt/{tf_dir.name}/{asset_dir.name}", "latest": latest})

    return items


def get_disk_usage() -> dict:
    """Get filesystem usage for the DATA_DB volume without walking every file."""
    usage = shutil.disk_usage(DATA_DB if DATA_DB.exists() else DATA_DB.anchor or ".")
    return {
        "used_gb": round(usage.used / (1024**3), 2),
        "total_gb": round(usage.total / (1024**3), 2),
        "free_gb": round(usage.free / (1024**3), 2),
    }


def _recent_months(days: int) -> list[str]:
    end = date.today()
    current = end
    months = []
    while current >= end - timedelta(days=days):
        ym = current.strftime("%Y-%m")
        if ym not in months:
            months.append(ym)
        current -= timedelta(days=15)
    return months


@st.cache_data(ttl=300)
def get_row_count_trend(days: int = 30) -> pl.DataFrame:
    """Daily row counts from raw 1m monthly parquet files."""
    base = DATA_DB / "MFT_raw" / "KR" / "price" / "1m"
    start = date.today() - timedelta(days=days - 1)
    frames = []

    for asset in ASSETS:
        asset_frames = []
        for ym in _recent_months(days):
            path = base / asset / f"{ym}.parquet"
            if not path.exists():
                continue
            try:
                df = pl.read_parquet(path, columns=["trade_time"])
            except Exception:
                continue
            if df.is_empty() or "trade_time" not in df.columns:
                continue
            asset_frames.append(
                df.with_columns(pl.col("trade_time").dt.date().alias("date"))
                .group_by("date")
                .len()
                .rename({"len": "rows"})
                .with_columns(pl.lit(asset).alias("asset"))
            )

        if asset_frames:
            frames.append(pl.concat(asset_frames, how="diagonal_relaxed"))

    if not frames:
        return pl.DataFrame({"date": [], "asset": [], "rows": []})

    return (
        pl.concat(frames, how="diagonal_relaxed")
        .group_by(["date", "asset"])
        .agg(pl.col("rows").sum())
        .filter(pl.col("date") >= start)
        .sort(["date", "asset"])
    )


@st.cache_data(ttl=300)
def get_silent_skip_history(months: int = 3) -> pl.DataFrame:
    """Read silent-skip metrics written by kis_minute_batch."""
    base = DATA_DB / "logs" / "kis_minute_silent_skip"
    if not base.exists():
        return pl.DataFrame({"date": [], "asset": [], "silent_skip_pct": []})

    frames = []
    for path in sorted(base.glob("*.parquet"))[-months:]:
        try:
            frames.append(pl.read_parquet(path))
        except Exception:
            pass

    if not frames:
        return pl.DataFrame({"date": [], "asset": [], "silent_skip_pct": []})

    return (
        pl.concat(frames, how="diagonal_relaxed")
        .with_columns(pl.col("date").cast(pl.Date))
        .sort(["date", "asset"])
    )


@st.cache_data(ttl=300)
def get_missing_calendar(days: int = 60) -> pl.DataFrame:
    """Build a recent calendar with missing trading-day outputs marked red."""
    try:
        from lib import missing_detector
    except Exception:
        return pl.DataFrame({"date": [], "weekday": [], "status": [], "missing_dags": []})

    end = date.today() - timedelta(days=1)
    result = missing_detector.scan_recent(end=end, days=min(days, 30))
    missing_by_date: dict[str, set[str]] = {}
    for item in result["missing"]:
        missing_by_date.setdefault(item["date"], set()).add(item["dag_id"])

    trading_days = set(result["dates"])
    rows = []
    for i in range(days):
        day = end - timedelta(days=days - 1 - i)
        day_str = day.isoformat()
        missing_dags = sorted(missing_by_date.get(day_str, set()))
        if missing_dags:
            status = "MISSING"
        elif day_str in trading_days:
            status = "OK"
        else:
            status = "NO_SESSION"
        rows.append(
            {
                "date": day,
                "weekday": day.weekday(),
                "status": status,
                "missing_dags": ", ".join(missing_dags),
            }
        )
    return pl.DataFrame(rows)


def _line_chart(df: pl.DataFrame, x: str, y: str, group: str, title: str) -> go.Figure:
    fig = go.Figure()
    if df.is_empty():
        fig.update_layout(title=title, height=280)
        return fig
    for key in df[group].unique().sort().to_list():
        series = df.filter(pl.col(group) == key).sort(x)
        fig.add_trace(go.Scatter(x=series[x].to_list(), y=series[y].to_list(), mode="lines+markers", name=str(key)))
    fig.update_layout(title=title, height=300, margin=dict(l=20, r=20, t=40, b=20), legend_title_text="")
    return fig


def _missing_calendar_figure(df: pl.DataFrame) -> go.Figure:
    colors = {"OK": "#16a34a", "MISSING": "#dc2626", "NO_SESSION": "#94a3b8"}
    labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    fig = go.Figure()
    for status, color in colors.items():
        series = df.filter(pl.col("status") == status)
        if series.is_empty():
            continue
        fig.add_trace(
            go.Scatter(
                x=series["date"].to_list(),
                y=series["weekday"].to_list(),
                mode="markers",
                name=status,
                marker=dict(size=16, color=color, symbol="square"),
                text=series["missing_dags"].to_list(),
                hovertemplate="%{x|%Y-%m-%d}<br>%{fullData.name}<br>%{text}<extra></extra>",
            )
        )
    fig.update_yaxes(tickmode="array", tickvals=list(range(7)), ticktext=labels, autorange="reversed")
    fig.update_layout(height=320, margin=dict(l=20, r=20, t=20, b=20), legend_title_text="")
    return fig


st.set_page_config(page_title="Market Data Pipeline", layout="wide")
st.title("Market Data Pipeline")

st.header("Data Freshness")
freshness = get_data_freshness()
if freshness:
    st.dataframe(pl.DataFrame(freshness), width="stretch")
else:
    st.info("No data found")

st.header("Recent Integrity Checks")
summaries = get_recent_summaries(7)
if not summaries.is_empty():
    summary_counts = summaries.group_by(["date", "status"]).agg(pl.len().alias("count")).sort("date", descending=True)
    st.dataframe(summary_counts, width="stretch")
    failures = summaries.filter(pl.col("status") == "FAIL")
    if not failures.is_empty():
        st.subheader("Failures")
        st.dataframe(failures, width="stretch")
else:
    st.info("No integrity check results found")

st.header("Disk Usage")
usage = get_disk_usage()
cols = st.columns(3)
cols[0].metric("Used", f"{usage['used_gb']} GB")
cols[1].metric("Free", f"{usage['free_gb']} GB")
cols[2].metric("Total", f"{usage['total_gb']} GB")

st.header("Row Count Trend")
row_counts = get_row_count_trend(30)
if row_counts.is_empty():
    st.info("No row count data found")
else:
    st.plotly_chart(_line_chart(row_counts, "date", "rows", "asset", "Daily collected rows, last 30 days"), width="stretch")
    st.dataframe(row_counts, width="stretch")

st.header("Silent-skip Rate")
silent_skip = get_silent_skip_history()
if silent_skip.is_empty():
    st.info("No silent-skip metrics found")
else:
    fig = _line_chart(silent_skip, "date", "silent_skip_pct", "asset", "Silent-skip percentage")
    fig.add_hline(y=5.0, line_dash="dash", line_color="#dc2626", annotation_text="5% threshold")
    st.plotly_chart(fig, width="stretch")
    st.dataframe(silent_skip, width="stretch")

st.header("Missing Calendar")
missing_calendar = get_missing_calendar(60)
if missing_calendar.is_empty():
    st.info("No missing-output data found")
else:
    st.plotly_chart(_missing_calendar_figure(missing_calendar), width="stretch")
    missing_only = missing_calendar.filter(pl.col("status") == "MISSING")
    if not missing_only.is_empty():
        st.dataframe(missing_only.select(["date", "missing_dags"]), width="stretch")
