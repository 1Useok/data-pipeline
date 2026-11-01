"""Pure comparison primitives — operate on polars DataFrames."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import polars as pl

NUMERIC_TOL_ABS = 1e-6
NUMERIC_TOL_REL = 1e-4    # 0.01%
ROW_COUNT_TOL_REL = 0.01  # 1%


@dataclass
class MetricResult:
    name: str
    local: Any
    server: Any
    diff: Any
    status: str  # "PASS" | "WARN" | "FAIL"


def compare_existence(local_path: Path, server_path: Path) -> MetricResult:
    """Both must exist for the comparison to proceed."""
    if not local_path.exists() and not server_path.exists():
        return MetricResult("existence", False, False, "missing both", "FAIL")
    if not local_path.exists():
        return MetricResult("existence", False, True, "missing local", "FAIL")
    if not server_path.exists():
        return MetricResult("existence", True, False, "missing server", "FAIL")
    return MetricResult("existence", True, True, 0, "PASS")


def filter_to_date(df: pl.DataFrame, target_date: str, date_col: str = "date") -> pl.DataFrame:
    """Filter df to rows matching target_date (YYYY-MM-DD).

    Tries date_col first, then falls back to timestamp-like columns.
    """
    if date_col in df.columns:
        # Cast to string so the comparison works for both Date and Utf8 dtypes.
        return df.filter(pl.col(date_col).cast(pl.Utf8) == target_date)
    for ts_col in ("timestamp", "trade_time", "ts"):
        if ts_col in df.columns:
            return df.filter(pl.col(ts_col).dt.date().cast(pl.Utf8) == target_date)
    raise ValueError(
        f"No date or timestamp column in {df.columns}; expected one of "
        f"['date', 'timestamp', 'trade_time', 'ts']"
    )


def compare_schema(df_local: pl.DataFrame, df_server: pl.DataFrame) -> list[MetricResult]:
    cols_local = set(df_local.columns)
    cols_server = set(df_server.columns)
    if cols_local != cols_server:
        return [MetricResult(
            name="columns",
            local=sorted(cols_local),
            server=sorted(cols_server),
            diff=(
                f"missing_local={sorted(cols_server - cols_local)}, "
                f"missing_server={sorted(cols_local - cols_server)}"
            ),
            status="FAIL",
        )]
    out: list[MetricResult] = []
    for col in df_local.columns:
        t_local = df_local.schema[col]
        t_server = df_server.schema[col]
        if t_local != t_server:
            out.append(MetricResult(
                name=f"{col}.dtype",
                local=str(t_local),
                server=str(t_server),
                diff="different",
                status="FAIL",
            ))
    if not out:
        out.append(MetricResult("schema", "ok", "ok", 0, "PASS"))
    return out


def compare_row_count(df_local: pl.DataFrame, df_server: pl.DataFrame) -> MetricResult:
    n_local = len(df_local)
    n_server = len(df_server)
    diff = n_local - n_server
    if n_server == 0:
        rel = 0.0 if diff == 0 else float("inf")
    else:
        rel = abs(diff) / n_server
    if diff == 0:
        status = "PASS"
    elif rel <= ROW_COUNT_TOL_REL:
        status = "WARN"
    else:
        status = "FAIL"
    return MetricResult("rows", n_local, n_server, diff, status)


def _num_status(v_local, v_server):
    if v_local is None and v_server is None:
        return "PASS", 0
    if v_local is None or v_server is None:
        return "FAIL", "one None"
    d = float(v_local) - float(v_server)
    if abs(d) < NUMERIC_TOL_ABS:
        return "PASS", d
    if v_server != 0 and abs(d / float(v_server)) < NUMERIC_TOL_REL:
        return "PASS", d
    return "FAIL", d


def compare_numeric_column(
    df_local: pl.DataFrame, df_server: pl.DataFrame, col: str
) -> list[MetricResult]:
    out: list[MetricResult] = []
    for stat in ("min", "max", "mean", "sum"):
        v_local = getattr(df_local[col], stat)()
        v_server = getattr(df_server[col], stat)()
        status, diff = _num_status(v_local, v_server)
        out.append(MetricResult(f"{col}.{stat}", v_local, v_server, diff, status))
    return out


def compare_string_column(
    df_local: pl.DataFrame, df_server: pl.DataFrame, col: str
) -> MetricResult:
    n_local = df_local[col].n_unique()
    n_server = df_server[col].n_unique()
    diff = n_local - n_server
    status = "PASS" if diff == 0 else "WARN"
    return MetricResult(f"{col}.unique", n_local, n_server, diff, status)
