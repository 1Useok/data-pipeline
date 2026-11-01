"""Unit tests for _compare_helpers."""
from __future__ import annotations
from pathlib import Path
import polars as pl
import pytest

from tests._compare_helpers import (
    MetricResult,
    compare_existence,
)


def test_compare_existence_both_present(tmp_path):
    a = tmp_path / "a.parquet"; a.write_bytes(b"x")
    b = tmp_path / "b.parquet"; b.write_bytes(b"x")
    r = compare_existence(a, b)
    assert r.status == "PASS"


def test_compare_existence_missing_local(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"; b.write_bytes(b"x")
    r = compare_existence(a, b)
    assert r.status == "FAIL"
    assert "missing local" in r.diff


def test_compare_existence_missing_server(tmp_path):
    a = tmp_path / "a.parquet"; a.write_bytes(b"x")
    b = tmp_path / "b.parquet"
    r = compare_existence(a, b)
    assert r.status == "FAIL"
    assert "missing server" in r.diff


def test_compare_existence_missing_both(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    r = compare_existence(a, b)
    assert r.status == "FAIL"
    assert "missing both" in r.diff


def _make_df_with_date():
    return pl.DataFrame({
        "date": ["2026-04-21", "2026-04-22", "2026-04-22", "2026-04-23"],
        "value": [1, 2, 3, 4],
    })


def _make_df_with_timestamp():
    import datetime as dt
    return pl.DataFrame({
        "timestamp": [
            dt.datetime(2026, 4, 21, 9, 0),
            dt.datetime(2026, 4, 22, 9, 0),
            dt.datetime(2026, 4, 22, 15, 0),
            dt.datetime(2026, 4, 23, 9, 0),
        ],
        "value": [1, 2, 3, 4],
    })


def test_filter_to_date_using_date_column():
    from tests._compare_helpers import filter_to_date
    df = _make_df_with_date()
    out = filter_to_date(df, "2026-04-22")
    assert len(out) == 2
    assert out["value"].to_list() == [2, 3]


def test_filter_to_date_using_timestamp_column():
    from tests._compare_helpers import filter_to_date
    df = _make_df_with_timestamp()
    out = filter_to_date(df, "2026-04-22")
    assert len(out) == 2


def test_filter_to_date_no_date_column_raises():
    from tests._compare_helpers import filter_to_date
    df = pl.DataFrame({"x": [1, 2]})
    with pytest.raises(ValueError, match="No date or timestamp column"):
        filter_to_date(df, "2026-04-22")


def test_compare_schema_identical():
    from tests._compare_helpers import compare_schema
    a = pl.DataFrame({"x": [1], "y": ["a"]})
    b = pl.DataFrame({"x": [2], "y": ["b"]})
    out = compare_schema(a, b)
    assert all(r.status == "PASS" for r in out)


def test_compare_schema_extra_column_local():
    from tests._compare_helpers import compare_schema
    a = pl.DataFrame({"x": [1], "y": ["a"], "z": [9]})
    b = pl.DataFrame({"x": [2], "y": ["b"]})
    out = compare_schema(a, b)
    assert any(r.status == "FAIL" for r in out)


def test_compare_schema_dtype_mismatch():
    from tests._compare_helpers import compare_schema
    a = pl.DataFrame({"x": pl.Series([1], dtype=pl.Int64)})
    b = pl.DataFrame({"x": pl.Series([1], dtype=pl.Float64)})
    out = compare_schema(a, b)
    assert any(r.status == "FAIL" and "dtype" in r.name for r in out)


def test_compare_row_count_equal():
    from tests._compare_helpers import compare_row_count
    a = pl.DataFrame({"x": [1, 2, 3]})
    b = pl.DataFrame({"x": [1, 2, 3]})
    r = compare_row_count(a, b)
    assert r.status == "PASS" and r.diff == 0


def test_compare_row_count_within_1pct_warns():
    from tests._compare_helpers import compare_row_count
    a = pl.DataFrame({"x": list(range(1000))})
    b = pl.DataFrame({"x": list(range(1005))})
    r = compare_row_count(a, b)
    assert r.status == "WARN"


def test_compare_row_count_large_diff_fails():
    from tests._compare_helpers import compare_row_count
    a = pl.DataFrame({"x": list(range(100))})
    b = pl.DataFrame({"x": list(range(200))})
    r = compare_row_count(a, b)
    assert r.status == "FAIL"


def test_compare_numeric_column_equal():
    from tests._compare_helpers import compare_numeric_column
    a = pl.DataFrame({"v": [1.0, 2.0, 3.0]})
    b = pl.DataFrame({"v": [1.0, 2.0, 3.0]})
    out = compare_numeric_column(a, b, "v")
    assert all(r.status == "PASS" for r in out)
    assert {r.name for r in out} == {"v.min", "v.max", "v.mean", "v.sum"}


def test_compare_numeric_column_small_relative_diff_passes():
    from tests._compare_helpers import compare_numeric_column
    a = pl.DataFrame({"v": [100.0, 200.0]})
    b = pl.DataFrame({"v": [100.000005, 200.0]})  # ~5e-8 relative
    out = compare_numeric_column(a, b, "v")
    assert all(r.status == "PASS" for r in out)


def test_compare_numeric_column_large_diff_fails():
    from tests._compare_helpers import compare_numeric_column
    a = pl.DataFrame({"v": [1.0, 2.0]})
    b = pl.DataFrame({"v": [1.0, 5.0]})
    out = compare_numeric_column(a, b, "v")
    failed = [r for r in out if r.status == "FAIL"]
    assert any(r.name == "v.sum" for r in failed)


def test_compare_string_column_equal_uniques():
    from tests._compare_helpers import compare_string_column
    a = pl.DataFrame({"s": ["x", "y", "x"]})
    b = pl.DataFrame({"s": ["x", "y", "y"]})
    r = compare_string_column(a, b, "s")
    assert r.status == "PASS"
    assert r.local == 2 and r.server == 2


def test_compare_string_column_diff_uniques_warns():
    from tests._compare_helpers import compare_string_column
    a = pl.DataFrame({"s": ["x", "y", "z"]})
    b = pl.DataFrame({"s": ["x", "y"]})
    r = compare_string_column(a, b, "s")
    assert r.status == "WARN"
