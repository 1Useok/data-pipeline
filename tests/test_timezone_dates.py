"""Regression tests for Asia/Seoul midnight date handling."""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
from pandas import Timestamp

import convert_raw_to_bt
import data_quality_check
from lib import missing_detector


def _write_kst_midnight(path):
    frame = pl.DataFrame(
        {
            "datetime": [datetime(2026, 8, 3, tzinfo=ZoneInfo("Asia/Seoul"))],
            "value": [1],
        }
    )
    frame.write_parquet(path)


def test_quality_reader_preserves_kst_calendar_date(tmp_path):
    path = tmp_path / "month.parquet"
    _write_kst_midnight(path)

    result = data_quality_check._read_day(path, "datetime", Timestamp("2026-08-03"))

    assert len(result) == 1


def test_conversion_date_probe_preserves_kst_calendar_date(tmp_path):
    path = tmp_path / "month.parquet"
    _write_kst_midnight(path)

    assert convert_raw_to_bt._month_file_has_date(
        path, Timestamp("2026-08-03"), "datetime"
    )


def test_missing_detector_preserves_kst_calendar_date(tmp_path):
    path = tmp_path / "month.parquet"
    _write_kst_midnight(path)

    assert missing_detector._month_file_has_date(
        path, Timestamp("2026-08-03").date(), "datetime"
    )
