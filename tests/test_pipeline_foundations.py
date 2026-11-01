"""Regression tests for date resolution, storage, and pipeline ordering."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pendulum
import polars as pl
import pytest

import common
from catch_up import _active_run_for_date
from krx_convert_adj_ratio import _should_continue_pipeline
from lib.pipeline import PIPELINE_STAGES, earliest_missing_stage, find_active_run_for_date
from mft_common import storage


@dataclass
class FakeDagRun:
    conf: dict
    state: str = "queued"
    run_id: str = "fake"


def test_resolve_target_date_prefers_conf_over_interval_and_logical_date():
    value = common.resolve_target_date(
        logical_date=pendulum.datetime(2026, 4, 20, tz="UTC"),
        data_interval_end=pendulum.datetime(2026, 4, 22, 23, tz="Asia/Seoul"),
        dag_run=FakeDagRun({"date": "2026-04-21"}),
    )
    assert value == datetime(2026, 4, 21)


def test_resolve_target_date_uses_data_interval_end_for_scheduled_run():
    value = common.resolve_target_date(
        logical_date=pendulum.datetime(2026, 4, 21, 23, tz="Asia/Seoul"),
        data_interval_end=pendulum.datetime(2026, 4, 22, 23, tz="Asia/Seoul"),
    )
    assert value == datetime(2026, 4, 22)


def test_to_naive_timestamp_preserves_market_time():
    value = common.to_naive_timestamp(
        pendulum.datetime(2026, 4, 22, 9, 17, tz="Asia/Seoul")
    )
    assert (value.hour, value.minute) == (9, 17)


def test_atomic_write_preserves_old_file_when_replace_fails(tmp_path, monkeypatch):
    target = tmp_path / "month.parquet"
    pl.DataFrame({"value": [1]}).write_parquet(target)

    def fail_replace(source, destination):
        raise OSError("simulated replacement failure")

    monkeypatch.setattr(storage.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated"):
        storage.atomic_write_parquet(pl.DataFrame({"value": [2]}), target)

    assert pl.read_parquet(target)["value"].to_list() == [1]
    assert not list(tmp_path.glob(".*.tmp"))


def test_earliest_missing_stage_never_selects_downstream_first():
    assert earliest_missing_stage({"data_quality_check", "kis_minute_batch"}) == "kis_minute_batch"
    assert earliest_missing_stage({"kis_minute_batch", "krx_convert_adj_ratio"}) == "krx_convert_adj_ratio"
    assert earliest_missing_stage({"convert_raw_to_bt"}) == "convert_raw_to_bt"
    assert earliest_missing_stage({"krx_index_constituents"}) is None


def test_adjustment_ratio_is_an_explicit_pipeline_stage():
    assert PIPELINE_STAGES.index("krx_convert_adj_ratio") == (
        PIPELINE_STAGES.index("krx_daily_batch") + 1
    )
    assert PIPELINE_STAGES.index("kis_minute_batch") == (
        PIPELINE_STAGES.index("krx_convert_adj_ratio") + 1
    )


def test_manual_adjustment_repair_does_not_launch_kis_implicitly():
    assert not _should_continue_pipeline(FakeDagRun({}))
    assert _should_continue_pipeline(
        FakeDagRun({"triggered_by": "krx_daily_batch"})
    )
    assert _should_continue_pipeline(
        FakeDagRun({"continue_pipeline": True})
    )


def test_catch_up_reuses_pipeline_run_for_same_date():
    other_date = FakeDagRun({"date": "2026-07-28"}, run_id="other")
    pipeline = FakeDagRun(
        {"date": "2026-07-27"},
        state="running",
        run_id="pipeline__2026-07-27",
    )

    assert _active_run_for_date([other_date, pipeline], "2026-07-27") is pipeline


def test_cross_dag_trigger_reuses_active_run_for_same_date():
    other_date = FakeDagRun({"date": "2026-08-05"}, run_id="other")
    active = FakeDagRun(
        {"date": "2026-08-06"},
        state="queued",
        run_id="pipeline__2026-08-06",
    )

    assert find_active_run_for_date([other_date, active], "2026-08-06") is active
